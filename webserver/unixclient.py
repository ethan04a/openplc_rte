import os
import socket
from threading import Lock
from typing import Optional

from webserver.logger import get_logger

logger, _ = get_logger(use_buffer=True)
mutex = Lock()

# Must match core/src/plc_app/image_snapshot.h (BUFFER_SIZE * IMAGE_SNAPSHOT_ROW_BYTES).
IMAGE_SNAPSHOT_EXPECTED_BYTES = 1024 * 68
IMAGE_SNAPSHOT_PROTOCOL_VERSION = 1


def _recv_exact(sock: socket.socket, n: int, timeout: float | None) -> Optional[bytes]:
    """Read exactly n bytes or return None on EOF/timeout/error."""
    if n <= 0:
        return b""
    sock.settimeout(timeout)
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        try:
            chunk = sock.recv(remaining)
        except (socket.timeout, OSError):
            return None
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class SyncUnixClient:
    def __init__(self, socket_path="/run/runtime/plc_runtime.socket"):
        self.socket_path = socket_path
        self.sock: Optional[socket.socket] = None

    def is_connected(self):
        with mutex:
            if self.sock is None:
                return False
            return True

    def connect(self):
        """Connect to the Unix socket server"""
        if not os.path.exists(self.socket_path):
            raise FileNotFoundError(f"Socket not found: {self.socket_path}")

        try:
            logger.debug("Connecting to socket %s", self.socket_path)
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.settimeout(1.0)  # 1s timeout on blocking calls
            self.sock.connect(self.socket_path)
            logger.debug("Connected to server socket %s", self.socket_path)
        except Exception as e:
            logger.error("Failed to connect: %s", e)

    def send_message(self, msg: str):
        if not self.sock:
            raise RuntimeError("Socket not connected")

        with mutex:
            data = msg.encode()
            try:
                self.sock.sendall(data)
                # logger.info("Sent message: %s", data)
            except Exception as e:
                logger.error("Error sending message: %s", e)

    def recv_message(self, timeout: float = 0.5) -> Optional[str]:
        """Receive message from the server. Reads until newline to ensure complete message."""
        if not self.sock:
            raise RuntimeError("Socket not connected")

        with mutex:
            self.sock.settimeout(timeout)
            try:
                buffer = bytearray()
                max_size = 8192 * 2 + 256

                while len(buffer) < max_size:
                    chunk = self.sock.recv(4096)
                    if not chunk:
                        if buffer:
                            break
                        return None

                    buffer.extend(chunk)

                    if b"\n" in buffer:
                        break

                if not buffer:
                    return None

                message = buffer.decode("utf-8").strip()
                logger.debug(
                    "Received message: %s",
                    message[:200] + "..." if len(message) > 200 else message,
                )
                return message
            except socket.timeout:
                logger.warning("Timeout waiting for message")
                return None
            except Exception:
                return None

    def image_snapshot_get(self, timeout: float = 3.0) -> Optional[bytes]:
        """Pull full I/O image from plc_main (binary trailing payload)."""
        if not self.sock:
            raise RuntimeError("Socket not connected")

        with mutex:
            try:
                self.sock.settimeout(timeout)
                self.sock.sendall(b"IMAGE_SNAPSHOT_GET\n")
            except OSError as e:
                logger.error("IMAGE_SNAPSHOT_GET send failed: %s", e)
                return None

            buf = bytearray()
            max_hdr = 256
            while len(buf) < max_hdr:
                try:
                    chunk = self.sock.recv(65536)
                except socket.timeout:
                    logger.warning("IMAGE_SNAPSHOT_GET header timeout")
                    return None
                if not chunk:
                    return None
                buf.extend(chunk)
                if b"\n" in buf:
                    break

            idx = buf.index(b"\n")
            line = buf[:idx].decode("utf-8", errors="replace").strip()
            rest = bytes(buf[idx + 1 :])
            if not line.startswith("IMAGE_SNAPSHOT_HDR:"):
                logger.warning("IMAGE_SNAPSHOT_GET unexpected header: %s", line[:120])
                return None
            parts = line.split(":")
            if len(parts) != 3 or parts[0] != "IMAGE_SNAPSHOT_HDR":
                return None
            try:
                ver = int(parts[1])
                length = int(parts[2])
            except ValueError:
                return None
            if ver != IMAGE_SNAPSHOT_PROTOCOL_VERSION or length != IMAGE_SNAPSHOT_EXPECTED_BYTES:
                logger.warning(
                    "IMAGE_SNAPSHOT_GET bad version/length: %s %s", ver, length
                )
                return None

            body = rest[:length]
            tail = rest[length:]
            if len(body) < length:
                need = length - len(body)
                extra = _recv_exact(self.sock, need, timeout)
                if extra is None or len(extra) != need:
                    return None
                body = body + extra
            elif tail:
                # Extra bytes after snapshot should not happen; tolerate by leaving in socket buffer
                pass
            return bytes(body)

    def image_snapshot_set(self, payload: bytes, timeout: float = 5.0) -> bool:
        """Apply full I/O image on plc_main (standby shadow execution)."""
        if not self.sock:
            raise RuntimeError("Socket not connected")
        if len(payload) != IMAGE_SNAPSHOT_EXPECTED_BYTES:
            logger.warning(
                "IMAGE_SNAPSHOT_SET wrong size: %s != %s",
                len(payload),
                IMAGE_SNAPSHOT_EXPECTED_BYTES,
            )
            return False

        hdr = (
            f"IMAGE_SNAPSHOT_SET:{IMAGE_SNAPSHOT_PROTOCOL_VERSION}:"
            f"{len(payload)}\n"
        ).encode("ascii")

        with mutex:
            try:
                self.sock.settimeout(timeout)
                self.sock.sendall(hdr)
                self.sock.sendall(payload)
            except OSError as e:
                logger.error("IMAGE_SNAPSHOT_SET send failed: %s", e)
                return False

            buf = bytearray()
            max_line = 512
            while len(buf) < max_line:
                try:
                    chunk = self.sock.recv(4096)
                except socket.timeout:
                    return False
                if not chunk:
                    return False
                buf.extend(chunk)
                if b"\n" in buf:
                    break
            line_end = buf.index(b"\n")
            resp = buf[:line_end].decode("utf-8", errors="replace").strip()
            return resp == "IMAGE_SNAPSHOT_SET:OK"

    def send_and_receive(self, msg: str, timeout: float = 0.5) -> Optional[str]:
        """
        Send a message and receive response atomically with mutex held.
        This ensures no other thread can interleave send/recv operations.
        """
        if not self.sock:
            raise RuntimeError("Socket not connected")

        with mutex:
            data = msg.encode()
            try:
                self.sock.sendall(data)
            except Exception as e:
                logger.error("Error sending message: %s", e)
                return None

            self.sock.settimeout(timeout)
            try:
                buffer = bytearray()
                max_size = 8192 * 2 + 256

                while len(buffer) < max_size:
                    chunk = self.sock.recv(4096)
                    if not chunk:
                        if buffer:
                            break
                        return None

                    buffer.extend(chunk)

                    if b"\n" in buffer:
                        break

                if not buffer:
                    return None

                message = buffer.decode("utf-8").strip()
                logger.debug(
                    "Received message: %s",
                    message[:200] + "..." if len(message) > 200 else message,
                )
                return message
            except socket.timeout:
                logger.warning("Timeout waiting for message")
                return None
            except Exception:
                return None

    def close(self):
        if self.sock:
            logger.debug("Closing connection")
            try:
                self.sock.close()
            finally:
                self.sock = None
