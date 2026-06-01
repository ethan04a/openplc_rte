import errno
import ipaddress
import json
import os
import socket
import struct
import subprocess
import threading
import time
import zlib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# psutil is optional - not available on MSYS2/Cygwin platforms
try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    psutil = None
    HAS_PSUTIL = False

from webserver.logger import get_logger
from webserver.redundancy_role_config import (
    DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME,
    FunctionalNicRole,
    REDUNDANCY_ROLE_FILENAME,
    REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS,
    REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4,
    REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4,
    functional_nics_from_role_document,
    load_redundancy_role_document,
    peer_ipv4s_from_role_document,
    read_functional_cidrs_for_project,
    read_standby_backup_cidrs_for_project,
    redundancy_heartbeat_nic_from_role_document,
    write_redundancy_role_functional_cidrs,
    write_redundancy_role_standby_backup_cidrs,
)
from webserver.unixclient import (
    IMAGE_SNAPSHOT_EXPECTED_BYTES,
    IMAGE_SNAPSHOT_PROTOCOL_VERSION,
    SyncUnixClient,
)
from webserver.unixserver import UnixLogServer

logger, buffer = get_logger("logger", use_buffer=True)

# Log once if psutil is not available
if not HAS_PSUTIL:
    logger.info("psutil not available - process detection features disabled")


MAX_RAPID_CRASHES = 3
RAPID_CRASH_WINDOW = 30  # seconds

# Hot redundancy: TCP heartbeat and ports; interface names come from redundancy_role.json (with defaults)
REDUNDANCY_HEARTBEAT_PORT = 57575
REDUNDANCY_IMAGE_SYNC_PORT = 57576
REDUNDANCY_IMAGE_UDP_DATA_MAGIC = b"OPUD"
REDUNDANCY_IMAGE_UDP_ACK_MAGIC = b"OPAK"
REDUNDANCY_IMAGE_UDP_DATA_FRAGMENT = 1
REDUNDANCY_IMAGE_UDP_ACK_FRAME = 2
REDUNDANCY_IMAGE_UDP_RESERVED = 0
REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX = 1200
REDUNDANCY_IMAGE_UDP_ACK_TIMEOUT_SEC = 0.005
REDUNDANCY_IMAGE_UDP_FRAME_TIMEOUT_SEC = 0.015
REDUNDANCY_IMAGE_UDP_SESSION_EPOCH_FILE = ".redundancy_image_sync_epoch"
REDUNDANCY_IMAGE_UDP_DATA_HEADER = struct.Struct("!4sHBBQQHHIII")
# Phase 1 ACK (version=1); still accepted on the master for backward compatibility.
REDUNDANCY_IMAGE_UDP_ACK_HEADER = struct.Struct("!4sHBBQQQ")
# Phase 2 ACK (version=2): adds timestamp_ns for RTT / observability (DATA fragments stay v1).
REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2 = 2
REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2 = struct.Struct("!4sHBBQQQQ")
REDUNDANCY_IMAGE_PHASE_SNAPSHOT_ASYNC = 0
REDUNDANCY_IMAGE_PHASE_SCAN_END = 1
REDUNDANCY_IMAGE_SCAN_END_WAIT_TIMEOUT_SEC = 1.0
REDUNDANCY_IMAGE_DELTA_MAGIC = b"OPDL"
REDUNDANCY_IMAGE_DELTA_MAX_WIRE_BYTES = 1100
REDUNDANCY_IMAGE_UDP_ACK_LATENCY_EMA_ALPHA = 0.2

REDUNDANCY_IMAGE_ACK_STATUS_OK = 0
REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR = 1
REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER = 2
REDUNDANCY_IMAGE_ACK_STATUS_NOT_READY = 3
REDUNDANCY_IMAGE_ACK_STATUS_NOT_SHADOW = 4
REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR = 5
REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ = 6
REDUNDANCY_IMAGE_ACK_STATUS_FRAME_INCOMPLETE = 7
REDUNDANCY_IMAGE_ACK_STATUS_NAMES = {
    REDUNDANCY_IMAGE_ACK_STATUS_OK: "OK",
    REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR: "CRC_ERROR",
    REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER: "BAD_HEADER",
    REDUNDANCY_IMAGE_ACK_STATUS_NOT_READY: "NOT_READY",
    REDUNDANCY_IMAGE_ACK_STATUS_NOT_SHADOW: "NOT_SHADOW",
    REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR: "APPLY_ERROR",
    REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ: "OLD_SEQ",
    REDUNDANCY_IMAGE_ACK_STATUS_FRAME_INCOMPLETE: "FRAME_INCOMPLETE",
}
# Hot redundancy: HTTP peer sync (receive-program / sync-role-ini header X-OpenPLC-Redundancy-Sync)
REDUNDANCY_SYNC_SECRET = "openplc"
REDUNDANCY_HB_PAYLOAD = b"OPENPLC_REDUNDANCY_HB_V1\n"
REDUNDANCY_FUNC_SYNC_MAGIC = b"OPENPLC_REDUNDANCY_FUNC_V1\n"
REDUNDANCY_FUNC_SYNC_MAX_JSON_BYTES = 65536
REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC = 1.0
REDUNDANCY_STANDBY_RECV_IDLE_SEC = 1.0
# Standby: seconds without TCP heartbeat before ping master for failover decision
REDUNDANCY_STANDBY_LOST_THRESHOLD_SEC = 5

# Throttle STATUS polling used by redundancy I/O mirror gating (avoid unix chatter).
PLC_STATUS_CACHE_TTL_SEC = 0.2


@dataclass(frozen=True)
class RedundancyImageUdpFragment:
    session_id: int
    frame_seq: int
    fragment_index: int
    fragment_count: int
    fragment_offset: int
    total_len: int
    payload_crc32: int
    payload: bytes


@dataclass(frozen=True)
class RedundancyImageUdpAck:
    status: int
    session_id: int
    ack_frame_seq: int
    applied_seq: int
    timestamp_ns: int = 0
    protocol_version: int = 1


@dataclass(frozen=True)
class RedundancyImageFrameMetadata:
    """Per-frame sync metadata attached to each UDP snapshot (phase 3: scan cycle end)."""

    scan_counter: int = 0
    tick: int = 0
    phase: int = REDUNDANCY_IMAGE_PHASE_SCAN_END
    timestamp_ns: int = 0


@dataclass
class RedundancyImageUdpMasterStats:
    """UDP I/O image sync counters (master sender thread)."""

    frame_send_count: int = 0
    fragment_send_count: int = 0
    ack_ok_count: int = 0
    ack_timeout_count: int = 0
    ack_error_count: int = 0
    current_session_id: int = 0
    last_send_frame_seq: int = 0
    last_ack_frame_seq: int = 0
    last_applied_seq: int = 0
    last_send_monotonic: float = 0.0
    last_ack_monotonic: float = 0.0
    last_ack_latency_ms: float = 0.0
    ack_latency_ema_ms: float = 0.0
    consecutive_ack_miss_count: int = 0
    last_ack_status: int = -1
    last_ack_status_name: str = ""
    last_send_timestamp_ns: int = 0
    last_scan_counter: int = 0
    last_tick: int = 0
    last_phase: int = REDUNDANCY_IMAGE_PHASE_SNAPSHOT_ASYNC
    payload_crc32: int = 0
    scan_end_timeout_count: int = 0


@dataclass
class RedundancyImageUdpStandbyStats:
    """UDP I/O image sync counters (standby receiver thread)."""

    fragment_rx_count: int = 0
    frame_complete_count: int = 0
    frame_incomplete_count: int = 0
    bad_source_count: int = 0
    bad_header_count: int = 0
    session_reset_count: int = 0
    old_session_count: int = 0
    old_seq_count: int = 0
    crc_error_count: int = 0
    not_ready_count: int = 0
    not_shadow_count: int = 0
    apply_error_count: int = 0
    applied_count: int = 0
    frame_superseded_count: int = 0
    last_rx_frame_seq: int = 0
    last_applied_seq: int = 0
    last_frame_complete_monotonic: float = 0.0
    last_successful_apply_monotonic: float = 0.0
    last_ack_sent_monotonic: float = 0.0
    last_error_status: int = -1
    last_error_status_name: str = ""
    last_rx_timestamp_ns: int = 0
    consecutive_apply_fail_count: int = 0
    active_session_id: int = 0


@dataclass
class _RedundancyImagePendingFrame:
    session_id: int
    frame_seq: int
    fragment_count: int
    total_len: int
    payload_crc32: int
    first_seen: float
    source_addr: tuple[str, int]
    fragments: dict[int, bytes] = field(default_factory=dict)


def _redundancy_image_crc32(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def _redundancy_image_fragment_count(total_len: int) -> int:
    if total_len <= 0:
        raise ValueError("snapshot payload must not be empty")
    return (total_len + REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX - 1) // (
        REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
    )


def _is_redundancy_delta_payload(payload: bytes) -> bool:
    return len(payload) >= 4 and payload[:4] == REDUNDANCY_IMAGE_DELTA_MAGIC


def _parse_delta_wire_metadata(payload: bytes) -> RedundancyImageFrameMetadata | None:
    if not _is_redundancy_delta_payload(payload) or len(payload) < 44:
        return None
    scan_counter, tick_u64, phase, _changed, timestamp_ns = struct.unpack_from(
        "<QQB3xIQ", payload, 8
    )
    return RedundancyImageFrameMetadata(
        scan_counter=int(scan_counter),
        tick=int(tick_u64),
        phase=int(phase),
        timestamp_ns=int(timestamp_ns),
    )


def _iter_redundancy_image_udp_fragments(
    session_id: int, frame_seq: int, payload: bytes
) -> list[bytes]:
    if len(payload) == 0 or len(payload) > IMAGE_SNAPSHOT_EXPECTED_BYTES:
        raise ValueError(f"invalid sync payload size {len(payload)}")
    fragment_count = _redundancy_image_fragment_count(len(payload))
    if fragment_count > 0xFFFF:
        raise ValueError("snapshot requires too many UDP fragments")
    payload_crc32 = _redundancy_image_crc32(payload)
    packets: list[bytes] = []
    for fragment_index in range(fragment_count):
        fragment_offset = fragment_index * REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
        fragment_payload = payload[
            fragment_offset : fragment_offset + REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
        ]
        header = REDUNDANCY_IMAGE_UDP_DATA_HEADER.pack(
            REDUNDANCY_IMAGE_UDP_DATA_MAGIC,
            IMAGE_SNAPSHOT_PROTOCOL_VERSION,
            REDUNDANCY_IMAGE_UDP_DATA_FRAGMENT,
            REDUNDANCY_IMAGE_UDP_RESERVED,
            session_id,
            frame_seq,
            fragment_index,
            fragment_count,
            fragment_offset,
            len(payload),
            payload_crc32,
        )
        packets.append(header + fragment_payload)
    return packets


def _parse_redundancy_image_udp_fragment(
    packet: bytes,
) -> tuple[RedundancyImageUdpFragment | None, int]:
    if len(packet) < REDUNDANCY_IMAGE_UDP_DATA_HEADER.size:
        return None, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER
    (
        magic,
        version,
        packet_type,
        reserved,
        session_id,
        frame_seq,
        fragment_index,
        fragment_count,
        fragment_offset,
        total_len,
        payload_crc32,
    ) = REDUNDANCY_IMAGE_UDP_DATA_HEADER.unpack_from(packet)
    fragment_payload = packet[REDUNDANCY_IMAGE_UDP_DATA_HEADER.size :]
    if (
        magic != REDUNDANCY_IMAGE_UDP_DATA_MAGIC
        or version != IMAGE_SNAPSHOT_PROTOCOL_VERSION
        or packet_type != REDUNDANCY_IMAGE_UDP_DATA_FRAGMENT
        or reserved != REDUNDANCY_IMAGE_UDP_RESERVED
    ):
        return None, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER
    if total_len == 0 or total_len > IMAGE_SNAPSHOT_EXPECTED_BYTES or fragment_count == 0:
        return None, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER
    if fragment_index >= fragment_count:
        return None, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER
    expected_fragment_count = _redundancy_image_fragment_count(total_len)
    if fragment_count != expected_fragment_count:
        return None, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER
    expected_offset = fragment_index * REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
    if fragment_offset != expected_offset or fragment_offset >= total_len:
        return None, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER
    expected_len = min(
        REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX,
        total_len - fragment_offset,
    )
    if len(fragment_payload) != expected_len:
        return None, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER
    return (
        RedundancyImageUdpFragment(
            session_id=session_id,
            frame_seq=frame_seq,
            fragment_index=fragment_index,
            fragment_count=fragment_count,
            fragment_offset=fragment_offset,
            total_len=total_len,
            payload_crc32=payload_crc32,
            payload=fragment_payload,
        ),
        REDUNDANCY_IMAGE_ACK_STATUS_OK,
    )


def _redundancy_image_udp_header_ids(packet: bytes) -> tuple[int, int]:
    if len(packet) < REDUNDANCY_IMAGE_UDP_DATA_HEADER.size:
        return 0, 0
    try:
        fields = REDUNDANCY_IMAGE_UDP_DATA_HEADER.unpack_from(packet)
    except struct.error:
        return 0, 0
    return int(fields[4]), int(fields[5])


def _pack_redundancy_image_udp_ack(
    status: int,
    session_id: int,
    ack_frame_seq: int,
    applied_seq: int,
    timestamp_ns: int | None = None,
) -> bytes:
    if status not in REDUNDANCY_IMAGE_ACK_STATUS_NAMES:
        raise ValueError(f"unknown redundancy image ACK status: {status}")
    if timestamp_ns is None:
        timestamp_ns = time.time_ns()
    return REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2.pack(
        REDUNDANCY_IMAGE_UDP_ACK_MAGIC,
        REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2,
        REDUNDANCY_IMAGE_UDP_ACK_FRAME,
        status,
        session_id,
        ack_frame_seq,
        applied_seq,
        timestamp_ns,
    )


def _parse_redundancy_image_udp_ack(
    packet: bytes,
) -> tuple[RedundancyImageUdpAck | None, int]:
    if len(packet) >= REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2.size:
        (
            magic,
            version,
            packet_type,
            status,
            session_id,
            ack_frame_seq,
            applied_seq,
            timestamp_ns,
        ) = REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2.unpack_from(packet)
        if (
            magic == REDUNDANCY_IMAGE_UDP_ACK_MAGIC
            and version == REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2
            and packet_type == REDUNDANCY_IMAGE_UDP_ACK_FRAME
            and status in REDUNDANCY_IMAGE_ACK_STATUS_NAMES
        ):
            return (
                RedundancyImageUdpAck(
                    status=status,
                    session_id=session_id,
                    ack_frame_seq=ack_frame_seq,
                    applied_seq=applied_seq,
                    timestamp_ns=timestamp_ns,
                    protocol_version=REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2,
                ),
                REDUNDANCY_IMAGE_ACK_STATUS_OK,
            )
    if len(packet) == REDUNDANCY_IMAGE_UDP_ACK_HEADER.size:
        (
            magic,
            version,
            packet_type,
            status,
            session_id,
            ack_frame_seq,
            applied_seq,
        ) = REDUNDANCY_IMAGE_UDP_ACK_HEADER.unpack(packet)
        if (
            magic == REDUNDANCY_IMAGE_UDP_ACK_MAGIC
            and version == IMAGE_SNAPSHOT_PROTOCOL_VERSION
            and packet_type == REDUNDANCY_IMAGE_UDP_ACK_FRAME
            and status in REDUNDANCY_IMAGE_ACK_STATUS_NAMES
        ):
            return (
                RedundancyImageUdpAck(
                    status=status,
                    session_id=session_id,
                    ack_frame_seq=ack_frame_seq,
                    applied_seq=applied_seq,
                    protocol_version=IMAGE_SNAPSHOT_PROTOCOL_VERSION,
                ),
                REDUNDANCY_IMAGE_ACK_STATUS_OK,
            )
    return None, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER


def _record_redundancy_image_master_ack_latency(
    stats: RedundancyImageUdpMasterStats,
    send_monotonic: float,
    ack: RedundancyImageUdpAck,
) -> None:
    now = time.monotonic()
    latency_ms = max(0.0, (now - send_monotonic) * 1000.0)
    stats.last_ack_monotonic = now
    stats.last_ack_latency_ms = latency_ms
    if stats.ack_latency_ema_ms <= 0.0:
        stats.ack_latency_ema_ms = latency_ms
    else:
        alpha = REDUNDANCY_IMAGE_UDP_ACK_LATENCY_EMA_ALPHA
        stats.ack_latency_ema_ms = (
            alpha * latency_ms + (1.0 - alpha) * stats.ack_latency_ema_ms
        )
    stats.last_ack_status = ack.status
    stats.last_ack_status_name = REDUNDANCY_IMAGE_ACK_STATUS_NAMES.get(
        ack.status, str(ack.status)
    )


class _RedundancyImageUdpFrameAssembler:
    def __init__(
        self,
        timeout_sec: float = REDUNDANCY_IMAGE_UDP_FRAME_TIMEOUT_SEC,
        stats: RedundancyImageUdpStandbyStats | None = None,
    ):
        self.timeout_sec = timeout_sec
        self.stats = stats
        self.active_session_id = 0
        self.last_applied_seq = 0
        self._pending: _RedundancyImagePendingFrame | None = None

    def _note_rx_frame_seq(self, session_id: int, frame_seq: int) -> None:
        if self.stats is None or session_id != self.active_session_id:
            return
        if frame_seq > self.stats.last_rx_frame_seq:
            self.stats.last_rx_frame_seq = frame_seq

    def expire_pending(
        self, now: float
    ) -> tuple[tuple[str, int], int, int, int] | None:
        pending = self._pending
        if pending is None or now - pending.first_seen <= self.timeout_sec:
            return None
        self._pending = None
        if self.stats is not None:
            self.stats.frame_incomplete_count += 1
        return (
            pending.source_addr,
            pending.session_id,
            pending.frame_seq,
            REDUNDANCY_IMAGE_ACK_STATUS_FRAME_INCOMPLETE,
        )

    def add_fragment(
        self,
        fragment: RedundancyImageUdpFragment,
        source_addr: tuple[str, int],
        now: float,
    ) -> tuple[int | None, int, bytes | None]:
        if self.active_session_id and fragment.session_id < self.active_session_id:
            if self.stats is not None:
                self.stats.old_session_count += 1
            return None, fragment.frame_seq, None
        if fragment.session_id > self.active_session_id:
            if self.active_session_id and self.stats is not None:
                self.stats.session_reset_count += 1
            self.active_session_id = fragment.session_id
            self.last_applied_seq = 0
            self._pending = None
            if self.stats is not None:
                self.stats.last_applied_seq = 0
                self.stats.active_session_id = fragment.session_id
        if fragment.frame_seq <= self.last_applied_seq:
            if self.stats is not None:
                self.stats.old_seq_count += 1
            return REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ, fragment.frame_seq, None

        pending = self._pending
        if pending is not None:
            if fragment.session_id != pending.session_id:
                self._pending = None
                pending = None
            elif fragment.frame_seq < pending.frame_seq:
                if self.stats is not None:
                    self.stats.old_seq_count += 1
                return REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ, fragment.frame_seq, None
            elif fragment.frame_seq > pending.frame_seq:
                if self.stats is not None:
                    self.stats.frame_superseded_count += 1
                self._pending = None
                pending = None

        if pending is None:
            pending = _RedundancyImagePendingFrame(
                session_id=fragment.session_id,
                frame_seq=fragment.frame_seq,
                fragment_count=fragment.fragment_count,
                total_len=fragment.total_len,
                payload_crc32=fragment.payload_crc32,
                first_seen=now,
                source_addr=source_addr,
            )
            self._pending = pending
            if self.stats is not None:
                self.stats.last_rx_timestamp_ns = time.time_ns()

        self._note_rx_frame_seq(fragment.session_id, fragment.frame_seq)

        if (
            pending.fragment_count != fragment.fragment_count
            or pending.total_len != fragment.total_len
            or pending.payload_crc32 != fragment.payload_crc32
            or pending.source_addr != source_addr
        ):
            self._pending = None
            if self.stats is not None:
                self.stats.bad_header_count += 1
            return REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER, fragment.frame_seq, None

        existing = pending.fragments.get(fragment.fragment_index)
        if existing is not None:
            if existing != fragment.payload:
                self._pending = None
                if self.stats is not None:
                    self.stats.bad_header_count += 1
                return REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER, fragment.frame_seq, None
        else:
            pending.fragments[fragment.fragment_index] = fragment.payload

        if len(pending.fragments) != pending.fragment_count:
            return None, fragment.frame_seq, None

        payload = b"".join(pending.fragments[index] for index in range(pending.fragment_count))
        self._pending = None
        if len(payload) != pending.total_len:
            if self.stats is not None:
                self.stats.bad_header_count += 1
            return REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER, fragment.frame_seq, None
        if _redundancy_image_crc32(payload) != pending.payload_crc32:
            if self.stats is not None:
                self.stats.crc_error_count += 1
            return REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR, fragment.frame_seq, None
        if self.stats is not None:
            self.stats.frame_complete_count += 1
            self.stats.last_frame_complete_monotonic = time.monotonic()
        return REDUNDANCY_IMAGE_ACK_STATUS_OK, fragment.frame_seq, payload

    def record_applied(self, session_id: int, frame_seq: int) -> None:
        if session_id > self.active_session_id:
            self.active_session_id = session_id
            self.last_applied_seq = 0
            if self.stats is not None:
                self.stats.active_session_id = session_id
        if session_id == self.active_session_id and frame_seq > self.last_applied_seq:
            self.last_applied_seq = frame_seq
            if self.stats is not None:
                self.stats.last_applied_seq = frame_seq
                self.stats.applied_count += 1
                self.stats.last_successful_apply_monotonic = time.monotonic()
                self.stats.consecutive_apply_fail_count = 0


def _open_redundancy_image_master_udp_socket(local_ip: str) -> socket.socket:
    last_error: OSError | None = None
    for source_port in range(
        REDUNDANCY_IMAGE_SYNC_PORT + 1, REDUNDANCY_IMAGE_SYNC_PORT + 65
    ):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((local_ip, source_port))
            return sock
        except OSError as e:
            last_error = e
            try:
                sock.close()
            except OSError:
                pass

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((local_ip, 0))
        bound_port = sock.getsockname()[1]
        if bound_port == REDUNDANCY_IMAGE_SYNC_PORT:
            raise OSError(errno.EADDRINUSE, "master UDP ACK source port must not be 57576")
        return sock
    except OSError:
        try:
            sock.close()
        except OSError:
            pass
        if last_error is not None:
            raise last_error
        raise


def _send_redundancy_image_udp_ack(
    sock: socket.socket,
    addr: tuple[str, int],
    status: int,
    session_id: int,
    frame_seq: int,
    applied_seq: int,
) -> None:
    sock.sendto(
        _pack_redundancy_image_udp_ack(status, session_id, frame_seq, applied_seq),
        addr,
    )


def _process_redundancy_image_master_ack(
    stats: RedundancyImageUdpMasterStats,
    session_id: int,
    frame_seq: int,
    ack: RedundancyImageUdpAck,
    peer_ip: str,
    addr: tuple[str, int],
) -> bool:
    """
    Validate a frame ACK from standby.

    Returns True only when ack_frame_seq matches the frame just sent.
    """
    if addr[0] != peer_ip or addr[1] != REDUNDANCY_IMAGE_SYNC_PORT:
        return False
    if ack.session_id != session_id:
        return False
    if ack.ack_frame_seq > stats.last_send_frame_seq:
        return False
    if ack.ack_frame_seq < stats.last_ack_frame_seq:
        return False
    if ack.applied_seq < stats.last_applied_seq:
        return False
    if ack.ack_frame_seq > stats.last_ack_frame_seq:
        stats.last_ack_frame_seq = ack.ack_frame_seq
    if ack.applied_seq > stats.last_applied_seq:
        stats.last_applied_seq = ack.applied_seq
    if ack.ack_frame_seq != frame_seq:
        return False
    if ack.status == REDUNDANCY_IMAGE_ACK_STATUS_OK:
        stats.ack_ok_count += 1
    else:
        stats.ack_error_count += 1
    return True


def _redundancy_image_master_stats_to_dict(
    stats: RedundancyImageUdpMasterStats,
) -> dict[str, Any]:
    data = asdict(stats)
    data["role"] = "master"
    if data.get("last_phase") == REDUNDANCY_IMAGE_PHASE_SCAN_END:
        data["phase_name"] = "SCAN_END"
    else:
        data["phase_name"] = "SNAPSHOT_ASYNC"
    return data


def _redundancy_image_sync_metadata_view(
    stats_payload: dict[str, Any], role: str
) -> dict[str, Any]:
    if role == "master":
        timestamp_ns = int(stats_payload.get("last_send_timestamp_ns", 0))
    elif role == "standby":
        timestamp_ns = int(stats_payload.get("last_rx_timestamp_ns", 0))
    else:
        timestamp_ns = 0
    phase = int(
        stats_payload.get("last_phase", REDUNDANCY_IMAGE_PHASE_SNAPSHOT_ASYNC)
    )
    phase_name = (
        "SCAN_END"
        if phase == REDUNDANCY_IMAGE_PHASE_SCAN_END
        else "SNAPSHOT_ASYNC"
    )
    return {
        "scan_counter": int(stats_payload.get("last_scan_counter", 0)),
        "tick": int(stats_payload.get("last_tick", 0)),
        "phase": phase,
        "phase_name": phase_name,
        "timestamp_ns": timestamp_ns,
    }


def _redundancy_image_standby_stats_to_dict(
    stats: RedundancyImageUdpStandbyStats,
) -> dict[str, Any]:
    data = asdict(stats)
    data["role"] = "standby"
    if stats.last_error_status >= 0:
        data["last_error_status_name"] = REDUNDANCY_IMAGE_ACK_STATUS_NAMES.get(
            stats.last_error_status, str(stats.last_error_status)
        )
    else:
        data["last_error_status_name"] = ""
    return data


def _record_redundancy_image_standby_ack_status(
    stats: RedundancyImageUdpStandbyStats, status: int
) -> None:
    if status not in (
        REDUNDANCY_IMAGE_ACK_STATUS_OK,
        REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ,
    ):
        stats.last_error_status = status
        stats.last_error_status_name = REDUNDANCY_IMAGE_ACK_STATUS_NAMES.get(
            status, str(status)
        )
    if status == REDUNDANCY_IMAGE_ACK_STATUS_NOT_READY:
        stats.not_ready_count += 1
    elif status == REDUNDANCY_IMAGE_ACK_STATUS_NOT_SHADOW:
        stats.not_shadow_count += 1
    elif status == REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR:
        stats.apply_error_count += 1


REDUNDANCY_IMAGE_UDP_STATS_LOG_EVERY = 500


def _maybe_log_redundancy_image_udp_master_stats(
    stats: RedundancyImageUdpMasterStats,
) -> None:
    if stats.frame_send_count == 0 or stats.frame_send_count % REDUNDANCY_IMAGE_UDP_STATS_LOG_EVERY:
        logger.debug(
            "[hot-redundancy][master] UDP image sync stats "
            "frames=%s frags=%s ack_ok=%s ack_timeout=%s ack_err=%s "
            "session=%s last_tx=%s last_ack=%s last_applied=%s",
            stats.frame_send_count,
            stats.fragment_send_count,
            stats.ack_ok_count,
            stats.ack_timeout_count,
            stats.ack_error_count,
            stats.current_session_id,
            stats.last_send_frame_seq,
            stats.last_ack_frame_seq,
            stats.last_applied_seq,
            stats.last_ack_latency_ms,
            stats.ack_latency_ema_ms,
            stats.consecutive_ack_miss_count,
            stats.last_ack_status_name,
            stats.scan_end_timeout_count,
        )


def _maybe_log_redundancy_image_udp_standby_stats(
    stats: RedundancyImageUdpStandbyStats,
) -> None:
    total_rx = stats.fragment_rx_count
    if total_rx == 0 or total_rx % (REDUNDANCY_IMAGE_UDP_STATS_LOG_EVERY * 60):
        logger.debug(
            "[hot-redundancy][standby] UDP image sync stats "
            "frags=%s complete=%s incomplete=%s applied=%s "
            "old_sess=%s old_seq=%s crc_err=%s last_rx=%s last_applied=%s",
            stats.fragment_rx_count,
            stats.frame_complete_count,
            stats.frame_incomplete_count,
            stats.applied_count,
            stats.old_session_count,
            stats.old_seq_count,
            stats.crc_error_count,
            stats.last_rx_frame_seq,
            stats.last_applied_seq,
            stats.last_successful_apply_monotonic,
            stats.last_error_status_name,
            stats.consecutive_apply_fail_count,
        )


class RuntimeManager:
    def __init__(self, runtime_path, plc_socket, log_socket, print_debug=False):
        self.runtime_path = runtime_path
        self.plc_socket = plc_socket
        self.log_socket = log_socket
        self.print_debug = print_debug
        self.process = None
        self.log_server = UnixLogServer(log_socket)
        self.runtime_socket = SyncUnixClient(plc_socket)
        self.monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self.running = False
        self._crash_lock = threading.Lock()
        self._crash_times: list[float] = []
        self._safe_mode = False

        # Hot redundancy: from redundancy_role.json (defaults until _evaluate_redundancy_role)
        self._redundancy_heartbeat_nic = DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME
        self._redundancy_functional_nics: list[FunctionalNicRole] = []
        self.is_master = False
        self.is_redundancy = False
        self._redundancy_master_ip: str | None = None
        self._redundancy_standby_ip: str | None = None
        self._redundancy_local_heartbeat_ip: str | None = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_threads: list[threading.Thread] = []
        # 备机暂时升主后为 True；升主后仍监听冗余口，收到原主机心跳载荷则异步回切
        self._standby_switched_to_master = False
        # True → plc_main 影子备机；暂时升主后为 False（非影子 PLC）
        self._plc_shadow_standby = False
        # 备机已暂时升主且 PLC 非影子（永久 is_master 仍由 redundancy_role.json 中冗余口对端 IPv4 决定）
        self._promoted_standby_acting_master = False
        # 备升主过程中避免 monitor 线程误重启 PLC
        self._manual_plc_restart_in_progress = False
        # 主机本地记录完成后，等待“TCP 心跳已连接备机”时再同步 functional_nics 中 permanent_master_* 到备机
        self._functional_lines_pending_sync: list[str] | None = None
        self._functional_sync_lock = threading.Lock()
        self._plc_status_cache_lock = threading.Lock()
        self._plc_status_cache_monotonic: float = 0.0
        self._plc_status_cache_running: bool = False
        self._image_udp_master_stats = RedundancyImageUdpMasterStats()
        self._image_udp_standby_stats = RedundancyImageUdpStandbyStats()
        self._image_udp_stats_lock = threading.Lock()
        self._plc_image_data_plane_active = False

    def get_redundancy_image_sync_status(self) -> dict[str, Any]:
        """
        Phase 2 observability: UDP I/O image sync counters and latency (REDUNDANCY_SYNC_STATUS).
        """
        with self._image_udp_stats_lock:
            if self.is_redundancy and self.is_master:
                stats_payload: dict[str, Any] = _redundancy_image_master_stats_to_dict(
                    self._image_udp_master_stats
                )
                role = "master"
            elif self.is_redundancy:
                stats_payload = _redundancy_image_standby_stats_to_dict(
                    self._image_udp_standby_stats
                )
                role = "standby"
            else:
                stats_payload = {}
                role = "none"
        return {
            "enabled": self.is_redundancy,
            "role": role,
            "transport": "udp_fragment",
            "sync_trigger": "scan_end" if self.is_master and self.is_redundancy else "n/a",
            "data_plane": (
                "plc_main" if self._plc_image_data_plane_active else "webserver"
            ),
            "payload_mode": "delta_or_full",
            "data_protocol_version": IMAGE_SNAPSHOT_PROTOCOL_VERSION,
            "ack_protocol_version": REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2,
            "sync_port": REDUNDANCY_IMAGE_SYNC_PORT,
            "local_heartbeat_ip": self._redundancy_local_heartbeat_ip,
            "peer_heartbeat_ip": (
                self._redundancy_standby_ip
                if self.is_master
                else self._redundancy_master_ip
            ),
            "shadow_standby": self._plc_shadow_standby,
            "plc_running": self._plc_runtime_is_running(),
            "metadata": _redundancy_image_sync_metadata_view(stats_payload, role),
            "stats": stats_payload,
            "updated_monotonic": time.monotonic(),
        }

    @staticmethod
    def _openplc_project_root() -> Path:
        """Repository / install root (parent of webserver/)."""
        return Path(__file__).resolve().parent.parent

    def _next_redundancy_image_session_id(self) -> int:
        epoch_path = self._openplc_project_root() / REDUNDANCY_IMAGE_UDP_SESSION_EPOCH_FILE
        mask64 = (1 << 64) - 1
        fallback = time.monotonic_ns() & mask64
        if fallback == 0:
            fallback = 1
        try:
            previous_text = epoch_path.read_text(encoding="ascii").strip()
            previous = int(previous_text) if previous_text else 0
        except (OSError, ValueError):
            previous = 0
        session_id = max(previous + 1, fallback)
        if session_id > mask64:
            session_id = fallback
        try:
            epoch_path.write_text(f"{session_id}\n", encoding="ascii")
        except OSError as e:
            logger.warning("Failed to persist redundancy UDP session epoch: %s", e)
        return session_id

    def _try_enable_plc_redundancy_data_plane(self) -> bool:
        """Phase 6: start UDP sync inside plc_main (webserver stays control plane)."""
        if not self.is_redundancy:
            return False
        local_ip = self._redundancy_local_heartbeat_ip
        peer_ip = (
            self._redundancy_standby_ip
            if self.is_master
            else self._redundancy_master_ip
        )
        if not local_ip or not peer_ip:
            return False
        mode = "master_sender" if self.is_master else "standby_receiver"
        cfg = json.dumps(
            {
                "enabled": True,
                "data_plane_mode": mode,
                "local_heartbeat_ip": local_ip,
                "peer_heartbeat_ip": peer_ip,
                "udp_port": REDUNDANCY_IMAGE_SYNC_PORT,
                "mode": "scan_end_delta",
            }
        )
        try:
            if not self.runtime_socket.is_connected():
                self._safe_connect_runtime_socket()
            self.runtime_socket.send_message(f"REDUNDANCY_SYNC_CONFIG:{cfg}\n")
            resp = self.runtime_socket.recv_message(timeout=2.0)
            if not resp or "REDUNDANCY_SYNC_CONFIG:OK" not in resp:
                return False
            self.runtime_socket.send_message("REDUNDANCY_SYNC_START\n")
            resp = self.runtime_socket.recv_message(timeout=2.0)
            if resp and "REDUNDANCY_SYNC_START:OK" in resp:
                self._plc_image_data_plane_active = True
                logger.info(
                    "[hot-redundancy] plc_main UDP data plane started (mode=%s)",
                    mode,
                )
                return True
        except (OSError, RuntimeError) as e:
            logger.warning("[hot-redundancy] plc_main data plane start failed: %s", e)
        return False

    @staticmethod
    def _format_functional_nic_names_for_log(functional_nics: list[FunctionalNicRole]) -> str:
        if not functional_nics:
            return "(无)"
        return ", ".join(entry.linux_ifname for entry in functional_nics)

    _IP_ADDR_SCOPE_FLAGS = frozenset(
        {
            "global",
            "link",
            "host",
            "noprefixroute",
            "secondary",
            "dynamic",
            "permanent",
        }
    )
    _IP_ADDR_LIFETIME_TOKENS = frozenset({"valid_lft", "preferred_lft", "forever"})

    @staticmethod
    def _normalize_ip_addr_label_token(token: str) -> str:
        return token.rstrip("\\").strip()

    @classmethod
    def _interface_label_from_ip_o_addr_parts(cls, parts: list[str], inet_idx: int) -> str | None:
        """
        Extract the address label from one `ip -o addr` line.

        Modern ip may append ``valid_lft forever preferred_lft forever`` after the label;
        the label is the token immediately before ``valid_lft``, not the last token.
        """
        try:
            vidx = parts.index("valid_lft", inet_idx + 1)
            if vidx > inet_idx + 1:
                return cls._normalize_ip_addr_label_token(parts[vidx - 1])
        except ValueError:
            pass

        for i in range(len(parts) - 1, inet_idx + 1, -1):
            tok = cls._normalize_ip_addr_label_token(parts[i])
            if not tok or tok in cls._IP_ADDR_LIFETIME_TOKENS or tok in cls._IP_ADDR_SCOPE_FLAGS:
                continue
            if tok in ("brd", "scope", "inet", "dev"):
                continue
            if "/" in tok or tok.replace(".", "").isdigit():
                continue
            return tok
        return None

    @classmethod
    def _parse_ip_o_addr_line(cls, line: str) -> tuple[str | None, str | None]:
        """
        Parse one line of `ip -4 -o addr show` output.

        Returns (ipv4_cidr, label) where label is the address label on the line
        (e.g. eth0:2). When one physical NIC has several IPv4 aliases, each line has a
        distinct label; callers must match label to the requested ifname.
        """
        stripped = line.strip()
        if not stripped:
            return None, None
        parts = stripped.split()
        try:
            inet_idx = parts.index("inet")
        except ValueError:
            return None, None
        if inet_idx + 1 >= len(parts):
            return None, None
        cidr = parts[inet_idx + 1].strip()
        label = cls._interface_label_from_ip_o_addr_parts(parts, inet_idx)
        return cidr, label

    @classmethod
    def _ipv4_cidr_from_ip_addr_show_output(cls, out: str, ifname: str) -> str | None:
        """Pick the IPv4 CIDR on the line whose label exactly equals ifname."""
        cidrs = cls._ipv4_cidrs_from_ip_addr_show_output(out, ifname)
        return cidrs[0] if cidrs else None

    @classmethod
    def _ipv4_cidrs_from_ip_addr_show_output(cls, out: str, ifname: str) -> list[str]:
        """All IPv4 CIDRs on lines whose address label equals ifname."""
        result: list[str] = []
        for line in out.splitlines():
            cidr, label = cls._parse_ip_o_addr_line(line)
            if not cidr or label != ifname:
                continue
            try:
                result.append(str(ipaddress.IPv4Interface(cidr)))
            except ValueError:
                continue
        return result

    @staticmethod
    def _netdev_and_address_label(ifname: str) -> tuple[str, str | None]:
        """
        Map configured linux_ifname to kernel netdev and optional address label.

        Legacy alias names (eth2:1 on netdev eth2) share one physical device with other
        labeled addresses (e.g. heartbeat on eth2:3). ip(8) must use ``dev eth2`` plus
        ``label eth2:1``, not ``dev eth2:1``, or other addresses on eth2 are affected.
        """
        name = ifname.strip()
        if ":" in name:
            return name.split(":", 1)[0], name
        return name, None

    @classmethod
    def _list_ipv4_cidrs_on_interface(cls, ifname: str) -> list[str]:
        """Return every IPv4 CIDR assigned to ifname (supports multi-address / alias NICs)."""
        out = cls._run_ip_addr_show(ifname)
        if out is None:
            return []
        return cls._ipv4_cidrs_from_ip_addr_show_output(out, ifname)

    @classmethod
    def _run_ip_addr_show(cls, ifname: str) -> str | None:
        netdev, address_label = cls._netdev_and_address_label(ifname)
        cmd = ["ip", "-4", "-o", "addr", "show", "dev", netdev]
        if address_label is not None:
            cmd.extend(["label", address_label])
        try:
            return subprocess.check_output(
                cmd,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=5,
            )
        except subprocess.CalledProcessError:
            if address_label is None:
                return None
            try:
                out = subprocess.check_output(
                    ["ip", "-4", "-o", "addr", "show", "dev", netdev],
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=5,
                )
            except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
                return None
            return out
        except (OSError, subprocess.TimeoutExpired):
            return None

    @staticmethod
    def _ipv4_for_interface(ifname: str) -> str | None:
        """Return IPv4 address on interface ifname (exact label match), or None."""
        # Prefer ip(8) with per-address label (eth2:1 vs eth2:3) over psutil's first IPv4.
        out = RuntimeManager._run_ip_addr_show(ifname)
        if out is not None:
            cidr = RuntimeManager._ipv4_cidr_from_ip_addr_show_output(out, ifname)
            if cidr is not None:
                return str(ipaddress.IPv4Interface(cidr).ip)

        if HAS_PSUTIL and psutil is not None:
            addrs = psutil.net_if_addrs().get(ifname)
            if addrs is not None:
                for entry in addrs:
                    if entry.family == socket.AF_INET and entry.address:
                        return str(entry.address)
        return None

    @classmethod
    def _ipv4_cidr_for_interface(cls, ifname: str) -> str | None:
        """Return IPv4 CIDR on ifname (exact label match), or None."""
        out = cls._run_ip_addr_show(ifname)
        if out is not None:
            cidr = cls._ipv4_cidr_from_ip_addr_show_output(out, ifname)
            if cidr is not None:
                return cidr

        if HAS_PSUTIL and psutil is not None:
            addrs = psutil.net_if_addrs().get(ifname)
            if addrs is not None:
                for entry in addrs:
                    if entry.family == socket.AF_INET and entry.netmask:
                        try:
                            prefix = ipaddress.IPv4Network(
                                f"0.0.0.0/{entry.netmask}", strict=False
                            ).prefixlen
                            return f"{entry.address}/{prefix}"
                        except ValueError:
                            if entry.address:
                                return f"{entry.address}/32"
                    elif entry.family == socket.AF_INET and entry.address:
                        return f"{entry.address}/32"
        return None

    @classmethod
    def _ip_addr_command_base(cls, ifname: str) -> tuple[list[str], str, str | None]:
        """Build shared ``ip addr`` prefix: returns (cmd_prefix, netdev, address_label)."""
        netdev, address_label = cls._netdev_and_address_label(ifname)
        return ["ip", "addr"], netdev, address_label

    @classmethod
    def _ip_addr_ifaddr_args(cls, cidr: str, netdev: str, address_label: str | None) -> list[str]:
        """IFADDR + dev for add (label is part of IFADDR, not valid on ``del``)."""
        args = [cidr, "dev", netdev]
        if address_label is not None:
            args.extend(["label", address_label])
        return args

    @classmethod
    def _ip_addr_flush_labeled(cls, ifname: str) -> bool:
        """
        Remove only addresses on netdev whose label matches ifname.

        ``ip addr del`` does not accept ``label``; flushing by label is the safe way to
        clear one alias without touching other labels on the same netdev (e.g. eth2:3).
        """
        netdev, address_label = cls._netdev_and_address_label(ifname)
        if address_label is None:
            return True
        try:
            r = subprocess.run(
                ["ip", "-4", "addr", "flush", "dev", netdev, "label", address_label],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode != 0:
                err = (r.stderr or r.stdout or "").strip()
                logger.warning(
                    "[热冗余] ip addr flush %s label %s: %s",
                    netdev,
                    address_label,
                    err,
                )
                return False
            return True
        except (OSError, subprocess.TimeoutExpired) as e:
            logger.warning(
                "[热冗余] ip addr flush 异常 %s label %s: %s",
                netdev,
                address_label,
                e,
            )
            return False

    @classmethod
    def _ip_addr_del_on_interface(cls, ifname: str, cidr: str) -> bool:
        netdev, address_label = cls._netdev_and_address_label(ifname)
        if address_label is not None:
            return cls._ip_addr_flush_labeled(ifname)
        try:
            r = subprocess.run(
                ["ip", "addr", "del", cidr, "dev", netdev],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode != 0:
                err = (r.stderr or r.stdout or "").strip()
                if "Cannot assign" in err or "not found" in err.lower():
                    return True
                logger.warning(
                    "[热冗余] ip addr del %s %s: %s",
                    ifname,
                    cidr,
                    err,
                )
                return False
            return True
        except (OSError, subprocess.TimeoutExpired) as e:
            logger.warning("[热冗余] ip addr del 异常 %s %s: %s", ifname, cidr, e)
            return False

    @classmethod
    def _ip_addr_add_on_interface(
        cls, ifname: str, cidr: str
    ) -> subprocess.CompletedProcess[str]:
        """Add IPv4 on the given logical interface (netdev + optional label)."""
        prefix, netdev, address_label = cls._ip_addr_command_base(ifname)
        cmd = [*prefix, "add", *cls._ip_addr_ifaddr_args(cidr, netdev, address_label)]
        return subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )

    @classmethod
    def _link_set_interface_up(cls, ifname: str) -> None:
        netdev, _ = cls._netdev_and_address_label(ifname)
        subprocess.run(
            ["ip", "link", "set", netdev, "up"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

    @classmethod
    def _apply_ipv4_cidr_to_linux_interface(
        cls,
        ifname: str,
        cidr: str,
        *,
        remove_cidr: str | None = None,
    ) -> bool:
        """
        Set one IPv4 on ifname without affecting other labeled addresses on the netdev.

        Uses ``ip addr del`` (or label-scoped flush) then ``ip addr add``. Plain ``ip addr del``
        does not accept ``label``; labeled ifnames use flush-by-label via
        ``_ip_addr_del_on_interface`` so other aliases on the same netdev stay intact.
        """
        try:
            target = str(ipaddress.IPv4Interface(cidr.strip()))
            target_ip = str(ipaddress.IPv4Interface(target).ip)

            def _has_target() -> bool:
                return any(
                    existing_cidr == target
                    or str(ipaddress.IPv4Interface(existing_cidr).ip) == target_ip
                    for existing_cidr in cls._list_ipv4_cidrs_on_interface(ifname)
                )

            if _has_target():
                cls._link_set_interface_up(ifname)
                logger.info("[热冗余] %s 已存在地址 %s，跳过添加", ifname, target)
                return True

            if remove_cidr:
                remove = str(ipaddress.IPv4Interface(remove_cidr.strip()))
                remove_ip = str(ipaddress.IPv4Interface(remove).ip)
                if remove_ip != target_ip:
                    if not cls._ip_addr_del_on_interface(ifname, remove):
                        logger.error(
                            "[热冗余] ip addr del 失败 %s %s（中止添加 %s）",
                            ifname,
                            remove,
                            target,
                        )
                        return False

            r = cls._ip_addr_add_on_interface(ifname, target)
            if r.returncode != 0:
                err = (r.stderr or r.stdout or "").strip()
                if "File exists" in err or "EEXIST" in err:
                    if _has_target():
                        cls._link_set_interface_up(ifname)
                        logger.info("[热冗余] %s 已存在地址 %s", ifname, target)
                        return True
                logger.error(
                    "[热冗余] ip addr add 失败 %s %s: %s",
                    ifname,
                    target,
                    err,
                )
                return False

            cls._link_set_interface_up(ifname)
            if remove_cidr:
                logger.info(
                    "[热冗余] %s 已切换地址：%s -> %s",
                    ifname,
                    str(ipaddress.IPv4Interface(remove_cidr.strip())),
                    target,
                )
            else:
                logger.info("[热冗余] 已为 %s 设置地址 %s", ifname, target)
            return True
        except (ValueError, OSError, subprocess.TimeoutExpired) as e:
            logger.error("[热冗余] 配置 %s 地址异常: %s", ifname, e)
            return False

    def _record_functional_ips_and_sync_standby_thread(self) -> None:
        """主机：将各功能口网卡 IPv4/掩码写入 functional_nics[].permanent_master_ipv4_cidr。"""
        try:
            if not self._redundancy_functional_nics:
                logger.info("[热冗余] 未配置功能网卡，跳过功能 IP 记录。")
                return
            project_root = self._openplc_project_root()
            role_json_path = project_root / REDUNDANCY_ROLE_FILENAME
            if not role_json_path.is_file():
                return
            recorded: list[str] = []
            for entry in self._redundancy_functional_nics:
                cidr = self._ipv4_cidr_for_interface(entry.linux_ifname)
                if not cidr:
                    logger.warning(
                        "[热冗余] 功能 IP 记录跳过：网卡 %s 无 IPv4（需所有已配置功能网卡均有地址）",
                        entry.linux_ifname,
                    )
                    return
                recorded.append(cidr)
            write_redundancy_role_functional_cidrs(role_json_path, recorded)
            pairs = ", ".join(
                f"{e.linux_ifname}={c}" for e, c in zip(self._redundancy_functional_nics, recorded)
            )
            logger.info(
                "[热冗余] 已记录 %d 个功能口地址到 %s.%s: %s",
                len(recorded),
                role_json_path,
                REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS,
                pairs,
            )
            with self._functional_sync_lock:
                self._functional_lines_pending_sync = list(recorded)
            logger.info(
                "[热冗余] functional_nics permanent_master 已标记待同步，等待主备 TCP 心跳连接建立后再推送。"
            )
        except Exception as e:
            logger.error("[热冗余] 功能 IP 记录异常: %s", e)

    @staticmethod
    def _encode_functional_cidr_sync_message(permanent_master_cidrs: list[str]) -> bytes:
        body = json.dumps(
            {"permanent_master_ipv4_cidrs": permanent_master_cidrs},
            ensure_ascii=False,
        ).encode("utf-8")
        if len(body) > REDUNDANCY_FUNC_SYNC_MAX_JSON_BYTES:
            raise ValueError("functional CIDR sync JSON too large")
        return REDUNDANCY_FUNC_SYNC_MAGIC + struct.pack("!I", len(body)) + body

    @staticmethod
    def _decode_functional_cidr_sync_body(body: bytes) -> list[str]:
        doc = json.loads(body.decode("utf-8"))
        raw = doc.get("permanent_master_ipv4_cidrs")
        if not isinstance(raw, list):
            raise ValueError("missing permanent_master_ipv4_cidrs array")
        cidrs = [str(c).strip() for c in raw if str(c).strip()]
        if not cidrs:
            raise ValueError("empty permanent_master_ipv4_cidrs")
        for cidr in cidrs:
            ipaddress.IPv4Interface(cidr)
        return cidrs

    @classmethod
    def _try_take_functional_sync_from_buffer(cls, buf: bytes) -> tuple[bytes, list[str] | None]:
        """
        If a complete functional-nic sync frame is present, return (remaining_buf, cidrs).
        Otherwise return (buf, None) and keep partial frame in buf.
        """
        idx = buf.find(REDUNDANCY_FUNC_SYNC_MAGIC)
        if idx < 0:
            if len(buf) > 131072:
                return buf[-8192:], None
            return buf, None
        if idx > 0:
            buf = buf[idx:]
        header_end = len(REDUNDANCY_FUNC_SYNC_MAGIC) + 4
        if len(buf) < header_end:
            return buf, None
        (body_len,) = struct.unpack("!I", buf[len(REDUNDANCY_FUNC_SYNC_MAGIC) : header_end])
        if body_len > REDUNDANCY_FUNC_SYNC_MAX_JSON_BYTES:
            return buf[1:], None
        frame_len = header_end + body_len
        if len(buf) < frame_len:
            return buf, None
        body = buf[header_end:frame_len]
        try:
            cidrs = cls._decode_functional_cidr_sync_body(body)
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
            return buf[1:], None
        return buf[frame_len:], cidrs

    @classmethod
    def _try_take_heartbeat_from_buffer(cls, buf: bytes) -> tuple[bytes, bool]:
        idx = buf.find(REDUNDANCY_HB_PAYLOAD)
        if idx < 0:
            if len(buf) > 65536:
                return buf[-4096:], False
            return buf, False
        return buf[idx + len(REDUNDANCY_HB_PAYLOAD) :], True

    def _consume_standby_redundancy_tcp_buffer(self, buf: bytes) -> tuple[bytes, bool]:
        """Parse heartbeat / functional-CIDR sync from standby TCP stream."""
        heartbeat_seen = False
        while True:
            progressed = False
            buf, cidrs = self._try_take_functional_sync_from_buffer(buf)
            if cidrs is not None:
                progressed = True
                self._apply_peer_functional_cidr_sync(cidrs)
            buf, hb = self._try_take_heartbeat_from_buffer(buf)
            if hb:
                progressed = True
                heartbeat_seen = True
            if not progressed:
                break
        return buf, heartbeat_seen

    def _apply_peer_functional_cidr_sync(self, cidrs: list[str]) -> None:
        role_json_path = self._openplc_project_root() / REDUNDANCY_ROLE_FILENAME
        if write_redundancy_role_functional_cidrs(role_json_path, cidrs):
            self.reload_functional_nics_from_disk()
            logger.info(
                "[热冗余][备机] 已通过冗余 TCP 写入 %d 项 functional_nics permanent_master",
                len(cidrs),
            )
        else:
            logger.error(
                "[热冗余][备机] 冗余 TCP 同步 permanent_master 写入 %s 失败",
                role_json_path,
            )

    def _push_functional_cidrs_over_heartbeat_tcp(
        self, sock: socket.socket, cidrs: list[str]
    ) -> bool:
        try:
            sock.sendall(self._encode_functional_cidr_sync_message(cidrs))
            logger.info(
                "[热冗余][主机] 已通过冗余 TCP 向备机发送 %d 项 functional_nics permanent_master",
                len(cidrs),
            )
            return True
        except OSError as e:
            logger.warning("[热冗余][主机] 冗余 TCP 发送 functional_nics 失败: %s", e)
            return False

    def _apply_master_functional_cidrs_locally(self, cidrs: list[str]) -> None:
        for entry, cidr in zip(self._redundancy_functional_nics, cidrs):
            if not self._apply_ipv4_cidr_to_linux_interface(entry.linux_ifname, cidr):
                logger.warning(
                    "[热冗余][主机] 同步成功后配置本机功能口 %s 失败",
                    entry.linux_ifname,
                )

    @staticmethod
    def _normalize_ipv4_cidr(cidr: str) -> str:
        return str(ipaddress.IPv4Interface(cidr.strip()))

    def _permanent_master_cidrs_from_json(self) -> list[str] | None:
        """Load permanent_master_ipv4_cidr list from redundancy_role.json only."""
        if not self._redundancy_functional_nics:
            return None
        permanent = read_functional_cidrs_for_project(self._openplc_project_root())
        if len(permanent) != len(self._redundancy_functional_nics) or not all(permanent):
            return None
        return [self._normalize_ipv4_cidr(str(c)) for c in permanent if c is not None]

    def _functional_nic_cidr_matches_expected(self, ifname: str, expected_cidr: str) -> bool:
        local_cidr = self._ipv4_cidr_for_interface(ifname)
        if not local_cidr:
            return False
        try:
            expected = self._normalize_ipv4_cidr(expected_cidr)
            local = self._normalize_ipv4_cidr(local_cidr)
        except ValueError:
            return False
        if local == expected:
            return True
        return str(ipaddress.IPv4Interface(local).ip) == str(
            ipaddress.IPv4Interface(expected).ip
        )

    def _master_local_functional_cidrs_match_json(self) -> tuple[bool, list[str] | None]:
        """
        Compare each functional NIC's live CIDR to redundancy_role.json permanent_master_*.
        Returns (all_match, expected_cidrs from JSON when loaded).
        """
        expected = self._permanent_master_cidrs_from_json()
        if not expected:
            return False, None
        for entry, cidr in zip(self._redundancy_functional_nics, expected):
            if not self._functional_nic_cidr_matches_expected(entry.linux_ifname, cidr):
                return False, expected
        return True, expected

    def _ensure_master_functional_ips_match_json_after_heartbeat_send_error(
        self, errno_value: int | None
    ) -> None:
        """
        After redundancy TCP heartbeat send fails with ECONNRESET (104) or EPIPE (32):
        compare live functional IPs to JSON; re-apply only if mismatched.
        """
        errno_label = "errno未知"
        if errno_value == errno.ECONNRESET:
            errno_label = "ECONNRESET(104)"
        elif errno_value == errno.EPIPE:
            errno_label = "EPIPE(32)"

        match, expected = self._master_local_functional_cidrs_match_json()
        if expected is None:
            logger.warning(
                "[热冗余][主机] 心跳发送异常(%s)后无法从 %s 读取有效的 functional_nics "
                "permanent_master，跳过本机 IP 校验",
                errno_label,
                REDUNDANCY_ROLE_FILENAME,
            )
            return
        if match:
            logger.info(
                "[热冗余][主机] 心跳发送异常(%s)后本机功能口 IP 与 %s 中 permanent_master 一致，无需重设",
                errno_label,
                REDUNDANCY_ROLE_FILENAME,
            )
            return
        mismatches: list[str] = []
        for entry, cidr in zip(self._redundancy_functional_nics, expected):
            local = self._ipv4_cidr_for_interface(entry.linux_ifname) or "(无)"
            if not self._functional_nic_cidr_matches_expected(entry.linux_ifname, cidr):
                mismatches.append(f"{entry.linux_ifname}: 本机={local} JSON={cidr}")
        logger.warning(
            "[热冗余][主机] 心跳发送异常(%s)后本机功能口 IP 与 %s 不一致，将按 JSON 重设: %s",
            errno_label,
            REDUNDANCY_ROLE_FILENAME,
            "; ".join(mismatches),
        )
        self._apply_master_functional_cidrs_locally(expected)

    def _clear_functional_lines_pending_sync(self) -> None:
        with self._functional_sync_lock:
            self._functional_lines_pending_sync = None

    def _sync_functional_lines_after_tcp_connect(
        self,
        heartbeat_sock: socket.socket | None = None,
        *,
        sync_attempt_kind: str = "on_connect",
    ) -> None:
        """
        同步 functional_nics permanent_master 到备机（冗余 TCP 优先，失败则 HTTPS）。

        sync_attempt_kind:
        - on_connect: TCP connect() 成功后立刻调用（pending 可能尚未由记录线程填充）。
        - periodic: 每次发送心跳前调用，消除与 _record_functional_ips_and_sync_standby_thread 的竞态。
        """
        standby_ip = self._redundancy_standby_ip
        if not standby_ip:
            return
        with self._functional_sync_lock:
            pending = self._functional_lines_pending_sync
        if not pending:
            return

        cidrs = list(pending)
        if sync_attempt_kind == "on_connect":
            logger.info(
                "[热冗余][主机] TCP 心跳连接已建立，开始同步 %s 中 %d 个 functional_nics permanent_master 到备机 %s。",
                REDUNDANCY_ROLE_FILENAME,
                len(cidrs),
                standby_ip,
            )
        else:
            logger.debug(
                "[热冗余][主机] 心跳周期补发：向备机 %s 同步 %d 个 functional_nics permanent_master",
                standby_ip,
                len(cidrs),
            )

        if heartbeat_sock is not None and self._push_functional_cidrs_over_heartbeat_tcp(
            heartbeat_sock, cidrs
        ):
            self._clear_functional_lines_pending_sync()
            self._apply_master_functional_cidrs_locally(cidrs)
            return

        try:
            from webserver.redundancy_program_sync import push_role_ini_functional_to_standby

            pushed = push_role_ini_functional_to_standby(
                standby_ip, cidrs, REDUNDANCY_SYNC_SECRET
            )
            if pushed:
                self._clear_functional_lines_pending_sync()
                self._apply_master_functional_cidrs_locally(cidrs)
            else:
                logger.warning(
                    "[热冗余][主机] HTTPS 同步 functional_nics 失败（备机 %s:8443 不可达或未监听），"
                    "将在下次 TCP 心跳重连时重试；也可在备机开放 8443 或检查 Web 服务是否已启动。",
                    standby_ip,
                )
        except Exception as e:
            logger.error(
                "[热冗余][主机] TCP 建连后同步 functional_nics 异常: %s "
                "（已尝试冗余 TCP%s）",
                e,
                "" if heartbeat_sock is not None else "，无可用心跳套接字",
            )

    def _evaluate_redundancy_role(self) -> None:
        """
        Load redundancy_role.json: first resolve NIC names, then compare local heartbeat-NIC IPv4
        to configured master/standby to set is_master / is_redundancy.
        """
        self.is_master = False
        self.is_redundancy = False
        self._redundancy_master_ip = None
        self._redundancy_standby_ip = None
        self._redundancy_local_heartbeat_ip = None
        self._plc_shadow_standby = False
        self._promoted_standby_acting_master = False
        self._redundancy_heartbeat_nic = DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME
        self._redundancy_functional_nics = []

        project_root = self._openplc_project_root()
        role_json_path = project_root / REDUNDANCY_ROLE_FILENAME

        if not role_json_path.is_file():
            logger.info(
                "[热冗余] 未找到 %s，本机不启用热冗余功能（is_redundancy=False, is_master=False）。",
                role_json_path,
            )
            return

        doc = load_redundancy_role_document(project_root)
        if doc is None:
            logger.info(
                "[热冗余] 冗余角色文件无效或无法解析，本机不启用热冗余功能（is_redundancy=False, is_master=False）。"
            )
            return

        self._redundancy_heartbeat_nic = redundancy_heartbeat_nic_from_role_document(doc)
        self._redundancy_functional_nics = functional_nics_from_role_document(doc)
        fnic_log = self._format_functional_nic_names_for_log(self._redundancy_functional_nics)
        logger.info(
            "[热冗余] 开始冗余角色检测: 项目根目录=%s, 配置文件=%s；冗余心跳网卡=%s, "
            "功能网卡 %d 个: %s",
            project_root,
            role_json_path,
            self._redundancy_heartbeat_nic,
            len(self._redundancy_functional_nics),
            fnic_log,
        )

        master_ip, standby_ip = peer_ipv4s_from_role_document(doc)
        if master_ip is None or standby_ip is None:
            logger.info(
                "[热冗余] JSON 中缺少有效的 %s / %s，本机不启用热冗余功能（is_redundancy=False, is_master=False）。",
                REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4,
                REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4,
            )
            return

        logger.info(
            "[热冗余] 从 %s 读取冗余口对端: 主机=%s, 备机=%s",
            REDUNDANCY_ROLE_FILENAME,
            master_ip,
            standby_ip,
        )

        self._redundancy_master_ip = master_ip
        self._redundancy_standby_ip = standby_ip
        logger.info(
            "[热冗余] 配置摘要: 主机 IP=%s, 备机 IP=%s（将与本机网卡 %s 的 IPv4 比较）",
            master_ip,
            standby_ip,
            self._redundancy_heartbeat_nic,
        )

        local_ip = self._ipv4_for_interface(self._redundancy_heartbeat_nic)
        self._redundancy_local_heartbeat_ip = local_ip
        if local_ip is None:
            logger.error(
                "[热冗余] 无法读取网卡 %s 的 IPv4 地址，本机不启用热冗余。"
                "请确认网卡存在且已配置地址。",
                self._redundancy_heartbeat_nic,
            )
            return

        logger.info(
            "[热冗余] 本机网卡 %s 的 IPv4 为: %s",
            self._redundancy_heartbeat_nic,
            local_ip,
        )

        if local_ip == master_ip:
            self.is_redundancy = True
            self.is_master = True
            logger.info(
                "[热冗余] 本机 IPv4 与配置中的主机一致，角色=主机。"
                "is_redundancy=True, is_master=True。"
                "将通过 TCP 经 %s 主动连接备机并每秒发送心跳（目标端口 %d）。",
                self._redundancy_heartbeat_nic,
                REDUNDANCY_HEARTBEAT_PORT,
            )
            threading.Thread(
                target=self._record_functional_ips_and_sync_standby_thread,
                daemon=True,
                name="redundancy-record-functional-ip",
            ).start()
            return

        if local_ip == standby_ip:
            self.is_redundancy = True
            self.is_master = False
            self._plc_shadow_standby = not self._standby_switched_to_master
            logger.info(
                "[热冗余] 本机 IPv4 与配置中的备机一致，角色=备机。"
                "is_redundancy=True, is_master=False。"
                "将在 %s 上监听 TCP 端口 %d，接收主机心跳。",
                self._redundancy_heartbeat_nic,
                REDUNDANCY_HEARTBEAT_PORT,
            )
            if self._plc_shadow_standby:
                logger.info(
                    "[热冗余] 备机将使用 plc_main --shadow-standby：运行相同 PLC 逻辑，"
                    "不加载现场 I/O 插件（仅与主机冗余通信由 Web 层负责）。"
                )
            return

        logger.warning(
            "[热冗余] 本机 %s 地址 %s 既不是配置的主机 %s 也不是备机 %s，"
            "不启用热冗余（is_redundancy=False, is_master=False）。",
            self._redundancy_heartbeat_nic,
            local_ip,
            master_ip,
            standby_ip,
        )

    def reload_functional_nics_from_disk(self) -> None:
        """Reload functional_nics[] from redundancy_role.json (e.g. after host CIDR sync)."""
        project_root = self._openplc_project_root()
        doc = load_redundancy_role_document(project_root)
        if doc is None:
            return
        self._redundancy_functional_nics = functional_nics_from_role_document(doc)
        logger.info(
            "[热冗余] 已从 %s 重新加载 %d 个功能网卡: %s",
            REDUNDANCY_ROLE_FILENAME,
            len(self._redundancy_functional_nics),
            self._format_functional_nic_names_for_log(self._redundancy_functional_nics),
        )

    def _shutdown_redundancy_heartbeat_threads(self) -> None:
        if self._plc_image_data_plane_active:
            try:
                if self.runtime_socket.is_connected():
                    self.runtime_socket.send_message("REDUNDANCY_SYNC_STOP\n")
            except (OSError, RuntimeError):
                pass
            self._plc_image_data_plane_active = False
        self._heartbeat_stop.set()
        for t in list(self._heartbeat_threads):
            t.join(timeout=3)
        self._heartbeat_threads.clear()

    def _redundancy_ping_master_ipv4_once(self, ip: str) -> bool:
        """单次 ICMP ping（Linux iputils），成功返回 True。"""
        try:
            proc = subprocess.run(
                ["ping", "-c", "1", "-W", "2", ip],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            return proc.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return False

    def _redundancy_ping_master_reachable(self) -> bool:
        """
        分别 ping 各功能网口在 redundancy_role.json 中 permanent_master 的 IPv4，不用冗余心跳口 IP。
        只要有一条功能 IP 能 ping 通即视为主机仍可达；均无响应则判定为故障（返回 False）。
        """
        project_root = self._openplc_project_root()
        role_json_path = project_root / REDUNDANCY_ROLE_FILENAME
        permanent_cidrs = read_functional_cidrs_for_project(project_root)
        fnic_log = self._format_functional_nic_names_for_log(self._redundancy_functional_nics)

        targets: list[tuple[str, str]] = []
        for entry, cidr in zip(self._redundancy_functional_nics, permanent_cidrs):
            if cidr:
                targets.append(
                    (entry.linux_ifname, str(ipaddress.IPv4Interface(cidr).ip))
                )

        if not targets:
            if not self._redundancy_functional_nics:
                logger.info(
                    "[热冗余][备机] 故障探测：未配置功能网卡（%s），跳过 ping，按主机不可达处理。",
                    fnic_log,
                )
            else:
                logger.warning(
                    "[热冗余][备机] 故障探测：网卡 %s 在 %s 中无有效 permanent_master_ipv4_cidr，"
                    "无法进行 ping，按主机不可达处理。",
                    fnic_log,
                    role_json_path,
                )
            return False

        any_ok = False
        for nic_name, ip in targets:
            ok = self._redundancy_ping_master_ipv4_once(ip)
            if ok:
                logger.info(
                    "[热冗余][备机] 故障探测：接口 %s 对应地址 %s 可达。",
                    nic_name,
                    ip,
                )
                any_ok = True
            else:
                logger.info(
                    "[热冗余][备机] 故障探测：接口 %s 对应地址 %s 不可达。",
                    nic_name,
                    ip,
                )

        if any_ok:
            return True
        logger.info(
            "[热冗余][备机] 故障探测：所有已配置的主机功能地址均无 ICMP 响应，判定为故障。"
        )
        return False

    def _redundancy_trigger_standby_to_master_switch(self) -> None:
        """
        备升主（暂时）：先将本机功能口地址写入 functional_nics[].standby_backup_*，
        再应用 functional_nics[].permanent_master_* 中记录的主机功能 IP；
        PLC 非影子；is_master 不变；继续监听冗余口，收到原主机心跳载荷后异步回切。
        """
        logger.info("[热冗余][备机] 备机升主机已触发（暂时，JSON 中永久主备角色不变）")
        if not self._redundancy_functional_nics:
            logger.error("[热冗余][备机] 升主中止：未配置功能网卡")
            return
        project_root = self._openplc_project_root()
        role_json_path = project_root / REDUNDANCY_ROLE_FILENAME
        standby_backups: list[str] = []
        for entry in self._redundancy_functional_nics:
            backup_cidr = self._ipv4_cidr_for_interface(entry.linux_ifname)
            if not backup_cidr:
                logger.error(
                    "[热冗余][备机] 升主中止：无法读取本机网卡 %s 的 IPv4/CIDR",
                    entry.linux_ifname,
                )
                return
            standby_backups.append(backup_cidr)
        try:
            write_redundancy_role_standby_backup_cidrs(role_json_path, standby_backups)
            pairs = ", ".join(
                f"{e.linux_ifname}={c}"
                for e, c in zip(self._redundancy_functional_nics, standby_backups)
            )
            logger.info(
                "[热冗余][备机] 已写入 %d 个备机功能地址到 %s.%s: %s",
                len(standby_backups),
                role_json_path,
                REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS,
                pairs,
            )
        except OSError as e:
            logger.error("[热冗余][备机] 写入 functional_nics standby_backup 失败: %s", e)
            return

        permanent_cidrs = read_functional_cidrs_for_project(project_root)
        if len(permanent_cidrs) != len(self._redundancy_functional_nics) or not all(permanent_cidrs):
            logger.error(
                "[热冗余][备机] 升主中止：%s 中 %s 缺少有效的 permanent_master_ipv4_cidr（需先由主机记录功能 IP）",
                role_json_path,
                REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS,
            )
            return
        for entry, master_cidr, backup_cidr in zip(
            self._redundancy_functional_nics, permanent_cidrs, standby_backups
        ):
            if not master_cidr:
                logger.error(
                    "[热冗余][备机] 升主中止：网卡 %s 无 permanent_master 地址",
                    entry.linux_ifname,
                )
                return
            if not self._apply_ipv4_cidr_to_linux_interface(
                entry.linux_ifname,
                master_cidr,
                remove_cidr=backup_cidr,
            ):
                return

        self._standby_switched_to_master = True
        self._plc_shadow_standby = False
        self._promoted_standby_acting_master = True

        try:
            if self._try_redundancy_shadow_exit():
                logger.info(
                    "[热冗余][备机] 已通过 REDUNDANCY_SHADOW_EXIT 平滑升主（保留 PLC 进程与 I/O 镜像）"
                )
            else:
                self._restart_plc_core_after_takeover()
        except Exception as e:
            logger.error("[热冗余][备机] 升主后切换 PLC 核心失败: %s", e)
        # Keep redundancy heartbeat/image threads alive; switch behavior via state flags only.

    def _try_redundancy_shadow_exit(self) -> bool:
        """Try to enable field I/O in-process without restarting plc_main."""
        try:
            self._safe_connect_runtime_socket()
            if not self.runtime_socket.is_connected():
                return False
            resp = self.runtime_socket.send_and_receive(
                "REDUNDANCY_SHADOW_EXIT\n",
                timeout=120.0,
            )
            return resp == "REDUNDANCY_SHADOW_EXIT:OK"
        except (OSError, RuntimeError, TypeError, ValueError) as e:
            logger.warning("[热冗余][备机] REDUNDANCY_SHADOW_EXIT 不可用或失败，将回退为重启 plc_main: %s", e)
            return False

    def _redundancy_trigger_failback_to_standby(self) -> None:
        """收到原主机冗余心跳后回切：按 functional_nics[].standby_backup_* 恢复各功能网卡，影子 PLC。"""
        logger.info("[热冗余][备机] 回切备机已触发")
        project_root = self._openplc_project_root()
        role_json_path = project_root / REDUNDANCY_ROLE_FILENAME
        backup_cidrs = read_standby_backup_cidrs_for_project(project_root)
        permanent_cidrs = read_functional_cidrs_for_project(project_root)
        restored = False
        for entry, backup, permanent in zip(
            self._redundancy_functional_nics, backup_cidrs, permanent_cidrs
        ):
            if backup:
                self._apply_ipv4_cidr_to_linux_interface(
                    entry.linux_ifname,
                    backup,
                    remove_cidr=permanent,
                )
                restored = True
        if self._redundancy_functional_nics and not restored:
            logger.warning(
                "[热冗余][备机] functional_nics standby_backup 无效，跳过恢复功能口 IP（请检查 %s）",
                role_json_path,
            )

        self._standby_switched_to_master = False
        self._promoted_standby_acting_master = False
        self._plc_shadow_standby = True
        try:
            self._restart_plc_core_shadow_standby_after_failback()
        except Exception as e:
            logger.warning("[热冗余][备机] 回切后重启影子 PLC 失败（可手动重启运行时）: %s", e)
        # Keep redundancy heartbeat/image threads alive; switch behavior via state flags only.

    def _restart_plc_core_after_takeover(self) -> None:
        """终止当前 plc_main 并以非影子方式重启并 START（需 root/rt 权限场景与常规一致）。"""
        self._manual_plc_restart_in_progress = True
        try:
            try:
                self.runtime_socket.send_message("STOP\n")
            except (OSError, socket.error, RuntimeError):
                pass
            time.sleep(0.5)
            self._safe_close_runtime_socket()
            if self.process:
                if HAS_PSUTIL and isinstance(self.process, psutil.Process):
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except (psutil.TimeoutExpired, psutil.Error):
                        self.process.kill()
                elif isinstance(self.process, subprocess.Popen):
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
                        self.process.kill()
                self.process = None
            time.sleep(0.5)
            self._safe_start_log_server()
            cmd = [self.runtime_path]
            if self.print_debug:
                cmd.append("--print-debug")
            self.process = subprocess.Popen(cmd)
            time.sleep(1)
            self._safe_connect_runtime_socket()
            self.start_plc()
            logger.info("[热冗余][备机] 已切换为非影子 plc_main 并已下发 START")
        finally:
            self._manual_plc_restart_in_progress = False

    def _restart_plc_core_shadow_standby_after_failback(self) -> None:
        """回切备机后重启影子 plc_main。"""
        self._manual_plc_restart_in_progress = True
        try:
            try:
                self.runtime_socket.send_message("STOP\n")
            except (OSError, socket.error, RuntimeError):
                pass
            time.sleep(0.5)
            self._safe_close_runtime_socket()
            if self.process:
                if HAS_PSUTIL and isinstance(self.process, psutil.Process):
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except (psutil.TimeoutExpired, psutil.Error):
                        self.process.kill()
                elif isinstance(self.process, subprocess.Popen):
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
                        self.process.kill()
                self.process = None
            time.sleep(0.5)
            self._safe_start_log_server()
            cmd = [self.runtime_path]
            if self.print_debug:
                cmd.append("--print-debug")
            cmd.append("--shadow-standby")
            self.process = subprocess.Popen(cmd)
            time.sleep(1)
            self._safe_connect_runtime_socket()
            self.start_plc()
            logger.info("[热冗余][备机] 已回切为影子 plc_main 并已下发 START")
        finally:
            self._manual_plc_restart_in_progress = False

    def _standby_tick_lost_times(self, lost_times: int) -> tuple[int, bool]:
        """
        After LostTimes 每秒 +1：超过阈值则 ping 主机；不通则触发备升主。
        Returns (new_lost_times, switched True if 备升主已触发).
        """
        if lost_times <= REDUNDANCY_STANDBY_LOST_THRESHOLD_SEC:
            return lost_times, False
        if self._redundancy_ping_master_reachable():
            logger.info(
                "[热冗余][备机] LostTimes=%d 已超过阈值 %d 秒，但功能网卡 %s 上仍有可达的主机地址，清零计数。",
                lost_times,
                REDUNDANCY_STANDBY_LOST_THRESHOLD_SEC,
                self._format_functional_nic_names_for_log(self._redundancy_functional_nics),
            )
            return 0, False
        self._schedule_async_standby_to_master_switch()
        return 0, True

    def _schedule_async_standby_to_master_switch(self) -> None:
        """备机心跳线程内触发升主须异步执行，避免 shutdown/join 当前线程死锁。"""

        def runner() -> None:
            time.sleep(0.05)
            try:
                self._redundancy_trigger_standby_to_master_switch()
            except Exception as e:
                logger.error("[热冗余][备机] 异步备升主异常: %s", e)

        threading.Thread(target=runner, daemon=True, name="async-standby-to-master").start()

    def _schedule_async_failback_to_standby(self) -> None:
        """备机心跳线程内触发回切须异步执行。"""

        def runner() -> None:
            time.sleep(0.05)
            try:
                self._redundancy_trigger_failback_to_standby()
            except Exception as e:
                logger.warning("[热冗余][备机] 异步回切异常: %s", e)

        threading.Thread(target=runner, daemon=True, name="async-failback-standby").start()

    def _redundancy_master_tcp_heartbeat_loop(self) -> None:
        """
        每秒：TCP 已连接则发送心跳；否则尝试连接备机。无数、无超时切换、永久循环直至 stop。
        """
        local_ip = self._redundancy_local_heartbeat_ip
        peer_ip = self._redundancy_standby_ip
        if not local_ip or not peer_ip:
            return
        sock: socket.socket | None = None
        first_send_logged = False
        master_hb_ever_connected = False
        while not self._heartbeat_stop.is_set():
            if sock is None:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.bind((local_ip, 0))
                    sock.settimeout(10.0)
                    sock.connect((peer_ip, REDUNDANCY_HEARTBEAT_PORT))
                    local_ep = sock.getsockname()
                    if master_hb_ever_connected:
                        logger.info(
                            "[热冗余][主机] 已重新连接备机 TCP %s:%d（本机 %s:%d，冗余口 %s）。",
                            peer_ip,
                            REDUNDANCY_HEARTBEAT_PORT,
                            local_ep[0],
                            local_ep[1],
                            self._redundancy_heartbeat_nic,
                        )
                    else:
                        logger.info(
                            "[热冗余][主机] 已成功连接备机 TCP %s:%d（本机 %s:%d，冗余口 %s）。",
                            peer_ip,
                            REDUNDANCY_HEARTBEAT_PORT,
                            local_ep[0],
                            local_ep[1],
                            self._redundancy_heartbeat_nic,
                        )
                    master_hb_ever_connected = True
                    first_send_logged = False
                    self._sync_functional_lines_after_tcp_connect(sock, sync_attempt_kind="on_connect")
                except OSError as e:
                    logger.warning(
                        "[热冗余][主机] 连接对端 TCP %s:%d 失败，将在 %.1f 秒后重试: %s",
                        peer_ip,
                        REDUNDANCY_HEARTBEAT_PORT,
                        REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC,
                        e,
                    )
                    if sock is not None:
                        try:
                            sock.close()
                        except OSError:
                            pass
                        sock = None
                    if self._heartbeat_stop.wait(REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC):
                        break
                    continue

            self._sync_functional_lines_after_tcp_connect(sock, sync_attempt_kind="periodic")

            try:
                sock.sendall(REDUNDANCY_HB_PAYLOAD)
            except OSError as e:
                logger.warning(
                    "[热冗余][主机] 发送 TCP 心跳失败，将断开并重连备机: %s",
                    e,
                )
                err_no = getattr(e, "errno", None)
                if err_no in (errno.ECONNRESET, errno.EPIPE):
                    self._ensure_master_functional_ips_match_json_after_heartbeat_send_error(
                        err_no
                    )
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None
                if self._heartbeat_stop.wait(REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC):
                    break
                continue

            if not first_send_logged:
                logger.info(
                    "[热冗余][主机] 已开始向备机发送 TCP 心跳（对端=%s:%d，冗余口本机 IP=%s）。",
                    peer_ip,
                    REDUNDANCY_HEARTBEAT_PORT,
                    local_ip,
                )
                first_send_logged = True

            if self._heartbeat_stop.wait(REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC):
                break

        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        logger.info("[热冗余][主机] TCP 心跳发送线程已退出。")

    def _redundancy_standby_tcp_heartbeat_loop(self) -> None:
        """
        备机：纯备机态 LostTimes / 升主；暂时升主后仍监听，accept 超时不计数，
        收到原主机发来的心跳载荷后异步回切（影子 PLC + 按 functional_nics standby_backup 恢复功能 IP）。
        """
        local_ip = self._redundancy_local_heartbeat_ip
        master_redundancy_ip = self._redundancy_master_ip
        if not local_ip:
            return
        server: socket.socket | None = None
        lost_times = 0
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((local_ip, REDUNDANCY_HEARTBEAT_PORT))
            server.listen(1)
            logger.info(
                "[热冗余][备机] TCP 冗余心跳监听已建立：绑定地址=%s:%d，等待主机连入。",
                local_ip,
                REDUNDANCY_HEARTBEAT_PORT,
            )
            server.settimeout(REDUNDANCY_STANDBY_RECV_IDLE_SEC)
            while not self._heartbeat_stop.is_set():
                promoted = self._standby_switched_to_master and self._promoted_standby_acting_master
                if promoted:
                    try:
                        client, addr = server.accept()
                    except TimeoutError:
                        continue
                    except OSError as e:
                        if self._heartbeat_stop.is_set():
                            break
                        logger.error("[热冗余][备机] accept 失败: %s", e)
                        continue
                    if master_redundancy_ip and addr[0] != master_redundancy_ip:
                        logger.warning(
                            "[热冗余][备机] 升主状态下收到非配置主机冗余 IP 的连入 %s，忽略",
                            addr[0],
                        )
                        try:
                            client.close()
                        except OSError:
                            pass
                        continue
                    logger.info(
                        "[热冗余][备机] 原主机 TCP 已连入（升主监听态），对端=%s:%d，等待心跳包以回切",
                        addr[0],
                        addr[1],
                    )
                    buf = b""
                    client_live = client
                    try:
                        client_live.settimeout(REDUNDANCY_STANDBY_RECV_IDLE_SEC)
                        while (
                            not self._heartbeat_stop.is_set()
                            and self._standby_switched_to_master
                            and self._promoted_standby_acting_master
                        ):
                            try:
                                chunk = client_live.recv(4096)
                            except (TimeoutError, socket.timeout):
                                continue
                            except OSError as e:
                                logger.warning("[热冗余][备机] 升主监听 recv 错误: %s", e)
                                break
                            if not chunk:
                                break
                            buf += chunk
                            buf, heartbeat_seen = self._consume_standby_redundancy_tcp_buffer(buf)
                            if heartbeat_seen:
                                logger.info("[热冗余][备机] 收到原主机心跳，触发自动回切")
                                self._schedule_async_failback_to_standby()
                                try:
                                    client_live.close()
                                except OSError:
                                    pass
                                client_live = None
                                break
                            if client_live is None:
                                break
                    finally:
                        if client_live is not None:
                            try:
                                client_live.close()
                            except OSError:
                                pass
                    continue

                try:
                    client, addr = server.accept()
                except TimeoutError:
                    lost_times += 1
                    lost_times, _ = self._standby_tick_lost_times(lost_times)
                    continue
                except OSError as e:
                    if self._heartbeat_stop.is_set():
                        break
                    logger.error("[热冗余][备机] accept 失败: %s", e)
                    continue

                logger.info(
                    "[热冗余][备机] 已接受主机 TCP 连接，对端=%s:%d，LostTimes 清零。",
                    addr[0],
                    addr[1],
                )
                lost_times = 0
                buf = b""
                try:
                    client.settimeout(REDUNDANCY_STANDBY_RECV_IDLE_SEC)
                    while (
                        not self._heartbeat_stop.is_set()
                        and not self._standby_switched_to_master
                    ):
                        try:
                            chunk = client.recv(4096)
                        except (TimeoutError, socket.timeout):
                            lost_times += 1
                            if lost_times > REDUNDANCY_STANDBY_LOST_THRESHOLD_SEC:
                                logger.info("[热冗余][备机] LostTimes 增加到 %d", lost_times)
                            lost_times, switched = self._standby_tick_lost_times(lost_times)
                            if switched:
                                break
                            continue
                        except OSError as e:
                            logger.warning("[热冗余][备机] 接收 TCP 数据错误: %s", e)
                            break
                        if not chunk:
                            logger.info(
                                "[热冗余][备机] 主机已关闭 TCP 连接，LostTimes 保持累计，返回等待连接。"
                            )
                            break
                        buf += chunk
                        buf, heartbeat_seen = self._consume_standby_redundancy_tcp_buffer(buf)
                        if heartbeat_seen:
                            lost_times = 0
                finally:
                    try:
                        client.close()
                    except OSError:
                        pass
        finally:
            if server is not None:
                try:
                    server.close()
                except OSError:
                    pass
            logger.info("[热冗余][备机] TCP 心跳监听线程已退出。")

    def _redundancy_image_sync_master_loop(self) -> None:
        """Push full I/O snapshots at each PLC scan cycle end (UDP frame fragments)."""
        standby_ip = self._redundancy_standby_ip
        local_ip = self._redundancy_local_heartbeat_ip
        if not standby_ip:
            logger.warning(
                "[hot-redundancy][master] standby heartbeat IP missing; skip I/O image sync"
            )
            return
        if not local_ip:
            logger.warning(
                "[hot-redundancy][master] heartbeat NIC %s has no local IP; skip I/O image sync",
                self._redundancy_heartbeat_nic,
            )
            return

        sock: socket.socket | None = None
        dest = (standby_ip, REDUNDANCY_IMAGE_SYNC_PORT)
        session_id = self._next_redundancy_image_session_id()
        frame_seq = 0
        last_scan_counter = 0
        udp_stats = self._image_udp_master_stats
        udp_stats.current_session_id = session_id
        if self._try_enable_plc_redundancy_data_plane():
            logger.info(
                "[hot-redundancy][master] webserver UDP sync disabled; plc_main data plane active"
            )
            while not self._heartbeat_stop.is_set() and self.is_master:
                self._heartbeat_stop.wait(1.0)
            return

        logger.info(
            "[hot-redundancy][master] UDP I/O image sync (scan-end, delta preferred) "
            "(local=%s, peer=%s:%s, session=%s)",
            local_ip,
            standby_ip,
            REDUNDANCY_IMAGE_SYNC_PORT,
            session_id,
        )
        try:
            while not self._heartbeat_stop.is_set():
                if not self.is_master:
                    break
                try:
                    if sock is None:
                        sock = _open_redundancy_image_master_udp_socket(local_ip)
                        sock.settimeout(REDUNDANCY_IMAGE_UDP_ACK_TIMEOUT_SEC)
                        logger.info(
                            "[hot-redundancy][master] UDP I/O image source bound to %s:%s",
                            local_ip,
                            sock.getsockname()[1],
                        )

                    payload = None
                    scan_meta = None
                    scan_err = None
                    delta_body, delta_err = (
                        self.runtime_socket.image_delta_get_scan_end(
                            last_scan_counter, REDUNDANCY_IMAGE_SCAN_END_WAIT_TIMEOUT_SEC
                        )
                    )
                    if (
                        delta_body
                        and not delta_err
                        and len(delta_body) <= REDUNDANCY_IMAGE_DELTA_MAX_WIRE_BYTES
                    ):
                        payload = delta_body
                        scan_meta = _parse_delta_wire_metadata(delta_body)
                    if payload is None:
                        payload, scan_meta, scan_err = (
                            self.runtime_socket.image_snapshot_get_scan_end(
                                last_scan_counter,
                                REDUNDANCY_IMAGE_SCAN_END_WAIT_TIMEOUT_SEC,
                            )
                        )
                    if scan_err == "scan_end_timeout":
                        udp_stats.scan_end_timeout_count += 1
                    if not payload or scan_meta is None:
                        self._heartbeat_stop.wait(0.05)
                        continue
                    if (
                        not _is_redundancy_delta_payload(payload)
                        and len(payload) != IMAGE_SNAPSHOT_EXPECTED_BYTES
                    ):
                        self._heartbeat_stop.wait(0.05)
                        continue

                    last_scan_counter = scan_meta.scan_counter
                    frame_meta = RedundancyImageFrameMetadata(
                        scan_counter=scan_meta.scan_counter,
                        tick=scan_meta.tick,
                        phase=scan_meta.phase,
                        timestamp_ns=scan_meta.timestamp_ns,
                    )

                    frame_seq = (frame_seq + 1) & ((1 << 64) - 1)
                    if frame_seq == 0:
                        session_id = self._next_redundancy_image_session_id()
                        udp_stats.current_session_id = session_id
                        frame_seq = 1

                    udp_stats.last_send_timestamp_ns = frame_meta.timestamp_ns
                    udp_stats.last_scan_counter = frame_meta.scan_counter
                    udp_stats.last_tick = frame_meta.tick
                    udp_stats.last_phase = frame_meta.phase
                    udp_stats.payload_crc32 = _redundancy_image_crc32(payload)

                    packets = _iter_redundancy_image_udp_fragments(
                        session_id, frame_seq, payload
                    )
                    for packet in packets:
                        sock.sendto(packet, dest)
                    send_done_monotonic = time.monotonic()
                    udp_stats.last_send_monotonic = send_done_monotonic
                    udp_stats.frame_send_count += 1
                    udp_stats.fragment_send_count += len(packets)
                    udp_stats.last_send_frame_seq = frame_seq

                    ack_seen = False
                    ack_deadline = send_done_monotonic + REDUNDANCY_IMAGE_UDP_ACK_TIMEOUT_SEC
                    max_ack_datagram = max(
                        REDUNDANCY_IMAGE_UDP_ACK_HEADER.size,
                        REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2.size,
                    ) + 64
                    while time.monotonic() < ack_deadline:
                        ack_timeout = max(0.0, ack_deadline - time.monotonic())
                        if ack_timeout <= 0:
                            break
                        sock.settimeout(ack_timeout)
                        try:
                            ack_packet, addr = sock.recvfrom(max_ack_datagram)
                        except (TimeoutError, socket.timeout):
                            break
                        ack, parse_status = _parse_redundancy_image_udp_ack(ack_packet)
                        if ack is None:
                            logger.debug(
                                "[hot-redundancy][master] bad UDP image ACK: %s",
                                REDUNDANCY_IMAGE_ACK_STATUS_NAMES.get(
                                    parse_status, parse_status
                                ),
                            )
                            continue
                        if _process_redundancy_image_master_ack(
                            udp_stats, session_id, frame_seq, ack, standby_ip, addr
                        ):
                            ack_seen = True
                            _record_redundancy_image_master_ack_latency(
                                udp_stats, send_done_monotonic, ack
                            )
                            udp_stats.consecutive_ack_miss_count = 0
                            if ack.status != REDUNDANCY_IMAGE_ACK_STATUS_OK:
                                status_name = REDUNDANCY_IMAGE_ACK_STATUS_NAMES.get(
                                    ack.status, str(ack.status)
                                )
                                log = (
                                    logger.warning
                                    if ack.status
                                    in (
                                        REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR,
                                        REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER,
                                        REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR,
                                    )
                                    else logger.debug
                                )
                                log(
                                    "[hot-redundancy][master] UDP image frame %s ACK=%s "
                                    "(applied=%s)",
                                    frame_seq,
                                    status_name,
                                    ack.applied_seq,
                                )
                            break
                    if not ack_seen:
                        udp_stats.ack_timeout_count += 1
                        udp_stats.consecutive_ack_miss_count += 1
                        logger.debug(
                            "[hot-redundancy][master] UDP image frame %s ACK timeout "
                            "(consecutive_miss=%s)",
                            frame_seq,
                            udp_stats.consecutive_ack_miss_count,
                        )
                    _maybe_log_redundancy_image_udp_master_stats(udp_stats)
                except OSError as e:
                    logger.debug(
                        "[hot-redundancy][master] UDP I/O image sync error; reconnect: %s",
                        e,
                    )
                    if sock is not None:
                        try:
                            sock.close()
                        except OSError:
                            pass
                        sock = None
                    self._heartbeat_stop.wait(0.5)
                except (RuntimeError, TypeError, ValueError) as e:
                    logger.warning("[hot-redundancy][master] I/O image sync error: %s", e)
                    self._heartbeat_stop.wait(0.5)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            logger.info("[hot-redundancy][master] UDP I/O image sync sender exited")

    def _redundancy_image_sync_standby_loop(self) -> None:
        """Receive UDP snapshot fragments from master and apply full frames."""
        local_ip = self._redundancy_local_heartbeat_ip
        master_ip = self._redundancy_master_ip
        if not local_ip or not master_ip:
            logger.warning(
                "[hot-redundancy][standby] heartbeat NIC %s has no local IP; "
                "skip I/O image listener",
                self._redundancy_heartbeat_nic,
            )
            return

        server: socket.socket | None = None
        udp_stats = self._image_udp_standby_stats
        assembler = _RedundancyImageUdpFrameAssembler(stats=udp_stats)
        max_datagram = (
            REDUNDANCY_IMAGE_UDP_DATA_HEADER.size
            + REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
            + 64
        )
        if self._try_enable_plc_redundancy_data_plane():
            logger.info(
                "[hot-redundancy][standby] webserver UDP sync disabled; plc_main data plane active"
            )
            while not self._heartbeat_stop.is_set():
                self._heartbeat_stop.wait(1.0)
            return

        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((local_ip, REDUNDANCY_IMAGE_SYNC_PORT))
            server.settimeout(REDUNDANCY_IMAGE_UDP_FRAME_TIMEOUT_SEC / 3)
            logger.info(
                "[hot-redundancy][standby] UDP I/O image listener (barrier apply) %s:%s "
                "(master=%s)",
                local_ip,
                REDUNDANCY_IMAGE_SYNC_PORT,
                master_ip,
            )
            while not self._heartbeat_stop.is_set():
                if self._promoted_standby_acting_master:
                    self._heartbeat_stop.wait(0.2)
                    continue

                expired = assembler.expire_pending(time.monotonic())
                if expired is not None:
                    addr, session_id, frame_seq, status = expired
                    try:
                        _send_redundancy_image_udp_ack(
                            server,
                            addr,
                            status,
                            session_id,
                            frame_seq,
                            assembler.last_applied_seq,
                        )
                    except OSError as e:
                        logger.debug(
                            "[hot-redundancy][standby] UDP image incomplete ACK failed: %s",
                            e,
                        )

                try:
                    packet, addr = server.recvfrom(max_datagram)
                except (TimeoutError, socket.timeout):
                    continue
                except OSError as e:
                    if self._heartbeat_stop.is_set():
                        break
                    logger.error(
                        "[hot-redundancy][standby] UDP I/O image recv failed: %s",
                        e,
                    )
                    continue

                if addr[0] != master_ip:
                    udp_stats.bad_source_count += 1
                    logger.warning(
                        "[hot-redundancy][standby] reject UDP I/O image packet from %s:%s",
                        addr[0],
                        addr[1],
                    )
                    continue

                fragment, parse_status = _parse_redundancy_image_udp_fragment(packet)
                if fragment is None:
                    udp_stats.bad_header_count += 1
                    session_id, frame_seq = _redundancy_image_udp_header_ids(packet)
                    if session_id and frame_seq:
                        try:
                            _send_redundancy_image_udp_ack(
                                server,
                                addr,
                                parse_status,
                                session_id,
                                frame_seq,
                                assembler.last_applied_seq,
                            )
                        except OSError as e:
                            logger.debug(
                                "[hot-redundancy][standby] bad-header ACK failed: %s",
                                e,
                            )
                    continue

                udp_stats.fragment_rx_count += 1
                status, frame_seq, payload = assembler.add_fragment(
                    fragment, addr, time.monotonic()
                )
                if status is None:
                    _maybe_log_redundancy_image_udp_standby_stats(udp_stats)
                    continue
                if payload is None:
                    _record_redundancy_image_standby_ack_status(udp_stats, status)
                    try:
                        _send_redundancy_image_udp_ack(
                            server,
                            addr,
                            status,
                            fragment.session_id,
                            frame_seq,
                            assembler.last_applied_seq,
                        )
                        udp_stats.last_ack_sent_monotonic = time.monotonic()
                    except OSError as e:
                        logger.debug(
                            "[hot-redundancy][standby] UDP image status ACK failed: %s",
                            e,
                        )
                    _maybe_log_redundancy_image_udp_standby_stats(udp_stats)
                    continue

                ack_status = REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR
                try:
                    if not self.runtime_socket.is_connected():
                        self._safe_connect_runtime_socket()
                    if not self._plc_shadow_standby:
                        ack_status = REDUNDANCY_IMAGE_ACK_STATUS_NOT_SHADOW
                    elif not self._plc_runtime_is_running():
                        ack_status = REDUNDANCY_IMAGE_ACK_STATUS_NOT_READY
                    elif self.runtime_socket.image_sync_pending_set(frame_seq, payload):
                        if self.runtime_socket.image_sync_wait_applied(frame_seq):
                            assembler.record_applied(fragment.session_id, frame_seq)
                            ack_status = REDUNDANCY_IMAGE_ACK_STATUS_OK
                        else:
                            ack_status = REDUNDANCY_IMAGE_ACK_STATUS_NOT_READY
                    else:
                        ack_status = REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR
                        udp_stats.consecutive_apply_fail_count += 1
                        logger.warning(
                            "[hot-redundancy][standby] pending queue failed"
                        )
                except (OSError, RuntimeError) as e:
                    ack_status = REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR
                    udp_stats.consecutive_apply_fail_count += 1
                    logger.warning(
                        "[hot-redundancy][standby] I/O image SET failed: %s", e
                    )

                _record_redundancy_image_standby_ack_status(udp_stats, ack_status)
                try:
                    _send_redundancy_image_udp_ack(
                        server,
                        addr,
                        ack_status,
                        fragment.session_id,
                        frame_seq,
                        assembler.last_applied_seq,
                    )
                    udp_stats.last_ack_sent_monotonic = time.monotonic()
                except OSError as e:
                    logger.debug(
                        "[hot-redundancy][standby] UDP image apply ACK failed: %s",
                        e,
                    )
                _maybe_log_redundancy_image_udp_standby_stats(udp_stats)
        finally:
            if server is not None:
                try:
                    server.close()
                except OSError:
                    pass
            logger.info("[hot-redundancy][standby] UDP I/O image listener exited")

    def _start_redundancy_heartbeat_threads(self) -> None:
        self._shutdown_redundancy_heartbeat_threads()
        # New cycle needs a clear stop event (previous stop() left it set).
        self._heartbeat_stop = threading.Event()
        if not self.is_redundancy:
            return
        if self.is_master:
            self._image_udp_master_stats = RedundancyImageUdpMasterStats()
        else:
            self._image_udp_standby_stats = RedundancyImageUdpStandbyStats()
        if self.is_master:
            t = threading.Thread(
                target=self._redundancy_master_tcp_heartbeat_loop,
                name="redundancy-master-tcp-hb",
                daemon=True,
            )
            self._heartbeat_threads.append(t)
            t.start()
            t_img = threading.Thread(
                target=self._redundancy_image_sync_master_loop,
                name="redundancy-master-io-sync",
                daemon=True,
            )
            self._heartbeat_threads.append(t_img)
            t_img.start()
            return
        t = threading.Thread(
            target=self._redundancy_standby_tcp_heartbeat_loop,
            name="redundancy-standby-tcp-hb",
            daemon=True,
        )
        self._heartbeat_threads.append(t)
        t.start()
        t_img = threading.Thread(
            target=self._redundancy_image_sync_standby_loop,
            name="redundancy-standby-io-sync",
            daemon=True,
        )
        self._heartbeat_threads.append(t_img)
        t_img.start()

    def find_running_process(self):
        """
        Find the running PLC runtime process.
        Returns None if psutil is not available (MSYS2/Cygwin).
        """
        if not HAS_PSUTIL:
            # Cannot detect existing processes without psutil
            return None

        # Find the running PLC runtime process by executable path
        for proc in psutil.process_iter(["pid", "exe", "cmdline"]):
            try:
                # First try to match by executable path (most reliable)
                if proc.info["exe"] and os.path.samefile(proc.info["exe"], self.runtime_path):
                    return proc

                # Alternatively, match by command line (fallback)
                cmdline = proc.info.get("cmdline")
                if cmdline and isinstance(cmdline, (list, tuple)) and len(cmdline) > 0:
                    cmdline_str = " ".join(str(arg) for arg in cmdline if arg is not None)
                    if self.runtime_path in cmdline_str:
                        return proc

            except (OSError, psutil.Error, TypeError, ValueError):
                continue
        return None

    def _safe_start_log_server(self):
        try:
            self.log_server.start()
        except (OSError, socket.error) as e:
            logger.error("Failed to start log server: %s", e)
        except Exception as e:
            logger.error("Failed to start log server (unexpected): %s", e)

    def _safe_connect_runtime_socket(self):
        try:
            self.runtime_socket.connect()
        except (FileNotFoundError, OSError, socket.error) as e:
            logger.error("Failed to connect to runtime socket: %s", e)
        except Exception as e:
            logger.error("Failed to connect to runtime socket (unexpected): %s", e)

    def _safe_stop_log_server(self):
        try:
            self.log_server.stop()
        except (OSError, socket.error) as e:
            logger.error("Failed to stop log server: %s", e)
        except Exception as e:
            logger.error("Failed to stop log server (unexpected): %s", e)

    def _safe_close_runtime_socket(self):
        try:
            self.runtime_socket.close()
        except (OSError, socket.error) as e:
            logger.error("Failed to close runtime socket: %s", e)
        except Exception as e:
            logger.error("Failed to close runtime socket (unexpected): %s", e)

    def _plc_runtime_is_running(self) -> bool:
        """True if plc_main reports STATUS:RUNNING (I/O image tables safe for snapshot)."""
        now = time.monotonic()
        with self._plc_status_cache_lock:
            if self._plc_status_cache_monotonic > 0.0 and (
                now - self._plc_status_cache_monotonic
            ) < PLC_STATUS_CACHE_TTL_SEC:
                return self._plc_status_cache_running

        try:
            if not self.runtime_socket.is_connected():
                self._safe_connect_runtime_socket()
            if not self.runtime_socket.is_connected():
                with self._plc_status_cache_lock:
                    self._plc_status_cache_running = False
                    self._plc_status_cache_monotonic = time.monotonic()
                return False
            status = self.runtime_socket.send_and_receive("STATUS\n", timeout=0.5)
            ok = status == "STATUS:RUNNING"
            with self._plc_status_cache_lock:
                self._plc_status_cache_running = ok
                self._plc_status_cache_monotonic = time.monotonic()
            return ok
        except (OSError, RuntimeError, TypeError, ValueError):
            with self._plc_status_cache_lock:
                self._plc_status_cache_running = False
                self._plc_status_cache_monotonic = time.monotonic()
            return False

    def start(self):
        """
        Start the runtime manager and the PLC runtime process
        """
        if self.running:
            logger.warning("Runtime manager already running")
            return

        self._evaluate_redundancy_role()

        self.running = True

        # Ensure UNIX socket paths exist
        plc_socket_dir = os.path.dirname(self.plc_socket)
        log_socket_dir = os.path.dirname(self.log_socket)
        if not os.path.exists(plc_socket_dir):
            try:
                os.makedirs(plc_socket_dir)
                logger.info("Created directory for PLC socket: %s", plc_socket_dir)
            except OSError as e:
                logger.error("Failed to create directory for PLC socket: %s", e)
        if not os.path.exists(log_socket_dir):
            try:
                os.makedirs(log_socket_dir)
                logger.info("Created directory for log socket: %s", log_socket_dir)
            except OSError as e:
                logger.error("Failed to create directory for log socket: %s", e)

        # Start runtime process if not already running
        running_process = self.find_running_process()
        if running_process:
            logger.info("Found existing PLC runtime process with PID %d", running_process.pid)
            self.process = running_process
            self._safe_start_log_server()
            self._safe_connect_runtime_socket()
        else:
            logger.info("Starting PLC runtime core...")
            self._safe_start_log_server()
            try:
                cmd = [self.runtime_path]
                if self.print_debug:
                    cmd.append("--print-debug")
                if self._plc_shadow_standby:
                    cmd.append("--shadow-standby")
                self.process = subprocess.Popen(cmd)
            except (OSError, subprocess.SubprocessError) as e:
                logger.error("Failed to start PLC runtime process: %s", e)
                self.process = None
            time.sleep(1)  # Give time to start
            self._safe_connect_runtime_socket()

        # Start monitor thread
        if not self.monitor_thread.is_alive():
            self.monitor_thread = threading.Thread(target=self._monitor, daemon=True)
            self.monitor_thread.start()

        self._start_redundancy_heartbeat_threads()

    def is_runtime_alive(self):
        """
        Check if the PLC runtime process is alive
        """
        if self.process is None:
            return False
        if HAS_PSUTIL and isinstance(self.process, psutil.Process):
            if self.process.is_running() and self.process.status() != psutil.STATUS_ZOMBIE:
                return True
        elif isinstance(self.process, subprocess.Popen):
            if self.process.poll() is None:
                return True
        return False

    def _start_runtime_process(self, safe_mode=False):
        """Start the runtime process, optionally in safe mode."""
        self._safe_start_log_server()
        try:
            cmd = [self.runtime_path]
            if self.print_debug:
                cmd.append("--print-debug")
            if safe_mode:
                cmd.append("--safe-mode")
            elif self._plc_shadow_standby:
                cmd.append("--shadow-standby")
            self.process = subprocess.Popen(cmd)
        except (OSError, subprocess.SubprocessError) as e:
            logger.error("Failed to start PLC runtime process: %s", e)
            self.process = None
        time.sleep(1)  # Give time to start
        self._safe_connect_runtime_socket()

    def _record_crash_and_check_safe_mode(self):
        """Record a crash timestamp and check if safe mode should be entered."""
        with self._crash_lock:
            now = time.time()
            # Keep only crashes within the time window
            self._crash_times = [t for t in self._crash_times if now - t < RAPID_CRASH_WINDOW]
            self._crash_times.append(now)
            return len(self._crash_times) >= MAX_RAPID_CRASHES

    def _monitor(self):
        """
        Monitor the PLC runtime process and restart if it dies.
        Tracks crash frequency and enters safe mode after repeated failures.
        """
        while self.running:
            if self._manual_plc_restart_in_progress:
                time.sleep(0.3)
                continue
            if not self.is_runtime_alive():
                logger.warning("PLC runtime process died unexpectedly")
                self._safe_stop_log_server()
                self._safe_close_runtime_socket()

                if self._record_crash_and_check_safe_mode():
                    with self._crash_lock:
                        if not self._safe_mode:
                            logger.error(
                                "PLC program caused %d crashes within %d seconds. "
                                "Restarting runtime in SAFE MODE - "
                                "PLC program will NOT be loaded. "
                                "Upload a corrected program to recover.",
                                MAX_RAPID_CRASHES,
                                RAPID_CRASH_WINDOW,
                            )
                            self._safe_mode = True
                    self._start_runtime_process(safe_mode=True)
                else:
                    logger.warning("Restarting PLC runtime...")
                    self._start_runtime_process(safe_mode=False)
            else:
                # Make sure log server and socket are connected
                if not self.log_server.running:
                    self._safe_start_log_server()
                if not self.runtime_socket.is_connected():
                    self._safe_connect_runtime_socket()

            time.sleep(2)

    def stop(self):
        """ "
        Stop the runtime manager and the PLC runtime process
        """
        self._shutdown_redundancy_heartbeat_threads()
        try:
            self.runtime_socket.send_message("STOP\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to send STOP to PLC runtime: %s", e)
        except Exception as e:
            logger.error("Failed to send STOP to PLC runtime (unexpected): %s", e)
        self.running = False
        self.monitor_thread.join(timeout=5)
        time.sleep(1)
        if self.process:
            if HAS_PSUTIL and isinstance(self.process, psutil.Process):
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except (psutil.TimeoutExpired, psutil.Error):
                    self.process.kill()
            elif isinstance(self.process, subprocess.Popen):
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except (subprocess.TimeoutExpired, subprocess.SubprocessError):
                    self.process.kill()
            self.process = None
        self._safe_stop_log_server()
        self._safe_close_runtime_socket()

    def reset_crash_tracking(self):
        """Reset crash tracking state after a successful program upload."""
        with self._crash_lock:
            self._crash_times.clear()
            self._safe_mode = False

    def get_logs(self, min_id=None, level=None):
        """
        Get current logs from the runtime
        """
        try:
            _logs = buffer.normalize_logs(buffer.get_logs(min_id=min_id, level=level))
            return _logs
        except AttributeError as e:
            logger.error("Failed to get logs from buffer: %s", e)
            return []

    def ping(self):
        """
        Send PING and wait for PONG
        """
        try:
            return self.runtime_socket.send_and_receive("PING\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to ping PLC runtime: %s", e)
            return "PING:ERROR\n"
        except Exception as e:
            logger.error("Failed to ping PLC runtime (unexpected): %s", e)
            return "PING:ERROR\n"

    def start_plc(self):
        """
        Send START command
        """
        try:
            return self.runtime_socket.send_and_receive("START\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to start PLC runtime: %s", e)
            return "START:ERROR\n"
        except Exception as e:
            logger.error("Failed to start PLC runtime (unexpected): %s", e)
            return "START:ERROR\n"

    def stop_plc(self):
        """
        Send STOP command
        """
        try:
            return self.runtime_socket.send_and_receive("STOP\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to stop PLC runtime: %s", e)
            return "STOP:ERROR\n"
        except Exception as e:
            logger.error("Failed to stop PLC runtime (unexpected): %s", e)
            return "STOP:ERROR\n"

    def status_plc(self):
        """
        Send STATUS command
        """
        try:
            return self.runtime_socket.send_and_receive("STATUS\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to get PLC status: %s", e)
            return "STATUS:ERROR\n"
        except Exception as e:
            logger.error("Failed to get PLC status (unexpected): %s", e)
            return "STATUS:ERROR\n"

    def stats_plc(self):
        """
        Send STATS command to get timing statistics
        """
        try:
            return self.runtime_socket.send_and_receive("STATS\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to get PLC stats: %s", e)
            return None
        except Exception as e:
            logger.error("Failed to get PLC stats (unexpected): %s", e)
            return None
