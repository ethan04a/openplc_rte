import sys

# Parse --print-debug argument before any logger imports
# This must happen first so LoggerConfig.print_debug is set before loggers are created
_print_debug = "--print-debug" in sys.argv

from webserver.logger.config import LoggerConfig

LoggerConfig.print_debug = _print_debug

import errno
import ipaddress
import json
import os
import platform
import ssl
import subprocess
import threading
from pathlib import Path
from typing import Callable, Final, Optional

import flask
import flask_login

from webserver.credentials import CertGen
from webserver.debug_websocket import init_debug_websocket
from webserver.logger import get_logger
from webserver.plcapp_management import (
    MAX_FILE_SIZE,
    BuildStatus,
    apply_program_zip_upload,
    build_state,
)
from webserver.restapi import (
    app_restapi,
    db,
    register_callback_get,
    register_callback_post,
    restapi_bp,
)
from webserver.runtimemanager import RuntimeManager

logger, _ = get_logger("logger", use_buffer=True)

app = flask.Flask(__name__)
app.secret_key = str(os.urandom(16))
login_manager = flask_login.LoginManager()
login_manager.init_app(app)

runtime_manager = RuntimeManager(
    runtime_path="./build/plc_main",
    plc_socket="/run/runtime/plc_runtime.socket",
    log_socket="/run/runtime/log_runtime.socket",
    print_debug=_print_debug,
)

runtime_manager.start()

BASE_DIR: Final[Path] = Path(__file__).parent
CERT_FILE: Final[Path] = (BASE_DIR / "certOPENPLC.pem").resolve()
KEY_FILE: Final[Path] = (BASE_DIR / "keyOPENPLC.pem").resolve()
HOSTNAME: Final[str] = "localhost"

FUNCTIONAL_IP_INI: Final[Path] = (BASE_DIR.parent / "functional_ip.ini").resolve()


def _apply_ip_to_interface(interface: str, cidr: str) -> bool:
    """
    使用 ip 命令将 CIDR 地址配置到指定网卡（先 flush 再 add，最后 up）。
    返回是否成功。
    """
    flush = subprocess.run(
        ["ip", "addr", "flush", "dev", interface],
        capture_output=True,
        text=True,
    )
    if flush.returncode != 0:
        logger.warning(
            "清除网卡 %s 地址失败（返回码 %s）：%s",
            interface,
            flush.returncode,
            (flush.stderr or flush.stdout or "").strip(),
        )

    add = subprocess.run(
        ["ip", "addr", "add", cidr, "dev", interface],
        capture_output=True,
        text=True,
    )
    if add.returncode != 0:
        logger.error(
            "为网卡 %s 设置地址 %s 失败（返回码 %s）：%s",
            interface,
            cidr,
            add.returncode,
            (add.stderr or add.stdout or "").strip(),
        )
        return False

    up = subprocess.run(
        ["ip", "link", "set", interface, "up"],
        capture_output=True,
        text=True,
    )
    if up.returncode != 0:
        logger.warning(
            "启用网卡 %s 失败（返回码 %s）：%s",
            interface,
            up.returncode,
            (up.stderr or up.stdout or "").strip(),
        )
    return True


def configure_network_from_functional_ip() -> None:
    """
    读取 functional_ip.ini 前两行：第一行配置 ens33，第二行配置 ens34。
    每行格式为「IP/前缀长度」（CIDR）。文件不存在或内容为空时不做任何网卡设置。
    """
    if platform.system() != "Linux":
        logger.info("当前系统不是 Linux，跳过根据 functional_ip.ini 配置网卡。")
        return

    if not FUNCTIONAL_IP_INI.is_file():
        logger.info("未找到 functional_ip.ini，跳过网卡配置。")
        return

    try:
        content = FUNCTIONAL_IP_INI.read_text(encoding="utf-8")
    except OSError as exc:
        logger.error("读取 functional_ip.ini 失败：%s", exc)
        return

    if not content.strip():
        logger.info("functional_ip.ini 为空，跳过网卡配置。")
        return

    raw_lines = content.splitlines()
    line1 = raw_lines[0].strip() if len(raw_lines) > 0 else ""
    line2 = raw_lines[1].strip() if len(raw_lines) > 1 else ""

    pairs: list[tuple[str, str]] = []
    if line1:
        pairs.append(("ens33", line1))
    if line2:
        pairs.append(("ens34", line2))

    if not pairs:
        logger.info("functional_ip.ini 前两行均无有效内容，跳过网卡配置。")
        return

    for interface, line in pairs:
        try:
            ipaddress.ip_interface(line)
        except ValueError:
            logger.error("网卡 %s 对应行格式无效（需为 IP/前缀，例如 192.168.1.1/24）：%s", interface, line)
            continue

        logger.info("正在为网卡 %s 配置地址 %s …", interface, line)
        if _apply_ip_to_interface(interface, line):
            logger.info("网卡 %s 已设置为 %s。", interface, line)


def handle_start_plc(data: dict) -> dict:
    response = runtime_manager.start_plc()
    return {"status": response}


def handle_stop_plc(data: dict) -> dict:
    response = runtime_manager.stop_plc()
    return {"status": response}


def handle_runtime_logs(data: dict) -> dict:
    if "id" in data:
        min_id = int(data["id"])
    else:
        min_id = None
    if "level" in data:
        level = data["level"]
    else:
        level = None
    response = runtime_manager.get_logs(min_id=min_id, level=level)
    return {"runtime-logs": response}


def handle_compilation_status(data: dict) -> dict:
    return {
        "status": build_state.status.name,
        "logs": build_state.logs[:],  # all lines
        "exit_code": build_state.exit_code,
    }


def parse_timing_stats(stats_response: Optional[str]) -> Optional[dict]:
    """
    Parse the STATS response from the runtime.
    Expected format: STATS:{json_object}
    Returns the parsed JSON object or None if parsing fails.
    """
    if stats_response is None:
        return None

    # Remove the STATS: prefix
    if stats_response.startswith("STATS:"):
        json_str = stats_response[6:].strip()
    else:
        return None

    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def handle_status(data: dict) -> dict:
    response = runtime_manager.status_plc()
    if response is None:
        return {"status": "No response from runtime"}

    result: dict = {"status": response}

    # Only fetch timing stats if explicitly requested via include_stats parameter.
    # This avoids acquiring the stats mutex on every status poll, which could
    # introduce latency to the critical PLC scan cycle.
    include_stats = data.get("include_stats", "").lower() == "true"
    if include_stats:
        stats_response = runtime_manager.stats_plc()
        timing_stats = parse_timing_stats(stats_response)
        if timing_stats is not None:
            result["timing_stats"] = timing_stats

    return result


def handle_ping(data: dict) -> dict:
    response = runtime_manager.ping()
    return {"status": response}


def handle_list_serial_ports(data: dict) -> dict:
    """
    List available serial ports on the system.

    Returns:
        {
            "ports": [
                {"device": "/dev/ttyUSB0", "description": "USB-Serial Controller"},
                {"device": "/dev/ttyACM0", "description": "Arduino Uno"},
                ...
            ]
        }
    """
    try:
        import serial.tools.list_ports

        ports = serial.tools.list_ports.comports()
        port_list = [
            {
                "device": port.device,
                "description": port.description or port.device,
            }
            for port in ports
        ]
        return {"ports": port_list}
    except ImportError:
        return {"error": "pyserial not installed", "ports": []}
    except Exception as e:
        return {"error": str(e), "ports": []}


GET_HANDLERS: dict[str, Callable[[dict], dict]] = {
    "start-plc": handle_start_plc,
    "stop-plc": handle_stop_plc,
    "runtime-logs": handle_runtime_logs,
    "compilation-status": handle_compilation_status,
    "status": handle_status,
    "ping": handle_ping,
    "serial-ports": handle_list_serial_ports,
}


def restapi_callback_get(argument: str, data: dict) -> dict:
    """
    Dispatch GET callbacks by argument.
    """
    # logger.debug("GET | Received argument: %s, data: %s", argument, data)
    handler = GET_HANDLERS.get(argument)
    if handler:
        return handler(data)
    return {"error": "Unknown argument"}


def handle_upload_file(data: dict) -> dict:
    if build_state.status == BuildStatus.COMPILING:
        return {
            "UploadFileFail": "Runtime is compiling another program, please wait",
            "CompilationStatus": build_state.status.name,
        }

    build_state.clear()  # remove all previous build logs

    if "file" not in flask.request.files:
        build_state.status = BuildStatus.FAILED
        return {
            "UploadFileFail": "No file part in the request",
            "CompilationStatus": build_state.status.name,
        }

    zip_file = flask.request.files["file"]
    zip_bytes = zip_file.read()
    clen = getattr(zip_file, "content_length", None)
    if (clen is not None and clen > MAX_FILE_SIZE) or len(zip_bytes) > MAX_FILE_SIZE:
        build_state.status = BuildStatus.FAILED
        return {
            "UploadFileFail": "File is too large",
            "CompilationStatus": build_state.status.name,
        }

    return apply_program_zip_upload(runtime_manager, zip_bytes)


POST_HANDLERS: dict[str, Callable[[dict], dict]] = {
    "upload-file": handle_upload_file,
}


def restapi_callback_post(argument: str, data: dict) -> dict:
    """
    Dispatch POST callbacks by argument.
    """
    # logger.debug("POST | Received argument: %s, data: %s", argument, data)
    handler = POST_HANDLERS.get(argument)

    if not handler:
        return {"PostRequestError": "Unknown argument"}

    return handler(data)


def run_https():
    # rest api register
    from webserver.redundancy_program_sync import register_redundancy_sync_routes

    register_redundancy_sync_routes(runtime_manager)
    app_restapi.register_blueprint(restapi_bp, url_prefix="/api")
    register_callback_get(restapi_callback_get)
    register_callback_post(restapi_callback_post)

    socketio = init_debug_websocket(app_restapi, runtime_manager.runtime_socket)

    with app_restapi.app_context():
        try:
            db.create_all()
            db.session.commit()
            # logger.info("Database tables created successfully.")
        except Exception:
            # logger.error("Error creating database tables: %s", e)
            pass

    # On non-Linux platforms (MSYS2/Cygwin), patch Python SSL recv socket
    # to handle EAGAIN/EWOULDBLOCK errors that cause "Resource temporarily unavailable"
    is_linux = platform.system() == "Linux"
    if not is_linux:
        logger.info(f"Non-Linux platform detected ({platform.system()}). Patching recv socket...")
        _orig_recv = ssl.SSLSocket.recv

        def _patched_recv(self, buflen, flags=0):
            try:
                return _orig_recv(self, buflen, flags)
            except BlockingIOError as e:
                # Only swallow EAGAIN / EWOULDBLOCK (errno 11) - re-raise other errors
                if getattr(e, "errno", None) in (errno.EAGAIN, errno.EWOULDBLOCK, 11):
                    return b""
                raise

        ssl.SSLSocket.recv = _patched_recv

    try:
        cert_gen = CertGen(hostname=HOSTNAME, ip_addresses=["127.0.0.1"])

        # Check if certificate exists. If not, generate one
        if not os.path.exists(CERT_FILE) or not os.path.exists(KEY_FILE):
            # logger.info("Generating https certificate...")
            logger.info("Generating https certificate...")
            cert_gen.generate_self_signed_cert(cert_file=CERT_FILE, key_file=KEY_FILE)
        else:
            logger.warning("Credentials already generated!")

        context = (CERT_FILE, KEY_FILE)
        socketio.run(
            app_restapi,
            debug=False,
            host="0.0.0.0",
            port=8443,
            ssl_context=context,
            use_reloader=False,
            log_output=False,
            allow_unsafe_werkzeug=True,
        )

    except FileNotFoundError:
        # logger.error("Could not find SSL credentials! %s", e)
        pass
    except ssl.SSLError:
        # logger.error("SSL credentials FAIL! %s", e)
        pass
    except KeyboardInterrupt:
        # logger.info("HTTP server stopped by KeyboardInterrupt")
        pass
    finally:
        logger.info("Runtime manager stopped")
        runtime_manager.stop()


if __name__ == "__main__":
    configure_network_from_functional_ip()
    run_https()
