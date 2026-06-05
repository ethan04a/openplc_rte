import platform
import socket
import threading
import time
from datetime import datetime, timezone

from flask import jsonify, request
from flask_jwt_extended import jwt_required

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    psutil = None
    HAS_PSUTIL = False

from webserver.logger import get_logger
from webserver.restapi import restapi_bp

logger, _ = get_logger("logger", use_buffer=True)

NODE_INFO_CACHE_TTL_SEC = 1.0
VALID_INCLUDE_SECTIONS = {"system", "network"}

_node_info_cache_lock = threading.Lock()
_node_info_cache_ts = 0.0
_node_info_cache_payload: dict = {}


def _now_iso8601_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_include_param(raw_include: str | None) -> tuple[set[str], set[str]]:
    if raw_include is None or raw_include.strip() == "":
        return set(VALID_INCLUDE_SECTIONS), set()

    requested = {section.strip().lower() for section in raw_include.split(",") if section.strip()}
    invalid = requested - VALID_INCLUDE_SECTIONS
    if not requested:
        return set(VALID_INCLUDE_SECTIONS), invalid
    return requested, invalid


def _read_os_name() -> str:
    try:
        with open("/etc/os-release", "r", encoding="utf-8") as fp:
            fields: dict[str, str] = {}
            for line in fp:
                if "=" not in line:
                    continue
                key, value = line.strip().split("=", 1)
                fields[key] = value.strip('"')
    except OSError:
        return platform.platform()

    if fields.get("PRETTY_NAME"):
        return fields["PRETTY_NAME"]

    os_id = fields.get("ID", "linux").capitalize()
    version_id = fields.get("VERSION_ID", "").strip()
    return f"{os_id} {version_id}".strip()


def _collect_system_info() -> dict:
    system_info = {
        "os": _read_os_name(),
        "kernel": platform.release(),
        "cpu_usage_percent": 0.0,
        "ram_usage_percent": 0.0,
    }

    if not HAS_PSUTIL or psutil is None:
        return system_info

    cpu_usage = psutil.cpu_percent(interval=0.0)
    memory = psutil.virtual_memory()

    system_info["cpu_usage_percent"] = round(float(cpu_usage), 1)
    system_info["ram_usage_percent"] = round(float(memory.percent), 1)
    system_info["ram_total_mb"] = int(memory.total / (1024 * 1024))
    system_info["ram_used_mb"] = int(memory.used / (1024 * 1024))
    return system_info


def _normalize_mac(mac: str | None) -> str:
    if not mac:
        return "00:00:00:00:00:00"
    return mac.lower()


def _collect_interfaces_with_psutil() -> list[dict]:
    interface_stats = psutil.net_if_stats()
    interfaces = []

    for ifname, addrs in psutil.net_if_addrs().items():
        ipv4 = None
        mac = "00:00:00:00:00:00"

        for addr in addrs:
            if addr.family == socket.AF_INET and addr.address:
                ipv4 = ipv4 or addr.address
                continue

            if addr.family == psutil.AF_LINK and addr.address:
                mac = _normalize_mac(addr.address)

        state = "up" if interface_stats.get(ifname) and interface_stats[ifname].isup else "down"
        interfaces.append(
            {
                "interface": ifname,
                "ip": ipv4,
                "mac": mac,
                "state": state,
            }
        )

    interfaces.sort(key=lambda item: item["interface"])
    return interfaces


def _collect_network_info() -> dict:
    interfaces = []

    if HAS_PSUTIL and psutil is not None:
        interfaces = _collect_interfaces_with_psutil()

    return {"interfaces": interfaces}


def _collect_node_info_payload() -> dict:
    return {
        "system": _collect_system_info(),
        "network": _collect_network_info(),
        "timestamp": _now_iso8601_utc(),
    }


def _get_cached_node_info_payload() -> dict:
    global _node_info_cache_ts, _node_info_cache_payload

    now = time.monotonic()
    with _node_info_cache_lock:
        if _node_info_cache_payload and (now - _node_info_cache_ts) <= NODE_INFO_CACHE_TTL_SEC:
            return _node_info_cache_payload

        payload = _collect_node_info_payload()
        _node_info_cache_payload = payload
        _node_info_cache_ts = now
        return payload


def register_node_info_routes() -> None:
    @restapi_bp.route("/node-info", methods=["GET"])
    @jwt_required()
    def node_info():
        requested_sections, invalid_sections = _parse_include_param(request.args.get("include"))
        if invalid_sections:
            return (
                jsonify(
                    {
                        "error": "Invalid include sections",
                        "detail": f"Allowed values: {','.join(sorted(VALID_INCLUDE_SECTIONS))}",
                    }
                ),
                400,
            )

        try:
            payload = _get_cached_node_info_payload()
            response_body = {"timestamp": payload["timestamp"]}
            if "system" in requested_sections:
                response_body["system"] = payload["system"]
            if "network" in requested_sections:
                response_body["network"] = payload["network"]
            return jsonify(response_body), 200
        except Exception as exc:
            logger.error("Failed to collect node info: %s", exc)
            return jsonify({"error": "Failed to collect node info", "detail": str(exc)}), 500
