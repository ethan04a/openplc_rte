"""
Hot redundancy: after master builds and starts PLC, push the last uploaded ZIP to standby.
Standby receives via /api/redundancy/receive-program (shared secret header).
"""

from __future__ import annotations

import ipaddress
import threading
import time
from pathlib import Path

from flask import jsonify, request

from webserver.logger import get_logger
from webserver.plcapp_management import (
    BuildStatus,
    LAST_UPLOADED_PROGRAM_ZIP,
    apply_program_zip_upload,
    build_state,
)
from webserver.redundancy_role_config import REDUNDANCY_ROLE_FILENAME, write_redundancy_role_functional_cidrs
from webserver.restapi import restapi_bp
from webserver.runtimemanager import REDUNDANCY_SYNC_SECRET, RuntimeManager

logger, _ = get_logger("runtime", use_buffer=True)


def register_redundancy_sync_routes(runtime_manager: RuntimeManager) -> None:
    """Add unauthenticated peer sync endpoint (protected by REDUNDANCY_SYNC_SECRET)."""

    @restapi_bp.route("/redundancy/receive-program", methods=["POST"])
    def redundancy_receive_program():
        if request.headers.get("X-OpenPLC-Redundancy-Sync") != REDUNDANCY_SYNC_SECRET:
            return jsonify({"error": "forbidden"}), 403
        if build_state.status == BuildStatus.COMPILING:
            return (
                jsonify(
                    {
                        "UploadFileFail": "Runtime is compiling",
                        "CompilationStatus": build_state.status.name,
                    }
                ),
                409,
            )
        if "file" not in request.files:
            build_state.status = BuildStatus.FAILED
            return (
                jsonify(
                    {
                        "UploadFileFail": "No file part in the request",
                        "CompilationStatus": build_state.status.name,
                    }
                ),
                400,
            )

        upload = request.files["file"]
        zip_bytes = upload.read()
        build_state.clear()
        result = apply_program_zip_upload(runtime_manager, zip_bytes)
        if result.get("UploadFileFail"):
            return jsonify(result), 400
        return jsonify(result), 200

    @restapi_bp.route("/redundancy/sync-role-ini", methods=["POST"])
    def redundancy_sync_role_ini():
        if request.headers.get("X-OpenPLC-Redundancy-Sync") != REDUNDANCY_SYNC_SECRET:
            return jsonify({"error": "forbidden"}), 403
        data = request.get_json(silent=True) or {}
        line3 = str(data.get("line3", "")).strip()
        line4 = str(data.get("line4", "")).strip()
        try:
            ipaddress.IPv4Interface(line3)
            ipaddress.IPv4Interface(line4)
        except ValueError:
            return jsonify({"error": "line3 and line4 must be IPv4/prefix"}), 400
        role_json_path = RuntimeManager._openplc_project_root() / REDUNDANCY_ROLE_FILENAME
        try:
            write_redundancy_role_functional_cidrs(role_json_path, line3, line4)
        except OSError as e:
            return jsonify({"error": str(e)}), 500
        logger.info(
            "[热冗余] 已接收主机同步的 %s 中 permanent_master_functional_*: %s, %s",
            REDUNDANCY_ROLE_FILENAME,
            line3,
            line4,
        )
        return jsonify({"ok": True}), 200


def _wait_for_running(runtime_manager: RuntimeManager, timeout_sec: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        try:
            st = runtime_manager.status_plc()
        except Exception:
            st = None
        if st and "RUNNING" in st:
            return True
        time.sleep(0.25)
    return False


def push_program_zip_to_standby(standby_ip: str, zip_path: Path, secret: str) -> None:
    import urllib3
    import requests

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    url = f"https://{standby_ip}:8443/api/redundancy/receive-program"
    with zip_path.open("rb") as fp:
        resp = requests.post(
            url,
            headers={"X-OpenPLC-Redundancy-Sync": secret},
            files={"file": ("program.zip", fp, "application/zip")},
            verify=False,
            timeout=180,
        )
    if resp.status_code >= 400:
        logger.error(
            "[热冗余] 向备机推送程序失败: HTTP %s %s",
            resp.status_code,
            resp.text[:500],
        )
        return
    logger.info("[热冗余] 已向备机 %s 推送程序并开始其编译流程（HTTP %s）", standby_ip, resp.status_code)


def push_role_ini_functional_to_standby(standby_ip: str, line3: str, line4: str, secret: str) -> bool:
    import urllib3
    import requests

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    url = f"https://{standby_ip}:8443/api/redundancy/sync-role-ini"
    resp = requests.post(
        url,
        headers={"X-OpenPLC-Redundancy-Sync": secret, "Content-Type": "application/json"},
        json={"line3": line3, "line4": line4},
        verify=False,
        timeout=60,
    )
    if resp.status_code >= 400:
        logger.error(
            "[热冗余] 同步 %s 中 permanent_master_functional_* 至备机失败: HTTP %s %s",
            REDUNDANCY_ROLE_FILENAME,
            resp.status_code,
            resp.text[:500],
        )
        return False
    logger.info(
        "[热冗余] 已向备机 %s 同步 %s 中 permanent_master_functional_*（HTTP %s）",
        standby_ip,
        REDUNDANCY_ROLE_FILENAME,
        resp.status_code,
    )
    return True


def schedule_master_to_standby_sync(runtime_manager: RuntimeManager) -> None:
    """If this node is redundancy master, push last PLC ZIP to standby after local RUNNING."""
    if not (
        runtime_manager.is_redundancy
        and runtime_manager.is_master
        and runtime_manager._redundancy_standby_ip
    ):
        return

    standby_ip = runtime_manager._redundancy_standby_ip

    def worker() -> None:
        if not _wait_for_running(runtime_manager):
            logger.warning("[热冗余] 等待本机 PLC RUNNING 超时，仍尝试向备机推送程序")
        if not LAST_UPLOADED_PROGRAM_ZIP.is_file():
            logger.error("[热冗余] 找不到 %s，无法同步到备机", LAST_UPLOADED_PROGRAM_ZIP)
            return
        try:
            push_program_zip_to_standby(
                standby_ip, LAST_UPLOADED_PROGRAM_ZIP, REDUNDANCY_SYNC_SECRET
            )
        except Exception as e:
            logger.error("[热冗余] 向备机推送程序异常: %s", e)

    threading.Thread(target=worker, daemon=True, name="redundancy-push-zip").start()
