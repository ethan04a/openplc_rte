"""Unit tests for redundancy REST API helpers on RuntimeManager."""

import threading
import time
from pathlib import Path

from webserver.redundancy_identity import (
    BASIS_COMPETITION,
    BASIS_JOIN_INCUMBENT,
    BASIS_TAKEOVER_FACT,
    REDUNDANCY_IDENTITY_STATE_FILENAME,
    IdentityOutcome,
    IdentityRecord,
    RedundancyIdentityAgent,
    save_identity_record,
)
from webserver.runtimemanager import (
    RedundancyImageUdpMasterStats,
    RedundancyImageUdpStandbyStats,
    RuntimeManager,
)


def _bare_runtime_manager() -> RuntimeManager:
    rm = RuntimeManager.__new__(RuntimeManager)
    rm._heartbeat_status_lock = threading.Lock()
    rm._heartbeat_tcp_state = "disabled"
    rm._heartbeat_local_port = None
    rm._heartbeat_peer_port = None
    rm._heartbeat_last_activity_monotonic = 0.0
    rm._heartbeat_lost_times = 0
    rm._image_udp_stats_lock = threading.Lock()
    rm._image_udp_master_stats = RedundancyImageUdpMasterStats()
    rm._image_udp_standby_stats = RedundancyImageUdpStandbyStats()
    rm._plc_image_data_plane_active = False
    rm._redundancy_local_heartbeat_ip = None
    rm._redundancy_master_ip = None
    rm._redundancy_standby_ip = None
    rm._promoted_standby_acting_master = False
    rm._takeover_in_progress = False
    rm.is_redundancy = False
    rm.is_master = False
    rm._plc_shadow_standby = False
    return rm


def test_format_plc_status_shadow_standby():
    rm = _bare_runtime_manager()
    rm.is_redundancy = True
    rm.is_master = False
    rm._plc_shadow_standby = True

    assert rm.format_plc_status_for_api("STATUS:RUNNING") == "STATUS:RUNNING shadow"
    assert rm.format_plc_status_for_api("STATUS:STOPPED") == "STATUS:STOPPED"
    assert rm.format_plc_status_for_api(None) == "No response from runtime"


def test_format_plc_status_master_unchanged():
    rm = _bare_runtime_manager()
    rm.is_redundancy = True
    rm.is_master = True

    assert rm.format_plc_status_for_api("STATUS:RUNNING") == "STATUS:RUNNING"


def test_get_redundancy_heartbeat_status_disabled():
    rm = _bare_runtime_manager()
    payload = rm.get_redundancy_heartbeat_status()
    assert payload["enabled"] is False
    assert payload["role"] == "none"
    assert payload["connection_state"] == "disabled"


def test_peer_heartbeat_ip_is_the_other_address():
    rm = _bare_runtime_manager()
    rm._redundancy_master_ip = "192.168.100.10"
    rm._redundancy_standby_ip = "192.168.100.20"

    rm._redundancy_local_heartbeat_ip = "192.168.100.10"
    rm.is_master = False
    assert rm._redundancy_peer_heartbeat_ip() == "192.168.100.20"

    rm._redundancy_local_heartbeat_ip = "192.168.100.20"
    rm.is_master = True
    assert rm._redundancy_peer_heartbeat_ip() == "192.168.100.10"

    rm._redundancy_local_heartbeat_ip = "192.168.100.30"
    assert rm._redundancy_peer_heartbeat_ip() is None
    rm._redundancy_local_heartbeat_ip = None
    assert rm._redundancy_peer_heartbeat_ip() is None


def test_heartbeat_status_peer_ip_follows_local_heartbeat_address():
    rm = _bare_runtime_manager()
    rm.is_redundancy = True
    rm.is_master = True
    rm._redundancy_local_heartbeat_ip = "192.168.100.10"
    rm._redundancy_master_ip = "192.168.100.10"
    rm._redundancy_standby_ip = "192.168.100.20"

    payload = rm.get_redundancy_heartbeat_status()
    assert payload["role"] == "master"
    assert payload["local_ip"] == "192.168.100.10"
    assert payload["peer_ip"] == "192.168.100.20"

    rm.is_master = False
    rm._redundancy_local_heartbeat_ip = "192.168.100.20"
    payload = rm.get_redundancy_heartbeat_status()
    assert payload["role"] == "standby"
    assert payload["peer_ip"] == "192.168.100.10"


def test_image_udp_status_peer_ip_follows_local_heartbeat_address():
    rm = _bare_runtime_manager()
    rm.is_redundancy = True
    rm.is_master = True
    rm._redundancy_local_heartbeat_ip = "192.168.100.10"
    rm._redundancy_master_ip = "192.168.100.10"
    rm._redundancy_standby_ip = "192.168.100.20"

    payload = rm.get_redundancy_image_udp_status()
    assert payload["peer_ip"] == "192.168.100.20"


def test_apply_confirmed_identity_sets_existing_role_flags() -> None:
    rm = _bare_runtime_manager()
    master = IdentityOutcome(
        status="confirmed",
        role="A",
        session_id="s",
        basis=BASIS_COMPETITION,
        local_committed=True,
        peer_confirmed=True,
        master_ip="192.168.100.10",
        reason=None,
    )
    assert rm._apply_confirmed_identity(master) is True
    assert rm.is_master is True
    assert rm._plc_shadow_standby is False

    standby = IdentityOutcome(
        status="confirmed",
        role="B",
        session_id="s",
        basis=BASIS_JOIN_INCUMBENT,
        local_committed=True,
        peer_confirmed=True,
        master_ip="192.168.100.10",
        reason=None,
    )
    assert rm._apply_confirmed_identity(standby) is True
    assert rm.is_master is False
    assert rm._plc_shadow_standby is True


def test_conflict_keeps_committed_master_role() -> None:
    rm = _bare_runtime_manager()
    outcome = IdentityOutcome(
        status="conflict",
        role="A",
        session_id="s",
        basis=BASIS_TAKEOVER_FACT,
        local_committed=True,
        peer_confirmed=False,
        master_ip="192.168.100.20",
        reason="双方已提交的主身份矛盾",
    )
    assert rm._apply_confirmed_identity(outcome) is True
    assert rm.is_master is True
    assert rm._plc_shadow_standby is False


def test_cmdline_without_shadow_drives_field_io() -> None:
    assert RuntimeManager._cmdline_drives_field_io(["/usr/bin/plc_main"]) is True
    assert RuntimeManager._cmdline_drives_field_io(["/usr/bin/plc_main", "--print-debug"]) is True
    shadow = ["/usr/bin/plc_main", "--shadow-standby"]
    assert RuntimeManager._cmdline_drives_field_io(shadow) is False
    assert RuntimeManager._cmdline_drives_field_io(None) is False
    assert RuntimeManager._cmdline_drives_field_io([]) is False


def test_field_io_process_is_incumbent_even_if_history_says_standby(tmp_path: Path) -> None:
    rm = _bare_runtime_manager()
    rm._redundancy_local_heartbeat_ip = "192.168.100.20"
    rm._redundancy_master_ip = "192.168.100.10"
    rm._redundancy_standby_ip = "192.168.100.20"
    rm._openplc_project_root = lambda: tmp_path
    rm._running_plc_is_field_master = lambda: True
    save_identity_record(
        tmp_path / REDUNDANCY_IDENTITY_STATE_FILENAME,
        IdentityRecord(
            session_id="old",
            role="B",
            basis=BASIS_COMPETITION,
            local_committed=True,
            peer_confirmed=True,
            local_ip="192.168.100.20",
            peer_ip="192.168.100.10",
            master_ip="192.168.100.10",
        ),
    )
    agent = rm._new_identity_agent()
    assert agent.outcome.local_committed is True
    assert agent.outcome.role == "A"
    assert agent.outcome.master_ip == "192.168.100.20"


def test_saved_master_without_field_io_is_not_incumbent(tmp_path: Path) -> None:
    rm = _bare_runtime_manager()
    rm._redundancy_local_heartbeat_ip = "192.168.100.10"
    rm._redundancy_master_ip = "192.168.100.10"
    rm._redundancy_standby_ip = "192.168.100.20"
    rm._openplc_project_root = lambda: tmp_path
    rm._running_plc_is_field_master = lambda: False
    save_identity_record(
        tmp_path / REDUNDANCY_IDENTITY_STATE_FILENAME,
        IdentityRecord(
            session_id="old",
            role="A",
            basis=BASIS_COMPETITION,
            local_committed=True,
            peer_confirmed=True,
            local_ip="192.168.100.10",
            peer_ip="192.168.100.20",
            master_ip="192.168.100.10",
        ),
    )
    agent = rm._new_identity_agent()
    agent.start(0)
    assert agent.outcome.local_committed is False
    assert isinstance(agent, RedundancyIdentityAgent)


def test_publish_takeover_identity_sets_a_without_election(tmp_path: Path) -> None:
    rm = _bare_runtime_manager()
    rm.is_redundancy = True
    rm.is_master = False
    rm._plc_shadow_standby = True
    agent = RedundancyIdentityAgent(
        "192.168.100.20",
        "192.168.100.10",
        state_path=tmp_path / REDUNDANCY_IDENTITY_STATE_FILENAME,
    )
    restarted: list[str] = []
    rm._identity_exchange = type("Exchange", (), {"agent": agent})()
    rm._start_redundancy_heartbeat_threads = lambda: restarted.append("restart")
    rm._publish_takeover_identity()
    assert rm.is_master is True
    assert rm._plc_shadow_standby is False
    assert agent.outcome.role == "A"
    assert agent.outcome.basis == BASIS_TAKEOVER_FACT
    assert agent.outcome.master_ip == "192.168.100.20"
    assert restarted == ["restart"]


def test_failed_field_switch_does_not_publish_takeover() -> None:
    rm = _bare_runtime_manager()
    rm.is_master = False
    rm._plc_shadow_standby = True
    rm._takeover_in_progress = True
    called: list[str] = []
    rm._publish_takeover_identity = lambda: called.append("publish")
    rm._finish_standby_takeover(False)
    assert called == []
    assert rm.is_master is False
    assert rm._plc_shadow_standby is True
    assert rm._promoted_standby_acting_master is False
    assert rm._takeover_in_progress is True
    rm._finish_standby_takeover(True)
    assert called == ["publish"]


def test_takeover_latch_blocks_a_second_promotion() -> None:
    rm = _bare_runtime_manager()
    rm._takeover_in_progress = True
    started: list[str] = []
    rm._redundancy_trigger_standby_to_master_switch = lambda: started.append("go")
    rm._schedule_async_standby_to_master_switch()
    time.sleep(0.1)
    assert started == []
    assert rm._standby_tick_lost_times(6) == (6, False)


def test_promoted_peer_heartbeat_does_not_fail_back() -> None:
    rm = _bare_runtime_manager()
    rm.is_master = True
    rm._plc_shadow_standby = False
    rm._standby_switched_to_master = True
    rm._promoted_standby_acting_master = True
    rm._on_promoted_peer_heartbeat()
    rm._schedule_async_failback_to_standby()
    assert rm.is_master is True
    assert rm._plc_shadow_standby is False
    assert rm._standby_switched_to_master is True
    assert rm._promoted_standby_acting_master is True


def test_apply_unconfirmed_identity_does_not_assume_master() -> None:
    rm = _bare_runtime_manager()
    outcome = IdentityOutcome(
        status="unconfirmed",
        role=None,
        session_id=None,
        basis=None,
        local_committed=False,
        peer_confirmed=False,
        master_ip=None,
        reason="协商重试耗尽，保持未确认",
    )
    assert rm._apply_confirmed_identity(outcome) is False
    assert rm.is_master is False
    assert rm._plc_shadow_standby is False


def test_start_does_not_launch_plc_when_identity_is_unconfirmed(tmp_path: Path) -> None:
    rm = RuntimeManager(
        "/bin/true",
        str(tmp_path / "plc.sock"),
        str(tmp_path / "log.sock"),
    )

    def evaluate() -> None:
        rm.is_redundancy = True
        rm.is_master = False

    rm._evaluate_redundancy_role = evaluate  # type: ignore[method-assign]
    rm._confirm_redundancy_identity = lambda: False  # type: ignore[method-assign]
    rm.start()
    assert rm.running is False
    assert rm.process is None


def test_get_redundancy_image_udp_status_master_not_sent():
    rm = _bare_runtime_manager()
    rm.is_redundancy = True
    rm.is_master = True
    rm._redundancy_standby_ip = "192.168.200.20"

    payload = rm.get_redundancy_image_udp_status()
    assert payload["role"] == "master"
    assert payload["has_sent_udp"] is False
    assert payload["frame_send_count"] == 0
