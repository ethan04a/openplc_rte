"""Unit tests for redundancy REST API helpers on RuntimeManager."""

import threading

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


def test_get_redundancy_image_udp_status_master_not_sent():
    rm = _bare_runtime_manager()
    rm.is_redundancy = True
    rm.is_master = True
    rm._redundancy_standby_ip = "192.168.200.20"

    payload = rm.get_redundancy_image_udp_status()
    assert payload["role"] == "master"
    assert payload["has_sent_udp"] is False
    assert payload["frame_send_count"] == 0
