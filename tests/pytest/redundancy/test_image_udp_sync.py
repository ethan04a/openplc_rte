"""Unit tests for hot-redundancy UDP I/O image snapshot sync (phase 1)."""

from __future__ import annotations

from webserver.unixclient import (
    IMAGE_SNAPSHOT_EXPECTED_BYTES,
    IMAGE_SNAPSHOT_PROTOCOL_VERSION,
    _parse_image_snapshot_hdr_line,
)

from webserver.runtimemanager import (
    REDUNDANCY_IMAGE_PHASE_SCAN_END,
    REDUNDANCY_IMAGE_UDP_ACK_HEADER,
    REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER,
    REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR,
    REDUNDANCY_IMAGE_ACK_STATUS_FRAME_INCOMPLETE,
    REDUNDANCY_IMAGE_ACK_STATUS_OK,
    REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ,
    REDUNDANCY_IMAGE_SYNC_PORT,
    REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2,
    REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX,
    REDUNDANCY_IMAGE_UDP_FRAME_TIMEOUT_SEC,
    REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2,
    RedundancyImageFrameMetadata,
    RedundancyImageUdpAck,
    RedundancyImageUdpFragment,
    RedundancyImageUdpMasterStats,
    RedundancyImageUdpStandbyStats,
    _RedundancyImageUdpFrameAssembler,
    _iter_redundancy_image_udp_fragments,
    _pack_redundancy_image_udp_ack,
    _parse_redundancy_image_udp_ack,
    _parse_redundancy_image_udp_fragment,
    _process_redundancy_image_master_ack,
    _record_redundancy_image_master_ack_latency,
    _redundancy_image_crc32,
    _redundancy_image_fragment_count,
)


def _sample_snapshot_payload() -> bytes:
    return bytes(i % 256 for i in range(IMAGE_SNAPSHOT_EXPECTED_BYTES))


def _fragments_from_payload(session_id: int, frame_seq: int, payload: bytes):
    packets = _iter_redundancy_image_udp_fragments(session_id, frame_seq, payload)
    fragments = []
    for packet in packets:
        fragment, status = _parse_redundancy_image_udp_fragment(packet)
        assert status == REDUNDANCY_IMAGE_ACK_STATUS_OK
        assert fragment is not None
        fragments.append(fragment)
    return fragments


def test_parse_extended_image_snapshot_hdr():
    line = f"IMAGE_SNAPSHOT_HDR:1:{IMAGE_SNAPSHOT_EXPECTED_BYTES}:42:100:1:999\n"
    parsed = _parse_image_snapshot_hdr_line(line.strip())
    assert parsed is not None
    ver, length, meta = parsed
    assert ver == 1
    assert length == IMAGE_SNAPSHOT_EXPECTED_BYTES
    assert meta is not None
    assert meta.scan_counter == 42
    assert meta.tick == 100
    assert meta.phase == REDUNDANCY_IMAGE_PHASE_SCAN_END
    assert meta.timestamp_ns == 999


def test_parse_legacy_image_snapshot_hdr():
    line = f"IMAGE_SNAPSHOT_HDR:1:{IMAGE_SNAPSHOT_EXPECTED_BYTES}\n"
    parsed = _parse_image_snapshot_hdr_line(line.strip())
    assert parsed is not None
    ver, length, meta = parsed
    assert meta is None
    assert length == IMAGE_SNAPSHOT_EXPECTED_BYTES


def test_fragment_count_for_full_snapshot():
    assert _redundancy_image_fragment_count(IMAGE_SNAPSHOT_EXPECTED_BYTES) == 59


def test_udp_fragments_cover_snapshot_without_gaps():
    payload = _sample_snapshot_payload()
    packets = _iter_redundancy_image_udp_fragments(100, 1, payload)
    assert len(packets) == 59
    reassembled = bytearray()
    expected_crc = _redundancy_image_crc32(payload)
    for packet in packets:
        fragment, status = _parse_redundancy_image_udp_fragment(packet)
        assert status == REDUNDANCY_IMAGE_ACK_STATUS_OK
        assert fragment is not None
        assert fragment.payload_crc32 == expected_crc
        assert fragment.total_len == IMAGE_SNAPSHOT_EXPECTED_BYTES
        reassembled[fragment.fragment_offset : fragment.fragment_offset + len(fragment.payload)] = (
            fragment.payload
        )
        if fragment.fragment_index + 1 < fragment.fragment_count:
            assert len(fragment.payload) == REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
    assert bytes(reassembled) == payload


def test_assembler_reassembles_full_frame():
    payload = _sample_snapshot_payload()
    stats = RedundancyImageUdpStandbyStats()
    assembler = _RedundancyImageUdpFrameAssembler(stats=stats)
    addr = ("192.168.200.10", 57577)
    now = 1000.0
    result_payload = None
    final_status = None
    for fragment in _fragments_from_payload(1, 1, payload):
        final_status, _, result_payload = assembler.add_fragment(fragment, addr, now)
    assert final_status == REDUNDANCY_IMAGE_ACK_STATUS_OK
    assert result_payload == payload
    assert stats.frame_complete_count == 1


def test_old_session_fragments_are_silent():
    payload = _sample_snapshot_payload()
    stats = RedundancyImageUdpStandbyStats()
    assembler = _RedundancyImageUdpFrameAssembler(stats=stats)
    assembler.active_session_id = 10
    fragment = _fragments_from_payload(9, 1, payload)[0]
    status, _, body = assembler.add_fragment(fragment, ("10.0.0.1", 1), 0.0)
    assert status is None
    assert body is None
    assert stats.old_session_count == 1


def test_old_frame_seq_returns_old_seq_ack():
    payload = _sample_snapshot_payload()
    assembler = _RedundancyImageUdpFrameAssembler()
    assembler.active_session_id = 1
    assembler.last_applied_seq = 5
    fragment = _fragments_from_payload(1, 5, payload)[0]
    status, frame_seq, body = assembler.add_fragment(fragment, ("10.0.0.1", 1), 0.0)
    assert status == REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ
    assert frame_seq == 5
    assert body is None


def test_crc_mismatch_returns_crc_error():
    payload = _sample_snapshot_payload()
    assembler = _RedundancyImageUdpFrameAssembler(
        stats=RedundancyImageUdpStandbyStats()
    )
    fragments = _fragments_from_payload(1, 1, payload)
    fragments[-1] = RedundancyImageUdpFragment(
        session_id=fragments[-1].session_id,
        frame_seq=fragments[-1].frame_seq,
        fragment_index=fragments[-1].fragment_index,
        fragment_count=fragments[-1].fragment_count,
        fragment_offset=fragments[-1].fragment_offset,
        total_len=fragments[-1].total_len,
        payload_crc32=fragments[-1].payload_crc32 ^ 0xFFFFFFFF,
        payload=fragments[-1].payload,
    )
    addr = ("10.0.0.1", 1)
    now = 0.0
    for fragment in fragments[:-1]:
        assert assembler.add_fragment(fragment, addr, now)[0] is None
    status, _, body = assembler.add_fragment(fragments[-1], addr, now)
    assert status == REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR
    assert body is None
    assert assembler.stats.crc_error_count == 1


def test_pending_frame_expires_as_incomplete():
    payload = _sample_snapshot_payload()
    stats = RedundancyImageUdpStandbyStats()
    assembler = _RedundancyImageUdpFrameAssembler(stats=stats)
    fragment = _fragments_from_payload(1, 1, payload)[0]
    assert assembler.add_fragment(fragment, ("10.0.0.1", 1), 0.0)[0] is None
    expired = assembler.expire_pending(REDUNDANCY_IMAGE_UDP_FRAME_TIMEOUT_SEC + 1.0)
    assert expired is not None
    assert expired[3] == REDUNDANCY_IMAGE_ACK_STATUS_FRAME_INCOMPLETE
    assert stats.frame_incomplete_count == 1


def test_new_frame_supersedes_incomplete_pending():
    payload = _sample_snapshot_payload()
    stats = RedundancyImageUdpStandbyStats()
    assembler = _RedundancyImageUdpFrameAssembler(stats=stats)
    addr = ("10.0.0.1", 1)
    first = _fragments_from_payload(1, 1, payload)[0]
    assert assembler.add_fragment(first, addr, 0.0)[0] is None
    second = _fragments_from_payload(1, 2, payload)[0]
    assert assembler.add_fragment(second, addr, 0.0)[0] is None
    assert stats.frame_superseded_count == 1


def test_duplicate_fragment_conflict_is_bad_header():
    payload = _sample_snapshot_payload()
    stats = RedundancyImageUdpStandbyStats()
    assembler = _RedundancyImageUdpFrameAssembler(stats=stats)
    addr = ("10.0.0.1", 1)
    fragment = _fragments_from_payload(1, 1, payload)[0]
    assert assembler.add_fragment(fragment, addr, 0.0)[0] is None
    conflict_payload = bytearray(fragment.payload)
    conflict_payload[0] ^= 0xFF
    conflict = RedundancyImageUdpFragment(
        session_id=fragment.session_id,
        frame_seq=fragment.frame_seq,
        fragment_index=fragment.fragment_index,
        fragment_count=fragment.fragment_count,
        fragment_offset=fragment.fragment_offset,
        total_len=fragment.total_len,
        payload_crc32=fragment.payload_crc32,
        payload=bytes(conflict_payload),
    )
    status, _, _ = assembler.add_fragment(conflict, addr, 0.0)
    assert status == REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER
    assert stats.bad_header_count == 1


def test_master_ack_processing_current_frame():
    stats = RedundancyImageUdpMasterStats()
    stats.last_send_frame_seq = 7
    ack = RedundancyImageUdpAck(
        status=REDUNDANCY_IMAGE_ACK_STATUS_OK,
        session_id=42,
        ack_frame_seq=7,
        applied_seq=7,
    )
    assert _process_redundancy_image_master_ack(
        stats, 42, 7, ack, "192.168.200.20", ("192.168.200.20", REDUNDANCY_IMAGE_SYNC_PORT)
    )
    assert stats.ack_ok_count == 1
    assert stats.last_ack_frame_seq == 7
    assert stats.last_applied_seq == 7


def test_master_ack_ignores_stale_frame_seq():
    stats = RedundancyImageUdpMasterStats()
    stats.last_send_frame_seq = 10
    stats.last_ack_frame_seq = 8
    ack = RedundancyImageUdpAck(
        status=REDUNDANCY_IMAGE_ACK_STATUS_OK,
        session_id=1,
        ack_frame_seq=9,
        applied_seq=9,
    )
    assert not _process_redundancy_image_master_ack(
        stats, 1, 10, ack, "192.168.200.20", ("192.168.200.20", REDUNDANCY_IMAGE_SYNC_PORT)
    )
    assert stats.ack_ok_count == 0


def test_udp_ack_v1_still_parsed():
    packet = REDUNDANCY_IMAGE_UDP_ACK_HEADER.pack(
        b"OPAK",
        IMAGE_SNAPSHOT_PROTOCOL_VERSION,
        2,
        REDUNDANCY_IMAGE_ACK_STATUS_OK,
        1,
        5,
        5,
    )
    ack, status = _parse_redundancy_image_udp_ack(packet)
    assert status == REDUNDANCY_IMAGE_ACK_STATUS_OK
    assert ack is not None
    assert ack.protocol_version == IMAGE_SNAPSHOT_PROTOCOL_VERSION
    assert ack.timestamp_ns == 0


def test_udp_ack_v2_roundtrip():
    packet = _pack_redundancy_image_udp_ack(
        REDUNDANCY_IMAGE_ACK_STATUS_OK, 99, 12, 12, timestamp_ns=123456789
    )
    assert len(packet) == REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2.size
    ack, status = _parse_redundancy_image_udp_ack(packet)
    assert status == REDUNDANCY_IMAGE_ACK_STATUS_OK
    assert ack is not None
    assert ack.protocol_version == REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2
    assert ack.timestamp_ns == 123456789
    assert ack.ack_frame_seq == 12


def test_frame_metadata_defaults():
    meta = RedundancyImageFrameMetadata(
        scan_counter=10, tick=20, phase=REDUNDANCY_IMAGE_PHASE_SCAN_END, timestamp_ns=1
    )
    assert meta.scan_counter == 10
    assert meta.tick == 20
    assert meta.phase == REDUNDANCY_IMAGE_PHASE_SCAN_END


def test_master_ack_latency_recording():
    stats = RedundancyImageUdpMasterStats()
    stats.last_send_frame_seq = 3
    ack = RedundancyImageUdpAck(
        status=REDUNDANCY_IMAGE_ACK_STATUS_OK,
        session_id=1,
        ack_frame_seq=3,
        applied_seq=3,
        protocol_version=REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2,
    )
    send_mono = 1000.0
    assert _process_redundancy_image_master_ack(
        stats, 1, 3, ack, "192.168.200.20", ("192.168.200.20", REDUNDANCY_IMAGE_SYNC_PORT)
    )
    _record_redundancy_image_master_ack_latency(stats, send_mono, ack)
    assert stats.last_ack_latency_ms >= 0.0
    assert stats.last_ack_status_name == "OK"


def test_master_ack_rejects_wrong_source_port():
    stats = RedundancyImageUdpMasterStats()
    ack = RedundancyImageUdpAck(
        status=REDUNDANCY_IMAGE_ACK_STATUS_OK,
        session_id=1,
        ack_frame_seq=1,
        applied_seq=1,
    )
    assert not _process_redundancy_image_master_ack(
        stats, 1, 1, ack, "192.168.200.20", ("192.168.200.20", 57577)
    )
