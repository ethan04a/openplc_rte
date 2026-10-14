"""Identity confirmation without starting the PLC."""

import json
import socket
from pathlib import Path

from webserver.redundancy_identity import (
    BASIS_COMPETITION,
    BASIS_JOIN_INCUMBENT,
    BASIS_RESTORED,
    BASIS_SOLO_NO_PEER,
    BASIS_TAKEOVER_FACT,
    REDUNDANCY_IDENTITY_PORT,
    REDUNDANCY_IDENTITY_RETRY_LIMIT,
    IdentityMessage,
    IdentityRecord,
    RedundancyIdentityAgent,
    bind_identity_socket,
    load_identity_record,
    pack_identity_message,
    save_identity_record,
    unpack_identity_message,
)

IP_SMALL = "192.168.100.10"
IP_LARGE = "192.168.100.20"


def _agent(
    local_ip: str,
    peer_ip: str,
    path: Path,
    *,
    incumbent: bool = False,
) -> RedundancyIdentityAgent:
    return RedundancyIdentityAgent(
        local_ip,
        peer_ip,
        incumbent=incumbent,
        state_path=path,
        startup_instance=f"boot-{local_ip}",
    )


def _drive(
    left: RedundancyIdentityAgent,
    right: RedundancyIdentityAgent,
    until_ms: int,
    drop=None,
) -> None:
    inbox: dict[int, list[bytes]] = {id(left): [], id(right): []}
    pair = ((left, right), (right, left))
    now = 0
    while now <= until_ms:
        for src, dst in pair:
            for packet in src.poll(now):
                if drop is not None and drop(src, packet):
                    continue
                inbox[id(dst)].append(packet)
        progressed = True
        while progressed:
            progressed = False
            for src, dst in pair:
                queue = inbox[id(src)]
                if not queue:
                    continue
                progressed = True
                packet = queue.pop(0)
                for reply in src.handle(packet, now):
                    if drop is not None and drop(src, reply):
                        continue
                    inbox[id(dst)].append(reply)
        now += 50


def _record(
    local_ip: str,
    peer_ip: str,
    role: str,
    session_id: str,
    master_ip: str,
) -> IdentityRecord:
    return IdentityRecord(
        session_id=session_id,
        role=role,
        basis=BASIS_SOLO_NO_PEER,
        local_committed=True,
        peer_confirmed=False,
        local_ip=local_ip,
        peer_ip=peer_ip,
        master_ip=master_ip,
    )


def test_solo_small_address_becomes_master(tmp_path: Path) -> None:
    agent = _agent(IP_SMALL, IP_LARGE, tmp_path / "state.json")
    agent.start(0)
    assert agent.outcome.local_committed is False
    now = 0
    while now <= 5000:
        agent.poll(now)
        now += 50
    outcome = agent.outcome
    assert outcome.status == "confirmed"
    assert outcome.role == "A"
    assert outcome.basis == BASIS_SOLO_NO_PEER
    assert outcome.local_committed is True
    assert outcome.peer_confirmed is False
    saved = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert saved["role"] == "A"
    assert "proposal_id" not in saved


def test_solo_large_address_becomes_master(tmp_path: Path) -> None:
    agent = _agent(IP_LARGE, IP_SMALL, tmp_path / "state.json")
    agent.start(0)
    now = 0
    while now <= 5000:
        for packet in agent.poll(now):
            agent.handle(packet, now)
        now += 50
    assert agent.outcome.role == "A"
    assert agent.outcome.basis == BASIS_SOLO_NO_PEER
    assert agent.outcome.master_ip == IP_LARGE


def test_unconfirmed_pair_elects_smaller_address(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    _drive(small, large, 0)
    assert small.outcome.status == "confirmed"
    assert large.outcome.status == "confirmed"
    assert small.outcome.role == "A"
    assert large.outcome.role == "B"
    assert small.outcome.basis == BASIS_COMPETITION
    assert large.outcome.basis == BASIS_COMPETITION
    assert small.outcome.session_id == large.outcome.session_id
    assert small.outcome.master_ip == IP_SMALL
    assert small.outcome.peer_confirmed is True
    assert large.outcome.peer_confirmed is True


def test_later_node_joins_incumbent_without_comparing_address(tmp_path: Path) -> None:
    incumbent = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json", incumbent=True)
    joiner = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    assert incumbent.outcome.role == "A"
    assert incumbent.outcome.local_committed is True
    incumbent.start(0)
    joiner.start(0)
    _drive(incumbent, joiner, 0)
    assert incumbent.outcome.role == "A"
    assert incumbent.outcome.master_ip == IP_LARGE
    assert joiner.outcome.role == "B"
    assert joiner.outcome.basis == BASIS_JOIN_INCUMBENT
    assert joiner.outcome.session_id == incumbent.outcome.session_id
    assert incumbent.outcome.peer_confirmed is True


def test_crossed_self_proposals_still_elect_smaller_address(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    # Poll and deliver the larger address first. The smaller address still wins.
    _drive(large, small, 0)
    assert small.outcome.role == "A"
    assert large.outcome.role == "B"
    assert small.outcome.session_id == large.outcome.session_id


def test_lost_accept_retries_and_does_not_solo_commit(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    seen_accepts = {"count": 0}

    def drop(_src: RedundancyIdentityAgent, packet: bytes) -> bool:
        message = unpack_identity_message(packet)
        if message is None or message.msg_type != "accept":
            return False
        seen_accepts["count"] += 1
        return seen_accepts["count"] == 1

    _drive(small, large, 2500, drop=drop)
    assert seen_accepts["count"] >= 2
    assert small.outcome.status == "confirmed"
    assert small.outcome.basis == BASIS_COMPETITION
    assert small.outcome.basis != BASIS_SOLO_NO_PEER
    assert large.outcome.role == "B"
    assert small.outcome.session_id == large.outcome.session_id


def test_accepts_dropped_until_retry_limit_stays_unconfirmed(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)

    def drop(_src: RedundancyIdentityAgent, packet: bytes) -> bool:
        message = unpack_identity_message(packet)
        return message is not None and message.msg_type == "accept"

    _drive(small, large, 2000 * REDUNDANCY_IDENTITY_RETRY_LIMIT, drop=drop)
    assert small.outcome.status == "unconfirmed"
    assert small.outcome.local_committed is False
    assert small.outcome.role is None
    assert small.outcome.reason == "协商重试耗尽，保持未确认"
    assert not (tmp_path / "small.json").exists()
    assert large.outcome.basis != BASIS_SOLO_NO_PEER


def test_lost_commit_is_resent_without_revoking(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    commits = {"count": 0}

    def drop(_src: RedundancyIdentityAgent, packet: bytes) -> bool:
        message = unpack_identity_message(packet)
        if message is None or message.msg_type != "commit":
            return False
        commits["count"] += 1
        return commits["count"] == 1

    _drive(small, large, 200, drop=drop)
    assert commits["count"] >= 2
    assert small.outcome.local_committed is True
    assert small.outcome.role == "A"
    assert large.outcome.role == "B"
    assert small.outcome.session_id == large.outcome.session_id
    assert small.outcome.peer_confirmed is True


def test_stale_query_response_is_ignored(tmp_path: Path) -> None:
    agent = _agent(IP_SMALL, IP_LARGE, tmp_path / "state.json")
    agent.start(0)
    first = unpack_identity_message(agent.poll(0)[0])
    assert first is not None
    second = unpack_identity_message(agent.poll(200)[0])
    assert second is not None
    assert first.request_id != second.request_id
    stale = pack_identity_message(
        IdentityMessage(
            msg_type="query_response",
            startup_instance="peer-boot",
            local_ip=IP_LARGE,
            peer_ip=IP_SMALL,
            request_id=first.request_id,
            has_valid_master=True,
            local_committed=True,
            session_id="old-session",
            role="A",
            basis=BASIS_SOLO_NO_PEER,
            master_ip=IP_LARGE,
        )
    )
    assert agent.handle(stale, 200) == []
    assert agent.outcome.local_committed is False


def test_late_accept_after_revoke_does_not_commit(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    held: list[bytes] = []

    def drop(_src: RedundancyIdentityAgent, packet: bytes) -> bool:
        message = unpack_identity_message(packet)
        if message is None or message.msg_type != "accept":
            return False
        if not held:
            held.append(packet)
        return True

    _drive(small, large, 2000, drop=drop)
    assert held
    assert small.outcome.local_committed is False
    assert small.handle(held[0], 2000) == []
    assert small.outcome.local_committed is False


def test_restore_keeps_larger_address_as_master(tmp_path: Path) -> None:
    session_id = "session-large-master"
    large_path = tmp_path / "large.json"
    small_path = tmp_path / "small.json"
    save_identity_record(large_path, _record(IP_LARGE, IP_SMALL, "A", session_id, IP_LARGE))
    save_identity_record(small_path, _record(IP_SMALL, IP_LARGE, "B", session_id, IP_LARGE))
    large = _agent(IP_LARGE, IP_SMALL, large_path)
    small = _agent(IP_SMALL, IP_LARGE, small_path)
    assert large.outcome.local_committed is False
    assert small.outcome.local_committed is False
    large.start(0)
    small.start(0)
    _drive(large, small, 0)
    assert large.outcome.role == "A"
    assert small.outcome.role == "B"
    assert large.outcome.basis == BASIS_RESTORED
    assert small.outcome.basis == BASIS_RESTORED
    assert large.outcome.session_id == session_id
    assert small.outcome.master_ip == IP_LARGE


def test_contradictory_history_is_conflict(tmp_path: Path) -> None:
    large_path = tmp_path / "large.json"
    small_path = tmp_path / "small.json"
    save_identity_record(large_path, _record(IP_LARGE, IP_SMALL, "A", "session-large", IP_LARGE))
    save_identity_record(small_path, _record(IP_SMALL, IP_LARGE, "A", "session-small", IP_SMALL))
    large = _agent(IP_LARGE, IP_SMALL, large_path)
    small = _agent(IP_SMALL, IP_LARGE, small_path)
    large.start(0)
    small.start(0)
    _drive(large, small, 0)
    assert large.outcome.status == "conflict"
    assert small.outcome.status == "conflict"
    assert large.outcome.local_committed is False
    assert small.outcome.basis != BASIS_COMPETITION


def test_two_solo_masters_report_conflict_and_keep_role(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    now = 0
    while now <= 5000:
        small.poll(now)
        large.poll(now)
        now += 50
    assert small.outcome.role == "A"
    assert large.outcome.role == "A"
    _drive(small, large, 0)
    assert small.outcome.status == "conflict"
    assert large.outcome.status == "conflict"
    assert small.outcome.role == "A"
    assert large.outcome.role == "A"
    assert small.outcome.master_ip == IP_SMALL
    assert large.outcome.master_ip == IP_LARGE


def test_history_is_not_treated_as_current_master_before_discovery(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_identity_record(path, _record(IP_SMALL, IP_LARGE, "A", "old-session", IP_SMALL))
    agent = _agent(IP_SMALL, IP_LARGE, path)
    agent.start(0)
    assert agent.outcome.local_committed is False
    assert agent.outcome.role is None


def test_takeover_fact_records_local_master_without_election(tmp_path: Path) -> None:
    agent = _agent(IP_LARGE, IP_SMALL, tmp_path / "state.json")
    agent.commit_takeover_fact()
    outcome = agent.outcome
    assert outcome.role == "A"
    assert outcome.basis == BASIS_TAKEOVER_FACT
    assert outcome.master_ip == IP_LARGE
    assert load_identity_record(tmp_path / "state.json") is not None


def test_uncommitted_file_is_not_restored(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "session_id": "temp",
                "role": "A",
                "basis": BASIS_COMPETITION,
                "local_committed": False,
                "peer_confirmed": False,
                "local_ip": IP_SMALL,
                "peer_ip": IP_LARGE,
                "master_ip": IP_SMALL,
                "proposal_id": "should-not-load",
            }
        ),
        encoding="utf-8",
    )
    assert load_identity_record(path) is None


def test_identity_datagram_roundtrip_on_udp_port() -> None:
    message = IdentityMessage(
        msg_type="query",
        startup_instance="boot",
        local_ip=IP_SMALL,
        peer_ip=IP_LARGE,
        request_id="req-1",
        confirming=True,
    )
    assert unpack_identity_message(pack_identity_message(message)) == message
    try:
        server = bind_identity_socket("127.0.0.1", REDUNDANCY_IDENTITY_PORT)
    except OSError:
        return
    client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        payload = pack_identity_message(message)
        client.sendto(payload, ("127.0.0.1", REDUNDANCY_IDENTITY_PORT))
        server.settimeout(1.0)
        received, _addr = server.recvfrom(4096)
    finally:
        client.close()
        server.close()
    assert unpack_identity_message(received) == message
