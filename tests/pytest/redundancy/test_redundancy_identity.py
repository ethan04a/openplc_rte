"""Identity confirmation without starting the PLC."""

import json
import socket
from pathlib import Path

from webserver.redundancy_identity import (
    BASIS_COMPETITION,
    IdentityExchange,
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
    _PERSIST_FAILED_REASON,
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


def test_takeover_replaces_standby_and_recovering_master_joins_as_b(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    _drive(small, large, 0)
    assert large.outcome.role == "B"
    old_session = small.outcome.session_id
    large.commit_takeover_fact()
    assert large.outcome.role == "A"
    assert large.outcome.basis == BASIS_TAKEOVER_FACT
    assert large.outcome.master_ip == IP_LARGE
    assert large.outcome.session_id != old_session

    recovered = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    assert recovered.outcome.local_committed is False
    recovered.start(0)
    _drive(large, recovered, 0)
    assert large.outcome.role == "A"
    assert large.outcome.master_ip == IP_LARGE
    assert recovered.outcome.role == "B"
    assert recovered.outcome.basis == BASIS_JOIN_INCUMBENT
    assert recovered.outcome.master_ip == IP_LARGE
    assert recovered.outcome.session_id == large.outcome.session_id


def test_takeover_does_not_demote_a_still_running_original_master(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    _drive(small, large, 0)
    large.commit_takeover_fact()
    _drive(small, large, 0)
    assert small.outcome.status == "conflict"
    assert large.outcome.status == "conflict"
    assert small.outcome.role == "A"
    assert large.outcome.role == "A"
    assert small.outcome.master_ip == IP_SMALL
    assert large.outcome.master_ip == IP_LARGE


def test_saved_standby_stays_standby_when_peer_is_silent(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_identity_record(path, _record(IP_LARGE, IP_SMALL, "B", "old-session", IP_SMALL))
    agent = _agent(IP_LARGE, IP_SMALL, path)
    agent.start(0)
    now = 0
    while now <= 5000:
        agent.poll(now)
        now += 50
    assert agent.outcome.role == "B"
    assert agent.outcome.basis == BASIS_RESTORED
    assert agent.outcome.master_ip == IP_SMALL
    assert agent.outcome.session_id == "old-session"


def test_saved_master_is_restored_when_peer_is_silent(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_identity_record(path, _record(IP_LARGE, IP_SMALL, "A", "old-session", IP_LARGE))
    agent = _agent(IP_LARGE, IP_SMALL, path)
    agent.start(0)
    now = 0
    while now <= 5000:
        agent.poll(now)
        now += 50
    assert agent.outcome.role == "A"
    assert agent.outcome.basis == BASIS_RESTORED
    assert agent.outcome.master_ip == IP_LARGE
    assert agent.outcome.session_id == "old-session"


def _reply_to_confirm_query(
    replies: list[bytes],
    *,
    peer_ip: str,
    local_ip: str,
    session_id: str,
    master_ip: str,
    role: str,
    basis: str,
    startup_instance: str = "peer",
) -> bytes:
    query = None
    for packet in replies:
        message = unpack_identity_message(packet)
        if message is not None and message.msg_type == "query" and message.request_id:
            query = message
            break
    assert query is not None
    return pack_identity_message(
        IdentityMessage(
            msg_type="query_response",
            startup_instance=startup_instance,
            local_ip=peer_ip,
            peer_ip=local_ip,
            request_id=query.request_id,
            has_valid_master=True,
            local_committed=True,
            session_id=session_id,
            role=role,
            basis=basis,
            master_ip=master_ip,
        )
    )


def test_saved_master_joins_peer_during_restore_wait(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_identity_record(path, _record(IP_SMALL, IP_LARGE, "A", "old-session", IP_SMALL))
    agent = _agent(IP_SMALL, IP_LARGE, path)
    agent.start(0)
    agent.poll(3000)
    assert agent.outcome.local_committed is False
    peer = IdentityMessage(
        msg_type="query",
        startup_instance="peer",
        local_ip=IP_LARGE,
        peer_ip=IP_SMALL,
        has_valid_master=True,
        local_committed=True,
        session_id="takeover-session",
        role="A",
        basis=BASIS_TAKEOVER_FACT,
        master_ip=IP_LARGE,
    )
    replies = agent.handle(pack_identity_message(peer), 3500)
    assert agent.outcome.local_committed is False
    agent.handle(
        _reply_to_confirm_query(
            replies,
            peer_ip=IP_LARGE,
            local_ip=IP_SMALL,
            session_id="takeover-session",
            master_ip=IP_LARGE,
            role="A",
            basis=BASIS_TAKEOVER_FACT,
        ),
        3500,
    )
    assert agent.outcome.role == "B"
    assert agent.outcome.basis == BASIS_JOIN_INCUMBENT
    assert agent.outcome.master_ip == IP_LARGE
    assert agent.outcome.session_id == "takeover-session"
    agent.poll(5000)
    assert agent.outcome.role == "B"
    assert agent.outcome.session_id == "takeover-session"


def test_takeover_fact_records_local_master_without_election(tmp_path: Path) -> None:
    agent = _agent(IP_LARGE, IP_SMALL, tmp_path / "state.json")
    agent.commit_takeover_fact()
    outcome = agent.outcome
    assert outcome.role == "A"
    assert outcome.basis == BASIS_TAKEOVER_FACT
    assert outcome.master_ip == IP_LARGE
    assert load_identity_record(tmp_path / "state.json") is not None


def test_stale_master_query_is_not_adopted_without_fresh_reply(tmp_path: Path) -> None:
    agent = _agent(IP_SMALL, IP_LARGE, tmp_path / "state.json")
    agent.start(0)
    stale = pack_identity_message(
        IdentityMessage(
            msg_type="query",
            startup_instance="old-boot",
            local_ip=IP_LARGE,
            peer_ip=IP_SMALL,
            has_valid_master=True,
            local_committed=True,
            session_id="old-session",
            role="A",
            basis=BASIS_SOLO_NO_PEER,
            master_ip=IP_LARGE,
        )
    )
    replies = agent.handle(stale, 100)
    assert agent.outcome.local_committed is False
    assert agent.outcome.role is None
    types = []
    for packet in replies:
        message = unpack_identity_message(packet)
        if message is not None:
            types.append(message.msg_type)
    assert "query_response" in types
    assert "query" in types
    now = 100
    while now <= 2500:
        agent.poll(now)
        now += 50
    assert agent.outcome.local_committed is False
    assert agent.outcome.role is None


def test_fresh_confirm_reply_adopts_current_master_not_stale_claim(tmp_path: Path) -> None:
    agent = _agent(IP_SMALL, IP_LARGE, tmp_path / "state.json")
    agent.start(0)
    stale = pack_identity_message(
        IdentityMessage(
            msg_type="query",
            startup_instance="old-boot",
            local_ip=IP_LARGE,
            peer_ip=IP_SMALL,
            has_valid_master=True,
            local_committed=True,
            session_id="old-session",
            role="A",
            basis=BASIS_SOLO_NO_PEER,
            master_ip=IP_LARGE,
        )
    )
    replies = agent.handle(stale, 100)
    agent.handle(
        _reply_to_confirm_query(
            replies,
            peer_ip=IP_LARGE,
            local_ip=IP_SMALL,
            session_id="live-session",
            master_ip=IP_LARGE,
            role="A",
            basis=BASIS_TAKEOVER_FACT,
            startup_instance="live-boot",
        ),
        150,
    )
    assert agent.outcome.role == "B"
    assert agent.outcome.session_id == "live-session"
    assert agent.outcome.basis == BASIS_JOIN_INCUMBENT


def _old_matching_round(*order: str) -> list[bytes]:
    """A previous boot's proposal and commit that agree with each other."""
    proposal = pack_identity_message(
        IdentityMessage(
            msg_type="proposal",
            startup_instance="old-boot",
            local_ip=IP_SMALL,
            peer_ip=IP_LARGE,
            proposal_id="old-proposal",
            proposal_master_ip=IP_SMALL,
            proposal_session_id="old-session",
        )
    )
    commit = pack_identity_message(
        IdentityMessage(
            msg_type="commit",
            startup_instance="old-boot",
            local_ip=IP_SMALL,
            peer_ip=IP_LARGE,
            proposal_id="old-proposal",
            proposal_master_ip=IP_SMALL,
            proposal_session_id="old-session",
            session_id="old-session",
            role="A",
            basis=BASIS_COMPETITION,
            local_committed=True,
            has_valid_master=True,
            master_ip=IP_SMALL,
        )
    )
    if order and order[0] == "commit-first":
        return [commit, proposal]
    return [proposal, commit]


def _message_types(packets: list[bytes]) -> list[str]:
    types = []
    for packet in packets:
        message = unpack_identity_message(packet)
        if message is not None:
            types.append(message.msg_type)
    return types


def test_replayed_matching_proposal_and_commit_does_not_join_old_session(
    tmp_path: Path,
) -> None:
    agent = _agent(IP_LARGE, IP_SMALL, tmp_path / "state.json")
    agent.start(0)
    first, second = _old_matching_round()
    first_replies = agent.handle(first, 100)
    assert "accept" not in _message_types(first_replies)
    assert "query" in _message_types(first_replies)
    second_replies = agent.handle(second, 100)
    assert "commit_ack" not in _message_types(second_replies)
    assert agent.outcome.local_committed is False
    assert agent.outcome.session_id != "old-session"
    now = 100
    while now <= 6000:
        agent.poll(now)
        now += 50
    assert agent.outcome.status == "confirmed"
    assert agent.outcome.role == "A"
    assert agent.outcome.basis == BASIS_SOLO_NO_PEER
    assert agent.outcome.master_ip == IP_LARGE
    assert agent.outcome.session_id != "old-session"


def test_replayed_commit_then_proposal_still_follows_solo_confirmation(
    tmp_path: Path,
) -> None:
    agent = _agent(IP_LARGE, IP_SMALL, tmp_path / "state.json")
    agent.start(0)
    for packet in _old_matching_round("commit-first"):
        replies = agent.handle(packet, 100)
        assert "commit_ack" not in _message_types(replies)
        assert "accept" not in _message_types(replies)
    assert agent.outcome.local_committed is False
    now = 100
    while now <= 6000:
        agent.poll(now)
        now += 50
    assert agent.outcome.role == "A"
    assert agent.outcome.basis == BASIS_SOLO_NO_PEER
    assert agent.outcome.session_id != "old-session"


def test_replayed_round_with_no_reply_restores_saved_history(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    save_identity_record(path, _record(IP_LARGE, IP_SMALL, "B", "kept-session", IP_SMALL))
    agent = _agent(IP_LARGE, IP_SMALL, path)
    agent.start(0)
    for packet in _old_matching_round():
        agent.handle(packet, 100)
    now = 100
    while now <= 8000:
        agent.poll(now)
        now += 50
    assert agent.outcome.role == "B"
    assert agent.outcome.basis == BASIS_RESTORED
    assert agent.outcome.master_ip == IP_SMALL
    assert agent.outcome.session_id == "kept-session"


def test_fresh_reply_with_other_startup_discards_old_proposal(tmp_path: Path) -> None:
    agent = _agent(IP_LARGE, IP_SMALL, tmp_path / "state.json")
    agent.start(0)
    proposal, commit = _old_matching_round()
    replies = agent.handle(proposal, 100)
    agent.handle(commit, 100)
    response = _reply_to_confirm_query(
        replies,
        peer_ip=IP_SMALL,
        local_ip=IP_LARGE,
        session_id="live-session",
        master_ip=IP_SMALL,
        role="A",
        basis=BASIS_TAKEOVER_FACT,
        startup_instance="live-boot",
    )
    rewritten = unpack_identity_message(response)
    assert rewritten is not None
    rewritten.proposal_id = "live-proposal"
    rewritten.proposal_master_ip = IP_SMALL
    rewritten.proposal_session_id = "live-session"
    agent.handle(pack_identity_message(rewritten), 150)
    assert agent.outcome.role == "B"
    assert agent.outcome.basis == BASIS_JOIN_INCUMBENT
    assert agent.outcome.session_id == "live-session"
    assert agent.outcome.master_ip == IP_SMALL


def test_stale_commit_without_current_proposal_does_not_change_identity(tmp_path: Path) -> None:
    agent = _agent(IP_LARGE, IP_SMALL, tmp_path / "state.json")
    agent.start(0)
    stale = pack_identity_message(
        IdentityMessage(
            msg_type="commit",
            startup_instance="old-boot",
            local_ip=IP_SMALL,
            peer_ip=IP_LARGE,
            session_id="old-session",
            role="A",
            basis=BASIS_SOLO_NO_PEER,
            local_committed=True,
            has_valid_master=True,
            master_ip=IP_SMALL,
        )
    )
    replies = agent.handle(stale, 100)
    assert agent.outcome.local_committed is False
    assert agent.outcome.role is None
    types = [
        unpack_identity_message(packet).msg_type
        for packet in replies
        if unpack_identity_message(packet) is not None
    ]
    assert "query" in types
    assert "commit_ack" not in types


def test_duplicate_proposal_after_confirm_does_not_change_identity(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    proposals: list[bytes] = []

    def capture(src: RedundancyIdentityAgent, packet: bytes) -> bool:
        message = unpack_identity_message(packet)
        if src is small and message is not None and message.msg_type == "proposal":
            proposals.append(packet)
        return False

    _drive(small, large, 0, drop=capture)
    assert proposals
    session_id = large.outcome.session_id
    assert large.outcome.role == "B"
    replies = large.handle(proposals[-1], 50)
    assert large.outcome.role == "B"
    assert large.outcome.session_id == session_id
    assert large.outcome.master_ip == IP_SMALL
    assert "accept" in _message_types(replies)


def test_duplicate_commit_after_confirm_does_not_change_identity(tmp_path: Path) -> None:
    small = _agent(IP_SMALL, IP_LARGE, tmp_path / "small.json")
    large = _agent(IP_LARGE, IP_SMALL, tmp_path / "large.json")
    small.start(0)
    large.start(0)
    _drive(small, large, 0)
    session_id = small.outcome.session_id
    assert session_id is not None
    duplicate = pack_identity_message(
        IdentityMessage(
            msg_type="commit",
            startup_instance="boot-192.168.100.10",
            local_ip=IP_SMALL,
            peer_ip=IP_LARGE,
            session_id=session_id,
            role="A",
            basis=BASIS_COMPETITION,
            local_committed=True,
            has_valid_master=True,
            master_ip=IP_SMALL,
        )
    )
    replies = large.handle(duplicate, 50)
    assert large.outcome.role == "B"
    assert large.outcome.session_id == session_id
    assert large.outcome.master_ip == IP_SMALL
    types = [
        unpack_identity_message(packet).msg_type
        for packet in replies
        if unpack_identity_message(packet) is not None
    ]
    assert "commit_ack" in types


def test_persist_failure_on_first_commit_stays_unconfirmed(
    tmp_path: Path, monkeypatch
) -> None:
    def boom(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr("webserver.redundancy_identity.save_identity_record", boom)
    agent = _agent(IP_SMALL, IP_LARGE, tmp_path / "state.json")
    agent.start(0)
    now = 0
    while now <= 5000:
        agent.poll(now)
        now += 50
    assert agent.outcome.status == "unconfirmed"
    assert agent.outcome.local_committed is False
    assert agent.outcome.role is None
    assert agent.outcome.reason == _PERSIST_FAILED_REASON
    assert not (tmp_path / "state.json").exists()


def test_takeover_persist_failure_keeps_memory_role(tmp_path: Path, monkeypatch) -> None:
    agent = _agent(IP_LARGE, IP_SMALL, tmp_path / "state.json")

    def boom(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr("webserver.redundancy_identity.save_identity_record", boom)
    agent.commit_takeover_fact()
    assert agent.outcome.role == "A"
    assert agent.outcome.local_committed is True
    assert agent.outcome.basis == BASIS_TAKEOVER_FACT
    assert agent.outcome.master_ip == IP_LARGE
    assert load_identity_record(tmp_path / "state.json") is None


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


def test_udp_exchange_elects_smaller_address_and_keeps_listening(tmp_path: Path) -> None:
    small_ip = "127.0.0.1"
    large_ip = "127.0.0.2"
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.bind((large_ip, 0))
        probe.close()
    except OSError:
        return
    small = RedundancyIdentityAgent(
        small_ip,
        large_ip,
        state_path=tmp_path / "small.json",
        startup_instance="small",
    )
    large = RedundancyIdentityAgent(
        large_ip,
        small_ip,
        state_path=tmp_path / "large.json",
        startup_instance="large",
    )
    left = IdentityExchange(small)
    right = IdentityExchange(large)
    left.start()
    right.start()
    try:
        assert left.wait_settled(8)
        assert right.wait_settled(8)
        assert small.outcome.role == "A"
        assert large.outcome.role == "B"
        assert small.outcome.session_id == large.outcome.session_id
        assert small.outcome.local_committed is True
    finally:
        left.stop()
        right.stop()
