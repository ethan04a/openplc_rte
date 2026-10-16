"""Hot-redundancy identity confirmation over UDP.

This module decides who is A/master and who is B/standby. It does not start
plc_main, heartbeat, or image sync. Callers apply a committed result later.

Heartbeat addresses identify the two machines. They are not rewritten.
Only a committed result is stored in the local state file.
"""

from __future__ import annotations

import ipaddress
import json
import socket
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from webserver.logger import get_logger

logger, _ = get_logger("redundancy_identity", use_buffer=True)

REDUNDANCY_IDENTITY_PORT = 57574
REDUNDANCY_IDENTITY_MAGIC = "OPID"
REDUNDANCY_IDENTITY_VERSION = 1
REDUNDANCY_IDENTITY_STATE_FILENAME = "redundancy_identity_state.json"

REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS = 200
REDUNDANCY_IDENTITY_DISCOVERY_WINDOW_MS = 3000
REDUNDANCY_IDENTITY_PROPOSAL_WAIT_MS = 2000
REDUNDANCY_IDENTITY_RETRY_LIMIT = 3

BASIS_SOLO_NO_PEER = "单机未发现对端"
BASIS_COMPETITION = "竞争胜出"
BASIS_JOIN_INCUMBENT = "加入现任主机"
BASIS_TAKEOVER_FACT = "升主完成后反映事实"
BASIS_RESTORED = "恢复原分配"

MSG_QUERY = "query"
MSG_QUERY_RESPONSE = "query_response"
MSG_PROPOSAL = "proposal"
MSG_ACCEPT = "accept"
MSG_COMMIT = "commit"
MSG_COMMIT_ACK = "commit_ack"
MSG_REVOKE = "revoke"

_PHASE_DISCOVER = "discover"
_PHASE_SOLO_WAIT = "solo_wait"
_PHASE_RESTORE_WAIT = "restore_wait"
_PHASE_COMPETE = "compete_wait"
_PHASE_QUERY_PEER = "query_peer"
_PHASE_CONFIRM_CLAIM = "confirm_claim"
_PHASE_CONFIRM_PROPOSAL = "confirm_proposal"
_PHASE_COMMITTED = "committed"
_PHASE_UNCONFIRMED = "unconfirmed"
_PHASE_CONFLICT = "conflict"

_UNCONFIRMED_REASON = "协商重试耗尽，保持未确认"
_CONFLICT_REASON = "双方已提交的主身份矛盾"
_PERSIST_FAILED_REASON = "身份记录保存失败，保持未确认"


@dataclass
class IdentityRecord:
    """Committed identity stored on this machine only."""

    session_id: str
    role: str
    basis: str
    local_committed: bool
    peer_confirmed: bool
    local_ip: str
    peer_ip: str
    master_ip: str


@dataclass
class IdentityOutcome:
    """Current confirmation view. Unconfirmed does not imply this node is master."""

    status: str
    role: str | None
    session_id: str | None
    basis: str | None
    local_committed: bool
    peer_confirmed: bool
    master_ip: str | None
    reason: str | None


@dataclass
class IdentityMessage:
    """One identity datagram."""

    msg_type: str
    startup_instance: str
    local_ip: str
    peer_ip: str
    request_id: str = ""
    confirming: bool = False
    has_valid_master: bool = False
    session_id: str = ""
    role: str = ""
    basis: str = ""
    local_committed: bool = False
    peer_confirmed: bool = False
    proposal_id: str = ""
    proposal_master_ip: str = ""
    proposal_session_id: str = ""
    saved_session_id: str = ""
    saved_role: str = ""
    saved_master_ip: str = ""
    saved_basis: str = ""
    master_ip: str = ""


@dataclass
class _Proposal:
    proposal_id: str
    master_ip: str
    session_id: str


def ipv4_int(address: str) -> int:
    """Compare heartbeat addresses as integers, not as strings."""
    return int(ipaddress.IPv4Address(address))


def smaller_ipv4(left: str, right: str) -> str:
    return left if ipv4_int(left) <= ipv4_int(right) else right


def pack_identity_message(message: IdentityMessage) -> bytes:
    body = {
        "magic": REDUNDANCY_IDENTITY_MAGIC,
        "version": REDUNDANCY_IDENTITY_VERSION,
        **asdict(message),
    }
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")


def unpack_identity_message(payload: bytes) -> IdentityMessage | None:
    try:
        raw = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(raw, dict):
        return None
    if raw.get("magic") != REDUNDANCY_IDENTITY_MAGIC:
        return None
    if raw.get("version") != REDUNDANCY_IDENTITY_VERSION:
        return None
    msg_type = raw.get("msg_type")
    if not isinstance(msg_type, str) or not msg_type:
        return None
    text_fields = (
        "startup_instance",
        "local_ip",
        "peer_ip",
        "request_id",
        "session_id",
        "role",
        "basis",
        "proposal_id",
        "proposal_master_ip",
        "proposal_session_id",
        "saved_session_id",
        "saved_role",
        "saved_master_ip",
        "saved_basis",
        "master_ip",
    )
    values: dict[str, object] = {"msg_type": msg_type}
    for name in text_fields:
        value = raw.get(name, "")
        if not isinstance(value, str):
            return None
        values[name] = value
    for name in ("confirming", "has_valid_master", "local_committed", "peer_confirmed"):
        value = raw.get(name, False)
        if not isinstance(value, bool):
            return None
        values[name] = value
    return IdentityMessage(**values)  # type: ignore[arg-type]


def load_identity_record(path: Path) -> IdentityRecord | None:
    """Load a committed record. Temporary proposals are not a record."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(raw, dict) or raw.get("local_committed") is not True:
        return None
    try:
        record = IdentityRecord(
            session_id=str(raw["session_id"]),
            role=str(raw["role"]),
            basis=str(raw["basis"]),
            local_committed=True,
            peer_confirmed=bool(raw.get("peer_confirmed", False)),
            local_ip=str(raw["local_ip"]),
            peer_ip=str(raw["peer_ip"]),
            master_ip=str(raw["master_ip"]),
        )
    except (KeyError, TypeError, ValueError):
        return None
    if record.role not in ("A", "B") or not record.session_id:
        return None
    return record


def save_identity_record(path: Path, record: IdentityRecord) -> None:
    """Write this machine's committed result. Does not touch redundancy_role.json."""
    if not record.local_committed:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(asdict(record), ensure_ascii=False, indent=2, sort_keys=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(payload + "\n", encoding="utf-8")
    temporary.replace(path)


class RedundancyIdentityAgent:
    """Run discovery, solo confirm, competition, or join. Does not start the PLC."""

    def __init__(
        self,
        local_ip: str,
        peer_ip: str,
        *,
        incumbent: bool = False,
        state_path: Path | None = None,
        startup_instance: str | None = None,
    ) -> None:
        self.local_ip = local_ip
        self.peer_ip = peer_ip
        self.state_path = state_path
        self.startup_instance = startup_instance or str(uuid.uuid4())
        self._state_lock = threading.Lock()
        self._saved = self._load_saved()
        self._phase = _PHASE_DISCOVER
        self._reason: str | None = None
        self._session_id = ""
        self._role = ""
        self._basis = ""
        self._master_ip = ""
        self._committed = False
        self._peer_confirmed = False
        self._proposal: _Proposal | None = None
        self._last_proposal: _Proposal | None = None
        self._accepted_proposal_id = ""
        self._accepted_session_id = ""
        self._accepted_master_ip = ""
        self._accepted_startup_instance = ""
        self._accepted_local_startup = ""
        self._staged_proposal: _Proposal | None = None
        self._staged_peer_startup = ""
        self._phase_before_confirm = ""
        self._deadline_before_confirm = 0
        self._persist_retry = False
        self._revoked: set[str] = set()
        self._active_request_id = ""
        self._negotiation_started = False
        self._peer_seen = False
        self._retry_count = 0
        self._deadline_ms = 0
        self._next_send_ms = 0
        self._started = False
        if incumbent:
            self._hold_incumbent()

    def start(self, now_ms: int) -> None:
        """Open the discovery window. An incumbent is already committed."""
        self._started = True
        if self._phase == _PHASE_DISCOVER:
            self._deadline_ms = now_ms + REDUNDANCY_IDENTITY_DISCOVERY_WINDOW_MS
            self._next_send_ms = now_ms

    @property
    def outcome(self) -> IdentityOutcome:
        if self._phase == _PHASE_CONFLICT:
            status = "conflict"
        elif self._committed:
            status = "confirmed"
        elif self._phase == _PHASE_UNCONFIRMED:
            status = "unconfirmed"
        else:
            status = "unconfirmed"
        return IdentityOutcome(
            status=status,
            role=self._role or None,
            session_id=self._session_id or None,
            basis=self._basis or None,
            local_committed=self._committed,
            peer_confirmed=self._peer_confirmed,
            master_ip=self._master_ip or None,
            reason=self._reason,
        )

    def commit_takeover_fact(self) -> None:
        """Record a finished takeover as A. This replaces a previous B result."""
        with self._state_lock:
            if (
                self._committed
                and self._role == "A"
                and self._master_ip == self.local_ip
                and self._basis == BASIS_TAKEOVER_FACT
            ):
                return
            # Field I/O already moved. Keep that fact even if the file write fails.
            self._remember_proposal()
            self._committed = True
            self._role = "A"
            self._session_id = str(uuid.uuid4())
            self._basis = BASIS_TAKEOVER_FACT
            self._master_ip = self.local_ip
            self._peer_confirmed = False
            self._proposal = None
            self._phase = _PHASE_COMMITTED
            self._reason = None
            self._saved = None
            self._next_send_ms = 0
            if not self._persist():
                self._persist_retry = True
                logger.error(
                    "[热冗余][身份] %s 升主已完成，但身份记录保存失败，将重试。会话 %s",
                    self.local_ip,
                    self._session_id,
                )
            logger.info(
                "[热冗余][身份] %s 升主完成，身份写成 A，会话 %s",
                self.local_ip,
                self._session_id,
            )

    def poll(self, now_ms: int) -> list[bytes]:
        """Return datagrams to send. Time is milliseconds."""
        outgoing: list[bytes] = []
        if self._persist_retry and self._committed and now_ms >= self._next_send_ms:
            if self._persist():
                self._persist_retry = False
                logger.info("[热冗余][身份] %s 身份记录保存重试成功", self.local_ip)
            else:
                self._next_send_ms = now_ms + REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS
                logger.error("[热冗余][身份] %s 身份记录保存重试失败", self.local_ip)
        if not self._started or self._phase in (_PHASE_CONFLICT, _PHASE_UNCONFIRMED):
            return outgoing
        if self._phase in (_PHASE_CONFIRM_CLAIM, _PHASE_CONFIRM_PROPOSAL):
            if now_ms >= self._deadline_ms:
                if self._phase == _PHASE_CONFIRM_PROPOSAL:
                    finished = self._finish_confirm_proposal_timeout(now_ms)
                else:
                    finished = self._finish_confirm_timeout(now_ms)
                return outgoing + finished
            if now_ms >= self._next_send_ms:
                self._next_send_ms = now_ms + REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS
                return outgoing + [self._pack_query()]
            return outgoing
        if self._phase == _PHASE_DISCOVER:
            if now_ms >= self._deadline_ms:
                return self._begin_solo(now_ms)
            if now_ms >= self._next_send_ms:
                self._next_send_ms = now_ms + REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS
                return [self._pack_query()]
            return []
        if self._phase == _PHASE_RESTORE_WAIT:
            if now_ms >= self._deadline_ms:
                return self._restore_saved_after_silence()
            if now_ms >= self._next_send_ms:
                self._next_send_ms = now_ms + REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS
                return [self._pack_query()]
            return []
        if self._phase == _PHASE_SOLO_WAIT and now_ms >= self._deadline_ms:
            return self._finish_solo()
        if self._phase in (_PHASE_COMPETE, _PHASE_QUERY_PEER) and now_ms >= self._deadline_ms:
            if self._phase == _PHASE_COMPETE:
                self._phase = _PHASE_QUERY_PEER
                self._deadline_ms = now_ms + REDUNDANCY_IDENTITY_PROPOSAL_WAIT_MS
                return [self._pack_query()]
            return self._fail_round(now_ms)
        if self._phase == _PHASE_COMMITTED and not self._peer_confirmed:
            if now_ms >= self._next_send_ms:
                self._next_send_ms = now_ms + REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS
                return [self._pack_commit()]
        return []

    def handle(self, payload: bytes, now_ms: int) -> list[bytes]:
        """Apply one peer datagram and return replies."""
        message = unpack_identity_message(payload)
        if message is None:
            return []
        if message.local_ip != self.peer_ip or message.peer_ip != self.local_ip:
            return []
        if message.msg_type == MSG_QUERY:
            replies = self._react(message, now_ms, bound=False)
            return [self._pack_response(message.request_id), *replies]
        if message.msg_type == MSG_QUERY_RESPONSE:
            return self._react(message, now_ms, bound=True)
        if message.msg_type == MSG_PROPOSAL:
            return self._on_proposal(message, now_ms)
        if message.msg_type == MSG_ACCEPT:
            return self._on_accept(message)
        if message.msg_type == MSG_COMMIT:
            return self._on_commit(message, now_ms)
        if message.msg_type == MSG_COMMIT_ACK:
            return self._on_commit_ack(message)
        if message.msg_type == MSG_REVOKE:
            if message.proposal_id:
                self._revoked.add(message.proposal_id)
            return []
        return []

    def _load_saved(self) -> IdentityRecord | None:
        if self.state_path is None:
            return None
        record = load_identity_record(self.state_path)
        if record is None:
            return None
        if record.local_ip != self.local_ip or record.peer_ip != self.peer_ip:
            return None
        return record

    def _hold_incumbent(self) -> None:
        """This process is already master, including a promoted standby."""
        saved = self._saved
        if saved is not None and saved.role == "A" and saved.master_ip == self.local_ip:
            session_id = saved.session_id
            peer_confirmed = saved.peer_confirmed
        else:
            session_id = str(uuid.uuid4())
            peer_confirmed = False
        self._saved = None
        self._apply_commit(
            role="A",
            session_id=session_id,
            basis=BASIS_RESTORED,
            master_ip=self.local_ip,
            peer_confirmed=peer_confirmed,
            persist_required=False,
        )
        logger.info(
            "[热冗余][身份] 本机 %s 已在当主机，保留现任身份，会话 %s",
            self.local_ip,
            session_id,
        )

    def _apply_commit(
        self,
        *,
        role: str,
        session_id: str,
        basis: str,
        master_ip: str,
        peer_confirmed: bool,
        persist_required: bool = True,
    ) -> bool:
        """Store a commit. Returns False when it would replace an existing result."""
        if self._committed:
            same = (
                self._session_id == session_id
                and self._role == role
                and self._master_ip == master_ip
            )
            if same:
                if peer_confirmed and not self._peer_confirmed:
                    self._peer_confirmed = True
                    if not self._persist():
                        self._persist_retry = True
                return True
            self._mark_conflict()
            return False
        record = IdentityRecord(
            session_id=session_id,
            role=role,
            basis=basis,
            local_committed=True,
            peer_confirmed=peer_confirmed,
            local_ip=self.local_ip,
            peer_ip=self.peer_ip,
            master_ip=master_ip,
        )
        if persist_required and not self._write_record(record):
            self._phase = _PHASE_UNCONFIRMED
            self._reason = _PERSIST_FAILED_REASON
            self._proposal = None
            logger.error("[热冗余][身份] %s %s", self.local_ip, _PERSIST_FAILED_REASON)
            return False
        self._remember_proposal()
        self._committed = True
        self._role = role
        self._session_id = session_id
        self._basis = basis
        self._master_ip = master_ip
        self._peer_confirmed = peer_confirmed
        self._proposal = None
        self._phase = _PHASE_COMMITTED
        self._reason = None
        self._saved = None
        if not self._peer_confirmed:
            self._next_send_ms = 0
        if not persist_required and not self._write_record(record):
            self._persist_retry = True
            logger.error(
                "[热冗余][身份] %s 身份已生效，但记录保存失败，将重试",
                self.local_ip,
            )
        logger.info(
            "[热冗余][身份] %s 提交角色 %s，依据 %s，会话 %s",
            self.local_ip,
            role,
            basis,
            session_id,
        )
        return True

    def _remember_proposal(self) -> None:
        if self._proposal is not None:
            self._last_proposal = self._proposal

    def _current_proposal(self) -> _Proposal | None:
        return self._proposal or self._last_proposal

    def _write_record(self, record: IdentityRecord) -> bool:
        if self.state_path is None:
            return True
        try:
            save_identity_record(self.state_path, record)
            return True
        except OSError as exc:
            logger.error("[热冗余][身份] %s 保存身份记录失败: %s", self.local_ip, exc)
            return False

    def _persist(self) -> bool:
        if self.state_path is None or not self._committed:
            return True
        return self._write_record(
            IdentityRecord(
                session_id=self._session_id,
                role=self._role,
                basis=self._basis,
                local_committed=True,
                peer_confirmed=self._peer_confirmed,
                local_ip=self.local_ip,
                peer_ip=self.peer_ip,
                master_ip=self._master_ip,
            )
        )

    def _mark_conflict(self, message: IdentityMessage | None = None) -> None:
        if self._phase == _PHASE_CONFLICT:
            return
        self._phase = _PHASE_CONFLICT
        self._reason = _CONFLICT_REASON
        self._proposal = None
        local_session = self._session_id
        local_basis = self._basis
        local_master = self._master_ip
        if self._saved is not None and not self._committed:
            local_session = self._saved.session_id
            local_basis = self._saved.basis
            local_master = self._saved.master_ip
        if message is None:
            logger.warning(
                "[热冗余][身份] %s 与对端主身份矛盾，停止自动重新分配。"
                "本机会话 %s，依据 %s，主 %s",
                self.local_ip,
                local_session,
                local_basis,
                local_master,
            )
            return
        logger.warning(
            "[热冗余][身份] %s 与对端 %s 主身份矛盾，停止自动重新分配。"
            "本机会话 %s，依据 %s，主 %s；对端会话 %s，依据 %s，主 %s",
            self.local_ip,
            message.local_ip,
            local_session,
            local_basis,
            local_master,
            message.session_id or message.saved_session_id,
            message.basis or message.saved_basis,
            message.master_ip or message.saved_master_ip,
        )

    def _message(self, msg_type: str, **overrides: object) -> IdentityMessage:
        saved_session = ""
        saved_role = ""
        saved_master = ""
        saved_basis = ""
        if self._saved is not None and not self._committed:
            saved_session = self._saved.session_id
            saved_role = self._saved.role
            saved_master = self._saved.master_ip
            saved_basis = self._saved.basis
        proposal = self._current_proposal()
        message = IdentityMessage(
            msg_type=msg_type,
            startup_instance=self.startup_instance,
            local_ip=self.local_ip,
            peer_ip=self.peer_ip,
            confirming=(
                self._phase
                in (
                    _PHASE_DISCOVER,
                    _PHASE_SOLO_WAIT,
                    _PHASE_RESTORE_WAIT,
                    _PHASE_COMPETE,
                    _PHASE_QUERY_PEER,
                    _PHASE_CONFIRM_CLAIM,
                    _PHASE_CONFIRM_PROPOSAL,
                )
            ),
            has_valid_master=self._committed,
            session_id=self._session_id,
            role=self._role,
            basis=self._basis,
            local_committed=self._committed,
            peer_confirmed=self._peer_confirmed,
            proposal_id=proposal.proposal_id if proposal else "",
            proposal_master_ip=proposal.master_ip if proposal else "",
            proposal_session_id=proposal.session_id if proposal else "",
            saved_session_id=saved_session,
            saved_role=saved_role,
            saved_master_ip=saved_master,
            saved_basis=saved_basis,
            master_ip=self._master_ip,
        )
        for key, value in overrides.items():
            setattr(message, key, value)
        return message

    def _pack_query(self) -> bytes:
        self._active_request_id = str(uuid.uuid4())
        return pack_identity_message(self._message(MSG_QUERY, request_id=self._active_request_id))

    def _pack_response(self, request_id: str) -> bytes:
        return pack_identity_message(self._message(MSG_QUERY_RESPONSE, request_id=request_id))

    def _pack_proposal(self) -> bytes:
        assert self._proposal is not None
        return pack_identity_message(
            self._message(
                MSG_PROPOSAL,
                proposal_id=self._proposal.proposal_id,
                proposal_master_ip=self._proposal.master_ip,
                proposal_session_id=self._proposal.session_id,
            )
        )

    def _pack_accept(self, proposal_id: str, master_ip: str, session_id: str) -> bytes:
        return pack_identity_message(
            self._message(
                MSG_ACCEPT,
                proposal_id=proposal_id,
                proposal_master_ip=master_ip,
                proposal_session_id=session_id,
            )
        )

    def _pack_commit(self) -> bytes:
        return pack_identity_message(self._message(MSG_COMMIT, master_ip=self._master_ip))

    def _pack_commit_ack(self) -> bytes:
        return pack_identity_message(self._message(MSG_COMMIT_ACK))

    def _pack_revoke(self, proposal_id: str) -> bytes:
        return pack_identity_message(self._message(MSG_REVOKE, proposal_id=proposal_id))

    def _revoke_current(self) -> list[bytes]:
        proposal = self._proposal
        if proposal is None:
            return []
        self._revoked.add(proposal.proposal_id)
        self._proposal = None
        return [self._pack_revoke(proposal.proposal_id)]

    def _begin_solo(self, now_ms: int) -> list[bytes]:
        if self._negotiation_started or self._peer_seen or self._committed:
            return []
        if self._saved is not None:
            self._phase = _PHASE_RESTORE_WAIT
            self._deadline_ms = now_ms + REDUNDANCY_IDENTITY_PROPOSAL_WAIT_MS
            self._next_send_ms = now_ms + REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS
            logger.info(
                "[热冗余][身份] %s 未发现对端，有已提交记录，继续等待后再恢复",
                self.local_ip,
            )
            return [self._pack_query()]
        self._proposal = _Proposal(
            proposal_id=str(uuid.uuid4()),
            master_ip=self.local_ip,
            session_id=str(uuid.uuid4()),
        )
        self._phase = _PHASE_SOLO_WAIT
        self._deadline_ms = now_ms + REDUNDANCY_IDENTITY_PROPOSAL_WAIT_MS
        logger.info("[热冗余][身份] %s 未发现对端，提出担任主机", self.local_ip)
        return [self._pack_proposal()]

    def _finish_solo(self) -> list[bytes]:
        if self._negotiation_started or self._peer_seen or self._committed:
            return []
        proposal = self._proposal
        session_id = proposal.session_id if proposal else str(uuid.uuid4())
        if not self._apply_commit(
            role="A",
            session_id=session_id,
            basis=BASIS_SOLO_NO_PEER,
            master_ip=self.local_ip,
            peer_confirmed=False,
        ):
            return []
        return [self._pack_commit()]

    def _restore_saved_after_silence(self) -> list[bytes]:
        """Peer stayed silent. Restore the saved role instead of solo-electing A."""
        if self._negotiation_started or self._peer_seen or self._committed or self._saved is None:
            return []
        saved = self._saved
        role = "A" if saved.master_ip == self.local_ip else "B"
        logger.info(
            "[热冗余][身份] %s 对端仍无应答，按已保存结果恢复角色 %s",
            self.local_ip,
            role,
        )
        return self._reconfirm_saved()

    def _enter_competition(self, now_ms: int, *, restart: bool = False) -> list[bytes]:
        if self._committed or self._phase in (_PHASE_CONFLICT, _PHASE_UNCONFIRMED):
            return []
        if self._phase == _PHASE_COMPETE and self._proposal is not None and not restart:
            return []
        self._negotiation_started = True
        self._peer_seen = True
        self._proposal = _Proposal(
            proposal_id=str(uuid.uuid4()),
            master_ip=self.local_ip,
            session_id=str(uuid.uuid4()),
        )
        self._clear_accepted_proposal()
        self._clear_staged_proposal()
        self._phase = _PHASE_COMPETE
        self._deadline_ms = now_ms + REDUNDANCY_IDENTITY_PROPOSAL_WAIT_MS
        logger.info(
            "[热冗余][身份] %s 进入双机竞争，提案 %s",
            self.local_ip,
            self._proposal.proposal_id,
        )
        return [self._pack_proposal()]

    def _fail_round(self, now_ms: int) -> list[bytes]:
        """Revoke the temporary proposal and retry. Never falls through to solo commit."""
        self._retry_count += 1
        replies = self._revoke_current()
        if self._retry_count >= REDUNDANCY_IDENTITY_RETRY_LIMIT:
            self._phase = _PHASE_UNCONFIRMED
            self._reason = _UNCONFIRMED_REASON
            self._proposal = None
            logger.warning("[热冗余][身份] %s %s", self.local_ip, _UNCONFIRMED_REASON)
            return replies
        return replies + self._enter_competition(now_ms, restart=True)

    def _saved_relation(self, message: IdentityMessage) -> str:
        if self._saved is None or not message.saved_session_id or not message.saved_master_ip:
            return "none"
        same = (
            self._saved.session_id == message.saved_session_id
            and self._saved.master_ip == message.saved_master_ip
        )
        return "match" if same else "conflict"

    def _clear_accepted_proposal(self) -> None:
        self._accepted_proposal_id = ""
        self._accepted_session_id = ""
        self._accepted_master_ip = ""
        self._accepted_startup_instance = ""
        self._accepted_local_startup = ""

    def _clear_staged_proposal(self) -> None:
        self._staged_proposal = None
        self._staged_peer_startup = ""

    def _remember_staged_as_accepted(self) -> None:
        staged = self._staged_proposal
        if staged is None:
            return
        self._accepted_proposal_id = staged.proposal_id
        self._accepted_session_id = staged.session_id
        self._accepted_master_ip = staged.master_ip
        self._accepted_startup_instance = self._staged_peer_startup
        self._accepted_local_startup = self.startup_instance

    def _competition_commit_matches(self, message: IdentityMessage) -> bool:
        if not self._accepted_proposal_id:
            return False
        if self._accepted_local_startup != self.startup_instance:
            return False
        if message.proposal_id != self._accepted_proposal_id:
            return False
        if not message.session_id or message.session_id != self._accepted_session_id:
            return False
        if message.master_ip != self._accepted_master_ip:
            return False
        if message.startup_instance != self._accepted_startup_instance:
            return False
        return True

    def _same_accepted_proposal(self, message: IdentityMessage) -> bool:
        return bool(
            self._accepted_proposal_id
            and message.proposal_id == self._accepted_proposal_id
            and message.proposal_session_id == self._accepted_session_id
            and message.proposal_master_ip == self._accepted_master_ip
            and message.startup_instance == self._accepted_startup_instance
            and self._accepted_local_startup == self.startup_instance
        )

    def _same_committed_claim(self, message: IdentityMessage) -> bool:
        peer_master = message.master_ip or message.saved_master_ip
        return bool(
            self._committed
            and peer_master == self._master_ip
            and (message.session_id == self._session_id or message.local_committed)
        )

    def _begin_confirm_claim(self, message: IdentityMessage, now_ms: int) -> list[bytes]:
        """Ask the peer again. Do not accept an unbound 'I am master' claim yet."""
        if self._phase in (
            _PHASE_CONFIRM_CLAIM,
            _PHASE_CONFIRM_PROPOSAL,
            _PHASE_CONFLICT,
            _PHASE_UNCONFIRMED,
        ):
            return []
        if self._same_committed_claim(message):
            if not self._peer_confirmed:
                self._peer_confirmed = True
                if not self._persist():
                    self._persist_retry = True
            return [self._pack_commit_ack()] if message.msg_type == MSG_COMMIT else []
        self._phase_before_confirm = self._phase
        self._deadline_before_confirm = self._deadline_ms
        self._phase = _PHASE_CONFIRM_CLAIM
        self._deadline_ms = now_ms + REDUNDANCY_IDENTITY_PROPOSAL_WAIT_MS
        self._next_send_ms = now_ms + REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS
        logger.info(
            "[热冗余][身份] %s 收到主机声明，先向 %s 发起本次确认",
            self.local_ip,
            message.local_ip,
        )
        return [self._pack_query()]

    def _finish_confirm_timeout(self, now_ms: int) -> list[bytes]:
        previous = self._phase_before_confirm or _PHASE_DISCOVER
        previous_deadline = self._deadline_before_confirm
        self._phase = previous
        self._deadline_ms = previous_deadline
        self._phase_before_confirm = ""
        self._deadline_before_confirm = 0
        logger.info(
            "[热冗余][身份] %s 对端主机声明未得到本次确认，忽略该声明",
            self.local_ip,
        )
        if previous == _PHASE_DISCOVER and now_ms >= self._deadline_ms:
            return self._begin_solo(now_ms)
        return []

    def _begin_confirm_proposal(self, message: IdentityMessage, now_ms: int) -> list[bytes]:
        """Stage a proposal and ask whether it belongs to this startup pair.

        The proposal is not accepted, and it does not count as the peer being
        online for this boot, until a reply to this new request confirms it.
        """
        if self._phase in (
            _PHASE_CONFIRM_PROPOSAL,
            _PHASE_CONFIRM_CLAIM,
            _PHASE_CONFLICT,
            _PHASE_UNCONFIRMED,
        ):
            return []
        if (
            not message.proposal_id
            or not message.proposal_session_id
            or not message.proposal_master_ip
            or not message.startup_instance
        ):
            return []
        self._staged_proposal = _Proposal(
            proposal_id=message.proposal_id,
            master_ip=message.proposal_master_ip,
            session_id=message.proposal_session_id,
        )
        self._staged_peer_startup = message.startup_instance
        self._phase_before_confirm = self._phase
        self._deadline_before_confirm = self._deadline_ms
        self._phase = _PHASE_CONFIRM_PROPOSAL
        self._deadline_ms = now_ms + REDUNDANCY_IDENTITY_PROPOSAL_WAIT_MS
        self._next_send_ms = now_ms + REDUNDANCY_IDENTITY_QUERY_INTERVAL_MS
        logger.info(
            "[热冗余][身份] %s 收到提案 %s，先向 %s 核实是否属于本次启动",
            self.local_ip,
            message.proposal_id,
            message.local_ip,
        )
        return [self._pack_query()]

    def _staged_proposal_matches(self, message: IdentityMessage) -> bool:
        """True when this reply binds both current startups to the staged round."""
        staged = self._staged_proposal
        if staged is None or not self._staged_peer_startup:
            return False
        if message.startup_instance != self._staged_peer_startup:
            return False
        if message.proposal_id != staged.proposal_id:
            return False
        if message.proposal_session_id != staged.session_id:
            return False
        if message.proposal_master_ip != staged.master_ip:
            return False
        if message.has_valid_master or message.local_committed:
            if message.master_ip != staged.master_ip:
                return False
            if message.session_id != staged.session_id:
                return False
        return True

    def _accept_verified_proposal(self, now_ms: int) -> list[bytes]:
        staged = self._staged_proposal
        if staged is None:
            return []
        self._remember_staged_as_accepted()
        self._clear_staged_proposal()
        self._phase_before_confirm = ""
        self._deadline_before_confirm = 0
        replies = self._revoke_current()
        self._negotiation_started = True
        self._peer_seen = True
        self._phase = _PHASE_COMPETE
        self._deadline_ms = now_ms + REDUNDANCY_IDENTITY_PROPOSAL_WAIT_MS
        logger.info(
            "[热冗余][身份] %s 提案 %s 已通过本次确认，接受。本机启动 %s，对端启动 %s",
            self.local_ip,
            self._accepted_proposal_id,
            self.startup_instance,
            self._accepted_startup_instance,
        )
        replies.append(
            self._pack_accept(
                self._accepted_proposal_id,
                self._accepted_master_ip,
                self._accepted_session_id,
            )
        )
        return replies

    def _finish_confirm_proposal(self, message: IdentityMessage, now_ms: int) -> list[bytes]:
        """Accept the staged proposal only when this request's reply confirms it."""
        matched = self._staged_proposal_matches(message)
        previous = self._phase_before_confirm or _PHASE_DISCOVER
        previous_deadline = self._deadline_before_confirm
        if matched and (message.has_valid_master or message.local_committed):
            replies = self._accept_verified_proposal(now_ms)
            replies.extend(self._commit_unconfirmed_peer(message))
            return replies
        if matched:
            return self._accept_verified_proposal(now_ms)
        self._clear_staged_proposal()
        self._phase_before_confirm = ""
        self._deadline_before_confirm = 0
        self._phase = previous
        self._deadline_ms = previous_deadline
        logger.info(
            "[热冗余][身份] %s 本次回复与暂存提案不一致，丢弃该提案",
            self.local_ip,
        )
        return self._react(message, now_ms, bound=True)

    def _finish_confirm_proposal_timeout(self, now_ms: int) -> list[bytes]:
        """Drop the staged proposal. Do not treat it as the peer being online."""
        previous = self._phase_before_confirm or _PHASE_DISCOVER
        previous_deadline = self._deadline_before_confirm
        self._clear_staged_proposal()
        self._phase = previous
        self._deadline_ms = previous_deadline
        self._phase_before_confirm = ""
        self._deadline_before_confirm = 0
        logger.info(
            "[热冗余][身份] %s 提案未得到本次确认，忽略该提案",
            self.local_ip,
        )
        if previous == _PHASE_DISCOVER and now_ms >= self._deadline_ms:
            return self._begin_solo(now_ms)
        if previous == _PHASE_SOLO_WAIT and now_ms >= self._deadline_ms:
            return self._finish_solo()
        if previous == _PHASE_RESTORE_WAIT and now_ms >= self._deadline_ms:
            return self._restore_saved_after_silence()
        return []

    def _react(self, message: IdentityMessage, now_ms: int, *, bound: bool) -> list[bytes]:
        if bound and message.request_id != self._active_request_id:
            return []
        if self._phase in (_PHASE_CONFLICT, _PHASE_UNCONFIRMED):
            return []
        if self._phase == _PHASE_CONFIRM_PROPOSAL:
            if not bound:
                return []
            return self._finish_confirm_proposal(message, now_ms)
        if message.has_valid_master and not bound:
            return self._begin_confirm_claim(message, now_ms)
        self._peer_seen = True
        if self._phase == _PHASE_CONFIRM_CLAIM and bound:
            if message.has_valid_master:
                return self._adopt_peer(message)
            self._phase = self._phase_before_confirm or _PHASE_DISCOVER
            self._phase_before_confirm = ""
        if self._phase == _PHASE_QUERY_PEER and bound:
            if message.has_valid_master:
                return self._adopt_peer(message)
            return self._fail_round(now_ms)
        if message.has_valid_master:
            return self._adopt_peer(message)
        relation = self._saved_relation(message)
        if relation == "conflict" and not self._committed:
            self._mark_conflict(message)
            return []
        if relation == "match" and not self._committed:
            return self._reconfirm_saved()
        if self._committed or self._phase in (_PHASE_COMPETE, _PHASE_QUERY_PEER, _PHASE_CONFLICT):
            return []
        if self._phase in (_PHASE_SOLO_WAIT, _PHASE_RESTORE_WAIT):
            replies = self._revoke_current()
            return replies + self._enter_competition(now_ms)
        return self._enter_competition(now_ms)

    def _reconfirm_saved(self) -> list[bytes]:
        assert self._saved is not None
        saved = self._saved
        role = "A" if saved.master_ip == self.local_ip else "B"
        applied = self._apply_commit(
            role=role,
            session_id=saved.session_id,
            basis=BASIS_RESTORED,
            master_ip=saved.master_ip,
            peer_confirmed=False,
        )
        if not applied:
            return []
        return [self._pack_commit()]

    def _adopt_peer(self, message: IdentityMessage) -> list[bytes]:
        """Follow a peer that already has a committed master. Do not compare addresses."""
        peer_master = message.master_ip or message.saved_master_ip
        if not peer_master:
            return []
        if self._committed:
            if peer_master != self._master_ip:
                self._mark_conflict(message)
                return []
            if message.session_id == self._session_id or message.local_committed:
                self._peer_confirmed = True
                if not self._persist():
                    self._persist_retry = True
            return [self._pack_commit_ack()]
        if message.proposal_id and message.proposal_id in self._revoked:
            return []
        replies = self._revoke_current()
        if peer_master == message.local_ip and not self._negotiation_started:
            basis = BASIS_JOIN_INCUMBENT
        elif self._negotiation_started:
            basis = BASIS_COMPETITION
        else:
            basis = BASIS_RESTORED
        role = "A" if peer_master == self.local_ip else "B"
        session_id = message.session_id or str(uuid.uuid4())
        applied = self._apply_commit(
            role=role,
            session_id=session_id,
            basis=basis,
            master_ip=peer_master,
            peer_confirmed=True,
        )
        if not applied:
            return replies
        return replies + [self._pack_commit_ack()]

    def _on_proposal(self, message: IdentityMessage, now_ms: int) -> list[bytes]:
        if not message.proposal_id or message.proposal_id in self._revoked:
            return []
        if self._phase in (_PHASE_CONFLICT, _PHASE_UNCONFIRMED):
            return []
        if self._same_accepted_proposal(message):
            # A repeated proposal may be answered again. It must not move identity.
            return [
                self._pack_accept(
                    message.proposal_id,
                    message.proposal_master_ip,
                    message.proposal_session_id,
                )
            ]
        if self._committed or self._phase in (_PHASE_CONFIRM_PROPOSAL, _PHASE_CONFIRM_CLAIM):
            return []
        smaller = smaller_ipv4(self.local_ip, self.peer_ip)
        would_accept = message.proposal_master_ip == smaller and self.local_ip != smaller
        if not would_accept:
            if self._phase in (_PHASE_DISCOVER, _PHASE_SOLO_WAIT, _PHASE_RESTORE_WAIT):
                self._peer_seen = True
                replies = self._revoke_current()
                return replies + self._enter_competition(now_ms)
            return []
        return self._begin_confirm_proposal(message, now_ms)

    def _on_accept(self, message: IdentityMessage) -> list[bytes]:
        proposal = self._proposal
        if proposal is None or message.proposal_id in self._revoked:
            return []
        if message.proposal_id != proposal.proposal_id:
            return []
        if message.proposal_master_ip != proposal.master_ip:
            return []
        smaller = smaller_ipv4(self.local_ip, self.peer_ip)
        if proposal.master_ip != smaller or self.local_ip != smaller:
            return []
        if self._committed:
            if self._session_id == proposal.session_id:
                return [self._pack_commit()]
            return []
        if not self._apply_commit(
            role="A",
            session_id=proposal.session_id,
            basis=BASIS_COMPETITION,
            master_ip=self.local_ip,
            peer_confirmed=False,
        ):
            return []
        return [self._pack_commit()]

    def _on_commit(self, message: IdentityMessage, now_ms: int) -> list[bytes]:
        if self._phase in (_PHASE_CONFLICT, _PHASE_UNCONFIRMED):
            return []
        if self._phase == _PHASE_CONFIRM_PROPOSAL and not self._committed:
            return []
        if message.proposal_id and message.proposal_id in self._revoked:
            return []
        master_ip = message.master_ip
        if master_ip not in (self.local_ip, self.peer_ip):
            return []
        if self._committed:
            same_master = master_ip == self._master_ip
            same_session = message.session_id == self._session_id
            if same_master and same_session:
                self._peer_confirmed = True
                if not self._persist():
                    self._persist_retry = True
                return [self._pack_commit_ack()]
            return self._begin_confirm_claim(message, now_ms)
        if self._accepted_proposal_id:
            if not self._competition_commit_matches(message):
                return []
            return self._commit_unconfirmed_peer(message)
        return self._begin_confirm_claim(message, now_ms)

    def _commit_unconfirmed_peer(self, message: IdentityMessage) -> list[bytes]:
        master_ip = message.master_ip
        role = "A" if master_ip == self.local_ip else "B"
        if self._negotiation_started:
            basis = BASIS_COMPETITION
        elif master_ip == message.local_ip and master_ip != self.local_ip:
            basis = BASIS_JOIN_INCUMBENT
        else:
            basis = message.basis or BASIS_RESTORED
        applied = self._apply_commit(
            role=role,
            session_id=message.session_id,
            basis=basis,
            master_ip=master_ip,
            peer_confirmed=True,
        )
        if not applied:
            return []
        return [self._pack_commit_ack()]

    def _on_commit_ack(self, message: IdentityMessage) -> list[bytes]:
        if not self._committed or message.session_id != self._session_id:
            return []
        if self._peer_confirmed:
            return []
        self._peer_confirmed = True
        if not self._persist():
            self._persist_retry = True
        return []


def bind_identity_socket(local_ip: str, port: int = REDUNDANCY_IDENTITY_PORT) -> socket.socket:
    """Bind the identity UDP socket. This is not the TCP heartbeat on 57575."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((local_ip, port))
    return sock


class IdentityExchange:
    """Exchange identity datagrams until the agent settles, then keep answering."""

    def __init__(
        self,
        agent: RedundancyIdentityAgent,
        *,
        port: int = REDUNDANCY_IDENTITY_PORT,
        peer_port: int | None = None,
        step_ms: int = 50,
        limit_ms: int = 20000,
    ) -> None:
        self.agent = agent
        self.port = port
        self.peer_port = port if peer_port is None else peer_port
        self._step_ms = step_ms
        self._limit_ms = limit_ms
        self._stop = threading.Event()
        self._settled = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop,
            name="redundancy-identity",
            daemon=True,
        )
        self._thread.start()

    def wait_settled(self, timeout_sec: float) -> bool:
        return self._settled.wait(timeout_sec)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def _loop(self) -> None:
        try:
            sock = bind_identity_socket(self.agent.local_ip, self.port)
        except OSError as exc:
            logger.error(
                "[热冗余][身份] 绑定 %s:%s 失败: %s",
                self.agent.local_ip,
                self.port,
                exc,
            )
            self._settled.set()
            return
        sock.setblocking(False)
        started = time.monotonic()
        self.agent.start(0)
        try:
            while not self._stop.is_set():
                elapsed = int((time.monotonic() - started) * 1000)
                self._exchange(sock, elapsed)
                if not self._settled.is_set():
                    outcome = self.agent.outcome
                    timed_out = elapsed >= self._limit_ms
                    terminal = (
                        outcome.local_committed
                        or outcome.status == "conflict"
                        or bool(outcome.reason)
                        or timed_out
                    )
                    if terminal:
                        self._settled.set()
                        if not outcome.local_committed:
                            logger.warning(
                                "[热冗余][身份] %s 未确认身份: %s",
                                self.agent.local_ip,
                                outcome.reason or outcome.status,
                            )
                            break
                        logger.info(
                            "[热冗余][身份] %s 已确认角色 %s，继续应答后续加入",
                            self.agent.local_ip,
                            outcome.role,
                        )
                if self._stop.wait(self._step_ms / 1000):
                    break
        finally:
            sock.close()
            self._settled.set()

    def _exchange(self, sock: socket.socket, now_ms: int) -> None:
        with self.agent._state_lock:
            outgoing = list(self.agent.poll(now_ms))
        for packet in outgoing:
            self._send(sock, packet)
        while True:
            try:
                payload, addr = sock.recvfrom(65535)
            except BlockingIOError:
                break
            if addr[0] != self.agent.peer_ip:
                continue
            with self.agent._state_lock:
                replies = list(self.agent.handle(payload, now_ms))
            for reply in replies:
                self._send(sock, reply)

    def _send(self, sock: socket.socket, packet: bytes) -> None:
        try:
            sock.sendto(packet, (self.agent.peer_ip, self.peer_port))
        except OSError:
            return
