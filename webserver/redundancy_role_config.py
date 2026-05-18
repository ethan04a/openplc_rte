"""
Load, parse, and persist hot-redundancy role state in redundancy_role.json.

Schema v2: functional_nics is an array of 0..N objects, each with linux_ifname,
permanent_master_ipv4_cidr, and standby_backup_ipv4_cidr_before_takeover.
"""

from __future__ import annotations

import ipaddress
import json
import re
from dataclasses import dataclass
from pathlib import Path

from webserver.logger import get_logger

logger, _ = get_logger("redundancy_role", use_buffer=True)

REDUNDANCY_ROLE_FILENAME = "redundancy_role.json"
REDUNDANCY_ROLE_SCHEMA_VERSION = 2

DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME = "ens35"
DEFAULT_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME = "ens33"
DEFAULT_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME = "ens34"

_LINUX_IFNAME_MAX_LEN = 15
# IFNAMSIZ=15; allow legacy alias names (eth2:1), dots, underscores, hyphens.
_LINUX_IFNAME_RE = re.compile(r"^[A-Za-z0-9.:_-]{1,15}$")

DEFAULT_REDUNDANCY_ROLE_ABOUT = (
    "Hot-redundancy role and addressing for this OpenPLC pair. "
    "Peer IPv4s must match the IPv4 on redundancy_heartbeat_nic_linux_ifname. "
    "functional_nics: array of 0..N objects (linux_ifname, permanent_master_ipv4_cidr, "
    "standby_backup_ipv4_cidr_before_takeover). "
    "Invalid linux_ifname in an element falls back to eth{N} for that index."
)

REDUNDANCY_ROLE_KEY_ABOUT = "about"
REDUNDANCY_ROLE_KEY_SCHEMA_VERSION = "schema_version"
REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME = "redundancy_heartbeat_nic_linux_ifname"
REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS = "functional_nics"
REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4 = "master_ipv4_on_redundancy_heartbeat_nic"
REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4 = "standby_ipv4_on_redundancy_heartbeat_nic"

FUNCTIONAL_NIC_KEY_LINUX_IFNAME = "linux_ifname"
FUNCTIONAL_NIC_KEY_PERMANENT_MASTER_CIDR = "permanent_master_ipv4_cidr"
FUNCTIONAL_NIC_KEY_STANDBY_BACKUP_CIDR = "standby_backup_ipv4_cidr_before_takeover"

_LEGACY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME = "functional_io_nic_a_linux_ifname"
_LEGACY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME = "functional_io_nic_b_linux_ifname"
_LEGACY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR = "permanent_master_functional_nic_a_ipv4_cidr"
_LEGACY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR = "permanent_master_functional_nic_b_ipv4_cidr"
_LEGACY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR = (
    "standby_backup_functional_nic_a_ipv4_cidr_before_takeover"
)
_LEGACY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR = (
    "standby_backup_functional_nic_b_ipv4_cidr_before_takeover"
)

_LEGACY_FLAT_KEYS: tuple[str, ...] = (
    _LEGACY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME,
    _LEGACY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME,
    _LEGACY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR,
    _LEGACY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR,
    _LEGACY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR,
    _LEGACY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR,
)

REDUNDANCY_ROLE_JSON_OUTPUT_KEY_ORDER: tuple[str, ...] = (
    REDUNDANCY_ROLE_KEY_ABOUT,
    REDUNDANCY_ROLE_KEY_SCHEMA_VERSION,
    REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME,
    REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS,
    REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4,
    REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4,
)


@dataclass
class FunctionalNicRole:
    """One functional I/O NIC entry in redundancy_role.json."""

    linux_ifname: str
    permanent_master_ipv4_cidr: str | None = None
    standby_backup_ipv4_cidr_before_takeover: str | None = None


def sanitize_linux_ifname(value: object, json_field: str, default: str) -> str:
    """Return a safe Linux interface name, or default if missing/invalid."""
    if not isinstance(value, str):
        if value is not None:
            logger.warning(
                "[热冗余] JSON 字段 %s 类型无效（期望字符串），使用默认网卡名 %r",
                json_field,
                default,
            )
        else:
            logger.warning(
                "[热冗余] JSON 字段 %s 缺失，使用默认网卡名 %r",
                json_field,
                default,
            )
        return default
    name = value.strip()
    if not name:
        logger.warning(
            "[热冗余] JSON 字段 %s 为空，使用默认网卡名 %r",
            json_field,
            default,
        )
        return default
    if len(name) > _LINUX_IFNAME_MAX_LEN or not _LINUX_IFNAME_RE.fullmatch(name):
        logger.warning(
            "[热冗余] JSON 字段 %s 值 %r 不是合法 Linux 网卡名，使用默认 %r",
            json_field,
            value,
            default,
        )
        return default
    return name


def _default_ifname_for_index(index: int) -> str:
    if index == 0:
        return DEFAULT_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME
    if index == 1:
        return DEFAULT_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME
    return f"eth{index}"


def _optional_ipv4_interface_string(raw: object, field: str) -> str | None:
    if raw is None:
        return None
    if not isinstance(raw, str):
        return None
    t = raw.strip()
    if not t:
        return None
    try:
        return str(ipaddress.IPv4Interface(t))
    except ValueError:
        logger.warning("[热冗余] JSON 字段 %s 不是有效 IPv4/CIDR: %r", field, raw)
        return None


def _parse_functional_nic_object(raw: object, index: int) -> FunctionalNicRole | None:
    field_prefix = f"{REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS}[{index}]"
    default_ifname = _default_ifname_for_index(index)

    if not isinstance(raw, dict):
        logger.warning(
            "[热冗余] %s 元素类型无效（期望对象），已跳过",
            field_prefix,
        )
        return None

    ifname = sanitize_linux_ifname(
        raw.get(FUNCTIONAL_NIC_KEY_LINUX_IFNAME),
        f"{field_prefix}.{FUNCTIONAL_NIC_KEY_LINUX_IFNAME}",
        default_ifname,
    )
    permanent = _optional_ipv4_interface_string(
        raw.get(FUNCTIONAL_NIC_KEY_PERMANENT_MASTER_CIDR),
        f"{field_prefix}.{FUNCTIONAL_NIC_KEY_PERMANENT_MASTER_CIDR}",
    )
    standby = _optional_ipv4_interface_string(
        raw.get(FUNCTIONAL_NIC_KEY_STANDBY_BACKUP_CIDR),
        f"{field_prefix}.{FUNCTIONAL_NIC_KEY_STANDBY_BACKUP_CIDR}",
    )
    return FunctionalNicRole(
        linux_ifname=ifname,
        permanent_master_ipv4_cidr=permanent,
        standby_backup_ipv4_cidr_before_takeover=standby,
    )


def _legacy_functional_nic_entries_from_doc(doc: dict) -> list[FunctionalNicRole]:
    """Build functional NIC list from schema v1 flat keys (0..2 entries)."""
    entries: list[FunctionalNicRole] = []
    if any(
        doc.get(k)
        for k in (
            _LEGACY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME,
            _LEGACY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR,
            _LEGACY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR,
        )
    ):
        entries.append(
            FunctionalNicRole(
                linux_ifname=sanitize_linux_ifname(
                    doc.get(_LEGACY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME),
                    _LEGACY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME,
                    DEFAULT_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME,
                ),
                permanent_master_ipv4_cidr=_optional_ipv4_interface_string(
                    doc.get(_LEGACY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR),
                    _LEGACY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR,
                ),
                standby_backup_ipv4_cidr_before_takeover=_optional_ipv4_interface_string(
                    doc.get(_LEGACY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR),
                    _LEGACY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR,
                ),
            )
        )
    if any(
        doc.get(k)
        for k in (
            _LEGACY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME,
            _LEGACY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR,
            _LEGACY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR,
        )
    ):
        entries.append(
            FunctionalNicRole(
                linux_ifname=sanitize_linux_ifname(
                    doc.get(_LEGACY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME),
                    _LEGACY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME,
                    DEFAULT_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME,
                ),
                permanent_master_ipv4_cidr=_optional_ipv4_interface_string(
                    doc.get(_LEGACY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR),
                    _LEGACY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR,
                ),
                standby_backup_ipv4_cidr_before_takeover=_optional_ipv4_interface_string(
                    doc.get(_LEGACY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR),
                    _LEGACY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR,
                ),
            )
        )
    return entries


def functional_nics_from_role_document(doc: dict) -> list[FunctionalNicRole]:
    """
    Return functional NIC entries from functional_nics array (0..N items).
    Empty array or absent key yields []. Legacy flat keys yield up to two entries.
    """
    raw_list = doc.get(REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS)

    if isinstance(raw_list, list):
        if not raw_list:
            return []
        entries: list[FunctionalNicRole] = []
        for i, raw in enumerate(raw_list):
            parsed = _parse_functional_nic_object(raw, i)
            if parsed is not None:
                entries.append(parsed)
        return entries

    if any(k in doc for k in _LEGACY_FLAT_KEYS):
        logger.info("[热冗余] 检测到旧版扁平字段，已迁移解析为 %s 数组", REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS)
        return _legacy_functional_nic_entries_from_doc(doc)

    return []


def redundancy_heartbeat_nic_from_role_document(doc: dict) -> str:
    return sanitize_linux_ifname(
        doc.get(REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME),
        REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME,
        DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME,
    )


def functional_nics_to_json_list(entries: list[FunctionalNicRole]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for entry in entries:
        obj: dict[str, str] = {FUNCTIONAL_NIC_KEY_LINUX_IFNAME: entry.linux_ifname}
        obj[FUNCTIONAL_NIC_KEY_PERMANENT_MASTER_CIDR] = entry.permanent_master_ipv4_cidr or ""
        obj[FUNCTIONAL_NIC_KEY_STANDBY_BACKUP_CIDR] = (
            entry.standby_backup_ipv4_cidr_before_takeover or ""
        )
        out.append(obj)
    return out


def _ordered_redundancy_role_doc_for_output(doc: dict) -> dict:
    out: dict[str, object] = {}
    for key in REDUNDANCY_ROLE_JSON_OUTPUT_KEY_ORDER:
        if key in doc:
            out[key] = doc[key]
    for key, value in doc.items():
        if key not in out and key not in _LEGACY_FLAT_KEYS:
            out[key] = value
    return out


def _ensure_functional_nics_in_document(doc: dict) -> None:
    entries = functional_nics_from_role_document(doc)
    doc[REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS] = functional_nics_to_json_list(entries)
    for legacy_key in _LEGACY_FLAT_KEYS:
        doc.pop(legacy_key, None)


def _ensure_role_document_metadata(doc: dict) -> None:
    if not str(doc.get(REDUNDANCY_ROLE_KEY_ABOUT, "")).strip():
        doc[REDUNDANCY_ROLE_KEY_ABOUT] = DEFAULT_REDUNDANCY_ROLE_ABOUT
    doc[REDUNDANCY_ROLE_KEY_SCHEMA_VERSION] = REDUNDANCY_ROLE_SCHEMA_VERSION
    doc[REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME] = (
        redundancy_heartbeat_nic_from_role_document(doc)
    )
    _ensure_functional_nics_in_document(doc)


def save_redundancy_role_document(json_path: Path, doc: dict) -> None:
    _ensure_role_document_metadata(doc)
    ordered = _ordered_redundancy_role_doc_for_output(doc)
    json_path.write_text(
        json.dumps(ordered, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def load_redundancy_role_document(project_root: Path) -> dict | None:
    json_path = project_root / REDUNDANCY_ROLE_FILENAME
    if not json_path.is_file():
        return None
    try:
        raw = json_path.read_text(encoding="utf-8", errors="replace")
        doc = json.loads(raw)
    except (OSError, json.JSONDecodeError) as e:
        logger.error("[热冗余] 读取 %s 失败: %s", json_path, e)
        return None
    if not isinstance(doc, dict):
        logger.error("[热冗余] %s 根类型必须是 JSON 对象", json_path)
        return None
    sv = doc.get(REDUNDANCY_ROLE_KEY_SCHEMA_VERSION, 1)
    if isinstance(sv, int) and sv > REDUNDANCY_ROLE_SCHEMA_VERSION:
        logger.warning(
            "[热冗余] %s 的 schema_version=%s 高于本运行时支持的最高版本 %s，将按当前解析逻辑继续尝试。",
            json_path,
            sv,
            REDUNDANCY_ROLE_SCHEMA_VERSION,
        )
    return doc


def peer_ipv4s_from_role_document(doc: dict) -> tuple[str | None, str | None]:
    raw_m = doc.get(REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4)
    raw_s = doc.get(REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4)
    if raw_m is None or raw_s is None:
        return None, None
    if not isinstance(raw_m, str) or not isinstance(raw_s, str):
        return None, None
    m = raw_m.strip()
    s = raw_s.strip()
    if not m or not s:
        return None, None
    try:
        master_ip = str(ipaddress.IPv4Address(m))
        standby_ip = str(ipaddress.IPv4Address(s))
    except ValueError:
        logger.warning(
            "[热冗余] JSON 中冗余口对端 IPv4 无效: %s=%r, %s=%r",
            REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4,
            raw_m,
            REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4,
            raw_s,
        )
        return None, None
    return master_ip, standby_ip


def permanent_master_cidrs_from_role_document(doc: dict) -> list[str | None]:
    return [e.permanent_master_ipv4_cidr for e in functional_nics_from_role_document(doc)]


def standby_backup_cidrs_from_role_document(doc: dict) -> list[str | None]:
    return [
        e.standby_backup_ipv4_cidr_before_takeover
        for e in functional_nics_from_role_document(doc)
    ]


def read_functional_cidrs_for_project(project_root: Path) -> list[str | None]:
    doc = load_redundancy_role_document(project_root)
    if doc is None:
        return []
    return permanent_master_cidrs_from_role_document(doc)


def read_standby_backup_cidrs_for_project(project_root: Path) -> list[str | None]:
    doc = load_redundancy_role_document(project_root)
    if doc is None:
        return []
    return standby_backup_cidrs_from_role_document(doc)


def _normalize_cidr_list(cidrs: list[str]) -> list[str]:
    return [str(ipaddress.IPv4Interface(c)) for c in cidrs]


def write_redundancy_role_functional_cidrs(
    role_json_path: Path, permanent_master_cidrs: list[str]
) -> bool:
    """
    Update permanent_master_ipv4_cidr for each functional_nics entry (by index).

    Returns True only when every received CIDR is written (counts must match local
    functional_nics length). Caller must align master/standby JSON array size and order.
    """
    root = role_json_path.parent
    doc = load_redundancy_role_document(root)
    if doc is None:
        logger.error(
            "[热冗余] 无法加载 %s，未写入主机同步的 permanent_master",
            role_json_path,
        )
        return False
    entries = functional_nics_from_role_document(doc)
    if not entries:
        logger.error(
            "[热冗余] %s 中无 functional_nics 条目，无法写入 %d 个主机功能 CIDR",
            role_json_path,
            len(permanent_master_cidrs),
        )
        return False
    try:
        normalized = _normalize_cidr_list(permanent_master_cidrs)
    except ValueError as e:
        logger.error("[热冗余] permanent_master CIDR 无效: %s", e)
        return False
    if len(normalized) != len(entries):
        logger.error(
            "[热冗余] 写入 permanent_master 失败: 主机发来 %d 个 CIDR，本机 functional_nics 有 %d 项 "
            "（主备 redundancy_role.json 中功能网卡数量与顺序须一致）",
            len(normalized),
            len(entries),
        )
        return False
    for i, cidr in enumerate(normalized):
        entries[i].permanent_master_ipv4_cidr = cidr
    doc[REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS] = functional_nics_to_json_list(entries)
    save_redundancy_role_document(role_json_path, doc)
    pairs = ", ".join(f"{e.linux_ifname}={e.permanent_master_ipv4_cidr}" for e in entries)
    logger.info(
        "[热冗余] 已写入 %d 项 permanent_master 到 %s: %s",
        len(entries),
        role_json_path,
        pairs,
    )
    return True


def write_redundancy_role_standby_backup_cidrs(
    role_json_path: Path, standby_backup_cidrs: list[str]
) -> bool:
    """Update standby_backup_ipv4_cidr_before_takeover for each functional_nics entry."""
    root = role_json_path.parent
    doc = load_redundancy_role_document(root)
    if doc is None:
        return False
    entries = functional_nics_from_role_document(doc)
    if not entries:
        return False
    try:
        normalized = _normalize_cidr_list(standby_backup_cidrs)
    except ValueError:
        return False
    if len(normalized) != len(entries):
        logger.warning(
            "[热冗余] 写入 standby_backup 时 CIDR 数量 (%d) 与 functional_nics 数量 (%d) 不一致",
            len(normalized),
            len(entries),
        )
        return False
    for i, cidr in enumerate(normalized):
        entries[i].standby_backup_ipv4_cidr_before_takeover = cidr
    doc[REDUNDANCY_ROLE_KEY_FUNCTIONAL_NICS] = functional_nics_to_json_list(entries)
    save_redundancy_role_document(role_json_path, doc)
    return True
