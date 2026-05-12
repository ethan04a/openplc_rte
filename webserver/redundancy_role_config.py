"""
Load, parse, and persist hot-redundancy role state in redundancy_role.json.

Includes Linux interface names for the redundancy heartbeat NIC and the two
functional I/O NICs. Defaults match historical hard-coded values (ens35 / ens33 / ens34).
"""

from __future__ import annotations

import ipaddress
import json
import re
from pathlib import Path

from webserver.logger import get_logger

logger, _ = get_logger("redundancy_role", use_buffer=True)

REDUNDANCY_ROLE_FILENAME = "redundancy_role.json"
REDUNDANCY_ROLE_SCHEMA_VERSION = 1

# Defaults when JSON omits a NIC field or supplies an invalid ifname (must match prior hard-coded behavior)
DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME = "ens35"
DEFAULT_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME = "ens33"
DEFAULT_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME = "ens34"

# Typical Linux userspace ifname length (IFNAMSIZ 16 including trailing NUL)
_LINUX_IFNAME_MAX_LEN = 15
_LINUX_IFNAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,15}$")

DEFAULT_REDUNDANCY_ROLE_ABOUT = (
    "Hot-redundancy role and addressing for this OpenPLC pair. "
    "Peer IPv4s must match the IPv4 on the interface named redundancy_heartbeat_nic_linux_ifname. "
    "functional_io_nic_*_linux_ifname: interfaces used for permanent_master_functional_* CIDR fields "
    "and for standby backup addresses. "
    "Omitted or invalid NIC names fall back to ens35 / ens33 / ens34."
)

REDUNDANCY_ROLE_KEY_ABOUT = "about"
REDUNDANCY_ROLE_KEY_SCHEMA_VERSION = "schema_version"
REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME = "redundancy_heartbeat_nic_linux_ifname"
REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME = "functional_io_nic_a_linux_ifname"
REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME = "functional_io_nic_b_linux_ifname"
REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4 = "master_ipv4_on_redundancy_heartbeat_nic"
REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4 = "standby_ipv4_on_redundancy_heartbeat_nic"
REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR = "permanent_master_functional_nic_a_ipv4_cidr"
REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR = "permanent_master_functional_nic_b_ipv4_cidr"
REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR = (
    "standby_backup_functional_nic_a_ipv4_cidr_before_takeover"
)
REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR = (
    "standby_backup_functional_nic_b_ipv4_cidr_before_takeover"
)

REDUNDANCY_ROLE_JSON_OUTPUT_KEY_ORDER: tuple[str, ...] = (
    REDUNDANCY_ROLE_KEY_ABOUT,
    REDUNDANCY_ROLE_KEY_SCHEMA_VERSION,
    REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME,
    REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME,
    REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME,
    REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4,
    REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4,
    REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR,
    REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR,
    REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR,
    REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR,
)


def sanitize_linux_ifname(value: object, json_field: str, default: str) -> str:
    """Return a safe Linux interface name, or default if missing/invalid."""
    if not isinstance(value, str):
        if value is not None:
            logger.warning(
                "[热冗余] JSON 字段 %s 类型无效（期望字符串），使用默认网卡名 %r",
                json_field,
                default,
            )
        return default
    name = value.strip()
    if not name:
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


def nic_interface_names_from_role_document(doc: dict) -> tuple[str, str, str]:
    """
    Read heartbeat + functional NIC Linux ifnames from JSON (defaults if absent/invalid).
    Call this before interpreting master/standby IPv4 so local addresses are read from the
    correct interfaces.
    """
    hb = sanitize_linux_ifname(
        doc.get(REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME),
        REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME,
        DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME,
    )
    fa = sanitize_linux_ifname(
        doc.get(REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME),
        REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME,
        DEFAULT_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME,
    )
    fb = sanitize_linux_ifname(
        doc.get(REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME),
        REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME,
        DEFAULT_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME,
    )
    return hb, fa, fb


def _ordered_redundancy_role_doc_for_output(doc: dict) -> dict:
    """Emit JSON with stable, human-friendly key order; preserve unknown keys last."""
    out: dict[str, object] = {}
    for key in REDUNDANCY_ROLE_JSON_OUTPUT_KEY_ORDER:
        if key in doc:
            out[key] = doc[key]
    for key, value in doc.items():
        if key not in out:
            out[key] = value
    return out


def _ensure_nic_interface_keys_in_document(doc: dict) -> None:
    """Normalize NIC name keys in-place before save (defaults if missing/invalid)."""
    hb, fa, fb = nic_interface_names_from_role_document(doc)
    doc[REDUNDANCY_ROLE_KEY_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME] = hb
    doc[REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME] = fa
    doc[REDUNDANCY_ROLE_KEY_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME] = fb


def _ensure_role_document_metadata(doc: dict) -> None:
    if not str(doc.get(REDUNDANCY_ROLE_KEY_ABOUT, "")).strip():
        doc[REDUNDANCY_ROLE_KEY_ABOUT] = DEFAULT_REDUNDANCY_ROLE_ABOUT
    doc.setdefault(REDUNDANCY_ROLE_KEY_SCHEMA_VERSION, REDUNDANCY_ROLE_SCHEMA_VERSION)
    _ensure_nic_interface_keys_in_document(doc)


def save_redundancy_role_document(json_path: Path, doc: dict) -> None:
    """Write redundancy role JSON with metadata filled and keys ordered for readability."""
    _ensure_role_document_metadata(doc)
    ordered = _ordered_redundancy_role_doc_for_output(doc)
    json_path.write_text(
        json.dumps(ordered, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def load_redundancy_role_document(project_root: Path) -> dict | None:
    """Load redundancy_role.json from project root, or None if missing or invalid."""
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
    sv = doc.get(REDUNDANCY_ROLE_KEY_SCHEMA_VERSION, REDUNDANCY_ROLE_SCHEMA_VERSION)
    if isinstance(sv, int) and sv > REDUNDANCY_ROLE_SCHEMA_VERSION:
        logger.warning(
            "[热冗余] %s 的 schema_version=%s 高于本运行时支持的最高版本 %s，将按当前解析逻辑继续尝试。",
            json_path,
            sv,
            REDUNDANCY_ROLE_SCHEMA_VERSION,
        )
    return doc


def peer_ipv4s_from_role_document(doc: dict) -> tuple[str | None, str | None]:
    """Read master/standby redundancy-NIC IPv4 from explicit JSON keys."""
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


def _optional_ipv4_interface_from_doc(doc: dict, key: str) -> str | None:
    raw = doc.get(key)
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
        logger.warning("[热冗余] JSON 字段 %s 不是有效 IPv4/CIDR: %r", key, raw)
        return None


def functional_cidrs_from_role_document(doc: dict) -> tuple[str | None, str | None]:
    """Read permanent_master_functional_nic_* (host-recorded functional interfaces)."""
    a = _optional_ipv4_interface_from_doc(doc, REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR)
    b = _optional_ipv4_interface_from_doc(doc, REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR)
    return a, b


def standby_backup_cidrs_from_role_document(doc: dict) -> tuple[str | None, str | None]:
    """Read standby_backup_functional_nic_* (saved before temporary takeover)."""
    a = _optional_ipv4_interface_from_doc(doc, REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR)
    b = _optional_ipv4_interface_from_doc(doc, REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR)
    return a, b


def read_functional_cidrs_for_project(project_root: Path) -> tuple[str | None, str | None]:
    """Load redundancy_role.json and return permanent_master_functional_nic_* CIDRs."""
    doc = load_redundancy_role_document(project_root)
    if doc is None:
        return None, None
    return functional_cidrs_from_role_document(doc)


def read_standby_backup_cidrs_for_project(project_root: Path) -> tuple[str | None, str | None]:
    """Load redundancy_role.json and return standby_backup_functional_nic_* CIDRs."""
    doc = load_redundancy_role_document(project_root)
    if doc is None:
        return None, None
    return standby_backup_cidrs_from_role_document(doc)


def write_redundancy_role_functional_cidrs(role_json_path: Path, nic_a_cidr: str, nic_b_cidr: str) -> None:
    """Update permanent_master_functional_nic_* (merge with existing document)."""
    root = role_json_path.parent
    doc = load_redundancy_role_document(root)
    if doc is None:
        return
    doc[REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR] = nic_a_cidr
    doc[REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR] = nic_b_cidr
    save_redundancy_role_document(role_json_path, doc)


def write_redundancy_role_standby_backup_cidrs(
    role_json_path: Path, nic_a_cidr: str, nic_b_cidr: str
) -> None:
    """Update standby_backup_functional_nic_*."""
    root = role_json_path.parent
    doc = load_redundancy_role_document(root)
    if doc is None:
        return
    doc[REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR] = nic_a_cidr
    doc[REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR] = nic_b_cidr
    save_redundancy_role_document(role_json_path, doc)
