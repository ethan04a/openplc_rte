import ipaddress
import os
import socket
import struct
import subprocess
import threading
import time
from pathlib import Path

# psutil is optional - not available on MSYS2/Cygwin platforms
try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    psutil = None
    HAS_PSUTIL = False

from webserver.logger import get_logger
from webserver.redundancy_role_config import (
    DEFAULT_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME,
    DEFAULT_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME,
    DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME,
    REDUNDANCY_ROLE_FILENAME,
    REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4,
    REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR,
    REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR,
    REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR,
    REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR,
    REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4,
    load_redundancy_role_document,
    nic_interface_names_from_role_document,
    peer_ipv4s_from_role_document,
    read_functional_cidrs_for_project,
    read_standby_backup_cidrs_for_project,
    write_redundancy_role_functional_cidrs,
    write_redundancy_role_standby_backup_cidrs,
)
from webserver.unixclient import (
    IMAGE_SNAPSHOT_EXPECTED_BYTES,
    IMAGE_SNAPSHOT_PROTOCOL_VERSION,
    SyncUnixClient,
)
from webserver.unixserver import UnixLogServer

logger, buffer = get_logger("logger", use_buffer=True)

# Log once if psutil is not available
if not HAS_PSUTIL:
    logger.info("psutil not available - process detection features disabled")


MAX_RAPID_CRASHES = 3
RAPID_CRASH_WINDOW = 30  # seconds

# Hot redundancy: TCP heartbeat and ports; interface names come from redundancy_role.json (with defaults)
REDUNDANCY_HEARTBEAT_PORT = 57575
REDUNDANCY_IMAGE_SYNC_PORT = 57576
REDUNDANCY_IMAGE_MAGIC = b"OPIM"
# Hot redundancy: HTTP peer sync (receive-program / sync-role-ini header X-OpenPLC-Redundancy-Sync)
REDUNDANCY_SYNC_SECRET = "openplc"
REDUNDANCY_HB_PAYLOAD = b"OPENPLC_REDUNDANCY_HB_V1\n"
REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC = 1.0
REDUNDANCY_STANDBY_RECV_IDLE_SEC = 1.0
# Standby: seconds without TCP heartbeat before ping master for failover decision
REDUNDANCY_STANDBY_LOST_THRESHOLD_SEC = 5

# Throttle STATUS polling used by redundancy I/O mirror gating (avoid unix chatter).
PLC_STATUS_CACHE_TTL_SEC = 0.2


def _tcp_recv_exact(conn: socket.socket, n: int, timeout: float | None) -> bytes | None:
    """Read exactly n bytes from TCP stream."""
    if n <= 0:
        return b""
    conn.settimeout(timeout)
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        try:
            chunk = conn.recv(remaining)
        except (TimeoutError, socket.timeout, OSError):
            return None
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


class RuntimeManager:
    def __init__(self, runtime_path, plc_socket, log_socket, print_debug=False):
        self.runtime_path = runtime_path
        self.plc_socket = plc_socket
        self.log_socket = log_socket
        self.print_debug = print_debug
        self.process = None
        self.log_server = UnixLogServer(log_socket)
        self.runtime_socket = SyncUnixClient(plc_socket)
        self.monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self.running = False
        self._crash_lock = threading.Lock()
        self._crash_times: list[float] = []
        self._safe_mode = False

        # Hot redundancy: interface names from redundancy_role.json (defaults until _evaluate_redundancy_role)
        self._redundancy_heartbeat_nic = DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME
        self._redundancy_functional_nic_a = DEFAULT_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME
        self._redundancy_functional_nic_b = DEFAULT_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME
        self.is_master = False
        self.is_redundancy = False
        self._redundancy_master_ip: str | None = None
        self._redundancy_standby_ip: str | None = None
        self._redundancy_local_ens35_ip: str | None = None
        self._heartbeat_stop = threading.Event()
        self._heartbeat_threads: list[threading.Thread] = []
        # 备机暂时升主后为 True；升主后仍监听冗余口，收到原主机心跳载荷则异步回切
        self._standby_switched_to_master = False
        # True → plc_main 影子备机；暂时升主后为 False（非影子 PLC）
        self._plc_shadow_standby = False
        # 备机已暂时升主且 PLC 非影子（永久 is_master 仍由 redundancy_role.json 中冗余口对端 IPv4 决定）
        self._promoted_standby_acting_master = False
        # 备升主过程中避免 monitor 线程误重启 PLC
        self._manual_plc_restart_in_progress = False
        # 主机本地记录完成后，等待“TCP 心跳已连接备机”时再同步 permanent_master_functional_* 到备机
        self._functional_lines_pending_sync: tuple[str, str] | None = None
        self._functional_sync_lock = threading.Lock()
        self._plc_status_cache_lock = threading.Lock()
        self._plc_status_cache_monotonic: float = 0.0
        self._plc_status_cache_running: bool = False

    @staticmethod
    def _openplc_project_root() -> Path:
        """Repository / install root (parent of webserver/)."""
        return Path(__file__).resolve().parent.parent

    @staticmethod
    def _ipv4_for_interface(ifname: str) -> str | None:
        """Return first IPv4 address on interface ifname, or None."""
        if HAS_PSUTIL and psutil is not None:
            addrs = psutil.net_if_addrs().get(ifname)
            if addrs:
                for entry in addrs:
                    if entry.family == socket.AF_INET and entry.address:
                        return str(entry.address)
        try:
            out = subprocess.check_output(
                ["ip", "-4", "-o", "addr", "show", "dev", ifname],
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return None
        for line in out.splitlines():
            parts = line.split()
            for i, token in enumerate(parts):
                if token == "inet" and i + 1 < len(parts):
                    return parts[i + 1].split("/")[0].strip()
        return None

    @staticmethod
    def _ipv4_cidr_for_interface(ifname: str) -> str | None:
        """First IPv4 address on ifname as 'a.b.c.d/prefix', or None."""
        try:
            out = subprocess.check_output(
                ["ip", "-4", "-o", "addr", "show", "dev", ifname],
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return None
        for line in out.splitlines():
            parts = line.split()
            for i, token in enumerate(parts):
                if token == "inet" and i + 1 < len(parts):
                    cidr = parts[i + 1].strip()
                    try:
                        return str(ipaddress.IPv4Interface(cidr))
                    except ValueError:
                        continue
        return None

    @staticmethod
    def _apply_ipv4_cidr_to_linux_interface(ifname: str, cidr: str) -> bool:
        """Replace primary IPv4 on interface using ip(8). Requires appropriate privileges."""
        try:
            subprocess.run(
                ["ip", "addr", "flush", "dev", ifname],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            r = subprocess.run(
                ["ip", "addr", "add", cidr, "dev", ifname],
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if r.returncode != 0:
                logger.error(
                    "[热冗余] ip addr add 失败 %s %s: %s",
                    ifname,
                    cidr,
                    (r.stderr or r.stdout or "").strip(),
                )
                return False
            subprocess.run(
                ["ip", "link", "set", ifname, "up"],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
            logger.info("[热冗余] 已为 %s 设置地址 %s", ifname, cidr)
            return True
        except (OSError, subprocess.TimeoutExpired) as e:
            logger.error("[热冗余] 配置 %s 地址异常: %s", ifname, e)
            return False

    def _record_functional_ips_and_sync_standby_thread(self) -> None:
        """主机：将 JSON 配置的功能口网卡 IPv4/掩码写入 permanent_master_functional_*。"""
        try:
            project_root = self._openplc_project_root()
            role_json_path = project_root / REDUNDANCY_ROLE_FILENAME
            if not role_json_path.is_file():
                return
            c33 = self._ipv4_cidr_for_interface(self._redundancy_functional_nic_a)
            c34 = self._ipv4_cidr_for_interface(self._redundancy_functional_nic_b)
            if not c33 or not c34:
                logger.warning(
                    "[热冗余] 功能 IP 记录跳过：%s=%s, %s=%s（需两网卡均有 IPv4）",
                    self._redundancy_functional_nic_a,
                    c33,
                    self._redundancy_functional_nic_b,
                    c34,
                )
                return
            write_redundancy_role_functional_cidrs(role_json_path, c33, c34)
            logger.info(
                "[热冗余] 已记录功能口地址到 %s 字段 %s/%s: %s=%s, %s=%s",
                role_json_path,
                REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR,
                REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR,
                self._redundancy_functional_nic_a,
                c33,
                self._redundancy_functional_nic_b,
                c34,
            )
            with self._functional_sync_lock:
                self._functional_lines_pending_sync = (c33, c34)
            logger.info(
                "[热冗余] permanent_master_functional_* 已标记待同步，等待主备 TCP 心跳连接建立后再推送。"
            )
        except Exception as e:
            logger.error("[热冗余] 功能 IP 记录异常: %s", e)

    def _sync_functional_lines_after_tcp_connect(self) -> None:
        """主机 TCP 心跳连上备机后，再尝试同步 redundancy_role.json 的 permanent_master_functional_* 到备机。"""
        standby_ip = self._redundancy_standby_ip
        if not standby_ip:
            return
        with self._functional_sync_lock:
            pending = self._functional_lines_pending_sync
        if not pending:
            return

        c33, c34 = pending
        try:
            from webserver.redundancy_program_sync import push_role_ini_functional_to_standby

            logger.info(
                "[热冗余][主机] TCP 心跳连接已建立，开始同步 %s 中 permanent_master_functional_* 到备机 %s。",
                REDUNDANCY_ROLE_FILENAME,
                standby_ip,
            )
            pushed = push_role_ini_functional_to_standby(
                standby_ip, c33, c34, REDUNDANCY_SYNC_SECRET
            )
            if pushed:
                ok_a = self._apply_ipv4_cidr_to_linux_interface(self._redundancy_functional_nic_a, c33)
                ok_b = self._apply_ipv4_cidr_to_linux_interface(self._redundancy_functional_nic_b, c34)
                if not ok_a or not ok_b:
                    logger.warning(
                        "[热冗余][主机] 同步成功后配置本机功能口部分失败: %s=%s, %s=%s",
                        self._redundancy_functional_nic_a,
                        ok_a,
                        self._redundancy_functional_nic_b,
                        ok_b,
                    )
        except Exception as e:
            logger.error("[热冗余][主机] TCP 建连后同步 permanent_master_functional_* 异常: %s", e)

    def _evaluate_redundancy_role(self) -> None:
        """
        Load redundancy_role.json: first resolve NIC names, then compare local heartbeat-NIC IPv4
        to configured master/standby to set is_master / is_redundancy.
        """
        self.is_master = False
        self.is_redundancy = False
        self._redundancy_master_ip = None
        self._redundancy_standby_ip = None
        self._redundancy_local_ens35_ip = None
        self._plc_shadow_standby = False
        self._promoted_standby_acting_master = False
        self._redundancy_heartbeat_nic = DEFAULT_REDUNDANCY_HEARTBEAT_NIC_LINUX_IFNAME
        self._redundancy_functional_nic_a = DEFAULT_FUNCTIONAL_IO_NIC_A_LINUX_IFNAME
        self._redundancy_functional_nic_b = DEFAULT_FUNCTIONAL_IO_NIC_B_LINUX_IFNAME

        project_root = self._openplc_project_root()
        role_json_path = project_root / REDUNDANCY_ROLE_FILENAME

        if not role_json_path.is_file():
            logger.info(
                "[热冗余] 未找到 %s，本机不启用热冗余功能（is_redundancy=False, is_master=False）。",
                role_json_path,
            )
            return

        doc = load_redundancy_role_document(project_root)
        if doc is None:
            logger.info(
                "[热冗余] 冗余角色文件无效或无法解析，本机不启用热冗余功能（is_redundancy=False, is_master=False）。"
            )
            return

        hb_nic, fa_nic, fb_nic = nic_interface_names_from_role_document(doc)
        self._redundancy_heartbeat_nic = hb_nic
        self._redundancy_functional_nic_a = fa_nic
        self._redundancy_functional_nic_b = fb_nic
        logger.info(
            "[热冗余] 开始冗余角色检测: 项目根目录=%s, 配置文件=%s；已从 JSON 解析网卡名 "
            "（无效或缺失字段时使用默认）: 冗余心跳=%s, 功能口A=%s, 功能口B=%s",
            project_root,
            role_json_path,
            hb_nic,
            fa_nic,
            fb_nic,
        )

        master_ip, standby_ip = peer_ipv4s_from_role_document(doc)
        if master_ip is None or standby_ip is None:
            logger.info(
                "[热冗余] JSON 中缺少有效的 %s / %s，本机不启用热冗余功能（is_redundancy=False, is_master=False）。",
                REDUNDANCY_ROLE_KEY_MASTER_REDUNDANCY_IPV4,
                REDUNDANCY_ROLE_KEY_STANDBY_REDUNDANCY_IPV4,
            )
            return

        logger.info(
            "[热冗余] 从 %s 读取冗余口对端: 主机=%s, 备机=%s",
            REDUNDANCY_ROLE_FILENAME,
            master_ip,
            standby_ip,
        )

        self._redundancy_master_ip = master_ip
        self._redundancy_standby_ip = standby_ip
        logger.info(
            "[热冗余] 配置摘要: 主机 IP=%s, 备机 IP=%s（将与本机网卡 %s 的 IPv4 比较）",
            master_ip,
            standby_ip,
            self._redundancy_heartbeat_nic,
        )

        local_ip = self._ipv4_for_interface(self._redundancy_heartbeat_nic)
        self._redundancy_local_ens35_ip = local_ip
        if local_ip is None:
            logger.error(
                "[热冗余] 无法读取网卡 %s 的 IPv4 地址，本机不启用热冗余。"
                "请确认网卡存在且已配置地址。",
                self._redundancy_heartbeat_nic,
            )
            return

        logger.info(
            "[热冗余] 本机网卡 %s 的 IPv4 为: %s",
            self._redundancy_heartbeat_nic,
            local_ip,
        )

        if local_ip == master_ip:
            self.is_redundancy = True
            self.is_master = True
            logger.info(
                "[热冗余] 本机 IPv4 与配置中的主机一致，角色=主机。"
                "is_redundancy=True, is_master=True。"
                "将通过 TCP 经 %s 主动连接备机并每秒发送心跳（目标端口 %d）。",
                self._redundancy_heartbeat_nic,
                REDUNDANCY_HEARTBEAT_PORT,
            )
            threading.Thread(
                target=self._record_functional_ips_and_sync_standby_thread,
                daemon=True,
                name="redundancy-record-functional-ip",
            ).start()
            return

        if local_ip == standby_ip:
            self.is_redundancy = True
            self.is_master = False
            self._plc_shadow_standby = not self._standby_switched_to_master
            logger.info(
                "[热冗余] 本机 IPv4 与配置中的备机一致，角色=备机。"
                "is_redundancy=True, is_master=False。"
                "将在 %s 上监听 TCP 端口 %d，接收主机心跳。",
                self._redundancy_heartbeat_nic,
                REDUNDANCY_HEARTBEAT_PORT,
            )
            if self._plc_shadow_standby:
                logger.info(
                    "[热冗余] 备机将使用 plc_main --shadow-standby：运行相同 PLC 逻辑，"
                    "不加载现场 I/O 插件（仅与主机冗余通信由 Web 层负责）。"
                )
            return

        logger.warning(
            "[热冗余] 本机 %s 地址 %s 既不是配置的主机 %s 也不是备机 %s，"
            "不启用热冗余（is_redundancy=False, is_master=False）。",
            self._redundancy_heartbeat_nic,
            local_ip,
            master_ip,
            standby_ip,
        )

    def _shutdown_redundancy_heartbeat_threads(self) -> None:
        self._heartbeat_stop.set()
        for t in list(self._heartbeat_threads):
            t.join(timeout=3)
        self._heartbeat_threads.clear()

    def _redundancy_ping_master_ipv4_once(self, ip: str) -> bool:
        """单次 ICMP ping（Linux iputils），成功返回 True。"""
        try:
            proc = subprocess.run(
                ["ping", "-c", "1", "-W", "2", ip],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            return proc.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return False

    def _redundancy_ping_master_reachable(self) -> bool:
        """
        分别 ping 主机两条功能网口在 redundancy_role.json 中 permanent_master_functional_* 的 IPv4，不用冗余心跳口 IP。
        只要有一条功能 IP 能 ping 通即视为主机仍可达；两条均无响应则判定为故障（返回 False）。
        """
        project_root = self._openplc_project_root()
        role_json_path = project_root / REDUNDANCY_ROLE_FILENAME
        c33, c34 = read_functional_cidrs_for_project(project_root)

        targets: list[tuple[str, str]] = []
        if c33:
            targets.append(
                (self._redundancy_functional_nic_a, str(ipaddress.IPv4Interface(c33).ip))
            )
        if c34:
            targets.append(
                (self._redundancy_functional_nic_b, str(ipaddress.IPv4Interface(c34).ip))
            )

        if not targets:
            logger.warning(
                "[热冗余][备机] 故障探测：无法从 %s 读取主机功能 IPv4（字段 %s / %s），"
                "无法进行 ping，按主机不可达处理。",
                role_json_path,
                REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR,
                REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR,
            )
            return False

        any_ok = False
        for nic_name, ip in targets:
            ok = self._redundancy_ping_master_ipv4_once(ip)
            if ok:
                logger.info(
                    "[热冗余][备机] 故障探测：接口 %s 对应地址 %s 可达。",
                    nic_name,
                    ip,
                )
                any_ok = True
            else:
                logger.info(
                    "[热冗余][备机] 故障探测：接口 %s 对应地址 %s 不可达。",
                    nic_name,
                    ip,
                )

        if any_ok:
            return True
        logger.info(
            "[热冗余][备机] 故障探测：所有已配置的主机功能地址均无 ICMP 响应，判定为故障。"
        )
        return False

    def _redundancy_trigger_standby_to_master_switch(self) -> None:
        """
        备升主（暂时）：先将本机 JSON 配置的两块功能网卡地址写入 standby_backup_functional_*，
        再应用 permanent_master_functional_* 中记录的主机功能 IP；
        PLC 非影子；is_master 不变；继续监听冗余口，收到原主机心跳载荷后异步回切。
        """
        logger.info("[热冗余][备机] 备机升主机已触发（暂时，JSON 中永久主备角色不变）")
        project_root = self._openplc_project_root()
        role_json_path = project_root / REDUNDANCY_ROLE_FILENAME
        standby33 = self._ipv4_cidr_for_interface(self._redundancy_functional_nic_a)
        standby34 = self._ipv4_cidr_for_interface(self._redundancy_functional_nic_b)
        if not standby33 or not standby34:
            logger.error(
                "[热冗余][备机] 升主中止：无法读取本机 %s/%s 的 IPv4/CIDR",
                self._redundancy_functional_nic_a,
                self._redundancy_functional_nic_b,
            )
            return
        try:
            write_redundancy_role_standby_backup_cidrs(role_json_path, standby33, standby34)
            logger.info(
                "[热冗余][备机] 已写入备机功能地址到 %s 字段 %s/%s: %s, %s",
                role_json_path,
                REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_A_CIDR,
                REDUNDANCY_ROLE_KEY_STANDBY_BACKUP_FUNCTIONAL_B_CIDR,
                standby33,
                standby34,
            )
        except OSError as e:
            logger.error("[热冗余][备机] 写入 standby_backup_functional_* 失败: %s", e)
            return

        c33, c34 = read_functional_cidrs_for_project(project_root)
        if not c33 or not c34:
            logger.error(
                "[热冗余][备机] 升主中止：%s 缺少有效的 %s / %s（需先由主机记录功能 IP）",
                role_json_path,
                REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_A_CIDR,
                REDUNDANCY_ROLE_KEY_PERMANENT_MASTER_FUNCTIONAL_B_CIDR,
            )
            return
        if not self._apply_ipv4_cidr_to_linux_interface(self._redundancy_functional_nic_a, c33):
            return
        if not self._apply_ipv4_cidr_to_linux_interface(self._redundancy_functional_nic_b, c34):
            return

        self._standby_switched_to_master = True
        self._plc_shadow_standby = False
        self._promoted_standby_acting_master = True

        try:
            if self._try_redundancy_shadow_exit():
                logger.info(
                    "[热冗余][备机] 已通过 REDUNDANCY_SHADOW_EXIT 平滑升主（保留 PLC 进程与 I/O 镜像）"
                )
            else:
                self._restart_plc_core_after_takeover()
        except Exception as e:
            logger.error("[热冗余][备机] 升主后切换 PLC 核心失败: %s", e)
        # Keep redundancy heartbeat/image threads alive; switch behavior via state flags only.

    def _try_redundancy_shadow_exit(self) -> bool:
        """Try to enable field I/O in-process without restarting plc_main."""
        try:
            self._safe_connect_runtime_socket()
            if not self.runtime_socket.is_connected():
                return False
            resp = self.runtime_socket.send_and_receive(
                "REDUNDANCY_SHADOW_EXIT\n",
                timeout=120.0,
            )
            return resp == "REDUNDANCY_SHADOW_EXIT:OK"
        except (OSError, RuntimeError, TypeError, ValueError) as e:
            logger.warning("[热冗余][备机] REDUNDANCY_SHADOW_EXIT 不可用或失败，将回退为重启 plc_main: %s", e)
            return False

    def _redundancy_trigger_failback_to_standby(self) -> None:
        """收到原主机冗余心跳后回切：按 standby_backup_functional_* 恢复两块功能网卡，影子 PLC。"""
        logger.info("[热冗余][备机] 回切备机已触发")
        project_root = self._openplc_project_root()
        role_json_path = project_root / REDUNDANCY_ROLE_FILENAME
        b33, b34 = read_standby_backup_cidrs_for_project(project_root)
        if b33 and b34:
            self._apply_ipv4_cidr_to_linux_interface(self._redundancy_functional_nic_a, b33)
            self._apply_ipv4_cidr_to_linux_interface(self._redundancy_functional_nic_b, b34)
        else:
            logger.warning(
                "[热冗余][备机] standby_backup_functional_* 无效，跳过恢复功能口 IP（请检查 %s）",
                role_json_path,
            )

        self._standby_switched_to_master = False
        self._promoted_standby_acting_master = False
        self._plc_shadow_standby = True
        try:
            self._restart_plc_core_shadow_standby_after_failback()
        except Exception as e:
            logger.warning("[热冗余][备机] 回切后重启影子 PLC 失败（可手动重启运行时）: %s", e)
        # Keep redundancy heartbeat/image threads alive; switch behavior via state flags only.

    def _restart_plc_core_after_takeover(self) -> None:
        """终止当前 plc_main 并以非影子方式重启并 START（需 root/rt 权限场景与常规一致）。"""
        self._manual_plc_restart_in_progress = True
        try:
            try:
                self.runtime_socket.send_message("STOP\n")
            except (OSError, socket.error, RuntimeError):
                pass
            time.sleep(0.5)
            self._safe_close_runtime_socket()
            if self.process:
                if HAS_PSUTIL and isinstance(self.process, psutil.Process):
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except (psutil.TimeoutExpired, psutil.Error):
                        self.process.kill()
                elif isinstance(self.process, subprocess.Popen):
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
                        self.process.kill()
                self.process = None
            time.sleep(0.5)
            self._safe_start_log_server()
            cmd = [self.runtime_path]
            if self.print_debug:
                cmd.append("--print-debug")
            self.process = subprocess.Popen(cmd)
            time.sleep(1)
            self._safe_connect_runtime_socket()
            self.start_plc()
            logger.info("[热冗余][备机] 已切换为非影子 plc_main 并已下发 START")
        finally:
            self._manual_plc_restart_in_progress = False

    def _restart_plc_core_shadow_standby_after_failback(self) -> None:
        """回切备机后重启影子 plc_main。"""
        self._manual_plc_restart_in_progress = True
        try:
            try:
                self.runtime_socket.send_message("STOP\n")
            except (OSError, socket.error, RuntimeError):
                pass
            time.sleep(0.5)
            self._safe_close_runtime_socket()
            if self.process:
                if HAS_PSUTIL and isinstance(self.process, psutil.Process):
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except (psutil.TimeoutExpired, psutil.Error):
                        self.process.kill()
                elif isinstance(self.process, subprocess.Popen):
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except (subprocess.TimeoutExpired, subprocess.SubprocessError):
                        self.process.kill()
                self.process = None
            time.sleep(0.5)
            self._safe_start_log_server()
            cmd = [self.runtime_path]
            if self.print_debug:
                cmd.append("--print-debug")
            cmd.append("--shadow-standby")
            self.process = subprocess.Popen(cmd)
            time.sleep(1)
            self._safe_connect_runtime_socket()
            self.start_plc()
            logger.info("[热冗余][备机] 已回切为影子 plc_main 并已下发 START")
        finally:
            self._manual_plc_restart_in_progress = False

    def _standby_tick_lost_times(self, lost_times: int) -> tuple[int, bool]:
        """
        After LostTimes 每秒 +1：超过阈值则 ping 主机；不通则触发备升主。
        Returns (new_lost_times, switched True if 备升主已触发).
        """
        if lost_times <= REDUNDANCY_STANDBY_LOST_THRESHOLD_SEC:
            return lost_times, False
        if self._redundancy_ping_master_reachable():
            logger.info(
                "[热冗余][备机] LostTimes=%d 已超过阈值 %d 秒，但主机功能口仍有可达地址，清零计数。",
                lost_times,
                REDUNDANCY_STANDBY_LOST_THRESHOLD_SEC,
            )
            return 0, False
        self._schedule_async_standby_to_master_switch()
        return 0, True

    def _schedule_async_standby_to_master_switch(self) -> None:
        """备机心跳线程内触发升主须异步执行，避免 shutdown/join 当前线程死锁。"""

        def runner() -> None:
            time.sleep(0.05)
            try:
                self._redundancy_trigger_standby_to_master_switch()
            except Exception as e:
                logger.error("[热冗余][备机] 异步备升主异常: %s", e)

        threading.Thread(target=runner, daemon=True, name="async-standby-to-master").start()

    def _schedule_async_failback_to_standby(self) -> None:
        """备机心跳线程内触发回切须异步执行。"""

        def runner() -> None:
            time.sleep(0.05)
            try:
                self._redundancy_trigger_failback_to_standby()
            except Exception as e:
                logger.warning("[热冗余][备机] 异步回切异常: %s", e)

        threading.Thread(target=runner, daemon=True, name="async-failback-standby").start()

    def _redundancy_master_tcp_heartbeat_loop(self) -> None:
        """
        每秒：TCP 已连接则发送心跳；否则尝试连接备机。无数、无超时切换、永久循环直至 stop。
        """
        local_ip = self._redundancy_local_ens35_ip
        peer_ip = self._redundancy_standby_ip
        if not local_ip or not peer_ip:
            return
        sock: socket.socket | None = None
        first_send_logged = False
        while not self._heartbeat_stop.is_set():
            if sock is None:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.bind((local_ip, 0))
                    sock.settimeout(10.0)
                    sock.connect((peer_ip, REDUNDANCY_HEARTBEAT_PORT))
                    self._sync_functional_lines_after_tcp_connect()
                except OSError as e:
                    logger.warning(
                        "[热冗余][主机] 连接对端 TCP %s:%d 失败，将在 %.1f 秒后重试: %s",
                        peer_ip,
                        REDUNDANCY_HEARTBEAT_PORT,
                        REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC,
                        e,
                    )
                    if sock is not None:
                        try:
                            sock.close()
                        except OSError:
                            pass
                        sock = None
                    if self._heartbeat_stop.wait(REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC):
                        break
                    continue

            try:
                sock.sendall(REDUNDANCY_HB_PAYLOAD)
            except OSError as e:
                logger.warning(
                    "[热冗余][主机] 发送 TCP 心跳失败，将断开并重连备机: %s",
                    e,
                )
                try:
                    sock.close()
                except OSError:
                    pass
                sock = None
                if self._heartbeat_stop.wait(REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC):
                    break
                continue

            if not first_send_logged:
                logger.info(
                    "[热冗余][主机] 第一次开始发送 TCP 心跳包（对端=%s:%d，本机源地址=%s）。",
                    peer_ip,
                    REDUNDANCY_HEARTBEAT_PORT,
                    local_ip,
                )
                first_send_logged = True

            if self._heartbeat_stop.wait(REDUNDANCY_MASTER_HEARTBEAT_INTERVAL_SEC):
                break

        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        logger.info("[热冗余][主机] TCP 心跳发送线程已退出。")

    def _redundancy_standby_tcp_heartbeat_loop(self) -> None:
        """
        备机：纯备机态 LostTimes / 升主；暂时升主后仍监听，accept 超时不计数，
        收到原主机发来的心跳载荷后异步回切（影子 PLC + 按 standby_backup_functional_* 恢复功能 IP）。
        """
        local_ip = self._redundancy_local_ens35_ip
        master_redundancy_ip = self._redundancy_master_ip
        if not local_ip:
            return
        server: socket.socket | None = None
        lost_times = 0
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((local_ip, REDUNDANCY_HEARTBEAT_PORT))
            server.listen(1)
            logger.info(
                "[热冗余][备机] TCP 冗余心跳监听已建立：绑定地址=%s:%d，等待主机连入。",
                local_ip,
                REDUNDANCY_HEARTBEAT_PORT,
            )
            server.settimeout(REDUNDANCY_STANDBY_RECV_IDLE_SEC)
            while not self._heartbeat_stop.is_set():
                promoted = self._standby_switched_to_master and self._promoted_standby_acting_master
                if promoted:
                    try:
                        client, addr = server.accept()
                    except TimeoutError:
                        continue
                    except OSError as e:
                        if self._heartbeat_stop.is_set():
                            break
                        logger.error("[热冗余][备机] accept 失败: %s", e)
                        continue
                    if master_redundancy_ip and addr[0] != master_redundancy_ip:
                        logger.warning(
                            "[热冗余][备机] 升主状态下收到非配置主机冗余 IP 的连入 %s，忽略",
                            addr[0],
                        )
                        try:
                            client.close()
                        except OSError:
                            pass
                        continue
                    logger.info(
                        "[热冗余][备机] 原主机 TCP 已连入（升主监听态），对端=%s:%d，等待心跳包以回切",
                        addr[0],
                        addr[1],
                    )
                    buf = b""
                    client_live = client
                    try:
                        client_live.settimeout(REDUNDANCY_STANDBY_RECV_IDLE_SEC)
                        while (
                            not self._heartbeat_stop.is_set()
                            and self._standby_switched_to_master
                            and self._promoted_standby_acting_master
                        ):
                            try:
                                chunk = client_live.recv(4096)
                            except (TimeoutError, socket.timeout):
                                continue
                            except OSError as e:
                                logger.warning("[热冗余][备机] 升主监听 recv 错误: %s", e)
                                break
                            if not chunk:
                                break
                            buf += chunk
                            while True:
                                idx = buf.find(REDUNDANCY_HB_PAYLOAD)
                                if idx < 0:
                                    if len(buf) > 65536:
                                        buf = buf[-4096:]
                                    break
                                buf = buf[idx + len(REDUNDANCY_HB_PAYLOAD) :]
                                logger.info("[热冗余][备机] 收到原主机心跳，触发自动回切")
                                self._schedule_async_failback_to_standby()
                                try:
                                    client_live.close()
                                except OSError:
                                    pass
                                client_live = None
                                break
                            if client_live is None:
                                break
                    finally:
                        if client_live is not None:
                            try:
                                client_live.close()
                            except OSError:
                                pass
                    continue

                try:
                    client, addr = server.accept()
                except TimeoutError:
                    lost_times += 1
                    lost_times, _ = self._standby_tick_lost_times(lost_times)
                    continue
                except OSError as e:
                    if self._heartbeat_stop.is_set():
                        break
                    logger.error("[热冗余][备机] accept 失败: %s", e)
                    continue

                logger.info(
                    "[热冗余][备机] 已接受主机 TCP 连接，对端=%s:%d，LostTimes 清零。",
                    addr[0],
                    addr[1],
                )
                lost_times = 0
                buf = b""
                try:
                    client.settimeout(REDUNDANCY_STANDBY_RECV_IDLE_SEC)
                    while (
                        not self._heartbeat_stop.is_set()
                        and not self._standby_switched_to_master
                    ):
                        try:
                            chunk = client.recv(4096)
                        except (TimeoutError, socket.timeout):
                            lost_times += 1
                            if lost_times > REDUNDANCY_STANDBY_LOST_THRESHOLD_SEC:
                                logger.info("[热冗余][备机] LostTimes 增加到 %d", lost_times)
                            lost_times, switched = self._standby_tick_lost_times(lost_times)
                            if switched:
                                break
                            continue
                        except OSError as e:
                            logger.warning("[热冗余][备机] 接收 TCP 数据错误: %s", e)
                            break
                        if not chunk:
                            logger.info(
                                "[热冗余][备机] 主机已关闭 TCP 连接，LostTimes 保持累计，返回等待连接。"
                            )
                            break
                        buf += chunk
                        while True:
                            idx = buf.find(REDUNDANCY_HB_PAYLOAD)
                            if idx < 0:
                                if len(buf) > 65536:
                                    buf = buf[-4096:]
                                break
                            buf = buf[idx + len(REDUNDANCY_HB_PAYLOAD) :]
                            lost_times = 0
                finally:
                    try:
                        client.close()
                    except OSError:
                        pass
        finally:
            if server is not None:
                try:
                    server.close()
                except OSError:
                    pass
            logger.info("[热冗余][备机] TCP 心跳监听线程已退出。")

    def _redundancy_image_sync_master_loop(self) -> None:
        """Push I/O snapshots to standby over TCP (redundancy NIC).

        Requires host plc_main RUNNING so IMAGE_SNAPSHOT_GET succeeds (same libplc as peer).
        """
        standby_ip = self._redundancy_standby_ip
        if not standby_ip:
            logger.warning("[热冗余][主机] 未配置备机冗余 IP，跳过 I/O 镜像同步发送")
            return

        sock: socket.socket | None = None
        logger.info(
            "[热冗余][主机] I/O 镜像同步发送线程启动 → %s:%s",
            standby_ip,
            REDUNDANCY_IMAGE_SYNC_PORT,
        )
        try:
            while not self._heartbeat_stop.is_set():
                if not self.is_master:
                    break
                try:
                    if sock is None:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        sock.settimeout(5.0)
                        sock.connect((standby_ip, REDUNDANCY_IMAGE_SYNC_PORT))
                        sock.settimeout(30.0)

                    payload = self.runtime_socket.image_snapshot_get()
                    if (
                        not payload
                        or len(payload) != IMAGE_SNAPSHOT_EXPECTED_BYTES
                    ):
                        time.sleep(0.1)
                        continue

                    header = struct.pack(
                        "!4sII",
                        REDUNDANCY_IMAGE_MAGIC,
                        IMAGE_SNAPSHOT_PROTOCOL_VERSION,
                        len(payload),
                    )
                    sock.sendall(header + payload)
                    time.sleep(0.02)
                except OSError as e:
                    logger.debug("[热冗余][主机] I/O 同步 TCP 异常（将重连）: %s", e)
                    if sock is not None:
                        try:
                            sock.close()
                        except OSError:
                            pass
                        sock = None
                    time.sleep(0.5)
                except (RuntimeError, TypeError, ValueError) as e:
                    logger.warning("[热冗余][主机] I/O 同步异常: %s", e)
                    time.sleep(0.5)
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            logger.info("[热冗余][主机] I/O 镜像同步发送线程已退出。")

    def _redundancy_image_sync_standby_loop(self) -> None:
        """Receive I/O snapshots from master and apply via Unix socket.

        Applies only when local plc_main is RUNNING (process image pointers ready).
        """
        local_ip = self._redundancy_local_ens35_ip
        master_ip = self._redundancy_master_ip
        if not local_ip or not master_ip:
            logger.warning("[热冗余][备机] 冗余 IP 未就绪，跳过 I/O 镜像监听")
            return

        server: socket.socket | None = None
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((local_ip, REDUNDANCY_IMAGE_SYNC_PORT))
            server.listen(2)
            logger.info(
                "[热冗余][备机] I/O 镜像监听 %s:%s（仅接受主机 %s）",
                local_ip,
                REDUNDANCY_IMAGE_SYNC_PORT,
                master_ip,
            )
            server.settimeout(1.0)
            while not self._heartbeat_stop.is_set():
                if self._promoted_standby_acting_master:
                    time.sleep(0.2)
                    continue
                try:
                    conn, addr = server.accept()
                except TimeoutError:
                    continue
                except OSError as e:
                    if self._heartbeat_stop.is_set():
                        break
                    logger.error("[热冗余][备机] I/O 镜像 accept 失败: %s", e)
                    continue

                if addr[0] != master_ip:
                    logger.warning(
                        "[热冗余][备机] I/O 镜像拒绝非主机连接 %s",
                        addr[0],
                    )
                    try:
                        conn.close()
                    except OSError:
                        pass
                    continue

                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                try:
                    while (
                        not self._heartbeat_stop.is_set()
                        and not self._promoted_standby_acting_master
                    ):
                        hdr = _tcp_recv_exact(conn, 12, 30.0)
                        if hdr is None or len(hdr) != 12:
                            break
                        magic, ver, ln = struct.unpack("!4sII", hdr)
                        if magic != REDUNDANCY_IMAGE_MAGIC:
                            break
                        if ver != IMAGE_SNAPSHOT_PROTOCOL_VERSION:
                            break
                        if ln != IMAGE_SNAPSHOT_EXPECTED_BYTES:
                            break
                        body = _tcp_recv_exact(conn, ln, 30.0)
                        if body is None or len(body) != ln:
                            break
                        try:
                            if not self.runtime_socket.is_connected():
                                self._safe_connect_runtime_socket()
                            if not self._plc_shadow_standby:
                                time.sleep(0.15)
                                continue
                            if not self._plc_runtime_is_running():
                                time.sleep(0.15)
                                continue
                            ok, defer = self.runtime_socket.image_snapshot_set(body)
                            if ok:
                                continue
                            if defer:
                                time.sleep(0.15)
                                continue
                            logger.warning(
                                "[热冗余][备机] I/O 镜像 SET 失败，关闭 TCP 连接"
                            )
                            break
                        except (OSError, RuntimeError) as e:
                            logger.warning("[热冗余][备机] I/O 镜像 SET 失败: %s", e)
                            break
                finally:
                    try:
                        conn.close()
                    except OSError:
                        pass
        finally:
            if server is not None:
                try:
                    server.close()
                except OSError:
                    pass
            logger.info("[热冗余][备机] I/O 镜像监听线程已退出。")

    def _start_redundancy_heartbeat_threads(self) -> None:
        self._shutdown_redundancy_heartbeat_threads()
        # New cycle needs a clear stop event (previous stop() left it set).
        self._heartbeat_stop = threading.Event()
        if not self.is_redundancy:
            return
        if self.is_master:
            t = threading.Thread(
                target=self._redundancy_master_tcp_heartbeat_loop,
                name="redundancy-master-tcp-hb",
                daemon=True,
            )
            self._heartbeat_threads.append(t)
            t.start()
            t_img = threading.Thread(
                target=self._redundancy_image_sync_master_loop,
                name="redundancy-master-io-sync",
                daemon=True,
            )
            self._heartbeat_threads.append(t_img)
            t_img.start()
            return
        t = threading.Thread(
            target=self._redundancy_standby_tcp_heartbeat_loop,
            name="redundancy-standby-tcp-hb",
            daemon=True,
        )
        self._heartbeat_threads.append(t)
        t.start()
        t_img = threading.Thread(
            target=self._redundancy_image_sync_standby_loop,
            name="redundancy-standby-io-sync",
            daemon=True,
        )
        self._heartbeat_threads.append(t_img)
        t_img.start()

    def find_running_process(self):
        """
        Find the running PLC runtime process.
        Returns None if psutil is not available (MSYS2/Cygwin).
        """
        if not HAS_PSUTIL:
            # Cannot detect existing processes without psutil
            return None

        # Find the running PLC runtime process by executable path
        for proc in psutil.process_iter(["pid", "exe", "cmdline"]):
            try:
                # First try to match by executable path (most reliable)
                if proc.info["exe"] and os.path.samefile(proc.info["exe"], self.runtime_path):
                    return proc

                # Alternatively, match by command line (fallback)
                cmdline = proc.info.get("cmdline")
                if cmdline and isinstance(cmdline, (list, tuple)) and len(cmdline) > 0:
                    cmdline_str = " ".join(str(arg) for arg in cmdline if arg is not None)
                    if self.runtime_path in cmdline_str:
                        return proc

            except (OSError, psutil.Error, TypeError, ValueError):
                continue
        return None

    def _safe_start_log_server(self):
        try:
            self.log_server.start()
        except (OSError, socket.error) as e:
            logger.error("Failed to start log server: %s", e)
        except Exception as e:
            logger.error("Failed to start log server (unexpected): %s", e)

    def _safe_connect_runtime_socket(self):
        try:
            self.runtime_socket.connect()
        except (FileNotFoundError, OSError, socket.error) as e:
            logger.error("Failed to connect to runtime socket: %s", e)
        except Exception as e:
            logger.error("Failed to connect to runtime socket (unexpected): %s", e)

    def _safe_stop_log_server(self):
        try:
            self.log_server.stop()
        except (OSError, socket.error) as e:
            logger.error("Failed to stop log server: %s", e)
        except Exception as e:
            logger.error("Failed to stop log server (unexpected): %s", e)

    def _safe_close_runtime_socket(self):
        try:
            self.runtime_socket.close()
        except (OSError, socket.error) as e:
            logger.error("Failed to close runtime socket: %s", e)
        except Exception as e:
            logger.error("Failed to close runtime socket (unexpected): %s", e)

    def _plc_runtime_is_running(self) -> bool:
        """True if plc_main reports STATUS:RUNNING (I/O image tables safe for snapshot)."""
        now = time.monotonic()
        with self._plc_status_cache_lock:
            if self._plc_status_cache_monotonic > 0.0 and (
                now - self._plc_status_cache_monotonic
            ) < PLC_STATUS_CACHE_TTL_SEC:
                return self._plc_status_cache_running

        try:
            if not self.runtime_socket.is_connected():
                self._safe_connect_runtime_socket()
            if not self.runtime_socket.is_connected():
                with self._plc_status_cache_lock:
                    self._plc_status_cache_running = False
                    self._plc_status_cache_monotonic = time.monotonic()
                return False
            status = self.runtime_socket.send_and_receive("STATUS\n", timeout=0.5)
            ok = status == "STATUS:RUNNING"
            with self._plc_status_cache_lock:
                self._plc_status_cache_running = ok
                self._plc_status_cache_monotonic = time.monotonic()
            return ok
        except (OSError, RuntimeError, TypeError, ValueError):
            with self._plc_status_cache_lock:
                self._plc_status_cache_running = False
                self._plc_status_cache_monotonic = time.monotonic()
            return False

    def start(self):
        """
        Start the runtime manager and the PLC runtime process
        """
        if self.running:
            logger.warning("Runtime manager already running")
            return

        self._evaluate_redundancy_role()

        self.running = True

        # Ensure UNIX socket paths exist
        plc_socket_dir = os.path.dirname(self.plc_socket)
        log_socket_dir = os.path.dirname(self.log_socket)
        if not os.path.exists(plc_socket_dir):
            try:
                os.makedirs(plc_socket_dir)
                logger.info("Created directory for PLC socket: %s", plc_socket_dir)
            except OSError as e:
                logger.error("Failed to create directory for PLC socket: %s", e)
        if not os.path.exists(log_socket_dir):
            try:
                os.makedirs(log_socket_dir)
                logger.info("Created directory for log socket: %s", log_socket_dir)
            except OSError as e:
                logger.error("Failed to create directory for log socket: %s", e)

        # Start runtime process if not already running
        running_process = self.find_running_process()
        if running_process:
            logger.info("Found existing PLC runtime process with PID %d", running_process.pid)
            self.process = running_process
            self._safe_start_log_server()
            self._safe_connect_runtime_socket()
        else:
            logger.info("Starting PLC runtime core...")
            self._safe_start_log_server()
            try:
                cmd = [self.runtime_path]
                if self.print_debug:
                    cmd.append("--print-debug")
                if self._plc_shadow_standby:
                    cmd.append("--shadow-standby")
                self.process = subprocess.Popen(cmd)
            except (OSError, subprocess.SubprocessError) as e:
                logger.error("Failed to start PLC runtime process: %s", e)
                self.process = None
            time.sleep(1)  # Give time to start
            self._safe_connect_runtime_socket()

        # Start monitor thread
        if not self.monitor_thread.is_alive():
            self.monitor_thread = threading.Thread(target=self._monitor, daemon=True)
            self.monitor_thread.start()

        self._start_redundancy_heartbeat_threads()

    def is_runtime_alive(self):
        """
        Check if the PLC runtime process is alive
        """
        if self.process is None:
            return False
        if HAS_PSUTIL and isinstance(self.process, psutil.Process):
            if self.process.is_running() and self.process.status() != psutil.STATUS_ZOMBIE:
                return True
        elif isinstance(self.process, subprocess.Popen):
            if self.process.poll() is None:
                return True
        return False

    def _start_runtime_process(self, safe_mode=False):
        """Start the runtime process, optionally in safe mode."""
        self._safe_start_log_server()
        try:
            cmd = [self.runtime_path]
            if self.print_debug:
                cmd.append("--print-debug")
            if safe_mode:
                cmd.append("--safe-mode")
            elif self._plc_shadow_standby:
                cmd.append("--shadow-standby")
            self.process = subprocess.Popen(cmd)
        except (OSError, subprocess.SubprocessError) as e:
            logger.error("Failed to start PLC runtime process: %s", e)
            self.process = None
        time.sleep(1)  # Give time to start
        self._safe_connect_runtime_socket()

    def _record_crash_and_check_safe_mode(self):
        """Record a crash timestamp and check if safe mode should be entered."""
        with self._crash_lock:
            now = time.time()
            # Keep only crashes within the time window
            self._crash_times = [t for t in self._crash_times if now - t < RAPID_CRASH_WINDOW]
            self._crash_times.append(now)
            return len(self._crash_times) >= MAX_RAPID_CRASHES

    def _monitor(self):
        """
        Monitor the PLC runtime process and restart if it dies.
        Tracks crash frequency and enters safe mode after repeated failures.
        """
        while self.running:
            if self._manual_plc_restart_in_progress:
                time.sleep(0.3)
                continue
            if not self.is_runtime_alive():
                logger.warning("PLC runtime process died unexpectedly")
                self._safe_stop_log_server()
                self._safe_close_runtime_socket()

                if self._record_crash_and_check_safe_mode():
                    with self._crash_lock:
                        if not self._safe_mode:
                            logger.error(
                                "PLC program caused %d crashes within %d seconds. "
                                "Restarting runtime in SAFE MODE - "
                                "PLC program will NOT be loaded. "
                                "Upload a corrected program to recover.",
                                MAX_RAPID_CRASHES,
                                RAPID_CRASH_WINDOW,
                            )
                            self._safe_mode = True
                    self._start_runtime_process(safe_mode=True)
                else:
                    logger.warning("Restarting PLC runtime...")
                    self._start_runtime_process(safe_mode=False)
            else:
                # Make sure log server and socket are connected
                if not self.log_server.running:
                    self._safe_start_log_server()
                if not self.runtime_socket.is_connected():
                    self._safe_connect_runtime_socket()

            time.sleep(2)

    def stop(self):
        """ "
        Stop the runtime manager and the PLC runtime process
        """
        self._shutdown_redundancy_heartbeat_threads()
        try:
            self.runtime_socket.send_message("STOP\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to send STOP to PLC runtime: %s", e)
        except Exception as e:
            logger.error("Failed to send STOP to PLC runtime (unexpected): %s", e)
        self.running = False
        self.monitor_thread.join(timeout=5)
        time.sleep(1)
        if self.process:
            if HAS_PSUTIL and isinstance(self.process, psutil.Process):
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except (psutil.TimeoutExpired, psutil.Error):
                    self.process.kill()
            elif isinstance(self.process, subprocess.Popen):
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except (subprocess.TimeoutExpired, subprocess.SubprocessError):
                    self.process.kill()
            self.process = None
        self._safe_stop_log_server()
        self._safe_close_runtime_socket()

    def reset_crash_tracking(self):
        """Reset crash tracking state after a successful program upload."""
        with self._crash_lock:
            self._crash_times.clear()
            self._safe_mode = False

    def get_logs(self, min_id=None, level=None):
        """
        Get current logs from the runtime
        """
        try:
            _logs = buffer.normalize_logs(buffer.get_logs(min_id=min_id, level=level))
            return _logs
        except AttributeError as e:
            logger.error("Failed to get logs from buffer: %s", e)
            return []

    def ping(self):
        """
        Send PING and wait for PONG
        """
        try:
            return self.runtime_socket.send_and_receive("PING\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to ping PLC runtime: %s", e)
            return "PING:ERROR\n"
        except Exception as e:
            logger.error("Failed to ping PLC runtime (unexpected): %s", e)
            return "PING:ERROR\n"

    def start_plc(self):
        """
        Send START command
        """
        try:
            return self.runtime_socket.send_and_receive("START\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to start PLC runtime: %s", e)
            return "START:ERROR\n"
        except Exception as e:
            logger.error("Failed to start PLC runtime (unexpected): %s", e)
            return "START:ERROR\n"

    def stop_plc(self):
        """
        Send STOP command
        """
        try:
            return self.runtime_socket.send_and_receive("STOP\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to stop PLC runtime: %s", e)
            return "STOP:ERROR\n"
        except Exception as e:
            logger.error("Failed to stop PLC runtime (unexpected): %s", e)
            return "STOP:ERROR\n"

    def status_plc(self):
        """
        Send STATUS command
        """
        try:
            return self.runtime_socket.send_and_receive("STATUS\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to get PLC status: %s", e)
            return "STATUS:ERROR\n"
        except Exception as e:
            logger.error("Failed to get PLC status (unexpected): %s", e)
            return "STATUS:ERROR\n"

    def stats_plc(self):
        """
        Send STATS command to get timing statistics
        """
        try:
            return self.runtime_socket.send_and_receive("STATS\n")
        except (OSError, socket.error) as e:
            logger.error("Failed to get PLC stats: %s", e)
            return None
        except Exception as e:
            logger.error("Failed to get PLC stats (unexpected): %s", e)
            return None
