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

# Hot redundancy: TCP heartbeat on redundancy NIC (ens35); standby listens, master sends
REDUNDANCY_ROLE_FILENAME = "redundancy_role.ini"
REDUNDANCY_NIC = "ens35"
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
# Functional I/O NICs (lines 3–4 of redundancy_role.ini record host IPv4/prefix)
REDUNDANCY_FUNCTIONAL_NIC_A = "ens33"
REDUNDANCY_FUNCTIONAL_NIC_B = "ens34"

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

        # Hot redundancy (see redundancy_role.ini, NIC ens35)
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
        # 备机已暂时升主且 PLC 非影子（永久 is_master 仍由 ini 第 1–2 行决定）
        self._promoted_standby_acting_master = False
        # 备升主过程中避免 monitor 线程误重启 PLC
        self._manual_plc_restart_in_progress = False
        # 主机本地记录完成后，等待“TCP 心跳已连接备机”时再同步第 3–4 行
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
    def _parse_ipv4_line(line: str) -> str | None:
        raw = line.split("#", 1)[0].strip()
        if not raw:
            return None
        try:
            return str(ipaddress.IPv4Address(raw))
        except ValueError:
            return None

    def _read_redundancy_role_ini(self, ini_path: Path) -> tuple[str | None, str | None]:
        """Read configured master and standby IPv4 from redundancy_role.ini (first two IP lines)."""
        master_ip: str | None = None
        standby_ip: str | None = None
        try:
            text = ini_path.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            logger.error(
                "[热冗余] 读取冗余配置文件失败: 路径=%s, 错误=%s",
                ini_path,
                e,
            )
            return None, None

        line_no = 0
        for raw_line in text.splitlines():
            line_no += 1
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#"):
                logger.debug(
                    "[热冗余] 配置文件第 %d 行已跳过（空行或注释）: %r",
                    line_no,
                    raw_line[:80],
                )
                continue
            ip_val = self._parse_ipv4_line(stripped)
            if ip_val is None:
                logger.warning(
                    "[热冗余] 配置文件第 %d 行不是有效 IPv4，已跳过: %r",
                    line_no,
                    stripped[:80],
                )
                continue
            if master_ip is None:
                master_ip = ip_val
                logger.info(
                    "[热冗余] 从配置文件解析到主机 IPv4（第 %d 行）: %s",
                    line_no,
                    master_ip,
                )
            elif standby_ip is None:
                standby_ip = ip_val
                logger.info(
                    "[热冗余] 从配置文件解析到备机 IPv4（第 %d 行）: %s",
                    line_no,
                    standby_ip,
                )
                break
            else:
                break

        if master_ip is None or standby_ip is None:
            logger.warning(
                "[热冗余] 配置文件中未能解析出主机与备机两条有效 IPv4，"
                "当前解析结果: 主机=%s, 备机=%s",
                master_ip,
                standby_ip,
            )
            return None, None

        return master_ip, standby_ip

    @staticmethod
    def write_redundancy_ini_functional_lines(ini_path: Path, line3: str, line4: str) -> None:
        """Write lines 3–4 (1-based) with functional NIC IPv4/prefix; preserve lines 1–2."""
        if ini_path.is_file():
            text = ini_path.read_text(encoding="utf-8", errors="replace")
        else:
            text = ""
        lines = text.splitlines()
        while len(lines) < 4:
            lines.append("")
        lines[2] = line3
        lines[3] = line4
        ini_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    @staticmethod
    def write_redundancy_ini_standby_backup_lines(ini_path: Path, line5: str, line6: str) -> None:
        """Write lines 5–6 (1-based): 升主前备机 ens33/ens34 功能地址，供回切恢复。"""
        if ini_path.is_file():
            text = ini_path.read_text(encoding="utf-8", errors="replace")
        else:
            text = ""
        lines = text.splitlines()
        while len(lines) < 6:
            lines.append("")
        lines[4] = line5
        lines[5] = line6
        ini_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _read_functional_cidr_from_ini(self, ini_path: Path) -> tuple[str | None, str | None]:
        """Parse lines 3–4 as IPv4 interface CIDR (comments stripped)."""
        try:
            text = ini_path.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            logger.error("[热冗余] 读取功能 IP 行失败: %s", e)
            return None, None

        lines = text.splitlines()
        while len(lines) < 4:
            lines.append("")

        def one(idx: int) -> str | None:
            raw = lines[idx].split("#", 1)[0].strip()
            if not raw:
                return None
            try:
                return str(ipaddress.IPv4Interface(raw))
            except ValueError:
                logger.warning("[热冗余] 第 %d 行不是有效 IPv4/CIDR: %r", idx + 1, raw[:64])
                return None

        return one(2), one(3)

    def _read_standby_backup_cidr_from_ini(self, ini_path: Path) -> tuple[str | None, str | None]:
        """Parse lines 5–6：回切时恢复到 ens33/ens34。"""
        try:
            text = ini_path.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            logger.error("[热冗余] 读取备机功能 IP 备份行失败: %s", e)
            return None, None

        lines = text.splitlines()
        while len(lines) < 6:
            lines.append("")

        def one(idx: int) -> str | None:
            raw = lines[idx].split("#", 1)[0].strip()
            if not raw:
                return None
            try:
                return str(ipaddress.IPv4Interface(raw))
            except ValueError:
                logger.warning("[热冗余] 第 %d 行不是有效 IPv4/CIDR: %r", idx + 1, raw[:64])
                return None

        return one(4), one(5)

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
        """主机：将 ens33/ens34 的 IPv4/掩码写入 redundancy_role.ini 第 3–4 行。"""
        try:
            ini_path = self._openplc_project_root() / REDUNDANCY_ROLE_FILENAME
            if not ini_path.is_file():
                return
            c33 = self._ipv4_cidr_for_interface(REDUNDANCY_FUNCTIONAL_NIC_A)
            c34 = self._ipv4_cidr_for_interface(REDUNDANCY_FUNCTIONAL_NIC_B)
            if not c33 or not c34:
                logger.warning(
                    "[热冗余] 功能 IP 记录跳过：%s=%s, %s=%s（需两网卡均有 IPv4）",
                    REDUNDANCY_FUNCTIONAL_NIC_A,
                    c33,
                    REDUNDANCY_FUNCTIONAL_NIC_B,
                    c34,
                )
                return
            self.write_redundancy_ini_functional_lines(ini_path, c33, c34)
            logger.info(
                "[热冗余] 已记录功能口地址到 %s 第3–4行: %s=%s, %s=%s",
                ini_path,
                REDUNDANCY_FUNCTIONAL_NIC_A,
                c33,
                REDUNDANCY_FUNCTIONAL_NIC_B,
                c34,
            )
            with self._functional_sync_lock:
                self._functional_lines_pending_sync = (c33, c34)
            logger.info("[热冗余] 功能口第3–4行已标记待同步，等待主备 TCP 心跳连接建立后再推送。")
        except Exception as e:
            logger.error("[热冗余] 功能 IP 记录异常: %s", e)

    def _sync_functional_lines_after_tcp_connect(self) -> None:
        """主机 TCP 心跳连上备机后，再尝试同步 redundancy_role.ini 第 3–4 行。"""
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
                "[热冗余][主机] TCP 心跳连接已建立，开始同步 redundancy_role.ini 第3–4行到备机 %s。",
                standby_ip,
            )
            push_role_ini_functional_to_standby(standby_ip, c33, c34, REDUNDANCY_SYNC_SECRET)
        except Exception as e:
            logger.error("[热冗余][主机] TCP 建连后同步第3–4行异常: %s", e)

    def _evaluate_redundancy_role(self) -> None:
        """
        Load redundancy_role.ini, compare with local ens35 address, set is_master / is_redundancy.
        is_master 仅由 ini 第 1–2 行决定；备升主为暂时状态，不改变 is_master。
        """
        self.is_master = False
        self.is_redundancy = False
        self._redundancy_master_ip = None
        self._redundancy_standby_ip = None
        self._redundancy_local_ens35_ip = None
        self._plc_shadow_standby = False
        self._promoted_standby_acting_master = False

        project_root = self._openplc_project_root()
        ini_path = project_root / REDUNDANCY_ROLE_FILENAME

        logger.info(
            "[热冗余] 开始冗余角色检测: 项目根目录=%s, 配置文件=%s, 冗余网卡=%s",
            project_root,
            ini_path,
            REDUNDANCY_NIC,
        )

        if not ini_path.is_file():
            logger.info(
                "[热冗余] 未找到配置文件 %s，本机不启用热冗余功能（is_redundancy=False, is_master=False）。",
                ini_path,
            )
            return

        logger.info("[热冗余] 已找到冗余配置文件，开始读取主机/备机 IP。")
        master_ip, standby_ip = self._read_redundancy_role_ini(ini_path)
        if master_ip is None or standby_ip is None:
            logger.info(
                "[热冗余] 配置文件内容无效，本机不启用热冗余功能（is_redundancy=False, is_master=False）。"
            )
            return

        self._redundancy_master_ip = master_ip
        self._redundancy_standby_ip = standby_ip
        logger.info(
            "[热冗余] 配置摘要: 主机 IP=%s, 备机 IP=%s（将与本机 %s 的 IPv4 比较）",
            master_ip,
            standby_ip,
            REDUNDANCY_NIC,
        )

        local_ip = self._ipv4_for_interface(REDUNDANCY_NIC)
        self._redundancy_local_ens35_ip = local_ip
        if local_ip is None:
            logger.error(
                "[热冗余] 无法读取网卡 %s 的 IPv4 地址，本机不启用热冗余。"
                "请确认网卡存在且已配置地址。",
                REDUNDANCY_NIC,
            )
            return

        logger.info(
            "[热冗余] 本机网卡 %s 的 IPv4 为: %s",
            REDUNDANCY_NIC,
            local_ip,
        )

        if local_ip == master_ip:
            self.is_redundancy = True
            self.is_master = True
            logger.info(
                "[热冗余] 本机 IPv4 与配置中的主机一致，角色=主机。"
                "is_redundancy=True, is_master=True。"
                "将通过 TCP 经 %s 主动连接备机并每秒发送心跳（目标端口 %d）。",
                REDUNDANCY_NIC,
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
                REDUNDANCY_NIC,
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
            REDUNDANCY_NIC,
            local_ip,
            master_ip,
            standby_ip,
        )

    def _shutdown_redundancy_heartbeat_threads(self) -> None:
        self._heartbeat_stop.set()
        for t in list(self._heartbeat_threads):
            t.join(timeout=3)
        self._heartbeat_threads.clear()

    def _redundancy_ping_master_reachable(self) -> bool:
        """ICMP ping configured master IPv4 once (Linux iputils ping)."""
        master_ip = self._redundancy_master_ip
        if not master_ip:
            return False
        try:
            proc = subprocess.run(
                ["ping", "-c", "1", "-W", "2", master_ip],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            return proc.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return False

    def _redundancy_trigger_standby_to_master_switch(self) -> None:
        """
        备升主（暂时）：先将本机 ens33/ens34 写入 ini 第 5–6 行，再应用第 3–4 行主机功能 IP；
        PLC 非影子；is_master 不变；继续监听冗余口，收到原主机心跳载荷后异步回切。
        """
        logger.info("[热冗余][备机] 备机升主机已触发（暂时，ini 永久角色不变）")
        ini_path = self._openplc_project_root() / REDUNDANCY_ROLE_FILENAME
        standby33 = self._ipv4_cidr_for_interface(REDUNDANCY_FUNCTIONAL_NIC_A)
        standby34 = self._ipv4_cidr_for_interface(REDUNDANCY_FUNCTIONAL_NIC_B)
        if not standby33 or not standby34:
            logger.error(
                "[热冗余][备机] 升主中止：无法读取本机 %s/%s 的 IPv4/CIDR",
                REDUNDANCY_FUNCTIONAL_NIC_A,
                REDUNDANCY_FUNCTIONAL_NIC_B,
            )
            return
        try:
            self.write_redundancy_ini_standby_backup_lines(ini_path, standby33, standby34)
            logger.info(
                "[热冗余][备机] 已写入备机功能地址到 %s 第 5–6 行: %s, %s",
                ini_path,
                standby33,
                standby34,
            )
        except OSError as e:
            logger.error("[热冗余][备机] 写入第 5–6 行失败: %s", e)
            return

        c33, c34 = self._read_functional_cidr_from_ini(ini_path)
        if not c33 or not c34:
            logger.error(
                "[热冗余][备机] 升主中止：%s 第 3、4 行缺少有效的 IPv4/CIDR（需先由主机记录功能 IP）",
                ini_path,
            )
            return
        if not self._apply_ipv4_cidr_to_linux_interface(REDUNDANCY_FUNCTIONAL_NIC_A, c33):
            return
        if not self._apply_ipv4_cidr_to_linux_interface(REDUNDANCY_FUNCTIONAL_NIC_B, c34):
            return

        self._standby_switched_to_master = True
        self._plc_shadow_standby = False
        self._promoted_standby_acting_master = True

        self._shutdown_redundancy_heartbeat_threads()
        try:
            if self._try_redundancy_shadow_exit():
                logger.info(
                    "[热冗余][备机] 已通过 REDUNDANCY_SHADOW_EXIT 平滑升主（保留 PLC 进程与 I/O 镜像）"
                )
            else:
                self._restart_plc_core_after_takeover()
        except Exception as e:
            logger.error("[热冗余][备机] 升主后切换 PLC 核心失败: %s", e)
        self._start_redundancy_heartbeat_threads()

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
        """收到原主机冗余心跳后回切：按 ini 第 5–6 行恢复 ens33/ens34，影子 PLC。"""
        logger.info("[热冗余][备机] 回切备机已触发")
        ini_path = self._openplc_project_root() / REDUNDANCY_ROLE_FILENAME
        b33, b34 = self._read_standby_backup_cidr_from_ini(ini_path)
        if b33 and b34:
            self._apply_ipv4_cidr_to_linux_interface(REDUNDANCY_FUNCTIONAL_NIC_A, b33)
            self._apply_ipv4_cidr_to_linux_interface(REDUNDANCY_FUNCTIONAL_NIC_B, b34)
        else:
            logger.warning(
                "[热冗余][备机] 第 5–6 行无效，跳过恢复功能口 IP（请检查 %s）",
                ini_path,
            )

        self._standby_switched_to_master = False
        self._promoted_standby_acting_master = False
        self._plc_shadow_standby = True
        self._shutdown_redundancy_heartbeat_threads()
        try:
            self._restart_plc_core_shadow_standby_after_failback()
        except Exception as e:
            logger.warning("[热冗余][备机] 回切后重启影子 PLC 失败（可手动重启运行时）: %s", e)
        self._start_redundancy_heartbeat_threads()

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
                "[热冗余][备机] LostTimes=%d 已超过阈值 %d 秒但 ping 主机可达，清零计数。",
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
        收到原主机发来的心跳载荷后异步回切（影子 PLC + 恢复第 5–6 行功能 IP）。
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
