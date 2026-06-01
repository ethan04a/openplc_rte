# 热冗余 IO 同步：运维风险与已知缺口

本文档汇总主备机 PLC 数据同步（计划见 `主备机PLC数据同步计划.md`）在 **develop 分支当前实现** 下，运维与现场联调时需要关注的风险、边界和未完全对齐项。

**文档性质**：风险清单与上线前检查参考，不代表代码缺陷的最终认定；部分项已在后续提交中修复，文中会注明「已修复」与「仍须关注」。

**适用版本**：含阶段一至六及 barrier / plc OPUD 相关修复的 `develop`（约 `83f785f` 及之后）。

**最后更新**：2026-06-01

---

## 1. 部署与环境前提

| 风险 | 说明 | 建议 |
| --- | --- | --- |
| 非 Linux 运行面 | `plc_main` 实时调度、ICMP 故障探测、Unix socket 路径均按 Linux 设计；Windows 仅适合开发。 | 生产与联调仅在 Linux 双机进行。 |
| 未重编 `plc_main` | C 端含 `scan_sync`、`image_snapshot_delta`、`redundancy_pending`、`redundancy_image_udp` 等；未 `cmake && make` 则 barrier、plc UDP 等行为与 Python/Web 不一致。 | 每次拉取含 `core/` 改动的版本后，在**主备两台**同版本重编并重启。 |
| 权限不足 | 实时 PLC 通常需 root/sudo 启动。 | 使用项目推荐的 `start_openplc.sh` / 安装脚本，确认 `plc_main` 实际在跑。 |
| `redundancy_role.json` 错误 | 心跳网卡名、主机/备机 IPv4 与现场不符时 `is_redundancy=False`，热冗余整链不启。 | 对照日志 `[热冗余]`；确认本机心跳口 IP 等于配置中的 master 或 standby 地址之一。 |
| 主备程序不一致 | IO 同步只保证 image tables 一致，不替代工程/逻辑版本管理。 | 升主前确认备机已同步同一 `program.zip` / 编译产物。 |

---

## 2. 数据面双轨（Web vs plc_main）

当前存在两条 IO 同步数据路径，由运行时自动选择：

```text
Web 数据面：  plc_main ←Unix→ webserver ←UDP OPUD/OPAK→ 对端 webserver ←Unix→ plc_main
plc 数据面：  plc_main ←UDP OPUD/OPAK→ 对端 plc_main（webserver 仅 CONFIG/START/STATUS）
```

| 风险 | 说明 | 建议 |
| --- | --- | --- |
| 仅一端 `REDUNDANCY_SYNC_START` 成功 | 成功端走 plc UDP，失败端仍走 Web UDP；协议虽同为 OPUD，但时序、统计、会话可能不一致，严重时表现为同步不稳定或看似「单通」。 | 联调时查 `GET /api/redundancy/image-sync-status` 的 `data_plane` 字段，**主备应同为 `webserver` 或同为 `plc_main`**。 |
| 默认自动尝试 plc 面 | `_redundancy_image_sync_*_loop` 启动时会调用 `_try_enable_plc_redundancy_data_plane()`。 | 若暂不信任 plc 面，需在运维策略中明确：先固定 Web 面验收，再开启 plc 面（或后续通过配置开关控制——当前无独立开关）。 |
| Web 统计在 plc 面下停滞 | plc 面激活后 Web UDP 循环空转，`stats` 不再更新；需看 `plc_sync`。 | 监控 API 时同时看 `data_plane` 与 `plc_sync`。 |
| plc 侧观测字段不全 | `REDUNDANCY_SYNC_STATUS` 缺少计划示例中的 `last_ack_frame_seq`、`missed_count`、`last_peer_ip`、`last_update_monotonic_ms` 等。 | 故障排查以 Web `stats` + 日志为主；plc 状态仅作辅助。 |

---

## 3. 协议与同步语义

| 风险 | 说明 | 状态 / 建议 |
| --- | --- | --- |
| barrier 与 ACK 时序 | 若「先 `scan_start` 再 apply」，会导致 `WAIT_APPLIED` 提前返回而备机尚未写入。 | **已修复**（先 `redundancy_pending_apply_at_barrier`，再 `scan_sync_notify_scan_start`）。升级后须重编 `plc_main`。 |
| `IMAGE_SNAPSHOT_SET` 绕过 barrier | shadow 备机若立即 import，会在周期中间改表。 | **已修复**（shadow 下改为入队 `QUEUED`）。勿在备机侧自行调用旧工具直写 SET 期望立即生效。 |
| scan end 与 export 窗口 | `wait_for_end` 返回后再加锁 export，中间可能多跑一个 scan，快照不一定严格对应「刚结束的那一拍」。 | 接受为设计折中；若业务要求严格同拍，需后续加强（如 export 与 notify 同事务）。 |
| 首包 / 大脏区全量分片 | delta 首帧或变化行过多时 payload 超过约 1100 字节，会回退 **69632 字节全量 OPUD 分片**，带宽与 CPU 瞬时升高。 | 上电后首分钟关注网络；属预期行为。 |
| delta 校验偏弱 | `image_delta_import` 对 dirty bitmap 与条目列表的一致性校验有限。 | 仅在可信冗余网内使用；勿将 57576 暴露到非冗余网段。 |
| 非法 pending 长度 | 非 OPDL 且非 69632 的 payload 在 barrier apply 时静默失败，对端可能 `WAIT_APPLIED` 超时或 `APPLY_ERROR`。 | 抓包确认帧类型；查备机 `apply_error_count` / 日志。 |
| `missed_count` 未实现 | 计划建议 `frame_seq > last_applied+1` 时记录丢帧间隔；当前为「只保留最新 pending」。 | 功能上符合「最新状态优先」，但无法从指标直接看到「跳号」。 |
| plc 备机无 15ms 组帧超时 | Web 备机有 `FRAME_INCOMPLETE` 与 `frame_incomplete_count`；plc `redundancy_image_udp` 组装器**无**等价超时清理。 | plc 面下偶发半帧残留时，依赖下一帧覆盖；长期应补齐与 Web 一致的超时逻辑。 |
| plc 主机 ACK 源端口 | Web 主机校验 ACK 来自 `57576`；plc 主机 `recv_ack` 主要校验对端 IP，**未校验源端口**。 | 异常网络或多播干扰时理论上有误判空间；现场应隔离冗余网。 |
| 阶段一字面与阶段五演进 | 文档阶段一写「收齐后 `IMAGE_SNAPSHOT_SET`」；现网备机为 **PENDING + barrier + WAIT_APPLIED**。 | 以阶段五语义为准；ACK 表示 barrier 应用结果，非「任意时刻 SET」。 |

---

## 4. 热冗余整体（超出 IO 同步计划的部分）

IO 同步只是热冗余的一环；以下能力来自原有框架，与 UDP 改造正交，但上线时一并验证：

| 能力 | 运维注意 |
| --- | --- |
| TCP 心跳（57575） | **切主仍以 TCP 心跳 / 功能网 ping 为准**；UDP ACK 丢失**不会**直接触发升主。 |
| 功能 IP 接管 | 依赖 `functional_nics` 与 CIDR 配置正确；与 IO 同步地址无关。 |
| 备机 shadow standby | 备机不驱动现场 I/O 插件；升主后需走 `REDUNDANCY_SHADOW_EXIT` 等既有逻辑。 |
| 升主瞬间 | 切换窗口内可能存在「最后几帧 UDP 未应用」；需结合 ACK/`last_applied_seq` 与工艺容忍度评估。 |

---

## 5. 监控与排障

### 5.1 建议关注的 API

`GET /api/redundancy/image-sync-status`（需 JWT）：

- `enabled`、`role`、`shadow_standby`、`plc_running`
- `data_plane`：`webserver` 或 `plc_main`
- `stats`：Web 路径计数（plc 面下可能不更新）
- `plc_sync`：plc 路径计数（仅 `data_plane=plc_main` 时有意义）
- `metadata`：`scan_counter`、`tick`、`phase` 等

### 5.2 日志关键字

- `[热冗余]`：角色识别、网卡、升主/回切
- `[hot-redundancy]`：UDP 同步、ACK 超时、来源 IP 拒绝

### 5.3 常见异常表象

| 表象 | 可能原因 |
| --- | --- |
| `is_redundancy=False` | `redundancy_role.json` 缺失、心跳 IP 与配置不匹配 |
| 备机 `NOT_SHADOW` ACK | 未 `--shadow-standby` 启动或已升主取消影子 |
| `ack_timeout_count` 持续涨 | 网络、备机未 RUNNING、barrier 超时、双轨不一致 |
| `crc_error_count` | 传损、半帧、版本不一致 |
| 主备内存态长期偏离 | 同步未启、仅一端同步、程序版本不同、delta 导入失败未察觉 |

---

## 6. 测试与生产成熟度

| 项 | 现状 |
| --- | --- |
| 单元测试 | `tests/pytest/redundancy/test_image_udp_sync.py` 覆盖 OPUD/组装/ACK 等（**非**端到端双机） |
| plc UDP 集成测试 | **无** 自动化双机 / plc_main 联调用例 |
| lock-step（阶段七） | **未实现**，勿按同周期比对验收 |
| 生产级 SLA | 建议在预生产完成：双机长时间 RUNNING、主机宕机升主、回切、IO 状态抽查 |

**建议上线态度**：

- **预生产 / 有监控联调**：可按正常流程部署，默认优先确认 Web 数据面稳定，再验证 plc 数据面。
- **无验收直接生产**：不推荐；至少完成重编一致、双机 `data_plane` 一致、升主演练与 `image-sync-status` 基线截图/告警。

---

## 7. 上线前检查清单（简版）

1. 主备均为 Linux，且已用**同一 git 提交**编译 `plc_main`。
2. 两台 `redundancy_role.json` 心跳网卡与 IP 配置正确，程序版本一致。
3. 冗余心跳网仅用于冗余；57575/57576 互通且防火墙放行。
4. 主机、备机 `plc_main` 均为 RUNNING；备机日志确认 `--shadow-standby`。
5. `image-sync-status` 中 `enabled=true`，`data_plane` 主备一致。
6. 观察若干分钟：`ack_timeout_count` 无异常飙升，`last_applied_seq`（或 `plc_sync.last_applied_seq`）随运行递增。
7. 演练：停主机进程或断心跳，备机升主与功能 IP 符合预期；回切后再观察 IO 同步恢复。
8. 若仅验收 IO 同步：暂不要求 lock-step；升主后工艺窗口由业务方定义容忍度。

---

## 8. 相关文档

- `docs/主备机PLC数据同步计划.md` — 分阶段设计与 wire format
- `docs/plc热冗余数据同步基线的业务逻辑.md` — 改造前基线与 Unix 协议
- 仓库根目录 `redundancy_role.json` — 配置示例（现场须按网卡修改）

---

## 9. 修订记录

| 日期 | 说明 |
| --- | --- |
| 2026-06-01 | 初版：汇总 develop 上 IO 同步实现后的运维风险与已知缺口 |
