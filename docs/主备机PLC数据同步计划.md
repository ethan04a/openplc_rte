# 主备机 PLC 数据同步计划

## 目标

在现有热冗余框架基础上，逐步提升主备机 PLC 数据同步的实时性、一致性和可观测性。

本计划的第一原则是：**先做局部改造，不做大范围重构**。当前阶段只关注 PLC 数据同步链路，暂不触碰主备 TCP 心跳、角色判断、升主、回切、功能 IP 接管等已有逻辑。

## 给未来维护者的防误解说明

如果未来维护者只读这一份文档，请先理解下面这些约束。它们不是建议，而是本计划的安全边界。

### 一句话架构边界

```text
webserver 始终是热冗余控制面和观察者
plc_main 始终是 PLC 实时执行面和数据面
```

即使后续把 UDP 数据同步下沉到 `plc_main`，也不能把主备身份判断、TCP 心跳、影子执行、升主回切、功能 IP 接管等控制逻辑搬进 `plc_main`。

### 绝对不要误解的点

1. **不要改 TCP 心跳**
   - TCP 心跳是主备在线检测和切换判断的一部分。
   - 本计划只讨论 IO image tables 数据同步链路。
   - `REDUNDANCY_HEARTBEAT_PORT = 57575` 暂时不动。

2. **不要把 UDP ACK 当成心跳**
   - UDP 轻量 ACK 只用于观察数据同步是否收到、是否应用、落后多少。
   - 初期 ACK 不参与升主、回切、故障检测决策。
   - 不要因为 ACK 丢失直接触发主备切换。

3. **不要把 UDP 做成 TCP**
   - 轻量 ACK 不是可靠传输协议。
   - 不要求旧包必达。
   - 不为了旧 snapshot 阻塞新状态。
   - 原则是最新状态优先，旧状态迟到就丢弃。

4. **不要使用功能 IP 做内部同步**
   - PLC 数据同步仍走心跳 IP。
   - 功能 IP 是对外服务和现场通信地址，用于升主/回切时接管或恢复。
   - 不要把 `_redundancy_functional_nics` 当成 UDP 同步地址来源。

5. **不要在第一阶段下沉到 plc_main**
   - 第一阶段只在原有 `webserver` 同步触发点，把 57576 上的 TCP 数据链路换成 UDP + 轻量 ACK。
   - `IMAGE_SNAPSHOT_GET` / `IMAGE_SNAPSHOT_SET` 仍然通过 Unix socket 走原路径。
   - 数据面下沉到 `plc_main` 是后续阶段，不要提前混做。

6. **不要在第一阶段引入 scan cycle 同步**
   - 第一阶段不改 PLC scan cycle。
   - 不改 `plc_cycle_thread()`。
   - 不改 `buffer_mutex` 锁语义。
   - 不改 snapshot 内容语义。

7. **不要让 plc_main 自己做热冗余决策**
   - `plc_main` 不读取 `redundancy_role.json`。
   - `plc_main` 不判断自己是主机还是备机。
   - `plc_main` 不决定开启 shadow standby。
   - `plc_main` 不决定升主或回切。
   - `plc_main` 不接管或恢复功能 IP。

8. **不要忽略 UDP 大包问题**
   - 当前 full snapshot 是 `1024 * 68 = 69632` 字节。
   - 这不仅远超常规 MTU，也超过 UDP 单 datagram 的可承载上限。
   - 第一阶段为了保持 full snapshot 语义，必须做最小应用层分片：一个 snapshot frame 拆成多个 UDP fragment datagram。
   - 第一阶段不做分片重传、不做 per-fragment ACK；某一帧 fragment 未收齐就丢弃该帧，等待下一帧。
   - 长期方案必须走 delta 或 dirty row，减少甚至避免分片。

### 代码锚点

读代码或准备实现时，优先看这些位置：

| 主题 | 文件 / 函数 |
| --- | --- |
| 主备角色判断、心跳、功能 IP、同步线程启动 | `webserver/runtimemanager.py` |
| 当前主机 IO 同步发送触发点 | `RuntimeManager._redundancy_image_sync_master_loop()` |
| 当前备机 IO 同步接收触发点 | `RuntimeManager._redundancy_image_sync_standby_loop()` |
| Web 层访问 plc_main 的 Unix socket 客户端 | `webserver/unixclient.py` |
| 当前 snapshot 拉取 | `UnixClient.image_snapshot_get()` |
| 当前 snapshot 写入 | `UnixClient.image_snapshot_set()` |
| plc_main Unix socket 服务端 | `core/src/plc_app/unix_socket.c` |
| C 端 snapshot 导出/导入命令 | `IMAGE_SNAPSHOT_GET` / `IMAGE_SNAPSHOT_SET` |
| image tables 定义 | `core/src/plc_app/image_tables.c` / `.h` |
| snapshot 二进制布局 | `core/src/plc_app/image_snapshot.c` / `.h` |
| PLC scan cycle | `core/src/plc_app/plc_state_manager.c::plc_cycle_thread()` |
| shadow standby 插件跳过逻辑 | `core/src/drivers/plugin_driver.c` |
| shadow standby 平滑升主 | `core/src/plc_app/redundancy_ipc.c` |

### 正确的路线理解

```text
先：在 webserver 原触发点做 TCP -> UDP 应用层分片 + frame ACK，并包含最小安全包头
再：补充协议观测字段和状态统计，让数据更可诊断
再：补 scan_counter、tick、phase，让数据可解释
再：把同步点逐步贴近 scan cycle end
再：从 full snapshot 过渡到 output/memory delta
再：备机在 scan start 或固定 barrier 应用 pending state
再：UDP 高频数据面下沉到 plc_main，但 webserver 仍是控制面
最后：评估 lock-step，同周期执行并比较关键 memory/output
```

任何实现都不应跳过路线边界做大改。每一步应该能独立验证、独立回滚。

### 第一阶段收敛规则

为避免把第一阶段理解成“随便把 TCP sendall 改成 UDP sendto”，这里明确第一阶段的最小闭环。

第一阶段必须做：

- 在 `webserver` 原有 IO image tables 同步触发点替换传输层。
- 保持 `IMAGE_SNAPSHOT_GET` / `IMAGE_SNAPSHOT_SET` 语义不变。
- 保持 full snapshot payload 语义不变。
- 使用心跳 IP 作为 UDP 通信地址。
- 使用 `REDUNDANCY_IMAGE_SYNC_PORT = 57576` 作为备机 UDP 接收端口。
- 因 full snapshot 为 69632 字节，第一阶段必须把一个 full snapshot frame 拆成多个 UDP fragment datagram。
- 增加最小 DATA fragment 包头：
  - `magic`
  - `version`
  - `packet_type`
  - `reserved`
  - `session_id`
  - `frame_seq`
  - `fragment_index`
  - `fragment_count`
  - `fragment_offset`
  - `total_len`
  - `payload_crc32`
- 增加最小 ACK 包头：
  - `magic`
  - `version`
  - `packet_type`
  - `status`
  - `session_id`
  - `ack_frame_seq`
  - `applied_seq`
- 主机发送一个 frame 的所有 fragments 后，可以非阻塞轮询或总预算不超过 5ms 的短轮询 ACK；超时立即进入下一帧，不能等待 ACK 阻塞下一帧发送。
- ACK 缺失只记录统计和日志，不参与主备切换。

第一阶段不做：

- 不做 scan cycle 同步。
- 不做 `scan_counter` / `tick` / `phase` 的真实接入。
- 不做 output/memory delta。
- 不做分片重传。
- 不做 per-fragment ACK。
- 不做 `plc_main` 数据面下沉。
- 不改 TCP 心跳。
- 不改升主/回切。

### ACK 端口规则

第一阶段 ACK 必须走主机发送 DATA fragment 所用 UDP socket 的源端口：

```text
主机 UDP socket bind(local_heartbeat_ip, 0 或明确配置的非 57576 本地源端口)
主机 sendto(DATA fragments, standby_heartbeat_ip:57576)
备机 recvfrom(DATA fragments)
备机用接收 DATA 的同一个 UDP socket sendto(frame ACK, DATA fragment 的源地址和源端口)
主机在同一个 UDP socket 上非阻塞或 5ms 总预算短轮询接收 ACK
```

这样可以避免主机再额外监听 `57576` 与备机监听端口产生角色歧义。

第一阶段没有独立 ACK 监听端口；ACK 只能回 DATA 的同一个 UDP socket。主机不得额外监听 `57576` 接 ACK，也不得把 DATA 源端口固定为 `57576`。备机必须用绑定在 `REDUNDANCY_IMAGE_SYNC_PORT = 57576` 的同一个 UDP socket 发送 ACK，因此主机必须校验 ACK 源端口为 `57576`。

如果后续确实需要固定主机 ACK 监听端口，必须在协议和文档中单独声明，不能由实现者自行猜测。

### 第一阶段与第二阶段的边界

第一阶段里的 `payload_crc32` 是 UDP 最小安全校验，不代表进入 scan 语义同步。

第二阶段关注的是增强观测和诊断，例如：

- 更完整的状态码。
- 更清晰的错误计数。
- ACK 延迟统计。
- 最近成功应用时间。
- 后续接入 `timestamp`、`scan_counter`、`tick`、`phase` 的字段预留。

换句话说：

```text
第一阶段：UDP 能以 fragment frame 传当前 full snapshot，并能轻量确认整帧是否应用。
第二阶段：让同步状态更容易监控、诊断和演进。
```

### 第一阶段 wire format 固定约束

第一阶段实现必须以本节为准。后文“后续阶段 DATA/ACK 包头草案”中的扩展字段属于预留方向，第一阶段未实现的字段不要偷偷进入 wire format；如果为了对齐保留字段，必须固定填 0，并在代码注释中写明。

第一阶段 UDP DATA fragment datagram：

| 字段 | 大小 | 类型 | 说明 |
| --- | ---: | --- | --- |
| `magic` | 4 | bytes | 固定 `OPUD` |
| `version` | 2 | uint16 | 固定 `1` |
| `packet_type` | 1 | uint8 | `1 = DATA_FRAGMENT` |
| `reserved` | 1 | uint8 | 固定 `0` |
| `session_id` | 8 | uint64 | 主机同步会话 ID；同一发送线程生命周期内固定 |
| `frame_seq` | 8 | uint64 | 主机 frame 序号，单调递增；一个 full snapshot 对应一个 frame |
| `fragment_index` | 2 | uint16 | 当前 fragment 序号，从 0 开始 |
| `fragment_count` | 2 | uint16 | 当前 frame 的 fragment 总数 |
| `fragment_offset` | 4 | uint32 | 当前 fragment 在 full snapshot payload 中的偏移 |
| `total_len` | 4 | uint32 | 必须等于 `IMAGE_SNAPSHOT_EXPECTED_BYTES` |
| `payload_crc32` | 4 | uint32 | 对完整 full snapshot payload bytes 计算 CRC32 |
| `fragment_payload` | N | bytes | full snapshot 的一段连续切片 |

字节序：

```text
network byte order / big-endian
```

Python `struct` 格式建议：

```python
DATA_FRAGMENT_HEADER = struct.Struct("!4sHBBQQHHIII")
```

第一阶段建议 fragment payload 不超过 1200 字节：

```text
REDUNDANCY_UDP_FRAGMENT_PAYLOAD_MAX = 1200
```

该值用于尽量避开 IP 分片；后续可根据现场网络 MTU 调整。一个 69632 字节 full snapshot 会拆成多个 DATA fragment datagram。第一阶段不做 fragment 重传；备机只在收齐同一 `frame_seq` 的全部 fragment 后才校验整帧 CRC 并调用 `IMAGE_SNAPSHOT_SET`。

第一阶段 `session_id` 规则：

- `session_id` 必须是可比较的单调会话号，不使用随机值。
- 推荐值：持久化 `session_epoch`，每次主机同步发送线程启动时递增并落盘。
- 仅在确认主机 OS 未重启、且进程重启不会导致 monotonic 基准倒退时，才可临时使用 `time.monotonic_ns()` 低 64 位，且必须非零。
- 同一同步发送线程生命周期内 `session_id` 固定，`frame_seq` 从 1 开始单调递增。
- 主机 webserver 重启或同步线程重启后必须生成更大的 `session_id`，允许 `frame_seq` 重新从 1 开始。
- 备机以 `(session_id, frame_seq)` 判断新旧帧。
- 如果 `session_id < active_session_id`，视为旧 session 的迟到 fragments，必须丢弃，不得回 ACK，不得回滚 image tables。
- 如果 `session_id == active_session_id`，按 `frame_seq` 正常处理。
- 如果 `session_id > active_session_id`，视为新 session：清空 pending frame，设置 `active_session_id = session_id`，并重置该 session 的 `last_applied_seq` / `current_pending_frame_seq` 基准。
- 如果无法保证主机或 OS 重启后 `session_id` 单调增大，则必须持久化 session epoch；不能退回随机 `session_id`。

第一阶段 UDP ACK packet：

| 字段 | 大小 | 类型 | 说明 |
| --- | ---: | --- | --- |
| `magic` | 4 | bytes | 固定 `OPAK` |
| `version` | 2 | uint16 | 固定 `1` |
| `packet_type` | 1 | uint8 | `2 = FRAME_ACK` |
| `status` | 1 | uint8 | 见下方状态码 |
| `session_id` | 8 | uint64 | ACK 对应的主机同步会话 ID；无法可信解析时填 0 |
| `ack_frame_seq` | 8 | uint64 | 备机已处理的 frame 序号 |
| `applied_seq` | 8 | uint64 | 最近成功 `IMAGE_SNAPSHOT_SET` 的序号；失败时保持上一次成功值 |

Python `struct` 格式建议：

```python
ACK_HEADER = struct.Struct("!4sHBBQQQ")
```

第一阶段 `status` 枚举：

| 值 | 名称 | 含义 |
| ---: | --- | --- |
| `0` | `OK` | payload 校验通过且 `IMAGE_SNAPSHOT_SET` 成功 |
| `1` | `CRC_ERROR` | payload CRC32 校验失败 |
| `2` | `BAD_HEADER` | magic/version/type/length 等头部非法 |
| `3` | `NOT_READY` | 备机 `plc_main` 未 RUNNING 或 runtime socket 不可用 |
| `4` | `NOT_SHADOW` | 备机不是 shadow standby，拒绝写入 |
| `5` | `APPLY_ERROR` | `IMAGE_SNAPSHOT_SET` 返回硬失败 |
| `6` | `OLD_SEQ` | `frame_seq <= last_applied_seq`，旧帧或重复帧被丢弃 |
| `7` | `FRAME_INCOMPLETE` | 同一 frame 的 fragments 未在不超过 15ms 的窗口内收齐，整帧丢弃 |

CRC 覆盖范围：

```text
payload_crc32 只覆盖完整 full snapshot payload，不覆盖 DATA fragment header。
```

CRC32 算法：

```python
payload_crc32 = zlib.crc32(full_snapshot_payload) & 0xffffffff
```

写入 wire format 时按 network byte order / big-endian 的 uint32 编码。

第一阶段 payload 规则：

```text
full snapshot payload = UnixClient.image_snapshot_get() 返回的 69632 字节 full image snapshot
UDP fragment payload = full snapshot payload 的一段连续切片
```

不要把旧 TCP `OPIM` header 放进 UDP fragment payload，也不要在 payload 前再嵌套旧 `OPIM` header。UDP DATA fragment header 已经替代旧 TCP header。

### 第一阶段 ACK 发送时机

备机 ACK 行为必须明确：

- 来源 IP 不是配置中的 master heartbeat IP：静默丢弃，不回 ACK。
- header 非法、fragment 下标越界、offset 不一致或长度不对：如果能解析出源地址，可以回 `BAD_HEADER`，但不得调用 `IMAGE_SNAPSHOT_SET`；无法可信解析 `session_id` / `frame_seq` 时，ACK 中 `session_id = 0`，`ack_frame_seq = last_rx_frame_seq` 或 `0`。
- `session_id < active_session_id`：旧 session 的迟到 fragments，静默丢弃，不回 ACK，不得调用 `IMAGE_SNAPSHOT_SET`。
- `session_id > active_session_id`：新 session，清空 pending frame，重置该 session 的 `last_applied_seq` / `current_pending_frame_seq` 基准。
- `frame_seq <= last_applied_seq`：回 `OLD_SEQ`，不得调用 `IMAGE_SNAPSHOT_SET`，防止 UDP 乱序旧帧回滚 image tables。
- 如果正在组装 `current_pending_frame_seq`，迟到的 `frame_seq < current_pending_frame_seq` fragments 必须丢弃，不得切回旧 frame。
- 收到更大的 `frame_seq` 时，丢弃尚未完成的旧 pending frame，转为组装最新 frame。
- 同一 frame 的 fragments 未在不超过 15ms 的窗口内收齐：整帧丢弃，必须记录 `session_id`、`frame_seq`、`elapsed_ms`、`FRAME_INCOMPLETE`，可回 `FRAME_INCOMPLETE`，不得调用 `IMAGE_SNAPSHOT_SET`。
- 收齐 frame 后 CRC 错误：回 `CRC_ERROR`，不得调用 `IMAGE_SNAPSHOT_SET`。
- runtime socket 不可用或 PLC 非 RUNNING：回 `NOT_READY`。
- `IMAGE_SNAPSHOT_SET` 返回 `NOT_SHADOW`：回 `NOT_SHADOW`。
- `IMAGE_SNAPSHOT_SET` 返回 `OK`：回 `OK`，并更新 `applied_seq = frame_seq`。
- `IMAGE_SNAPSHOT_SET` 返回其它硬失败：回 `APPLY_ERROR`，`applied_seq` 保持上一次成功值。

`ack_frame_seq` 表示备机已经处理到的 frame 序号；`applied_seq` 只在 `IMAGE_SNAPSHOT_SET` 成功后推进。

frame 组装计时规则：

- 15ms 窗口从某个 `(session_id, frame_seq)` 的第一个有效 fragment 到达时开始。
- 使用 monotonic clock 计时，不能使用 wall clock。
- 超时清理 pending frame 时必须增加 `frame_incomplete_count`。

主机 ACK 接收规则：

- ACK 接收预算从当前 frame 最后一个 fragment `sendto()` 完成后开始。
- ACK 总预算不超过 5ms，使用 monotonic clock 计时。
- 主机只在发送 DATA fragments 的同一个 UDP socket 上接收 ACK。
- 必须校验 ACK 来源 IP 等于 standby heartbeat IP。
- 必须校验 ACK 来源端口等于 `REDUNDANCY_IMAGE_SYNC_PORT = 57576`。
- 必须校验 `magic == OPAK`、`version == 1`、`packet_type == FRAME_ACK`。
- 必须校验 `session_id == current_session_id`。
- 必须校验 `ack_frame_seq <= last_send_frame_seq`。
- 只有 `ack_frame_seq == current_frame_seq` 的 ACK 才能满足当前帧 ACK。
- `ack_frame_seq < current_frame_seq` 的迟到 ACK 只能记录为历史/迟到 ACK，不得抵消当前帧 timeout。
- `ack_frame_seq < last_ack_frame_seq` 的旧 ACK 不得更新 `last_ack_frame_seq` / `last_applied_seq`。
- `applied_seq < last_applied_seq` 的旧 ACK 不得回退本地统计。
- 超时必须增加 `ack_timeout_count`，并立即进入下一帧发送。

### 第一阶段必须记录的统计

第一阶段实现必须至少记录这些统计，避免“UDP 应用层分片 + frame ACK”变成不可观测的大包发送：

主机侧：

- `frame_send_count`
- `fragment_send_count`
- `ack_ok_count`
- `ack_timeout_count`
- `ack_error_count`
- `current_session_id`
- `last_send_frame_seq`
- `last_ack_frame_seq`
- `last_applied_seq`

备机侧：

- `fragment_rx_count`
- `frame_complete_count`
- `frame_incomplete_count`
- `bad_source_count`
- `bad_header_count`
- `session_reset_count`
- `old_session_count`
- `old_seq_count`
- `crc_error_count`
- `not_ready_count`
- `not_shadow_count`
- `apply_error_count`
- `applied_count`
- `last_rx_frame_seq`
- `last_applied_seq`

这些统计第一阶段可以只写日志或内部变量；后续再纳入 `REDUNDANCY_SYNC_STATUS`。

fragment 组装验收规则：

- `fragment_offset` 必须等于 `fragment_index * REDUNDANCY_UDP_FRAGMENT_PAYLOAD_MAX`，包括最后一个 fragment。
- 非最后 fragment 的 `fragment_payload` 长度必须等于 `REDUNDANCY_UDP_FRAGMENT_PAYLOAD_MAX`。
- 最后一个 fragment 必须满足 `fragment_offset + len(fragment_payload) == total_len`。
- 同一 `(session_id, frame_seq)` 下所有 fragments 的 `fragment_count`、`total_len`、`payload_crc32` 必须一致。
- 重复收到相同 `fragment_index` 且内容一致，可以忽略。
- 重复收到相同 `fragment_index` 但内容不同，必须丢弃整个 pending frame，并记录 `bad_header_count` 或单独的 duplicate conflict 日志。
- `fragment_count` 必须等于 `ceil(total_len / REDUNDANCY_UDP_FRAGMENT_PAYLOAD_MAX)`。
- 收到更大 `frame_seq` 导致未完成 pending frame 被覆盖时，必须增加 `frame_incomplete_count` 或单独记录 `frame_superseded_count`。
- 只有收齐全部 fragment 且 CRC 通过后，才能调用 `IMAGE_SNAPSHOT_SET`。

### 第一阶段验收清单

第一阶段代码审查时必须逐项确认：

- 只修改 `webserver/runtimemanager.py` 中 IO image tables 同步链路及必要的局部 helper。
- 未修改 `REDUNDANCY_HEARTBEAT_PORT = 57575` 的 TCP 心跳逻辑。
- 未修改主备角色判断。
- 未修改功能 IP 接管或恢复逻辑。
- 未修改升主/回切逻辑。
- 未修改 `REDUNDANCY_SHADOW_EXIT`。
- 未修改 `plc_cycle_thread()`。
- 未修改 `core/src/plc_app/image_snapshot.c` 的 payload 语义。
- 未新增 `plc_main` UDP 线程。
- UDP 同步仍使用心跳 IP，不使用功能 IP。
- ACK 不参与主备切换判断。
- ACK 不阻塞下一帧 DATA 发送。
- ACK 回到 DATA fragment 的源地址和源端口，主机不额外监听 `57576` 接 ACK。
- 备机 ACK 必须从 `57576` 端口发出，主机必须校验 ACK 源端口为 `57576`。
- 当前帧 ACK 必须满足 `ack_frame_seq == current_frame_seq`，迟到 ACK 不得抵消当前帧 timeout。
- 69632 字节 full snapshot 被拆成多个 UDP fragment datagram，不作为单个 UDP datagram 发送。
- 未实现 fragment 重传和 per-fragment ACK。
- 使用 `session_id` 处理主机 webserver 或同步线程重启后的 `frame_seq` 重置。
- `session_id` 为单调可比较会话号，不使用随机值；如果无法保证单调，必须持久化 session epoch。
- 旧帧或乱序帧不会回滚 image tables。
- 迟到的旧 pending frame fragments 不会让备机切回旧 frame。
- UDP fragment payload 不包含旧 `OPIM` header。
- 对 fragment 丢失、frame 不完整、CRC 错误和应用失败有日志和统计。

### 文档编码

本文档应保存为 UTF-8。Windows PowerShell 默认编码读取可能出现乱码；审阅时应使用 UTF-8 读取。

## 当前边界

### 本阶段不改的内容

以下热冗余能力暂时保持现状：

- TCP 心跳链路。
- `redundancy_role.json` 的主备角色判断。
- 心跳 IP / 功能 IP 的配置与识别逻辑。
- 主机故障检测。
- 备机升主。
- 原主恢复后的回切。
- 功能 IP 接管与恢复。
- `REDUNDANCY_SHADOW_EXIT` 平滑升主逻辑。
- 当前 `IMAGE_SNAPSHOT_GET` / `IMAGE_SNAPSHOT_SET` 的 C 端导入导出语义。

### 本阶段只改的内容

只改当前 IO image tables 数据同步链路：

- 主机同步线程：`webserver/runtimemanager.py` 中的 `_redundancy_image_sync_master_loop()`
- 备机同步线程：`webserver/runtimemanager.py` 中的 `_redundancy_image_sync_standby_loop()`
- 当前同步端口：`REDUNDANCY_IMAGE_SYNC_PORT = 57576`
- 当前同步地址：主机心跳 IP 与备机心跳 IP

也就是说，只把 **57576 端口上的 IO image tables 同步传输方式** 从 TCP 改成 UDP 应用层分片 + frame ACK，其它逻辑暂不变化。

## 心跳 IP 与功能 IP 的角色

热冗余中需要明确区分两类 IP：

| 类型 | 用途 | 本计划是否改动 |
| --- | --- | --- |
| 心跳 IP | 主备内部冗余通信，包括心跳检测和 IO image tables 同步 | 只在 IO image tables 同步链路里继续使用 |
| 功能 IP | 对外服务、现场通信、业务访问，升主/回切时会接管或恢复 | 不改 |

本计划中的 UDP 数据同步仍然走心跳 IP：

```text
主机 _redundancy_local_heartbeat_ip
    -> 备机 _redundancy_standby_ip:57576

备机 _redundancy_local_heartbeat_ip
    -> DATA fragment 的源地址和源端口发送 ACK
```

不会改成功能 IP，也不会影响功能 IP 接管逻辑。

## 阶段一：IO image tables 链路改为 UDP 应用层分片 + frame ACK

### 改造目标

将当前数据同步链路：

```text
主机 TCP connect 备机心跳 IP:57576
主机 sendall(OPIM header + full image snapshot)
备机 accept()
备机 recv header/body
备机 IMAGE_SNAPSHOT_SET
```

改为：

```text
主机将 full snapshot 拆成 DATA fragments
主机 UDP sendto(备机心跳 IP:57576, DATA fragment datagrams)
备机 UDP recvfrom()
备机按 frame_seq 组装 fragments
备机校验来源 IP、协议头、fragment 完整性、整帧 crc32
备机 IMAGE_SNAPSHOT_SET
备机 UDP sendto(DATA 源地址和源端口, frame ACK packet)
主机接收 ACK 并记录备机最近同步状态
```

### 为什么不使用 TCP

PLC 热冗余同步更关心最新状态，而不是旧状态必达。TCP 的可靠流语义可能带来：

- 队头阻塞。
- 旧包重传挤占新状态。
- 连接异常后的恢复延迟。
- 数据流边界需要额外拆包。

UDP 更符合“最新状态优先”的同步模型。轻量 ACK 用于感知备机是否收到和应用成功，但不把 UDP 重新做成 TCP。

### 轻量 ACK 的原则

ACK 只回答三个问题：

- 备机最近处理到哪个 frame 序号。
- 备机最近成功应用哪个 frame 序号。
- 最近一次应用结果是否成功。

ACK 不承担以下职责：

- 不替代 TCP 心跳。
- 不参与升主/回切决策。
- 不要求主机重传旧 snapshot。
- 不保证每一帧必达。
- 不阻塞主机发送下一帧。

### 后续阶段 DATA 包头草案

下面是后续阶段的扩展草案，不是第一阶段 wire format。第一阶段必须以前文“第一阶段 wire format 固定约束”为准。后续草案是在第一阶段 envelope 基础上扩展，除非另行定义版本协商，不得丢弃 `session_id`。字段顺序应在具体版本设计时重新固定；本草案不作为兼容 wire format。

```text
magic              4 bytes   固定值，例如 OPUD
protocol_version   2 bytes   协议版本
packet_type        1 byte    DATA / DATA_FRAGMENT / DELTA 等
flags              1 byte    FULL / DELTA / FRAGMENT 等标志
frame_seq          8 bytes   同步帧序号
session_id         8 bytes   同步会话 ID，继承第一阶段语义
scan_counter       8 bytes   PLC scan 计数，后续接入 scan cycle
tick               8 bytes   PLC tick，后续接入 scan cycle
phase              1 byte    同步阶段，后续可为 SNAPSHOT_ASYNC / SCAN_END 等
reserved           7 bytes   对齐和扩展
timestamp_ns       8 bytes   主机发送时间
payload_len        4 bytes   payload 长度
payload_crc32      4 bytes   payload CRC32
header_crc32       4 bytes   可选，用于保护头部
payload            N bytes   当前 full image snapshot 或后续 delta
```

后续阶段仍可兼容现有 full image snapshot payload：

```text
IMAGE_SNAPSHOT_EXPECTED_BYTES = 1024 * 68 = 69632
```

但要明确：69KB 不仅超过常规 MTU，也超过 UDP 单 datagram 的可承载上限。第一阶段必须使用前文定义的最小应用层分片；正式长期方案应通过 delta / dirty row 降低 payload。

### 后续阶段 ACK 包头草案

下面是后续阶段的扩展草案，不是第一阶段 wire format。后续草案是在第一阶段 envelope 基础上扩展，除非另行定义版本协商，不得丢弃 `session_id`。

```text
magic              4 bytes   固定值，例如 OPAK
protocol_version   2 bytes
packet_type        1 byte    ACK = 2
status             1 byte    继承第一阶段 status 枚举；可继续扩展，示例非穷举
session_id         8 bytes   同步会话 ID，继承第一阶段语义
ack_frame_seq      8 bytes   最近处理的 DATA frame seq
applied_seq        8 bytes   最近成功应用的 DATA frame seq
standby_state      2 bytes   备机状态摘要
reserved           6 bytes
timestamp_ns       8 bytes   备机发送 ACK 时间
```

主机收到 frame ACK 后只做记录和日志，例如：

- 最近 ACK 序号。
- 最近成功应用序号。
- ACK 延迟估计。
- 连续 ACK 丢失次数。
- 最近错误状态。

初期不改变热冗余心跳和升主判断。

### 阶段一的实现边界

主机仍然在原触发点执行：

```text
runtime_socket.image_snapshot_get()
拆分 full snapshot 为 DATA fragments
UDP send DATA fragments
非阻塞轮询或有总预算的短轮询 frame ACK
sleep 原有节奏
```

备机仍然在原同步线程执行：

```text
UDP recv DATA fragments
校验来源必须是 master heartbeat IP
校验 magic/version/fragment/length/crc32
收齐 frame 后得到 full snapshot payload
确认 _plc_shadow_standby
确认 plc_main RUNNING
runtime_socket.image_snapshot_set(payload)
UDP send frame ACK 到 DATA 源地址和源端口
```

也就是说，同步触发点暂时不变，先只替换传输方式。

## 阶段二：snapshot 元数据与观测升级

在 UDP 链路稳定后，为 snapshot 增加更明确的同步语义，并强化同步状态观测。

第一阶段已经有最小 `frame_seq` 和 `payload_crc32`，阶段二继续完善或接入这些字段：

- `scan_counter`
- `tick`
- `phase`
- `timestamp`
- `crc32`

字段含义：

| 字段 | 作用 |
| --- | --- |
| `scan_counter` | 标识该数据属于主机哪个 PLC 扫描周期。 |
| `tick` | 对应 PLC 运行 tick，可与现有 `ext_config_run__(tick__++)` 对齐。 |
| `phase` | 标识数据采集点，例如 scan start、scan end、async snapshot。 |
| `timestamp` | 主机生成或发送该数据的时间，用于延迟和抖动观测。 |
| `crc32` | 第一阶段用于 payload 最小安全校验；阶段二继续强化为完整诊断字段，可扩展 header/payload 校验统计。 |

阶段二的关键不是改变同步时机，而是先让每份数据“可解释、可校验、可追踪”。这一阶段仍不要求真实 scan boundary 驱动；`scan_counter` / `tick` / `phase` 可以先以占位或异步快照语义接入，后续再和 PLC scan cycle 边界绑定。

## 阶段三：同步点下沉到 PLC scan cycle 边界

当前同步点在 Web 层定时循环中，约 20ms 一次。这种方式存在两个问题：

- 数据不一定对应某个明确的 PLC scan boundary。
- 主备机 PLC 逻辑没有扫描周期同步关系。

阶段三目标：

```text
从 Web 层 20ms 定时循环
下沉到 PLC scan cycle 边界触发
```

推荐先选择主机 scan cycle end：

```text
主机 scan cycle 执行结束
    -> 生成本周期 output/memory 状态
    -> 附带 scan_counter/tick/phase/timestamp/crc32
    -> 发送给备机
```

这一步可以逐步演进：

1. 先由 C 层在 scan end 生成状态并暴露给 Web 层读取。
2. 再考虑由 C 层直接通过 UDP 发送，绕开 Web 层调度抖动。

第一步更稳，第二步性能更好。

## 阶段四：从 full snapshot 改为 output/memory delta

当前 full snapshot 为 69632 字节，不适合高频 UDP 同步。后续应从全量同步演进到增量同步。

推荐主机在每个 scan cycle 结束后同步：

```text
output delta
memory delta
必要的 input 状态摘要
```

而不是每次同步完整：

```text
bool_input / bool_output / byte_input / byte_output / int_input / int_output / ...
```

delta 可以采用两种方案：

### 方案 A：entry list

```text
buffer_type
index
bit
value
```

适合变化点较少的场景。

### 方案 B：dirty row bitmap + changed rows

```text
dirty_row_bitmap
changed row payloads
```

适合按 row 批量变化的场景。

阶段四的目标是让每个 scan cycle 的同步 payload 尽量落在一个或少量 UDP datagram 内，减少 IP 分片和锁竞争。

## 阶段五：备机在 scan cycle 开始前或固定 barrier 点应用主机状态

当前备机收到 snapshot 后，会通过 `IMAGE_SNAPSHOT_SET` 在任意时刻写入 image tables。虽然 C 端使用 `buffer_mutex` 与 PLC scan cycle 互斥，但语义上仍然不是严格的扫描周期同步。

后续应改为：

```text
备机接收 DATA
    -> 先缓存为 pending state

备机 scan cycle start 或固定 barrier
    -> 应用最新 pending state
    -> 标记 applied_seq
    -> 发送 ACK
```

这样可以避免数据在备机 PLC 周期中间突然生效，让备机状态推进具备明确边界。

建议的应用策略：

- 如果 `frame_seq <= last_applied_seq`，丢弃。
- 如果 `frame_seq == last_applied_seq + 1`，正常应用。
- 如果 `frame_seq > last_applied_seq + 1`，接受最新状态，但记录 missed frame。
- 不为旧状态阻塞，不等待补包。

## 阶段六：UDP 数据面下沉到 plc_main

在前面阶段稳定后，可以进一步把 PLC 数据同步 UDP 链路从 `webserver` 层下沉到 `plc_main` 层。

这个阶段的核心目标不是改变热冗余控制逻辑，而是缩短高频数据路径、减少进程间通信和 Python 调度抖动。

### 当前数据路径

当前 Web 层承担数据同步转发：

```text
主机 plc_main image tables
    -> 主机 Unix socket
    -> 主机 webserver RuntimeManager
    -> UDP 网络
    -> 备机 webserver RuntimeManager
    -> 备机 Unix socket
    -> 备机 plc_main image tables
```

### 下沉后的数据路径

UDP 数据面下沉后，数据同步链路变为：

```text
主机 plc_main image tables
    -> UDP 网络
    -> 备机 plc_main image tables
```

这样可以减少两段高频 IPC：

- 主机 `plc_main -> webserver`
- 备机 `webserver -> plc_main`

同时也让同步逻辑更贴近 PLC scan cycle 边界。

### webserver 仍然是控制面

下沉 UDP 数据面不代表把主备热冗余逻辑搬进 `plc_main`。职责边界应保持为：

```text
webserver = redundancy control plane
plc_main  = redundancy data plane
```

`webserver` 继续负责：

- 读取 `redundancy_role.json`。
- 区分心跳 IP / 功能 IP。
- 判断本机是主机还是备机。
- TCP 心跳检测。
- 主机下线判断。
- 备机升主判断。
- 原主上线后的回切判断。
- 功能 IP 接管和恢复。
- 决定 `plc_main` 是否 shadow standby。
- 决定是否开启/关闭 PLC 数据同步 UDP。
- 监控 `plc_main` 的 PLC 状态和数据同步状态。
- 必要时重启 `plc_main` 或发送控制命令。

`plc_main` 只负责：

- 持有 IO image tables。
- 持有 PLC scan cycle。
- 按 webserver 下发的配置开启或关闭 UDP 数据同步。
- master 模式：在指定 scan boundary 发送 snapshot/delta。
- standby shadow 模式：接收 UDP 数据，缓存为 pending state 或在 barrier 应用。
- 维护同步运行状态，例如 `last_frame_seq`、`last_ack_frame_seq`、`last_applied_seq`、`crc_error_count`、`drop_count`、`apply_error_count`。
- 通过 Unix socket 把同步状态报告给 webserver。

### webserver 对 plc_main 的控制命令

该阶段需要扩展 `plc_main` 的 Unix socket 控制协议。建议增加：

```text
REDUNDANCY_SYNC_CONFIG:<json>\n
REDUNDANCY_SYNC_START\n
REDUNDANCY_SYNC_STOP\n
REDUNDANCY_SYNC_STATUS\n
```

示例配置内容：

```json
{
  "enabled": true,
  "data_plane_mode": "master_sender",
  "local_heartbeat_ip": "192.168.200.10",
  "peer_heartbeat_ip": "192.168.200.20",
  "udp_port": 57576,
  "mode": "scan_end_delta"
}
```

备机 shadow standby 示例：

```json
{
  "enabled": true,
  "data_plane_mode": "standby_receiver",
  "local_heartbeat_ip": "192.168.200.20",
  "peer_heartbeat_ip": "192.168.200.10",
  "udp_port": 57576,
  "mode": "shadow_barrier_apply"
}
```

`data_plane_mode` 只能由 webserver 控制面下发，表示 `plc_main` 数据面执行哪种同步行为；它不是 `plc_main` 自行判断出的主备身份。

### 状态上报

`REDUNDANCY_SYNC_STATUS` 应返回可被 webserver 监控的状态，例如：

```json
{
  "enabled": true,
  "data_plane_mode": "standby_receiver",
  "running": true,
  "active_session_id": 1234567890123,
  "last_rx_frame_seq": 10240,
  "last_applied_seq": 10240,
  "last_ack_frame_seq": 10240,
  "missed_count": 3,
  "crc_error_count": 0,
  "apply_error_count": 0,
  "last_peer_ip": "192.168.200.10",
  "last_update_monotonic_ms": 123456789
}
```

这些状态只用于监控和诊断。初期不直接参与主备切换，避免影响现有 TCP 心跳逻辑。

### 风险控制

这个阶段要遵守以下约束：

- `plc_main` 不读取 `redundancy_role.json`。
- `plc_main` 不判断主备身份。
- `plc_main` 不做 TCP 心跳检测。
- `plc_main` 不接管或恢复功能 IP。
- `plc_main` 不自行决定升主或回切。
- UDP 同步启停必须由 webserver 控制。
- webserver 始终观察和管理 `plc_main` 的运行状态、shadow standby 状态和同步状态。

### 预期收益

- 减少同步链路上的进程间通信。
- 减少 Python 线程调度和 GIL 带来的抖动。
- 同步点更容易贴近 `plc_cycle_thread()` 的 scan start / scan end。
- ACK 能表达“备机 plc_main 已应用到哪个 scan_seq”，语义比 Web 层转发 ACK 更准确。
- 为后续更强的 scan boundary 同步和 lock-step 打基础。

## 阶段七：后续再考虑 lock-step

在完成 UDP、metadata、scan boundary、delta、barrier，以及 `plc_main` 数据面下沉后，再考虑更强的 lock-step 模式。

lock-step 的目标：

```text
主备同周期执行 PLC
主备使用同一份输入快照
主备比较关键 memory/output
```

可能路径：

1. 主机在 scan start 同步 input snapshot。
2. 主备使用相同 `scan_counter` 和 input 执行 PLC。
3. 主机在 scan end 同步 output/memory hash。
4. 备机计算本地 hash 并对比。
5. 不一致时记录 divergence，必要时强制备机回放主机状态。

lock-step 对系统要求更高：

- PLC 程序必须具备足够确定性。
- 主备 scan boundary 需要更强控制。
- 时间相关函数、外部输入、插件行为需要规范化。
- 对比策略要避免误判和过度切换。

因此 lock-step 不应作为第一阶段目标，而应放在 UDP + scan boundary 同步稳定之后。

## 建议实施顺序

1. **局部替换 IO image tables 同步链路**
   - 仅将 `REDUNDANCY_IMAGE_SYNC_PORT = 57576` 上的数据同步从 TCP 改为 UDP。
   - 保留原触发点、原心跳、原升主回切逻辑。
   - 增加轻量 ACK。

2. **增强协议观测和诊断**
   - 第一阶段已经包含最小 `frame_seq` 和 `payload_crc32`。
   - 继续补充更完整的 ACK 状态、错误码、延迟统计和同步状态上报。
   - 主机记录最近 ACK、最近 applied frame seq、ACK 丢失次数和链路延迟。
   - 备机记录丢包、乱序、CRC 错误、应用失败和最近成功应用时间。

3. **加入 scan metadata**
   - 增加 `scan_counter`、`tick`、`phase`。
   - 初期可先填充占位值，再接入 C 层真实 scan 信息。

4. **同步点下沉到 scan cycle end**
   - 主机在每个 PLC scan cycle 结束后生成同步数据。
   - Web 层从定时驱动逐步变成 scan event 驱动。

5. **full snapshot 改 delta**
   - 先支持 full snapshot。
   - 再增加 output/memory delta。
   - 最终降低单周期同步 payload。

6. **备机固定 barrier 应用**
   - 备机不再任意时刻 import。
   - 改为 scan start 或固定 barrier 应用最新 pending state。

7. **UDP 数据面下沉到 plc_main**
   - webserver 继续作为控制面，负责角色、心跳、影子执行、升主回切和功能 IP。
   - plc_main 作为数据面，负责 UDP DATA/ACK、scan boundary 发送、barrier 应用和同步状态统计。
   - 通过 Unix socket 增加 `REDUNDANCY_SYNC_CONFIG`、`REDUNDANCY_SYNC_START`、`REDUNDANCY_SYNC_STOP`、`REDUNDANCY_SYNC_STATUS` 等控制命令。
   - webserver 持续监控 `plc_main` 上报的同步状态，但不把初期 UDP ACK 直接接入主备切换决策。

8. **评估 lock-step**
   - 主备同周期执行。
   - 比较关键 memory/output。
   - 作为后续增强，不放入初期改造。

## 风险控制原则

- 每个阶段都应可独立验证。
- 第一阶段只替换 IO image tables 同步传输，不改变同步语义。
- 不让 ACK 参与主备切换决策，避免影响现有心跳逻辑。
- 不在第一阶段引入 scan cycle 重构。
- 即使后续 UDP 数据面下沉到 `plc_main`，主备身份、心跳、影子执行、升主回切和功能 IP 接管仍由 webserver 控制。
- `plc_main` 不读取 `redundancy_role.json`，不自行判断主备身份，不自行决定升主或回切。
- UDP 大包问题要尽早暴露，但不要一开始就引入复杂分片和重传。
- 所有协议版本、长度、CRC、状态码都必须可日志化，便于现场排查。

## 一句话总结

先在原有 IO image tables 同步触发点上，把主备心跳 IP 之间的 TCP 数据同步局部替换为 UDP 应用层分片 + frame ACK；待链路稳定后，再逐步加入 scan metadata、scan cycle 边界触发、output/memory delta、备机 barrier 应用；随后把高频 UDP 数据面下沉到 `plc_main`，但 webserver 继续负责控制面和状态监控，最后再评估主备 lock-step 执行与结果比对。
