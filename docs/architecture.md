# 架构走读

本文按“一个字节从网卡进来，到一条消息从另一个客户端出去”的顺序，走读 Sol 的
结构。目标不是罗列文件，而是说明**每一层为什么存在、边界在哪里、改动一个功能
要动哪些文件**。

## 1. 总体形状

```
                        ┌─────────────────────────────────────────┐
                        │                 sol.c                   │
                        │  参数解析 / 信号 / daemon / start_server │
                        └───────────────────┬─────────────────────┘
                                            │
                        ┌───────────────────▼─────────────────────┐
                        │                server.c                 │
                        │ 监听 socket、事件循环、启动与关闭        │
                        └───────┬──────────┬────────────┘
                                │          │
                    ┌───────────▼──┐   ┌───▼────────────┐
                    │  client.c    │   │   cron.c       │
                    │连接生命周期、│   │统计/重传/保活/│
                    │网络回调      │   │内存/持久化任务│
                    └───────┬──────┘   └───────┬────────┘
                            │                  │
                 ┌──────────────▼──────────┐   ┌────────▼─────────────┐
                 │ handlers.c（分发表）      │   │  topic_store.c        │
                 │ connect.c  CONNECT/遗嘱  │   │  精确主题 + 通配索引   │
                 │ subscribe.c 订阅/保留消息 │   │  topic.c/subscriber.c │
                 │ publish.c PUBLISH/QoS ACK│   └────────┬─────────────┘
                 │ session.c 会话/inflight   │            │
                 └───┬────────┬────────┬───┘            │
                     │        │        │                │
        ┌────────────▼──┐ ┌───▼─────┐ ┌▼──────────┐ ┌───▼──────────┐
        │    mqtt.c     │ │ acl.c   │ │persistence│ │  memory.c    │
        │ 编解码/校验    │ │ 主题 ACL │ │ 快照+WAL  │ │ 记账/回收     │
        └───────┬───────┘ └─────────┘ └───────────┘ └──────────────┘
                │
        ┌───────▼───────┐   ┌──────────────┐   ┌──────────────────┐
        │ topic_match.c │   │    ev.c      │   │ config.c/logging │
        │ §4.7 纯匹配    │   │ epoll/poll/  │   │ 配置/日志         │
        │               │   │ select/kqueue│   │                  │
        └───────────────┘   └──────────────┘   └──────────────────┘
```

依赖方向基本是单向的：`sol.c → server.c → client.c/cron.c`，网络回调再进入
`handlers.c`（及 `connect/session/subscribe/publish`），最终使用
`mqtt/topic_store/persistence/acl → memory/util`。`handlers_internal.h` 和
`server_internal.h` 分别放命令模块、服务器模块之间共享的内部原型。
`topic_match.c` 是叶子模块，不依赖任何东西，这是它能在没有 broker 的情况下
被单测的原因。

## 2. 进程、线程与事件循环

- `main()` 在 `sol.c`，负责 `getopt`、读配置、`config_set_default()`、
  `daemonize()`（`-d`）、安装 `SIGINT/SIGTERM` 处理，然后调用
  `start_server()`。`struct config` 和 `struct server` 都是 `main()` 栈上的
  局部变量，`server.conf` 指向同一份配置：模块之间只通过显式参数传递，没有
  进程级 `conf` / `server` 单例。
- `start_server()` 创建监听 socket，按 `THREADSNR` 启动 1 + N 个
  `eventloop_start()` 线程。每个线程有自己的 `struct ev_ctx`，监听同一个
  socket（`SO_REUSEPORT` 由 `network.c` 决定），accept 到的连接归属该线程。
- 定时任务只注册在 **index 0** 的 loop 上（`loop_data->cronjobs`），避免 N 个
  线程同时做重传、统计、持久化。每个 loop 在 `ev_ctx.data` 里保存自己的
  `struct server *`，回调通过它访问共享表和配置，配合每客户端锁与工作线程
  交互。
- 没有“每请求一个线程”的模型：`read_callback()` 读到的完整包由同一个 loop
  线程调用 `process_message()` → `handle_command()` → 具体 handler，handler
  把回复直接写进该 client 的写缓冲。这是理解锁范围的关键：**同一个 client 的
  读写永远在拥有它的 loop 线程上**，跨线程只发生在投递给别的 client、踢重复
  client id、定时扫描这几处，由 `pthread_mutex_t mutex` 保护全局表。

事件循环抽象在 `ev.c` / `ev.h`，Linux 上是 epoll（可强制 poll/select），
BSD/macOS 上是 kqueue。接口只有 `ev_init/ev_run/ev_watch_fd/ev_register_event/
ev_register_cron/ev_fire_event/ev_stop`，上层不出现 `epoll_event` 之类的类型。
`conf->run`（Linux eventfd，其它平台 pipe）是唤醒循环、让 `ev_run` 退出的
“自唤醒”描述符；信号处理器只保存这个 fd 的副本，不去解引用配置结构。

## 3. 一个连接的完整生命周期

1. **accept**：`client_accept_callback()` 从 `server.pool` 取一个 `struct client`，
   初始化读写缓冲为 NULL/0（按需增长，空闲连接不占 64KB），注册 `EV_READ`。
2. **分帧**：`recv_packet()` 先读 2 字节固定头，再按 Remaining Length 的
   变长编码算出总长；超过 `conf->max_request_size` 直接断开。缓冲区按需
   `recv_reserve()`，上限同样是 `max_request_size`。
3. **解码**：`mqtt_unpack()` 校验 fixed header flags、包类型、长度和字段，
   失败返回负值；`process_message()` 把它翻译成协议错误并断开（有遗嘱则先
   发布遗嘱）。
4. **分发**：`handle_command()` 用函数指针表按包类型调用
   `connect_handler` / `subscribe_handler` / `publish_handler` / ...。
   `protocol_violation_handler` 占住客户端不该发送的类型（CONNACK、SUBACK、
   UNSUBACK 等）。
5. **状态机**：`client->status`（`CONNECTED`、`DISCONNECTING` 等）决定哪些
   包在 CONNECT 之前非法；`client->connected` 防止二次 CONNECT。
6. **关闭**：`client_close()` 释放缓冲、会话引用、连接；`client_deactivate()`
   从 `server.clients_map` 摘除并决定会话是删除（clean session）还是保留
   （持久会话 + 离线队列）。`client_shutdown()` 统一处理“发完 CONNACK 再关”
   与“直接关”。

## 4. 会话模型

`struct client` 是**一条网络连接**，`struct client_session` 是**一个 client id
的状态**，两者用引用计数（`ref.h`）分开：

- `server.clients_map`：client id → 当前在线连接（UTHASH）。
- `server.sessions`：client id → 会话（UTHASH），连接断开后仍可能存在。
- 会话保存：订阅列表、离线消息队列、QoS 1/2 出站 inflight、入站 QoS 2
  去重集合、clean session 标志、遗嘱（仅连接期间有效）。
- `clean_session=1` 断开时删除会话；`clean_session=0` 保留，重连时
  `session_present=1` 并把离线队列和 inflight 重发。
- 同一个 client id 再次 CONNECT：`server_kick_duplicate()` 关闭旧连接，
  旧连接的遗嘱按 §3.1.2.5 发布（在释放全局锁之后投递，避免递归加锁）。

**遗嘱不持久化**：它属于一条网络连接，不是会话状态。进程重启后不会补发。

## 5. 主题与投递

`topic_store.c` 维护两套索引：

- 精确主题：`Trie`，key 是完整主题名。`a` 与 `a/` 是两个不同的 key。
- 通配订阅：按 `/` 分段的前缀树（`wild_index`），`+` 匹配一段，`#` 匹配
  剩余部分；订阅时插入，退订时删除，并处理“同一会话同时有精确和通配订阅”
  的去重。

匹配规则集中在纯函数 `topic_match()`（`src/topic_match.c`）：

- `+` 恰好消费一个 level，空 level 也算；
- `#` 消费剩余所有 level，可以为零；
- 以通配符开头的过滤器不匹配 `$` 开头的主题（§4.7.2）；
- `multilevel` 参数只为兼容 v1 持久化记录（当时把 `#` 存成了前缀）。

一次 PUBLISH 的投递路径：

```
publish_handler
  → ACL 检查（acl.c，按 client id / username / topic）
  → 保留消息更新（retain=1 存 topic->retained，retain=0 且空 payload 清除）
  → topic_store 查精确订阅 + 通配索引
  → 对每个 subscriber：
       在线 → session_queue_message()/直接写，QoS 取 min(发布 QoS, 订阅 QoS)
       离线且持久会话 → 进 outgoing_msgs（受内存策略约束）
  → 入站 QoS1 回 PUBACK；QoS2 回 PUBREC 并记录 q2_recv_mids
```

出站 QoS 1/2 的状态在 `session->i_msgs`（按 packet id 稀疏存放）和
`pending_mids`（只扫描真正在途的 id）。`cron_inflight_msg_check()` 每秒扫描一次，
超时重发并置 DUP；收到 PUBACK/PUBCOMP 后清除，PUBREC 后改发 PUBREL。

## 6. 持久化

`persistence.c` 是“快照 + 预写日志”：

- 快照文件由 `persistence_file` 指定，默认关闭。内容：保留消息、持久会话的
  订阅、离线队列、inflight、PUBREL 阶段、入站 QoS2 去重集合。文件头有版本
  号，v1 记录在加载时迁移。
- 每次状态变更先追加到 `<file>.wal` 并 `fsync`，再改内存；
  `cron_persistence_check()`
  每秒检查 `persistence_interval`，到点写新快照并清空/轮转 WAL。
- 启动时先读快照，再按 generation 重放 WAL；已经在快照里的记录按序号跳过。
- 干净退出（SIGINT/SIGTERM）会再写一次快照。
- 取舍：每条 WAL 记录一次 fsync，吞吐换“最多丢最后一条记录”；遗嘱、在线
  连接、统计计数不落盘。

## 7. 内存策略

所有堆分配走 `memory.c` 的 `try_alloc/try_calloc/try_realloc/free_memory`，
它维护一个全局原子计数器，`memory_used()` 是 `max_memory` 判定的依据。
`cron_memory_check()` 每秒运行，超限时按顺序：

1. 回收没有任何订阅者/保留消息引用的主题（`topic_store_reclaim`）；
2. 丢弃离线队列里最旧的消息（`session_drop_oldest_queued`）；
3. 仍超限则置 `server.over_memory`，`client_accept_callback()` 拒绝新连接，直到
   内存回落到限制以下。

客户端结构体来自 `memorypool.c` 的固定块池，避免频繁 malloc；读写缓冲按需
增长，空闲连接只占结构体本身。

## 8. 可观测性

- `struct sol_info info`：连接数、收发消息数、字节数、启动时间/uptime，全部
  原子类型。
- `cron_publish_stats()` 每 `stats_pub_interval` 秒向 `$SOL/...` 发布统计；这些
  主题对以通配符开头的订阅不可见（§4.7.2），只能显式订阅。
- 日志在 `logging.c`，级别由 `log_level` 控制，`-v` 打开 DEBUG；支持
  `logpath` 写文件。

## 9. 错误处理约定

- `mqtt_unpack()` 返回 `MQTT_OK` 或负的 `-MQTT_ERR`，失败时自己释放已经
  分配的部分（topic、subscribe/unsubscribe tuples），调用方只需
  `mqtt_packet_destroy()` 成功包。
- `handle_command()` 返回 `SOL_OK` / 负的 `SOL_ERR`；协议违规由
  `process_message()` 统一转成断开 + 遗嘱。
- 内存不足不是致命错误：分配函数记日志并返回 NULL，调用方决定降级
  （丢弃回复、拒绝连接、丢最旧消息）。

## 10. 改一个功能要动哪里

| 想做的事 | 主要文件 |
|---|---|
| 新增/修改 MQTT 包字段 | `mqtt.c`、`mqtt.h`、`tests/protocol/mqtt_test.c` |
| 改主题匹配规则 | `topic_match.c`、`tests/protocol/topic_match_test.c` |
| 改订阅索引 | `topic_store.c`、`trie.c` |
| 改 QoS/会话行为 | `session.c`、`publish.c`、`sol_internal.h` |
| 改 CONNECT/遗嘱 | `connect.c` |
| 改订阅/保留消息 | `subscribe.c` |
| 改连接生命周期/网络回调 | `client.c`、`server_internal.h`、`network.c` |
| 改事件循环/监听启停 | `server.c`、`ev.c`、`network.c` |
| 改定时任务 | `cron.c`、`server_internal.h` |
| 改持久化格式 | `persistence.c`、`tests/integration/test_persistence.py` |
| 改配置项 | `config.c`、`config.h`、`etc/conf/sol.conf`、`tests/protocol/config_test.c` |
| 改 ACL | `acl.c`、`tests/integration/test_acl.py` |
| 改内存策略 | `memory.c`、`cron.c:cron_memory_check()` |

## 11. 已知边界

- 单进程、单机；没有集群、共享订阅、桥接。
- 事件循环线程数由 `THREADSNR` 编译期决定，运行期不可调。
- 配置和 broker 状态由 `main()` 持有并显式传递；`topic_match`、`mqtt`、
  `config`、`util` 这些纯模块不依赖 `struct server`，可以直接单测，但
  `handlers` / `server` / `persistence` 仍需要完整的 broker 上下文才能跑。
- 持久化是“整份快照 + 增量 WAL”，不是 LSM/数据库；状态越大快照越慢。
