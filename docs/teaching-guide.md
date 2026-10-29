# MQTT 教学路线

这份路线把 Sol 当作一个可运行、可测试的 MQTT 3.1.1 教学 broker。建议先读
规范里的对应章节，再沿代码走读，最后运行已有测试或修改一个小行为。

## 适用对象与准备

- 适合会读 C、熟悉指针和基本 socket 概念的学习者；初学者可先补 C 内存管理、
  TCP 字节流和 Make/CMake 基础。
- 需要 CMake、C11 编译器、OpenSSL 开发文件、Python 3 和 `paho-mqtt`。
- 所有实验都在本机运行。默认 broker 端口是 1883；启动测试前确认端口空闲。
- Sol 只实现 MQTT 3.1.1 的教学子集。特别是 `max_inflight` 和 `max_memory`
  会丢弃部分 QoS 1/2 投递但保留会话；不要把这种行为当作 MQTT 的可靠投递保证，
  详细边界见 [一致性矩阵](conformance.md)。

先构建并运行完整测试：

```sh
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## 课程路线

### 1. 一条 MQTT 报文如何穿过 broker

**目标**：分清 TCP 字节流、MQTT 固定头、Remaining Length、事件回调与命令分发。

**阅读**：`src/client.c` 的 `recv_packet()`、`read_callback()`、`process_message()`；
`src/mqtt.c` 的 `mqtt_unpack()`；`src/handlers.c` 的命令分发表。

**实验**：运行 `ctest --test-dir build -R integration --output-on-failure`，关注
`test_malformed.py`。在 `tests/fuzz/corpus/` 选一个报文，解释首字节、Remaining
Length 和控制报文类型；再用 `tests/fuzz/fuzz_unpack.c` 构建 fuzz 目标，观察解析器如何
拒绝畸形输入。

**讨论**：一次 `recv()` 为什么不等于一个 MQTT 包？包长度上限在哪些层起作用？

### 2. Topic Name、Topic Filter 与匹配

**目标**：理解主题名和订阅过滤器的区别、`+` / `#` 匹配、空层以及 `$` 规则。

**阅读**：`src/topic_match.c`、`src/topic_store.c`、
`tests/protocol/topic_match_test.c`、`tests/integration/test_subscribe.py`。

**实验**：先运行 `build/sol_proto_test` 查看匹配表；添加一组自己设计的边界用例，覆盖
`a`、`a/`、`a/+`、`a/#` 和 `$SOL/#`，再观察 `topic_store` 如何使用索引。

**讨论**：为什么精确主题和通配订阅分开索引？如何避免多个匹配订阅给同一会话
重复投递？

### 3. QoS 1 与 QoS 2 状态机

**目标**：从报文交换理解“至少一次”和“恰好一次”只描述一段传输，不是端到端
业务事务。

**阅读**：`src/publish.c` 的 `publish_handler()` 与 ACK handlers；
`src/session.c` 的 inflight 状态；`src/cron.c` 的重传扫描。

**实验**：运行 `tests/integration/test_delivery.py` 中的 QoS 1/2 测试，画出
PUBLISH/PUBACK 与 PUBLISH/PUBREC/PUBREL/PUBCOMP 时序；修改测试中的确认时机，
观察 DUP 和重传。

**讨论**：重复 PUBLISH 的 packet id 如何关联状态？QoS 2 为什么需要入站去重集合？

### 4. Retained message、订阅与遗嘱

**目标**：区分 retained message（主题当前状态）、持久会话（客户端状态）和 Will
（某条连接的异常关闭通知）。

**阅读**：`src/subscribe.c`、`src/connect.c`、`src/publish.c`、
`src/session.c`；相关测试在 `tests/integration/test_delivery.py` 和
`tests/integration/test_connect.py`。

**实验**：发布 retained 消息后再订阅；清空 retained payload；分别正常 DISCONNECT、
断网和使用相同 ClientId 重连，观察 Will 的发布时机。

**讨论**：为什么 Will 绑定连接而不属于可恢复会话？重叠订阅的 QoS 如何决定投递？

### 5. Clean Session 与崩溃恢复

**目标**：理解断线重连时会话状态与 broker 进程重启时磁盘状态的区别。

**阅读**：`src/session.c`、`src/persistence.c`、`docs/design.md`；测试在
`tests/integration/test_persistence.py`。

**实验**：在集成测试使用的独立端口（18885）上用
`tests/integration/persistence.conf` 启动 broker，创建持久会话并留下未完成 QoS
流程；重启 broker，检查订阅、离线队列、packet id 和 WAL 重放。

**讨论**：快照、WAL、generation 分别解决什么问题？哪些连接状态不会持久化？

### 6. 事件循环、所有权与并发

**目标**：追踪连接归属哪个 loop，理解跨线程投递、全局锁和 client 锁的边界。

**阅读**：`src/server.c`、`src/client.c`、`src/cron.c`、`src/ev.c`、
`src/server.h`、`src/server_internal.h`。

**实验**：用 `-DEVENTLOOP=poll` 或 `-DEVENTLOOP=select` 新建构建目录并跑 CTest；
再运行 `./tests/tools/run_tsan.sh` 观察线程检查覆盖哪些路径。

**讨论**：为什么一个 client 的 socket 读写固定在所属 loop？跨线程唤醒如何工作？

### 7. 资源策略与安全边界

**目标**：区分 MQTT 协议保证、broker 管理策略和网络部署防护。

**阅读**：`src/config.c`、`src/acl.c`、`src/network.c`、`src/util.c`；
`tests/integration/test_limits.py`、`test_acl.py`、`test_tls.py`。

**实验**：启用 ACL、TLS/mTLS 和各类 quota，分别测试拒绝连接、拒绝订阅、限流和
资源不足时的结果；对照 [一致性矩阵](conformance.md) 区分规范要求与 Sol 策略。

**讨论**：哪些限制只影响一条连接？哪些策略会让应用消息丢失？生产系统还需哪些
部署层保护？

### 8. 独立互操作验证与小型改动

**目标**：验证自写测试之外的互操作行为，并练习从规范条目定位到实现和测试。

**实验**：用 `mosquitto_pub` / `mosquitto_sub` 或其他独立 MQTT 3.1.1 客户端连到
Sol；再选择一致性矩阵的一项，依次找到规范条目、代码路径、已有测试，并为一个
尚未覆盖的边界补测试。

**完成标准**：能说明行为的协议依据、代码入口、状态所有权、测试方式和已知限制，
而不只是在终端里看到消息到达。

### 9. MQTT broker 与分布式系统

**目标**：区分分布式客户端使用 MQTT 与 broker 集群化；比较 Kafka 分区日志和 MQTT
会话/订阅路由状态，识别集群中的所有权、一致性和故障切换问题。

**阅读**：[MQTT broker 集群说明](distributed-architecture.md)、`docs/design.md`
的单进程线程模型，以及 `src/sol_internal.h` 中连接和会话状态的归属。

**讨论**：如果持久会话节点在 PUBACK 后、复制完成前宕机，客户端会观察到什么？
相同 ClientId 在网络分区两侧同时重连时，如何避免两个 owner 都发布遗嘱？
Kafka 的 partition key 能直接替代 MQTT 的 topic routing 吗？

**练习**：为“只扩容吞吐”和“容忍单节点故障”分别画一份最小架构，并标注哪些状态
需要复制、确认点在哪里、网络分区时哪一侧拒绝写入。不要先选数据库或共识库。

## 建议的课堂节奏

一次 90–120 分钟的练习可以覆盖第 1–3 节。第 4–5 节适合作为会话和持久化专题；
第 6–8 节适合作为并发、安全、互操作或课程项目。完整源码走读建议分多次进行，
每次围绕一条报文路径或一种状态所有权展开。
