# MQTT 3.1.1 一致性矩阵

对照 OASIS MQTT v3.1.1 规范逐条列出实现状态。状态含义：

- **完整**：规范要求的行为已实现，且有测试覆盖。
- **有意偏离**：规范允许实现自行决定，或本实现明确选择了更严格的策略，
  在“说明”里写清理由。
- **未实现**：v3.1.1 未要求，或属于已知边界。

测试列中 `proto:` 指 `tests/mqtt_test.c` / `tests/topic_match_test.c` /
`tests/config_test.c`，`it:` 指 `tests/integration/` 下的用例。

## 1. 固定头与通用规则

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §2.2 包类型 0 保留 | 完整 | `mqtt.c:mqtt_unpack` | `it:test_reserved_packet_type_zero` 断开连接 |
| §2.2.2 固定头 flags 必须合法 | 完整 | `valid_fixed_header_flags()` | `proto:mqtt::reject_*`、`it:test_subscribe_with_bad_flags_rejected` |
| §2.2.3 Remaining Length 最多 4 字节 | 完整 | `recv_packet()` | `it:test_remaining_length_five_bytes` |
| §2.2.3 Remaining Length 与实际字节一致 | 完整 | `mqtt_unpack()` 各分支 | 尾部多余字节按畸形包拒绝 |
| §3.1.2.2 协议名必须是 `MQTT` | 完整 | `unpack_mqtt_connect()` | `it:test_bad_protocol_name` |
| §3.1.2.2 协议级别必须是 4 | 完整 | `connect_handler()` | `it:test_connect_wrong_protocol_level` 回 0x01 |
| §3.1.2.8 客户端不得发送 CONNACK/SUBACK/UNSUBACK/PINGRESP | 完整 | `protocol_violation_handler` | `it:test_pingresp_from_client` |
| 包大小上限 | 有意偏离 | `conf->max_request_size` | 超过即断开，防止内存放大；v3.1.1 无协商机制 |
| 客户端发 CONNECT 之前的包 | 完整 | `client->status` 检查 | `it:test_subscribe_before_connect`、`test_publish_before_connect` |

## 2. CONNECT / CONNACK

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §3.1.2.1 client id | 完整 | `MQTT_CLIENT_ID_LEN` | 最长 63 字节；更长的 id 拒绝（`it:test_long_client_id`） |
| §3.1.2.3 clean session | 完整 | `connect_handler`、`client_deactivate` | 0 保留会话与离线队列，1 断开即删 |
| §3.1.2.3 空 client id + clean session 0 | 完整 | `connect_handler` | 回 0x02 Identifier Rejected |
| §3.1.2.5 遗嘱 flag 与 QoS/retain 组合 | 完整 | `unpack_mqtt_connect()` | 非法组合拒绝；遗嘱 payload 保留 NUL（`it:test_will_payload_keeps_nul_bytes`） |
| §3.1.2.5 遗嘱主题不能含通配符 | 完整 | `connect_handler` | `it:test_will_topic_with_wildcard_rejected` |
| §3.1.2.5 协议错误/踢下线时发布遗嘱 | 完整 | `process_message`、`server_kick_duplicate` | `it:test_will_published_on_protocol_error`、`test_will_published_on_client_id_takeover` |
| §3.1.2.6 用户名/密码 | 完整 | `config_read_passwd_file`、`check_passwd` | `allow_anonymous=0` 时校验；错误回 0x04 |
| §3.1.2.10 Keep Alive | 完整 | `keepalive_check()` | 1.5 倍超时断开；配置值是上限 |
| §3.1.4 重复 client id 踢旧连接 | 完整 | `server_kick_duplicate()` | `it:test_duplicate_client_id_kicks_old` |
| §3.2.2.2 session present | 完整 | `connect_handler` | 恢复持久会话时置 1 |
| §3.2.2.3 非零返回码后关闭连接 | 完整 | `close_after_write` | CONNACK 发完即断 |
| 二次 CONNECT | 完整 | `client->connected` | `it:test_double_connect` 断开 |

## 3. PUBLISH

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §3.3.1.2 QoS 0/1/2 | 完整 | `unpack_mqtt_publish`、`publish_handler` | `it:test_qos_one_roundtrip`、`test_qos_two_*` |
| §3.3.1.2 QoS 3 非法 | 完整 | `unpack_mqtt_publish` | `proto:mqtt::reject_qos3`、`it:test_publish_qos_three_rejected` |
| §3.3.1.2 DUP flag | 完整 | 重传时置位 | `it:test_qos_one_retransmit_sets_dup` |
| §3.3.1.3 主题名不能含 `+`/`#` | 完整 | `valid_topic_name()` | `it:test_publish_to_wildcard_topic` |
| §3.3.1.3 主题名不能为空 | 完整 | `valid_topic_name()` | `proto:mqtt::reject_empty_topic` |
| §3.3.1.1 QoS > 0 时 packet id 非 0 | 完整 | `unpack_mqtt_publish` | `it:test_publish_zero_mid_rejected` |
| §3.3.1.3 retain=1 保存保留消息 | 完整 | `publish_message()` | `it:test_retained_message` |
| §3.3.1.3 retain=1 且空 payload 清除 | 完整 | `publish_message()` | 订阅后收不到 |
| §3.3.1.3 发布到 `$` 主题 | 有意偏离 | `publish_handler` | 客户端发布被丢弃；`$SOL` 只由 broker 写 |
| §3.3.4 QoS 1 回 PUBACK | 完整 | `publish_handler` | 收到 PUBACK 后清 inflight |
| §3.3.4 QoS 2 回 PUBREC，PUBREL 后回 PUBCOMP | 完整 | `publish_handler`、`pubrel_handler` | `it:test_qos_two_counter_returns_to_zero` |
| §4.3.3 入站 QoS 2 去重 | 完整 | `q2_recv_mids` | `it:test_qos_two_duplicate_publish_deduplicated` |

## 4. PUBACK / PUBREC / PUBREL / PUBCOMP

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §3.4 PUBACK 清除出站 QoS1 | 完整 | `puback_handler` | `it:test_qos_one_roundtrip` |
| §3.5 PUBREC 进入 PUBREL 阶段 | 完整 | `pubrec_handler` | 只保留时间戳，等待 PUBCOMP |
| §3.6 PUBREL 清除入站 QoS2 状态并回 PUBCOMP | 完整 | `pubrel_handler` | `it:test_qos_two_*` |
| §3.7 PUBCOMP 清除出站 QoS2 | 完整 | `pubcomp_handler` | `it:test_qos_two_counter_returns_to_zero` |
| 未在途的 ack | 完整 | 各 ack handler | 忽略，不断开 |
| §4.4 重传间隔与 DUP | 完整 | `inflight_msg_check()` | `retransmit_interval`；`it:test_qos_one_retransmit_sets_dup`、`test_qos_two_pubrel_retransmit` |

## 5. SUBSCRIBE / SUBACK

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §3.8.1 固定头 flags 必须 0010 | 完整 | `valid_fixed_header_flags()` | `it:test_subscribe_with_bad_flags_rejected` |
| §3.8.3 至少一个 topic filter | 完整 | `unpack_mqtt_subscribe` | 空包拒绝 |
| §3.8.3 请求 QoS 0/1/2 | 完整 | `unpack_mqtt_subscribe` | `it:test_subscribe_with_qos_*`、`test_subscribe_with_invalid_qos` |
| §3.8.3 topic filter 语法 | 完整 | `valid_topic_filter()` | `+`/`#` 只能独占一层；非法项单独回 0x80 |
| §3.8.4 SUBACK 顺序与返回码 | 完整 | `subscribe_handler` | 逐项 0x00/0x01/0x02/0x80 |
| §3.8.4 ACL 拒绝的订阅回 0x80 | 完整 | `subscribe_handler` + `acl.c` | `it:test_denied_subscription_is_answered_with_failure_code` |
| §3.8.4 订阅后立即投递保留消息 | 完整 | `subscribe_handler` | 重叠过滤器只投一次（`it:test_retained_overlapping_filters_delivered_once`） |
| §3.8.4 保留消息按订阅 QoS 投递 | 完整 | `deliver_retained()` | `it:test_retained_delivered_at_subscription_qos` |

## 6. UNSUBSCRIBE / UNSUBACK

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §3.10.1 flags 必须 0010 | 完整 | `valid_fixed_header_flags()` | 非法即断 |
| §3.10.3 至少一个 filter | 完整 | `unpack_mqtt_unsubscribe` | 空包拒绝 |
| §3.10.4 逐项回 UNSUBACK | 完整 | `unsubscribe_handler` | `it:test_unsubscribe` |
| §4.7 退订只删对应 filter | 完整 | `session_unsubscribe()` | `a/#` 退订后精确订阅 `a` 保留（`it:test_wildcard_unsubscribe_leaves_no_phantom_subscription`） |

## 7. PINGREQ / PINGRESP / DISCONNECT

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §3.12.1 PINGREQ 无负载 | 完整 | `mqtt_unpack` | `proto:mqtt::reject_pingreq_with_body`、`it:test_pingreq_with_payload_rejected` |
| §3.12.2 回 PINGRESP | 完整 | `pingreq_handler` | 集成测试的连接保活 |
| §3.14.1 DISCONNECT 无负载 | 完整 | `mqtt_unpack` | 带负载即协议错误 |
| §3.14.2 DISCONNECT 后不发布遗嘱 | 完整 | `disconnect_handler` | 清 `has_lwt` |

## 8. 主题与通配符（§4.7）

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §4.7.1.2 `+` 匹配恰好一层 | 完整 | `topic_match()` | `proto:topic::match_table` |
| §4.7.1.3 `#` 匹配剩余所有层（可为 0） | 完整 | `topic_match()` | 同上 |
| §4.7.2 以通配符开头不匹配 `$` 主题 | 完整 | `topic_match()` | `$SOL` 只能显式订阅 |
| 空层与尾随 `/` | 完整 | `topic_match()` | `a` ≠ `a/`，`a/` 匹配 `a/+` |
| 过滤器语法（`#` 必须在末尾且独占一层） | 完整 | `valid_topic_filter()` | 非法过滤器回 0x80 |

## 9. 会话与消息顺序

| 条目 | 状态 | 实现 | 测试 / 说明 |
|---|---|---|---|
| §3.1.2.4 持久会话保存订阅 | 完整 | `session` + `persistence` | `it:test_persistent_session_offline_queue` |
| 离线队列按发布顺序 | 完整 | `session_queue_message()` | `it:test_offline_queue_keeps_publication_order` |
| §4.6 同一客户端消息顺序 | 完整 | 单队列 + 单写缓冲 | QoS 0 与 QoS 1/2 共用顺序 |
| 重连后重发 inflight | 完整 | `session_flush_queue()` | `it:test_inflight_message_survives_a_restart` |
| 内存压力下丢最旧离线消息 | 有意偏离 | `session_drop_oldest_queued()` | 保证进程存活；规范未规定服务器资源耗尽行为 |
| 遗嘱不持久化 | 有意偏离 | — | 遗嘱绑定网络连接，进程重启不补发 |

## 10. 安全与运维（规范之外）

| 能力 | 状态 | 实现 | 测试 |
|---|---|---|---|
| 用户名/密码（`crypt(3)` 哈希，`passwd.py` 生成） | 完整 | `util.c:check_passwd` | `it:test_acl*`、`passwd.py` |
| 主题级 ACL（读/写/遗嘱） | 完整 | `acl.c` | `it:test_acl.py` |
| TLS（1.2/1.3） | 完整 | `network.c`、`server.c` | `it:test_tls.py` |
| 匿名开关 | 完整 | `allow_anonymous` | 配置测试 |
| 守护进程 | 完整 | `daemonize()` | `it:test_daemon.py` |
| 统计主题 `$SOL` | 完整 | `publish_stats()` | `it:test_stats_topic` |

## 11. 明确未实现

- MQTT 5.0（属性、原因码、共享订阅、主题别名等）。
- 集群、桥接、共享订阅。
- WebSocket 传输。
- 服务端主动限速（当前只有 `max_request_size` 和内存上限）。
- 每会话出站 inflight 窗口上限（当前受 `max_request_size` 与内存总量间接约束，
  见 `docs/design.md`）。
