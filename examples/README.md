# MQTT 示例脚本

用 Python + [paho-mqtt](https://pypi.org/project/paho-mqtt/) 验证 broker 行为，
并对照日志与对应的源码路径。

## 准备

```sh
# 1. 编译并启动 broker (示例配置监听 127.0.0.1:8883)
cmake -S . -B build
cmake --build build -j
./build/sol -c etc/conf/sol.conf -v

# 2. 安装客户端依赖
pip install paho-mqtt
```

## 脚本

| 脚本 | 作用 |
| --- | --- |
| `mqtt_demo.py` | 入门演示：连接、订阅、发布 QoS 0/1/2、保留消息、通配符订阅、迟到订阅者 |
| `test_topic_matching.py` | 主题匹配矩阵：精确匹配、`+`（含行首/行尾/多一层/少一层边界）、`#`（不同深度、非末尾前缀）、retain |
| `test_features.py` | 裸 `#` 订阅、通配符订阅下发保留消息、`clean_session=false` 的会话保持 |
| `will_demo.py` | 遗嘱（LWT）：正常 DISCONNECT 不发、进程被 kill -9 发、keepalive 超时发、遗嘱带 retain |
| `stats_demo.py` | `$SOL` 统计主题；顺带验证 `#` 不匹配 `$` 开头的主题（§4.7.2） |

```sh
python3 examples/mqtt_demo.py
python3 examples/test_topic_matching.py   # 打印每个用例是否命中
python3 examples/test_features.py         # 打印 OK / FAIL
python3 examples/will_demo.py             # 会 fork 子进程并 kill -9 它, 演示崩溃掉线
python3 examples/stats_demo.py            # 等一轮统计约 10s (见 stats_publish_interval)
```

## 对照阅读

- 报文字节格式：`src/mqtt.c`（CONNECT/SUBSCRIBE/PUBLISH 的编解码）
- 各命令的业务逻辑：`src/connect.c`、`src/subscribe.c`、`src/publish.c`、
  `src/session.c`；`src/handlers.c` 负责分发
- 订阅存哪、如何匹配：`src/topic_store.c`、`src/trie.c`、`match_subscription()`
- 已知限制和协议偏差：`docs/limitations.md`；源码走读见
  `docs/architecture.md` 和 `docs/conformance.md`

一次完整的 QoS 1 交互：`CONNECT → CONNACK → SUBSCRIBE → SUBACK → PUBLISH → PUBACK`，
QoS 2 再多两趟 `PUBREC → PUBREL → PUBCOMP`。
