# MQTT 示例脚本

用 Python + [paho-mqtt](https://pypi.org/project/paho-mqtt/) 验证本 broker 的行为，
边看脚本边对照 broker 日志（`/tmp/sol.log`）是理解 MQTT 协议最快的方式。

## 准备

```sh
# 1. 编译并启动 broker (监听 127.0.0.1:8883, 见 etc/conf/sol.conf)
cmake . && make sol
./sol -c etc/conf/sol.conf -v

# 2. 安装客户端依赖
pip install paho-mqtt
```

## 脚本

| 脚本 | 作用 |
| --- | --- |
| `mqtt_demo.py` | 入门演示：连接、订阅、发布 QoS 0/1/2、保留消息、通配符订阅、迟到订阅者 |
| `test_topic_matching.py` | 主题匹配矩阵：精确匹配、`+`（含行首/行尾/多一层/少一层边界）、`#`（不同深度、非末尾前缀）、retain |
| `test_features.py` | 裸 `#` 订阅、通配符订阅下发保留消息、`clean_session=false` 的会话保持 |

```sh
python3 examples/mqtt_demo.py
python3 examples/test_topic_matching.py   # 打印每个用例是否命中
python3 examples/test_features.py         # 打印 OK / FAIL
```

## 对照阅读

- 报文字节格式：`src/mqtt.c`（CONNECT/SUBSCRIBE/PUBLISH 的编解码）
- 各命令的业务逻辑：`src/handlers.c`
- 订阅存哪、如何匹配：`src/topic_store.c`、`src/trie.c`、`match_subscription()`

一次完整的 QoS 1 交互：`CONNECT → CONNACK → SUBSCRIBE → SUBACK → PUBLISH → PUBACK`，
QoS 2 再多两趟 `PUBREC → PUBREL → PUBCOMP`。
