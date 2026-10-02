#!/usr/bin/env python3
"""
MQTT 入门演示 —— 连接本地 sol broker (127.0.0.1:8883)

先启动 broker:
    ./sol -c etc/conf/sol.conf -v

再运行本脚本:
    python3 examples/mqtt_demo.py

演示: 连接/订阅/发布/QoS 0,1,2/保留消息/通配符订阅/迟到订阅者
依赖: pip install paho-mqtt
"""
import threading
import time
import paho.mqtt.client as mqtt

HOST, PORT = "127.0.0.1", 8883


def on_connect(client, userdata, flags, rc, properties=None):
    # flags["session present"] == 1 表示 broker 恢复了之前的会话
    print(f"[sub] 连接成功 rc={rc} session_present={flags.get('session present')}")

    # 两个订阅: 精确层级 + 通配符 (+ 单层, # 多层)
    client.subscribe("home/+/temperature", qos=1)
    client.subscribe("sensor/#", qos=2)
    print("[sub] 已订阅 home/+/temperature 和 sensor/#")


def on_message(client, userdata, msg):
    print(
        f"[sub] 收到  topic={msg.topic:<24} qos={msg.qos} "
        f"retain={msg.retain} payload={msg.payload.decode()!r}"
    )


def on_publish(client, userdata, mid, *args):
    print(f"[pub] 消息 mid={mid} 已交给 broker")


def subscriber():
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="learner-sub")
    c.on_connect = on_connect
    c.on_message = on_message
    c.on_publish = on_publish
    c.connect(HOST, PORT, keepalive=30)
    c.loop_start()          # 起一个后台线程跑网络循环
    return c


def main():
    sub = subscriber()
    time.sleep(1.5)         # 等订阅生效

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="learner-pub")
    pub.connect(HOST, PORT, keepalive=30)
    pub.loop_start()
    time.sleep(0.5)

    print("\n--- 1) QoS 0 (最多一次, 发完即忘) ---")
    pub.publish("home/kitchen/temperature", "21.5C", qos=0)

    print("\n--- 2) QoS 1 (至少一次, 有 PUBACK 确认) ---")
    pub.publish("home/bedroom/temperature", "23.1C", qos=1).wait_for_publish(2)

    print("\n--- 3) QoS 2 (恰好一次, 四步握手) ---")
    pub.publish("sensor/cpu/load", "0.42", qos=2).wait_for_publish(3)

    print("\n--- 4) 保留消息 retain (新订阅者立刻收到最后一次的值) ---")
    pub.publish("sensor/door/status", "closed", qos=1, retain=True).wait_for_publish(2)

    time.sleep(1)
    print("\n--- 5) 一个新订阅者连上来, 会自动拿到 retain 消息 ---")
    sub2 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="learner-late")
    sub2.on_connect = lambda c, u, f, r, p=None: c.subscribe("sensor/#", qos=1)
    sub2.on_message = lambda c, u, m: print(
        f"[late-sub] 收到 topic={m.topic} retain={m.retain} payload={m.payload.decode()!r}"
    )
    sub2.connect(HOST, PORT, keepalive=30)
    sub2.loop_start()

    time.sleep(3)
    sub2.loop_stop()
    sub.loop_stop()
    pub.loop_stop()
    print("\n完成。注意对比: 只有 retain=True 的消息被『迟到的订阅者』收到了。")


if __name__ == "__main__":
    main()
