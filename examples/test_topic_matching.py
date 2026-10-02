#!/usr/bin/env python3
"""
主题匹配(通配符)行为验证, 每个用例使用一个全新的订阅者, 避免订阅互相污染.

先启动 broker:
    ./sol -c etc/conf/sol.conf -v

运行:
    python3 examples/test_topic_matching.py

覆盖: 精确匹配 / 单层通配符 '+' (含行首、行尾、多一层、少一层等边界)
      多层通配符 '#' (不同层级深度、非末尾前缀) / 保留消息 retain
"""
import time, threading, paho.mqtt.client as mqtt
HOST, PORT = "127.0.0.1", 8883

pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="P4")
pub.connect(HOST, PORT); pub.loop_start(); time.sleep(0.5)

def case(pattern, topic, retain=False, tag=""):
    got = []
    suback = threading.Event()
    cid = f"s-{abs(hash((pattern, topic, retain, tag)))%10**8}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=cid)
    c.on_subscribe = lambda cl, u, mid, g, p=None: suback.set()
    c.on_message = lambda cl, u, m: got.append((m.topic, m.retain))
    c.connect(HOST, PORT); c.loop_start()
    suback.wait(0)  # noop
    time.sleep(0.3)
    c.subscribe(pattern, qos=1)
    if not suback.wait(3):
        print(f"??    订阅 {pattern} 没有 SUBACK")
        c.loop_stop(); c.disconnect(); return
    time.sleep(0.4)
    pub.publish(topic, "x", qos=1, retain=retain).wait_for_publish(3)
    time.sleep(0.9)
    mark = "命中" if got else "未命中"
    print(f"{mark:<4} [{tag}] 订阅={pattern:<26} 发布={topic:<26} 收到={got}")
    if retain:  # 清掉保留消息
        pub.publish(topic, "", qos=1, retain=True).wait_for_publish(3)
        time.sleep(0.3)
    c.loop_stop(); c.disconnect(); time.sleep(0.2)

print("--- 精确匹配 ---")
case("x1/b", "x1/b", tag="exact-2")
case("x2/b/c", "x2/b/c", tag="exact-3")

print("\n--- 单层通配符 + (期望命中) ---")
case("p1/+/c", "p1/b/c", tag="+中")
case("+/q/c", "p1/q/c", tag="+首")
case("p1/q/+", "p1/q/r", tag="+尾")

print("\n--- 单层通配符 + 的边界 (期望未命中) ---")
case("p2/b/+", "p2/b/c/d", tag="+多一层")
case("p2/+/+", "p2/b", tag="+少一层")

print("\n--- 多层通配符 # (期望命中) ---")
case("m1/#", "m1/b", tag="#一层")
case("m2/#", "m2/b/c", tag="#两层")
case("m3/#", "m3/b/c/d", tag="#三层")
case("m4/#", "m4/b/c/d/e/f", tag="#五层")
case("m5/c/#", "m5/b/c/d", tag="#非末尾前缀")

print("\n--- 保留消息 retain ---")
case("r1/t", "r1/t", retain=True, tag="retain")

pub.loop_stop(); pub.disconnect()
