#!/usr/bin/env python3
"""
协议特性验证: 裸 '#' 订阅 / 通配符订阅下发保留消息 / 会话保持.

先启动 broker:
    ./sol -c etc/conf/sol.conf -v

运行:
    python3 examples/test_features.py

每个用例末尾会打印 OK / FAIL.
"""
import time, threading, paho.mqtt.client as mqtt
HOST, PORT = "127.0.0.1", 8883

pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="P5")
pub.connect(HOST, PORT); pub.loop_start(); time.sleep(0.5)

# --- 1) 裸 '#' 应收到所有主题 (同时也会下发匹配的保留消息, 属正常行为) ---
got = []
ack = threading.Event()
sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="star")
sub.on_subscribe = lambda c,u,m,g,p=None: ack.set()
sub.on_message = lambda c,u,m: got.append(m.topic)
sub.connect(HOST, PORT); sub.loop_start()
sub.subscribe("#", qos=1); ack.wait(3); time.sleep(0.3)
expected = {"any/topic", "a", "x/y/z/w"}
for t in expected:
    pub.publish(t, "1", qos=1).wait_for_publish(3)
time.sleep(1)
print("1) 裸 '#' 订阅收到:", got, "=>",
      "OK" if expected.issubset(set(got)) else "FAIL")

# --- 2) 迟到订阅者用通配符订阅, 应拿到匹配的保留消息 ---
pub.publish("r2/temp", "25.0", qos=1, retain=True).wait_for_publish(3)
time.sleep(0.5)
got2 = []
ack2 = threading.Event()
late = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="late-wc")
late.on_subscribe = lambda c,u,m,g,p=None: ack2.set()
late.on_message = lambda c,u,m: got2.append((m.topic, m.retain, m.payload.decode()))
late.connect(HOST, PORT); late.loop_start()
late.subscribe("r2/#", qos=1); ack2.wait(3); time.sleep(1.2)
print("2) 迟到订阅 r2/# 拿到保留消息:", got2, "=>",
      "OK" if got2 and got2[0][1] == 1 else "FAIL")

# --- 3) clean_session=false 的会话保持 ---
sess = []
c1 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="keeper",
                 clean_session=False)
c1.on_connect = lambda c,u,f,r,p=None: sess.append(("第一次", f.get("session present")))
c1.on_subscribe = lambda c,u,m,g,p=None: None
c1.connect(HOST, PORT); c1.loop_start()
c1.subscribe("keep/#", qos=1); time.sleep(0.8)
c1.disconnect(); c1.loop_stop()          # 掉线, 但 clean_session=False
time.sleep(0.5)
c2 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="keeper", clean_session=False)
c2.on_connect = lambda c,u,f,r,p=None: sess.append(("重连", f.get("session present")))
c2.on_message = lambda c,u,m: sess.append(("收到", m.topic))
c2.connect(HOST, PORT); c2.loop_start()
time.sleep(1.2)
pub.publish("keep/alive", "hi", qos=1).wait_for_publish(3)
time.sleep(1)
print("3) 会话保持:", sess, "=>",
      "OK" if ("重连", 1) in sess and ("收到", "keep/alive") in sess else "FAIL")
c2.loop_stop(); pub.loop_stop()
