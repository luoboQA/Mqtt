Sol
===

An oversimplified MQTT broker written from scratch, which mimics mosquitto's
features. Implemented to learn how the protocol works, it supports almost all
MQTT v3.1.1 commands on Linux and multiplexes its I/O over epoll (poll and
select are available too, see below). The basic development process is
documented in this [series of posts](https://codepr.github.io/posts/sol-mqtt-broker),
referring to the [tutorial branch](https://github.com/codepr/sol/tree/tutorial);
this fork keeps the teaching material (guided scripts, a protocol level test
suite, comments tied to the specification) and adds what a lab or a small
deployment needs: on disk persistence of the stored state, topic level
authorization, a memory policy, verified TLS, and sanitizer clean builds.

**Not production ready**, on purpose: what is missing for that is listed under
[To be implemented](#to-be-implemented), the known deviations from the
specification are spelled out under [Known
simplifications](#教学简化--known-simplifications), and none of them is
hidden behind a promise the code does not keep.

### Documentation

- [`docs/architecture.md`](docs/architecture.md) — a walk through the code
  following one byte from the socket to another client, and a map of which
  file to touch for each kind of change.
- [`docs/conformance.md`](docs/conformance.md) — the MQTT 3.1.1 conformance
  matrix: representative requirements, implementation locations, and tests,
  with known deviations called out explicitly.
- [`docs/design.md`](docs/design.md) — the design trade-offs: event loop and
  threads, explicit broker context, topic indexes, persistence, memory policy,
  security.
- [`docs/teaching-guide.md`](docs/teaching-guide.md) — a suggested lesson path
  with code-reading checkpoints and hands-on experiments.

                    ┌─────────────────────────────┐
                    │         sol.c (main)        │
                    │  解析命令行 / 加载配置 / 启动   │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │ server.c / client.c / cron.c │
                    │  event-loop + 回调 + 客户端池  │
                    └──┬────────┬────────┬─────────┘
                       │        │        │
        ┌──────────────▼─┐ ┌────▼────┐ ┌─▼──────────┐
        │ ev.c (多路复用) │ │network.c│ │handlers.c  │
        │epoll/poll/select││socket/TLS││各MQTT命令   │
        └────────────────┘ └─────────┘ └─┬──────────┘
                                         │
                       ┌─────────────────▼─────────────────┐
                       │  mqtt.c (编解码) + pack.c (字节序)  │
                       └─────────────────┬─────────────────┘
                                         │
        ┌────────────────────────────────▼──────────────────────────┐
        │ 数据结构: trie.c(主题树) bst.c(AVL子节点) list.c iterator.c  │
        │          topic_store.c(通配符索引) memory/memorypool.c      │
        └───────────────────────────────────────────────────────────┘

### Features

It is still a work in progress, but it handles most of the features expected
from an MQTT broker, and the parts of it that are hard to see (memory, threads)
are checked with address/undefined and thread sanitizers over the whole
integration suite rather than by eye.

- Configuration file on disk
- Support for QoS messages 0, 1 and 2
- Retained messages per topic
- On disk persistence of the retained messages and of the persistent sessions
  (their subscriptions, the messages queued for their disconnected clients and
  the in-flight QoS 1/2 state of their connections), snapshotted periodically
  and on a clean shutdown, with an fsync'd write-ahead log replayed between
  two snapshots
- `max_memory` enforcement: unused topics are reclaimed, the oldest queued
  messages are dropped under pressure and new connections are refused when
  nothing is left to give back (`reclaim_interval` sets the idle interval)
- Session present check and handling
- Last Will & Testament, delivered on any disconnection that is not a clean
  DISCONNECT (network error, protocol error, keepalive expiry)
- Periodic stats publishing
- Support multiple topics subscriptions through wildcard (#) and (+) for single
  level wildcard e.g. foo/+/bar/#
- Authentication through username and password, hashed with scrypt by default
  (`passwd.py`), with `crypt(3)` hashes still accepted for existing files
- Topic level authorization through an access control file (`acl_file`):
  per client or per user rules of `read`/`write`/`readwrite` over topic
  filters, evaluated in order, deny by default
- SSL/TLS connections, configuration accepts minimum protocols to be used,
  with optional mutual TLS (`require_client_cert` asks the client for a
  certificate signed by `cafile`)
- UTF-8 validation of every string the protocol carries (client id,
  username, topics, will, filters) as §1.5.3 requires
- Per-session inflight window (`max_inflight`), per-client publish rate
  (`max_publish_rate`) and per-address connection cap
  (`max_connections_per_ip`)
- Logging on disk
- Daemon mode
- Multiplexing I/O behind an abstraction: epoll, poll and select are all
  implemented, the default one is picked from the kernel and any of them can be
  forced at build time (`-DEVENTLOOP=...`), each exercised by the test suite
- Multithread load-balancing on connections for high concurrent performance

### To be implemented

The entries that used to sit here (persistence of the state of a disconnected
client, enforcing `max_memory`, the write-ahead log, the pid file, log
rotation) are done. What a deployment beyond a trusted network still wants, in
the order it usually starts to hurt:

- hot reload of the whole configuration (SIGHUP reopens the log file today,
  it does not re-read the file);
- the scale limits stay what they are: one process, no clustering.

### Maybe

- MQTT 5.0 support
- Porting for BSD/MAC osx
- Scaling (feeling brave?)

### 教学简化 / Known simplifications

为了代码好读，broker 有意做了几处简化。做实验时不要把下面这些当成 MQTT 规范：

- **`$` 是 broker 保留的命名空间**：客户端向 `$` 开头的主题 PUBLISH 会被丢弃（QoS
  握手仍然完成，避免客户端无限重传），带 `$` 遗嘱的 CONNECT 会被拒绝。订阅侧按
  §4.7.2 处理：`#`/`+` 开头的过滤器不匹配 `$` 开头的主题，必须显式订阅 `$SOL/#`
  （见 `examples/stats_demo.py`）。
- **`max_memory` 是三级策略而不是硬上限**：超限时依次"回收空主题 → 丢弃最旧的离线
  队列消息 → 拒绝新连接"，每一步都会打日志。低于进程基线（不到 1MB，客户端内存池
  只预分配 1024 个槽位，连接缓冲区按需从几 KB 增长）的限额会让 broker 一开始就
  拒收连接，启动时会有一条 warning。
- **持久化覆盖会话状态，但不含遗嘱**：保留消息、持久会话的订阅、离线队列、连接上
  仍在途的 QoS 1/2 消息、PUBREL 阶段和入站 QoS 2 去重集合都会落盘
  （`persistence_file` 及其 `.wal`），遗嘱属于某一条网络连接，不随会话恢复。
  快照之间由预写日志兜底，每条记录都会 fsync，硬崩溃（kill -9 / 断电）最多丢最后
  一条记录，而不是整个 `persistence_interval`。

## Build

```sh
$ cmake -S . -B build
$ cmake --build build -j
```

The binary lands in `build/sol`; `cmake --install build` puts it in
`${CMAKE_INSTALL_PREFIX}/bin` together with the sample configuration.

## Testing

Everything runs through CTest, from a fresh build directory:

```sh
$ cmake -S . -B build -DDEBUG=ON
$ cmake --build build -j
$ ctest --test-dir build --output-on-failure
```

Three suites make up the run:

- `unit` (`build/sol_test`) — the data structures: list, BST, trie.
- `proto` (`build/sol_proto_test`) — the pure protocol pieces: PUBLISH
  unpacking and packing, malformed packet rejection, the §4.7 topic matching
  table and the configuration parser. No broker, no sockets.
- `integration` (`tests/integration/run.py`) — starts real brokers and speaks
  MQTT to them: raw packets, delivery paths, sessions, persistence, the memory
  policy, the access control list, TLS and daemon mode. Each module starts a
  broker of its own where it needs a specific configuration;
  `tests/gen_certs.sh` prepares the certificates the TLS tests use (they are
  not part of the repository). CTest passes the freshly built binary through
  `SOL_BIN`, so no `./sol` symlink is needed. To run it by hand:

  ```sh
  $ python3 tests/integration/run.py
  ```

Two more tools are meant to be run by hand:

```sh
$ python3 tests/stress.py [--help]     # load generator, N clients fanning out
$ ./tests/run_tsan.sh                  # the suite against a TSan build
```

### Fuzzing

The parser has a libFuzzer entry point (`tests/fuzz_unpack.c`). It needs
clang:

```sh
$ cmake -S . -B build-fuzz -DFUZZ=ON -DCMAKE_C_COMPILER=clang
$ cmake --build build-fuzz --target sol_fuzz
$ ./build-fuzz/sol_fuzz -max_total_time=60 tests/fuzz_corpus
```

The first byte of every input is the fixed header, the rest is what the event
loop hands to `mqtt_unpack()`, so anything the fuzzer finds is something a
client can send.

### Coverage

```sh
$ cmake -S . -B build-cov -DCOVERAGE=ON
$ cmake --build build-cov -j
$ ctest --test-dir build-cov --output-on-failure
$ lcov --capture --directory build-cov --output-file coverage.info
$ lcov --remove coverage.info '/usr/*' '*/tests/*' --output-file coverage.info
$ genhtml coverage.info --output-directory coverage
```

The `coverage` job in `.github/workflows/ci.yml` runs exactly this and keeps
`coverage.info` as an artifact.

### Event loop backends

The multiplexing backend is picked from the kernel the broker is built for,
and can be forced at configure time to exercise the other implementations on
the same machine: `cmake -S . -B build-poll -DEVENTLOOP=poll` (or `select`,
`epoll`; Linux only). The running backend is printed at startup.

### Sanitizer builds

```sh
$ cmake -S . -B build-asan -DDEBUG=ON && cmake --build build-asan -j   # ASan + UBSan
$ cmake -S . -B build-tsan -DTSAN=ON && cmake --build build-tsan -j   # TSan
```

Both sanitizers are meant to be exercised through the integration suite: run
it with `SOL_BIN=build-asan/sol python3 tests/integration/run.py`. The
ThreadSanitizer one needs the address space randomization disabled (it refuses
to start otherwise on recent kernels), which `tests/run_tsan.sh` takes care of.

## Operations

`sol --check-config -c /etc/sol/sol.conf` parses the file, applies the same
cross checks the broker runs at startup (port range, TLS files, password and
ACL files) and exits non zero on the first problem, listing every error it
found. A file named with `-c` that cannot be opened or parsed is fatal at
startup too; the built in path may be absent and the broker then runs on its
defaults, as before.

```sh
$ sol --version                    # Sol v0.18.5 MQTT broker 3.1.1
$ sol --check-config -c conf       # Configuration OK: conf
$ sol -d -c /etc/sol/sol.conf      # daemonize
```

When `pid_file` is set, the daemon writes its PID after detaching and removes
it on a clean shutdown. `SIGHUP` reopens `log_path` (what logrotate needs
after renaming the old file); it is serviced on the next one second tick, not
inside the signal handler. `SIGTERM`/`SIGINT` stop the broker after a final
persistence snapshot.

`cmake --install build` installs the binary, the sample configuration and a
systemd unit (`lib/systemd/system/sol.service`). The unit runs as the `sol`
user with `StateDirectory=sol` and `LogsDirectory=sol`; adjust `ExecStart` and
the configuration to match your prefix. A `Dockerfile` is at the root of the
repository:

```sh
$ docker build -t sol .
$ docker run -p 1883:1883 -v sol-data:/var/lib/sol sol
```

The image listens on `0.0.0.0:1883`; the mounted volume is where
`persistence_file` should point if you enable persistence.

### Metrics

The `$SOL/broker/` feed is published every `stats_publish_interval` seconds
and only matches an explicit subscription (a filter starting with a wildcard
never matches `$` topics):

| Topic | Meaning |
|---|---|
| `$SOL/broker/clients/` | connections accepted since start |
| `$SOL/broker/clients/connected/` | currently connected |
| `$SOL/broker/clients/disconnected/` | accepted and then dropped |
| `$SOL/broker/messages/` | received + sent |
| `$SOL/broker/messages/received/`, `/sent/` | per direction |
| `$SOL/broker/messages/inflight/` | QoS 1/2 flows awaiting an ack |
| `$SOL/broker/messages/retained/` | retained messages in the store |
| `$SOL/broker/messages/dropped/` | dropped by the memory policy |
| `$SOL/broker/sessions/` | sessions (persistent ones included) |
| `$SOL/broker/subscriptions/` | subscriptions across all sessions |
| `$SOL/broker/topics/` | topics in the store |
| `$SOL/broker/bytes/sent/`, `/received/` | byte counters |
| `$SOL/broker/memory/used/`, `/limit/` | accounted memory and the cap |
| `$SOL/broker/uptime/`, `/uptime/sol/` | seconds, and uptime in sols (one sol = 88775.24 s) |

## Quickstart play

The broker can be tested using `mosquitto_sub` and `mosquitto_pub` or with
`paho-mqtt` python driver. There is a set of guided scripts under `examples/`
(connect/subscribe/publish, QoS 0-2, retained messages, wildcards, will
messages, `$SOL` statistics), each one printing what it expects to observe:
start the broker with `./sol -c etc/conf/sol.conf -v` and run them with
`python3 examples/<script>.py` — see `examples/README.md`.

To run the broker with DEBUG logging on:

```sh
$ ./sol -v
```

A simple configuration can be passed in with `-c` flag:

```sh
$ ./sol -c path/to/sol.conf
```

As of now the configuration is very small and it's possible to set some of the
most common parameters, default path is located to `/etc/sol/sol.conf`:

```sh
# Sol configuration file, uncomment and edit desired configuration

# Network configuration

# Comment ip_address and ip_port, set unix_socket and
# UNIX family socket will be used instead

ip_address 127.0.0.1
ip_port 8883

# unix_socket /tmp/sol.sock

# Logging configuration

# Could be either DEBUG, INFO/INFORMATION, WARNING, ERROR
log_level DEBUG

log_path /tmp/sol.log

# Max memory the broker may hold. Over this limit it acts in three steps,
# each one logged: it reclaims the topics nothing refers to anymore, then drops
# the oldest messages queued for the clients that are away, and when even that
# is not enough it refuses new connections until the memory comes back down
max_memory 2GB

# Seconds between two reclamations of the unused topics while the broker is
# under its memory limit (over the limit it reclaims on every check instead)
# reclaim_interval 60s

# State that must survive a restart: retained messages and persistent sessions
# (with their in-flight QoS 1/2 state) are snapshotted here, every
# persistence_interval seconds when something changed and once more on a clean
# shutdown. Mutations between snapshots go to <file>.wal and are fsynced
# before the broker moves on. Unset keeps everything in memory
# persistence_file /tmp/sol.db
# persistence_interval 30s

# Max memory that will be allocated for each request
max_request_size 2MB

# TCP backlog, size of the complete connection queue
tcp_backlog 128

# Interval of time between one stats publish on $SOL topics and the subsequent
stats_publish_interval 10s

# Seconds after which an unacknowledged QoS 1/2 message (or a PUBREL still
# waiting for its PUBCOMP) is retransmitted with the DUP flag
# retransmit_interval 20s

# Upper bound of the Keep Alive clients negotiate: a client asking for a
# longer period, or for none at all (Keep Alive 0), is still held to it, and
# dropped after one and a half times this value of silence. 0 disables the cap
# keepalive 60s

# Per session window of QoS 1/2 messages in flight, online or queued for an
# away client; over it new messages are dropped. 0 disables the cap
# max_inflight 20

# Maximum PUBLISH packets per second from one client; over it the packet is
# dropped but the QoS 1/2 acknowledgement still flows. 0 disables the limit
# max_publish_rate 1000

# Maximum simultaneous connections from one peer address; 0 disables the limit
# max_connections_per_ip 0

# TLS certs paths. cafile doubles as the flag turning TLS on, and the server
# does not ask the clients for a certificate of their own: it is the clients
# that need the authority to verify this server
# cafile /etc/sol/certs/ca.crt
# certfile /etc/sol/certs/cert.crt
# keyfile /etc/sol/certs/cert.key

# Mutual TLS: ask every client for a certificate signed by cafile and refuse
# the handshake when it is missing or untrusted. Requires the TLS files above
# require_client_cert true

# Authentication
# allow_anonymous false
# password_file /etc/sol/passwd

# Access control list, one rule per line:
#   <allow|deny> <read|write|readwrite> <client:id|user:name|*> <topic filter>
# evaluated in order, a topic no rule covers is denied
# acl_file /etc/sol/sol.acl

# TLS protocols, supported versions should be listed comma separated
# example:
# tls_protocols tlsv1_2,tlsv1_3
```

If `allow_anonymous` is false, a password file have to be specified. The
password file follows the standard format of username:password line by line.
To generate one just add all entries needed in a file and run passwd.py after
it. The default output is the memory-hard scrypt format
`$scrypt$N$r$p$salt$hash`; `--crypt` still produces a `crypt(3)` hash for
files that already use one:

```sh
$ cat sample_passwd_file
user1:pass1
user2:pass2
$ python passwd.py sample_passwd_file
$ cat sample_passwd_file
user1:$scrypt$16384$8$1$9f2c...$6a1d...
user2:$scrypt$16384$8$1$04be...$b7e0...
$ python passwd.py --crypt sample_passwd_file
$ cat sample_passwd_file
user1:$6$69qVAELLWuKXWQPQ$oO7lP/hNS4WPABTyK4nkJs4bcRLYFi365YX13cEc/QBJtQgqf2d5rOIUdqoUin.YVGXC3OXY9MSz7Z66ZDkCW/
user2:$6$vtHdafhGhxpXwgBa$Y3Etz8koC1YPSYhXpTnhz.2vJTZvCUGk3xUdjyLr9z9XgE8asNwfYDRLIKN4Apz48KKwKz0YntjHsPRiE6r3g/
```

## Concurrency

The broker provides an access through a simple IO multiplexing event-loop based
TCP server, the model is designed to be self-contained and thus easy to spread
on multiple threads: there are `THREADSNR` worker loops plus the one the main
thread runs, each with an event context and a subset of the connections, and a
connection stays on the loop that accepted it.

```
                            THREADS 1..N
                            [EVENT-LOOP]

    ACCEPT_CALLBACK         READ_CALLBACK         WRITE_CALLBACK
  -------------------    ------------------    --------------------
          |                      |                       |
       ACCEPT                    |                       |
          | -------------------> |                       |
          |                READ AND DECODE               |
          |                      |                       |
          |                      |                       |
          |                   PROCESS                    |
          |                      |                       |
          |                      |                       |
          |                      | --------------------> |
          |                      |                     WRITE
       ACCEPT                    |                       |
          | -------------------> | <-------------------- |
          |                      |                       |

```

However, the current model gives some advantages against the single-thread
implementation, as each thread can handle a share of the total number of
connected client effectively increasing the throughput of the broker, the
main drawback is that in worst cases a scenario that can originate is where some
threads are serving heavy-load clients and some others are esentially idling
without partecipating or helping.

What that costs in practice, and what the code does about it:

- the state that is shared between loops (the topic store, the sessions, the
  client map) is guarded by a global mutex, always taken before a client's one;
- a loop can send work to another one (a message for a client living there, a
  takeover, the keepalive scan) and the periodic tasks all run on the main
  loop, so they touch every connection;
- the per loop bookkeeping an outside thread has to touch is either atomic
  (the counters, `last_seen`, the Keep Alive) or behind a small lock (the
  monitored descriptor array), and the loops that cannot nudge a blocking wait
  by themselves (poll, select) watch a wakeup descriptor for that purpose;
- the whole integration suite is run under ThreadSanitizer
  (`tests/run_tsan.sh`), which is what found and closed the races listed in the
  commit history rather than leaving them to be discovered under load.

## Benchmarks

> The numbers below are the upstream project's, from 2020 and against mosquitto
> 1.6.8 out of the box. They are kept for context; `tests/stress.py` in this
> fork is what to use to measure the broker as it is now, on your own machine.

I've run some basic benchmarks comparing Sol and Mosquitto's performance, using
[mqtt-benchmark](https://github.com/krylovsk/mqtt-benchmark) tool to test
concurrent connections and different loads of traffic on my laptop, an
Intel(R) I5-8265U (8) @ 3.900 GHz CPUs and 8 GB RAM.

To be minded that currently Sol skips some checks like UTF-8 strings for topics
and the memory footprint is also much greater than Mosquitto's one; moreover
this benchmarks doesn't take into account real publish on multiple subscribers,
just the publish traffic load on multiple clients with different QoS levels.

![MQTT mosquitto vs sol comparison](MQTTcomparison.png)

All tests are run pointing to localhost:1883, apparently, out of QoS 0 tests
which are the least interesting as there's no real "handshakes" or overhead on
the communication between the peers, QoS 1 and even more QoS 2 is where Sol
seems to shine, highlighting a 95% increased throughtput over Mosquitto on the
QoS 2 1000 clients benchmark.
Of course these benchmarks are of little relevance and Mosquitto is compiled
without any optimizations, just an out of the box version binary shipped by
the OS repository, version 1.6.8, but in terms of sheer concurrency Sol does
pretty good.

## Contributing

Pull requests are welcome, just create an issue and fork it.
