Sol
===

An intentionally small MQTT broker written from scratch to teach how the
protocol works. It implements a broad MQTT 3.1.1 subset and multiplexes I/O
with epoll on Linux (poll/select can also be selected; BSD/macOS use kqueue).
The basic development process is
documented in this [series of posts](https://codepr.github.io/posts/sol-mqtt-broker),
referring to the [tutorial branch](https://github.com/codepr/sol/tree/tutorial);
this fork keeps the teaching material (guided scripts, a protocol level test
suite, comments tied to the specification) and adds what a lab or a small
deployment needs: on disk persistence of the stored state, topic level
authorization, a memory policy, verified TLS, and sanitizer clean builds.

**Not production ready**, on purpose: what is missing for that is listed under
[known limitations](docs/limitations.md), and the MQTT 3.1.1 scope is summarized
in the [conformance matrix](docs/conformance.md).

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
- [`docs/limitations.md`](docs/limitations.md) — known defects, their effects,
  and the intended scope of the teaching broker.

代码按 broker 生命周期、MQTT 命令、协议编解码、状态存储和网络事件循环拆分。
模块关系与单包调用路径见[架构走读](docs/architecture.md)。

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

### Scope

The target is a readable MQTT 3.1.1 teaching broker. MQTT 5, WebSocket,
bridging, shared subscriptions, clustered operation and full configuration
reload are outside the current implementation; see
[`docs/limitations.md`](docs/limitations.md).

### 教学简化 / Known limitations

教学时要明确区分 broker 策略和 MQTT 规范行为。详细影响见
[已知限制](docs/limitations.md)。

- **`$` 是 broker 保留的命名空间**：客户端向 `$` 开头的主题 PUBLISH 会被丢弃（QoS
  握手仍然完成，避免客户端无限重传），带 `$` 遗嘱的 CONNECT 会被拒绝。订阅侧按
  §4.7.2 处理：`#`/`+` 开头的过滤器不匹配 `$` 开头的主题，必须显式订阅 `$SOL/#`
  （见 `examples/stats_demo.py`）。
- **资源限额可能丢消息**：`max_memory`、`max_inflight` 和 `max_publish_rate`
  会在特定条件下丢弃投递或入站 PUBLISH。QoS 确认不代表应用消息已投递。
- **持久化不包含遗嘱**：快照/WAL 保存 retained 与持久会话状态；遗嘱绑定网络连接，
  broker 重启后不会恢复。

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

Tests are grouped by scope and execution mode; see the
[`tests/README.md`](tests/README.md) directory index.

Three suites make up the run:

- `unit` (`build/sol_test`) — the data structures: list, BST, trie.
- `proto` (`build/sol_proto_test`) — the pure protocol pieces: PUBLISH
  unpacking and packing, malformed packet rejection, the §4.7 topic matching
  table and the configuration parser. No broker, no sockets.
- `integration` (`tests/integration/run.py`) — starts real brokers and speaks
  MQTT to them: raw packets, delivery paths, sessions, persistence, the memory
  policy, the access control list, TLS and daemon mode. Each module starts a
  broker of its own where it needs a specific configuration;
  `tests/tools/gen_certs.sh` prepares the certificates the TLS tests use (they are
  not part of the repository). CTest passes the freshly built binary through
  `SOL_BIN`, so no `./sol` symlink is needed. To run it by hand:

  ```sh
  $ python3 tests/integration/run.py
  ```

Two more tools are meant to be run by hand:

```sh
$ python3 tests/tools/stress.py [--help]     # load generator, N clients fanning out
$ ./tests/tools/run_tsan.sh                  # the suite against a TSan build
```

### Fuzzing

The parser has a libFuzzer entry point (`tests/fuzz/fuzz_unpack.c`). It needs
clang:

```sh
$ cmake -S . -B build-fuzz -DFUZZ=ON -DCMAKE_C_COMPILER=clang
$ cmake --build build-fuzz --target sol_fuzz
$ ./build-fuzz/sol_fuzz -max_total_time=60 tests/fuzz/corpus
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
to start otherwise on recent kernels), which `tests/tools/run_tsan.sh` takes care of.

## Operations

`sol --check-config -c /etc/sol/sol.conf` parses the file, applies the same
cross checks the broker runs at startup (port range, TLS files, password and
ACL files) and exits non zero after reporting validation errors. A file named
with `-c` that cannot be opened or parsed is fatal at
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

## Quickstart

Build and start the sample broker. Its configuration listens on
`127.0.0.1:8883`:

```sh
cmake -S . -B build
cmake --build build -j
./build/sol -c etc/conf/sol.conf -v
```

The guided client scripts use `paho-mqtt`; install it with
`python3 -m pip install paho-mqtt`, then follow
[`examples/README.md`](examples/README.md).
For MQTT command-line clients, pass `-p 8883` to `mosquitto_pub` and
`mosquitto_sub`.

The sample configuration at [`etc/conf/sol.conf`](etc/conf/sol.conf) documents
all supported options. Validate a deployment file before starting the broker:

```sh
./build/sol --check-config -c /path/to/sol.conf
```

Authentication uses `username:password-hash` lines. Generate a scrypt hash file
with `passwd.py`; `--crypt` is available for legacy `crypt(3)` hashes. See
[`docs/limitations.md`](docs/limitations.md) before relying on delivery or
persistence behavior.

## Concurrency

The broker runs a fixed number of event loops, and each connection stays on the
loop that accepted it. Shared session and topic state uses the broker mutex;
per-client I/O stays on its owning loop. The detailed lifecycle and lock
boundaries are in [`docs/architecture.md`](docs/architecture.md).

## Contributing

Pull requests are welcome, just create an issue and fork it.
