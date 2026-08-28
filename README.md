# Simple Redis client

一个基于 C++20 的轻量级 Redis 客户端 Demo：实现了 RESP 协议编解码、基于 epoll 的异步网络框架、线程池，以及一个带连接池与 pipeline 能力的高层客户端 `SimpleRedisClient`。

A lightweight Redis client demo written in C++20: RESP protocol encode/decode, an epoll-based async network framework, a thread pool, and a high-level client `SimpleRedisClient` with connection pool and pipeline support.

# 项目介绍(Project introduce)

### 主要特性 / Features

- **RESP 协议**：提供完整的 RESP 命令构建函数 `buildRESPCommand` 与响应解析器 `RESP_Parser`，支持 Simple String、Error、Integer、Bulk String、Array、Null 六种类型，不完整的数据通过 `IncompleteRESPException` 处理。
- **异步网络框架**：`EpollContext` 基于 Linux epoll 与 eventfd 实现多事件循环线程，非阻塞读写，自动处理分包与粘包，仅支持 Linux 与 WSL2。
- **高层客户端**：`SimpleRedisClient` 内置 4 条连接，数量由 `DEFAULT_CLIENT_COUNT = 4` 指定，按线程局部计数轮询分发请求，支持 pipeline 批量发送后统一等待响应。
- **线程池**： `ThreadPool`包含自旋锁 `SpinLock`、任务队列调度、`Result` 结果回传与异常捕获。
- **日志系统**：`Logger` 支持 DEBUG、INFO、WARNING、ERROR 四个级别。

### 目录结构 / Directory Layout

```
src/
  app/          SimpleRedisClient、RESP 协议 redis.h
  net/          SocketManager、Linux/Windows Socket、EpollContext
  utils/        Logger、ThreadPool、SpinLock、队列
  main.cpp      epoll 异步版交互式客户端
  redis_cli.cpp SimpleRedisClient 版交互式客户端
test/
  test_redis_bench.cpp          SET 压力测试，单连接 epoll 异步
  test_redis_client_bench.cpp   pipeline 压力测试，SimpleRedisClient 
  test_redis_parser.cpp         RESP 解析与异步集成正确性测试
  test_thread_pool_benchmark.cpp 线程池正确性与性能基准
  test_thread_pool_result.cpp   线程池 Result 接口冒烟测试
```

### 构建目标 / Build Targets

| Target | 说明 |
| --- | --- |
| `main` | 交互式 Redis 客户端，epoll 异步版 |
| `redis_cli` | 交互式 Redis 客户端，SimpleRedisClient 版 |
| `test_redis_bench` | SET 压力测试，参数为命令条数 N，默认 200000 |
| `test_redis_client_acc` | pipeline 压力测试，1,000,000 条 SET，数量可在源码中调整 |
| `test_redis_acc` | RESP 解析正确性与异步集成测试 |
| `test_thread_pool` | 线程池正确性与性能基准 |
| `test_thread_pool_result` | 线程池 Result 接口冒烟测试 |

# 如何运行(How to run)

### 环境要求 / Prerequisites

- **Linux / WSL2**：`EpollContext` 仅支持 Linux，Windows 上编译会触发 `static_assert`。
- 编译器：`gcc-14 / g++-14`，`CMakeLists.txt` 中已固定路径 `/usr/bin/gcc-14` 与 `/usr/bin/g++-14`。
- CMake 3.10 或更高版本。
- 一个正在运行的 Redis 服务，默认连接 `127.0.0.1:6379`。

### 编译 / Build

```bash
cd project-path

# 以 Release 为例，Debug 与 RelWithDebInfo 同理
cmake -S . -B cmake-build-release-wsl -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release-wsl -j$(nproc)
```

### 运行客户端 / Run the CLI

```bash
# 推荐使用 SimpleRedisClient 版
./cmake-build-release-wsl/redis_cli

# epoll 异步版
./cmake-build-release-wsl/main
```

示例会话：

```
redis> PING
(string) PONG
redis> SET foo bar
(string) OK
redis> GET foo
(bulk) bar
redis> quit
```

### 运行测试与基准 / Run Tests & Benchmarks

```bash
cd project-path

# 正确性测试
./cmake-build-debug-wsl/test_redis_acc
./cmake-build-release-wsl/test_thread_pool_result

# 线程池性能基准
./cmake-build-release-wsl/test_thread_pool

# Redis 压力测试，需要先启动 Redis
./cmake-build-relwithdebinfo-wsl/test_redis_bench 200000     # 默认发送 200000 条 SET
./cmake-build-relwithdebinfo-wsl/test_redis_client_acc       # pipeline，1000 批 x 1000 条
```

# 测试结果(Benchmark Result)

测试环境：WSL2 Ubuntu 20.04，Ultra7 265k + 32GB内存，`gcc-14 / g++-14`，Redis 5.0.14.1 运行于 Windows 侧 `127.0.0.1:6379`。

### 1. Redis pipeline 压力测试 / `test_redis_client_acc`

SimpleRedisClient 4 条连接，每批 pipeline 深度 1000，共 1,000,000 条 `SET`：

| 指标 | 结果                 |
| --- |--------------------|
| 总命令数 Total commands | 1,000,000          |
| 成功 Successful | 1,000,000          |
| 失败 Failures | 0                  |
| 耗时 Elapsed | 1.36 s              |
| 吞吐 Throughput | **~733319 ops/s** |

### 2. Redis 异步 SET 压力测试 / `test_redis_bench`

单连接加 epoll 异步读写，N = 200,000：

| 指标 | 结果 |
| --- | --- |
| 发送命令 Commands sent | 200,000 |
| 成功 / 失败 | 200,000 / 0 |
| 耗时 Elapsed | 1.19 s |
| 吞吐 Throughput | **~167,421 req/s** |
| 结果 | Stress test PASSED |

> 说明：该测试的计时包含固定的 1 秒轮询等待，实际传输耗时远小于显示值；同一环境下多次运行的吞吐量约有 1% 波动。

### 3. 线程池性能基准 / `test_thread_pool`，Release

| 场景 | 参数 | 耗时 | 吞吐 |
| --- | --- | --- | --- |
| 空任务 Empty | 8 线程 x 1,000,000 任务 | 61.21 ms | ~16.3M tasks/s |
| CPU 密集 | 8 线程 x 1,000,000 任务 x 100 次迭代 | 865.46 ms | ~1.16M tasks/s |
| I/O 模拟，sleep 5ms | 8 线程 x 100 任务 | 66.48 ms | ~1,504 tasks/s |
| 空任务最佳点 | 2 线程 x 100,000 任务 | 2.34 ms | ~42.8M tasks/s |

正确性测试覆盖 int、double、string、vector、异常、并发结果、边界值，全部 **PASSED**。

### 4. 正确性测试 / `test_redis_acc`

RESP 命令构建、响应解析与异步 Redis 集成测试全部通过，覆盖简单字符串、错误、整数、批量字符串、空值、数组、不完整数据等场景。
