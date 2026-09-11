# Simple Redis Client

一个基于 C++20 的轻量级 Redis 客户端示例实现，包含 RESP 协议编解码、基于 epoll 的异步网络框架、线程池，以及一个带连接池和 pipeline 能力的高层客户端 `SimpleRedisClient`。

---

## 主要特性

- **RESP 协议**：提供完整的命令构建函数 `buildRESPCommand` 与响应解析器 `RESP_Parser`，支持 Simple String、Error、Integer、Bulk String、Array、Null 六种类型。当接收到的数据不完整时，解析器会抛出 `IncompleteRESPException`，调用方可据此继续等待后续数据。
- **异步网络框架**：`EpollContext` 基于 Linux epoll 和 eventfd 实现多事件循环线程，非阻塞读写，自动处理分包与粘包。**仅支持 Linux / WSL2**。
- **高层客户端**：`SimpleRedisClient` 内置 4 条连接（由 `DEFAULT_CLIENT_COUNT` 指定），采用**线程局部轮询**策略：每个线程维护自己的计数器，依次将请求分发到各连接，无需加锁，适合多线程环境。
- **线程池**：包含自旋锁 `SpinLock`、任务队列调度、`Result` 结果回传与异常捕获。
- **日志系统**：`Logger` 支持 DEBUG、INFO、WARNING、ERROR 四个级别，通过宏 `LOG(level)` 使用。

---

## 环境要求与快速开始

### 环境要求
- **操作系统**：Linux 或 WSL2（`EpollContext` 依赖 epoll，Windows 原生不支持）。
- **编译器**：GCC 14 或更高版本（CMakeLists.txt 中已指定 `/usr/bin/gcc-14` 和 `/usr/bin/g++-14`）。
- **构建工具**：CMake 3.10+。

### 编译
```bash
cd project-path

# Release 编译（推荐）
cmake -S . -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release -j$(nproc)
```

### 运行交互式客户端
```bash
# 使用 SimpleRedisClient 版（推荐）
./cmake-build-release/redis_cli

# 使用 epoll 异步版
./cmake-build-release/main
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
Bye.
```

---

## 基本用法

### 1. 网络框架：实现一个 ECHO 服务器
```cpp
#include <iostream>
#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/LinuxSocket.h"
#include "socket/SocketManager.h"

int main() {
    Logger::getInstance().set_log_level(WARNING);

    auto context = std::make_shared<EpollContext>();
    auto socket = SocketManager::getInstance().getSocket(context);

    socket->bind("127.0.0.1", 8081);
    socket->listen(ISocket::DEFAULT_BACKLOG);

    std::vector<std::shared_ptr<ISocket>> clients;

    socket->asyncAccept([&clients](std::shared_ptr<ISocket> client) {
        if (auto c = std::static_pointer_cast<LinuxSocket>(client)) {
            LOG(INFO) << "Client accept fd: " << c->getNative();
        }
        clients.push_back(client);
        client->asyncRead([client](const std::string& buf, int size) -> int {
            if (size == 0) {
                LOG(INFO) << "client closed write, close socket";
                client->close();
                return 0;
            }
            // ECHO 回显
            auto buffer = buf.substr(0, size);
            client->asyncWriteOnce([](bool success) {
                if (!success) LOG(ERR) << "writeOnce failed";
            }, std::make_shared<std::string>(std::move(buffer)));
            return size;
        });
    });

    context->run(true); // 阻塞运行
}
```

### 2. 高层客户端：交互式 Redis CLI
```cpp
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include "app/SimpleRedisClient.h"
#include "logger/Logger.h"

std::vector<std::string> splitArgs(const std::string& line) {
    std::vector<std::string> args;
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) args.push_back(std::move(token));
    return args;
}

int main() {
    Logger::getInstance().set_log_level(WARNING);
    SimpleRedisClient client;
    if (!client.connect("127.0.0.1", 6379)) {
        LOG(ERR) << "Connect failed";
        return 1;
    }

    std::cout << "Connected to Redis at 127.0.0.1:6379" << std::endl;
    std::cout << "Type 'quit' or 'exit' to disconnect." << std::endl;

    std::string input;
    while (true) {
        std::cout << "redis> ";
        if (!std::getline(std::cin, input)) {
            std::cout << std::endl;
            break;
        }
        if (input.empty()) continue;
        if (input == "quit" || input == "exit") break;

        auto args = splitArgs(input);
        if (args.empty()) continue;

        auto res = client.execute(args);
        auto reply = res->get_future().get();
        std::cout << formatResponse(reply) << std::endl;
    }
    std::cout << "Bye." << std::endl;
    return 0;
}
```

---

## 目录结构
```
src/
  app/          SimpleRedisClient、RESP 协议（redis.h）
  net/          SocketManager、LinuxSocket、EpollContext
  utils/        Logger、ThreadPool、SpinLock、无锁队列
  main.cpp      epoll 异步版交互式客户端
  redis_cli.cpp SimpleRedisClient 版交互式客户端
test/
  test_net/             网络框架基本测试
  test_queue/           无锁队列性能测试
  test_redis/
      test_redis_bench.cpp         epoll 异步 SET 压力测试，直接使用lowapi
      test_redis_client_bench.cpp  pipeline 压力测试
      test_redis_parser.cpp        RESP 解析与异步集成正确性测试
  test_thread_pool/
      test_thread_pool_benchmark.cpp  线程池正确性与性能基准
      test_thread_pool_result.cpp     线程池 Result 接口冒烟测试
```

---

## 构建目标

| 目标                        | 说明                                                                 |
|---------------------------|--------------------------------------------------------------------|
| `main`                    | 交互式 Redis 客户端（epoll 异步版）                                           |
| `redis_cli`               | 交互式 Redis 客户端（SimpleRedisClient 版）                                 |
| `test_redis_bench`        | SET 压力测试，连续 1,000,000 条 SET，和 test_redis_client_acc 区别是不走封装的client |
| `test_redis_client_acc`   | pipeline 压力测试，连续 1,000,000 条 SET                                   |
| `test_redis_acc`          | RESP 解析正确性与异步集成测试                                                  |
| `test_thread_pool`        | 线程池正确性与性能基准                                                        |
| `test_thread_pool_result` | 线程池 Result 接口冒烟测试                                                  |
| `test_spsc_queue`         | 无锁队列性能基准测试                                                         |
| `test_echo_server`        | 简单的EchoServer实现                                                    |

---

## 测试与基准结果
**测试环境**：WSL2 Ubuntu 20.04，Ultra7 265k + 32GB 内存，GCC 14，Redis 6.0.16 运行于远程 k8s docker 容器，未开启持久化。

### 1. pipeline 压力测试（`test_redis_client_acc`）
SimpleRedisClient 使用 4 条连接，共发送 1,000,000 条 `SET`，发送完所有指令后统一接收返回数据。

| 指标 | 结果                            |
| --- |-------------------------------|
| 总命令数 | 1,000,000                     |
| 成功 | 1,000,000                     |
| 失败 | 0                             |
| 耗时 | 0.851863 s                    |
| 吞吐量 | **≈ 1.1739e+06 ops/s≈ 1.17M** |

### 2. 异步 SET 压力测试（`test_redis_bench`）
4连接 + epoll 异步读写，N = 1,000,000。

| 指标 | 结果                               |
| --- |----------------------------------|
| 发送命令 | 1,000,000                        |
| 成功 / 失败 | 1,000,000 / 0                    |
| 耗时 | 0.677078 s                       |
| 吞吐量 | **≈ 1.47693e+06 req/s ≈ 1.47M ** |

### 3. memtier_benchmark测试结果
测试结果吞吐大约 1.80M
```bash
memtier_benchmark -s ${redis_ip} -p ${redis_port} -t 4  -c 4 -n 1000000 --ratio=1:0 -d 10 --pipeline=1000
Writing results to stdout
[RUN #1] Preparing benchmark client...
[RUN #1] Launching threads now...
[RUN #1 11%,   0 secs]  4 threads  4 conns:     1837059 ops, 1837744 (avg: 1837744) ops/sec, 99.70MB/sec (avg: 99.70MB/s[RUN #1 24%,   1 secs]  4 threads  4 conns:     3781027 ops, 1943775 (avg: 1890772) ops/sec, 105.46MB/sec (avg: 102.58MB[RUN #1 36%,   2 secs]  4 threads  4 conns:     5721070 ops, 1939854 (avg: 1907135) ops/sec, 105.25MB/sec (avg: 103.47MB[RUN #1 48%,   3 secs]  4 threads  4 conns:     7609985 ops, 1888682 (avg: 1902521) ops/sec, 102.47MB/sec (avg: 103.22MB[RUN #1 59%,   5 secs]  4 threads  4 conns:     9485104 ops, 1874888 (avg: 1896994) ops/sec, 101.72MB/sec (avg: 102.92MB[RUN #1 71%,   6 secs]  4 threads  4 conns:    11323404 ops, 1838106 (avg: 1887179) ops/sec, 99.73MB/sec (avg: 102.39MB/[RUN #1 82%,   7 secs]  4 threads  4 conns:    13141903 ops, 1818295 (avg: 1877338) ops/sec, 98.65MB/sec (avg: 101.85MB/[RUN #1 93%,   8 secs]  4 threads  4 conns:    14895801 ops, 1753689 (avg: 1861880) ops/sec, 95.14MB/sec (avg: 101.01MB/[RUN #1 100%,   8 secs]  0 threads  4 conns:    16000000 ops, 1753689 (avg: 1874795) ops/sec, 95.14MB/sec (avg: 101.71MB/sec),  9.07 (avg:  8.50) msec latency

4         Threads
4         Connections per thread
1000000   Requests per client


ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec
----------------------------------------------------------------------------------------------------------------------------
Sets      1870534.90          ---          ---         8.49588         8.12700        11.83900        24.44700    103918.31
Gets            0.00         0.00         0.00             ---             ---             ---             ---         0.00
Waits           0.00          ---          ---             ---             ---             ---             ---          ---
Totals    1870534.90         0.00         0.00         8.49588         8.12700        11.83900        24.44700    103918.31

CPU Utilization Summary
Total CPU time:   6.622s  (user 5.875s, sys 0.746s)
Wall time:        8.632s
Cores used:       0.767   (avg 19.2% across 4 worker threads)
Peak utilization: 80.4%
```
### 4. 线程池性能基准

| 场景 | 参数 | 耗时 | 吞吐量 |
| --- | --- | --- | --- |
| 空任务 | 8 线程 × 1,000,000 任务 | 61.21 ms | ~16.3M tasks/s |
| CPU 密集型 | 8 线程 × 1,000,000 任务 × 100 次迭代 | 865.46 ms | ~1.16M tasks/s |
| I/O 模拟（sleep 5ms） | 8 线程 × 100 任务 | 66.48 ms | ~1,504 tasks/s |
| 空任务最佳点 | 2 线程 × 100,000 任务 | 2.34 ms | ~42.8M tasks/s |
---

## 已知限制

### 当前限制
- `SimpleRedisClient` **不维护客户端状态**，因此暂不支持 `MULTI`/`EXEC`、`SUBSCRIBE` 等需要上下文状态的命令。
- 网络层目前仅支持 Linux / WSL2，无 Windows 原生支持。
- ~~性能分析显示 `write` 系统调用和智能指针复制是主要瓶颈，有待进一步优化。~~
  1. write的性能瓶颈集中在postTask函数中每一次提交任务都需要唤醒epoll线程，所以加入了一个变量判断是否已经唤醒线程，配合writev，和扩大系统写缓冲区，将瓶颈消除
- 目前性能分析显示，主要的瓶颈在智能指针以及内存分配
    ```bash
    Total: 355 samples
           1   0.3%   0.3%      183  51.5% EventLoopThread::run
           0   0.0%   0.3%      183  51.5% clone
           0   0.0%   0.3%      183  51.5% start_thread
           0   0.0%   0.3%      183  51.5% std::error_code::default_error_condition
           0   0.0%   0.3%      172  48.5% __libc_start_main
           0   0.0%   0.3%      172  48.5% _start
           0   0.0%   0.3%      172  48.5% main
           0   0.0%   0.3%      172  48.5% pipeline_stress_test
           1   0.3%   0.6%      136  38.3% SimpleRedisClient::execute
           1   0.3%   0.8%      121  34.1% SimpleRedisClient::ClientImpl::execute
           0   0.0%   0.8%       87  24.5% std::function::operator (inline)
           1   0.3%   1.1%       78  22.0% LinuxSocket::asyncWriteOnce
           0   0.0%   1.1%       74  20.8% EpollContextImpl::asyncWriteOnce
           0   0.0%   1.1%       60  16.9% EventLoopThread::handleRead
           1   0.3%   1.4%       55  15.5% SimpleRedisClient::ClientImpl::read [clone .isra.0]
           0   0.0%   1.4%       51  14.4% EventLoopThread::postTask (inline)
          44  12.4%  13.8%       46  13.0% epoll_wait
           8   2.3%  16.1%       45  12.7% std::__shared_ptr::__shared_ptr (inline)
           0   0.0%  16.1%       45  12.7% std::shared_ptr::shared_ptr (inline)
           0   0.0%  16.1%       44  12.4% __libc_write
          43  12.1%  28.2%       44  12.4% __libc_write (inline)
           0   0.0%  28.2%       40  11.3% std::allocator_traits::construct (inline)
          12   3.4%  31.5%       40  11.3% std::function::function (inline)
           1   0.3%  31.8%       38  10.7% EventLoopThread::drainTasks
           1   0.3%  32.1%       37  10.4% std::__shared_count::__shared_count (inline)
           4   1.1%  33.2%       32   9.0% EventLoopThread::handleWrite
           0   0.0%  33.2%       32   9.0% std::__new_allocator::allocate (inline)
           0   0.0%  33.2%       32   9.0% std::allocator::allocate (inline)
           0   0.0%  33.2%       32   9.0% std::allocator_traits::allocate (inline)
    ```
---