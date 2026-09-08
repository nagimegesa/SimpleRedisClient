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
- **Redis 服务**：默认连接 `127.0.0.1:6379`，请确保 Redis 已启动。

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
      test_redis_bench.cpp         单连接 epoll 异步 SET 压力测试
      test_redis_client_bench.cpp  pipeline 压力测试（SimpleRedisClient）
      test_redis_parser.cpp         RESP 解析与异步集成正确性测试
  test_thread_pool/
      test_thread_pool_benchmark.cpp  线程池正确性与性能基准
      test_thread_pool_result.cpp     线程池 Result 接口冒烟测试
```

---

## 构建目标

| 目标                        | 说明                                              |
|---------------------------|-------------------------------------------------|
| `main`                    | 交互式 Redis 客户端（epoll 异步版）                        |
| `redis_cli`               | 交互式 Redis 客户端（SimpleRedisClient 版）              |
| `test_redis_bench`        | SET 压力测试，参数为命令条数 N（默认 200000）                   |
| `test_redis_client_acc`   | pipeline 压力测试，1000 批 × 1000 条，共 1,000,000 条 SET |
| `test_redis_acc`          | RESP 解析正确性与异步集成测试                               |
| `test_thread_pool`        | 线程池正确性与性能基准                                     |
| `test_thread_pool_result` | 线程池 Result 接口冒烟测试                               |
| `test_spsc_queue`         | 无锁队列性能基准测试                                      |
| `test_echo_server`        | 简单的EchoServer实现                                 |

---

## 测试与基准结果
**测试环境**：WSL2 Ubuntu 20.04，Ultra7 265k + 32GB 内存，GCC 14，Redis 5.0.14.1 运行于 Windows 侧（127.0.0.1:6379，未开启持久化）。

### 1. pipeline 压力测试（`test_redis_client_acc`）
SimpleRedisClient 使用 4 条连接，每批 pipeline 深度 1000，共发送 1,000,000 条 `SET`。

| 指标 | 结果 |
| --- | --- |
| 总命令数 | 1,000,000 |
| 成功 | 1,000,000 |
| 失败 | 0 |
| 耗时 | 1.36 s |
| 吞吐量 | **≈ 733,319 ops/s** |

### 2. 异步 SET 压力测试（`test_redis_bench`）
单连接 + epoll 异步读写，N = 200,000。

| 指标 | 结果 |
| --- | --- |
| 发送命令 | 200,000 |
| 成功 / 失败 | 200,000 / 0 |
| 耗时 | 1.19 s（含固定 1 秒轮询等待，实际传输时间更短） |
| 吞吐量 | **≈ 167,421 req/s** |
| 结论 | 压力测试通过 |

### 3. 线程池性能基准（Release 模式）

| 场景 | 参数 | 耗时 | 吞吐量 |
| --- | --- | --- | --- |
| 空任务 | 8 线程 × 1,000,000 任务 | 61.21 ms | ~16.3M tasks/s |
| CPU 密集型 | 8 线程 × 1,000,000 任务 × 100 次迭代 | 865.46 ms | ~1.16M tasks/s |
| I/O 模拟（sleep 5ms） | 8 线程 × 100 任务 | 66.48 ms | ~1,504 tasks/s |
| 空任务最佳点 | 2 线程 × 100,000 任务 | 2.34 ms | ~42.8M tasks/s |

**正确性测试**：覆盖 int、double、string、vector、异常、并发结果、边界值，全部通过。

### 4. 正确性测试（`test_redis_acc`）
RESP 命令构建、响应解析与异步 Redis 集成测试全部通过，覆盖简单字符串、错误、整数、批量字符串、空值、数组、不完整数据等多种场景。

---

## 已知限制

### 当前限制
- `SimpleRedisClient` **不维护客户端状态**，因此暂不支持 `MULTI`/`EXEC`、`SUBSCRIBE` 等需要上下文状态的命令。
- 网络层目前仅支持 Linux / WSL2，无 Windows 原生支持。
- 性能分析显示 `write` 系统调用和智能指针复制是主要瓶颈，有待进一步优化。
---