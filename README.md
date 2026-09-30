# Simple Redis Client

一个基于 C++20 的轻量级 Redis 客户端示例实现，包含 RESP 协议编解码、基于 epoll 的异步网络框架、定时器、线程池，以及一个带连接池和 pipeline 能力的高层客户端 `SimpleRedisClient`。在此之上还实现了一个基于 protobuf 与 Nacos 服务发现的 RPC 服务端 `RpcServer`，配套的 RPC 客户端见 [SimpleRpcClient](https://github.com/nagimegesa/SimpleRpcClient)。

## 目录

- [主要特性](#主要特性)
- [环境要求与快速开始](#环境要求与快速开始)
  - [环境要求](#环境要求)
  - [编译](#编译)
  - [运行交互式客户端](#运行交互式客户端)
  - [运行 RPC 服务端](#运行-rpc-服务端)
- [Redis 客户端](#redis-客户端)
  - [RESP 协议](#resp-协议)
  - [高层客户端：交互式 Redis CLI](#高层客户端交互式-redis-cli)
  - [压测结果](#压测结果)
- [RPC 服务端](#rpc-服务端)
  - [注册并暴露一个服务](#注册并暴露一个服务)
  - [RPC 协议](#rpc-协议)
  - [配套 RPC 客户端](#配套-rpc-客户端)
- [网络框架与定时器](#网络框架与定时器)
  - [ECHO 服务器](#echo-服务器)
  - [定时器](#定时器)
- [目录结构](#目录结构)
- [构建目标](#构建目标)
- [其他测试与基准结果](#测试与基准结果)
  - [线程池性能基准](#线程池性能基准)
- [已知限制](#已知限制)

---

## 主要特性

- **RESP 协议**：提供完整的命令构建函数 `buildRESPCommand` 与响应解析器 `RESP_Parser`，支持 Simple String、Error、Integer、Bulk String、Array、Null 六种类型。当接收到的数据不完整时，解析器会抛出 `IncompleteRESPException`，调用方可据此继续等待后续数据。
- **异步网络框架**：`EpollContext` 基于 Linux epoll 和 eventfd 实现多事件循环线程，非阻塞读写，自动处理分包与粘包。提供单次写和批量写接口，可以通过注册高低水位线实现背压。**目前仅支持 Linux / WSL2**，`WindowsSocket` 尚未实现。
- **定时器**：`EpollContext::addTimer` 基于 `timerfd` 实现，支持 context 级别与 socket 级别两种注册方式。
- **高层客户端**：`SimpleRedisClient` 内置 4 条连接，采用**线程局部轮询**策略：每个线程维护自己的计数器，依次将请求分发到各连接，无需加锁，适合多线程环境。
- **RPC 服务端**：`RpcServer` 使用自定义二进制协议承载请求，请求转发到线程池执行，并支持向 Nacos 注册服务实例用于服务发现。
- **线程池**：包含自旋锁、任务队列调度、结果回传与异常捕获。Utils里面简单实现了一套基于CAS和环形缓冲区的无锁队列。
- **日志系统**：`Logger` 支持 DEBUG、INFO、WARNING、ERROR 四个级别。

---

## 环境要求与快速开始

### 环境要求
- **操作系统**：Linux 或 WSL2。
- **编译器**：GCC 14 或更高版本。
- **构建工具**：CMake 3.10+。
- **第三方依赖**：`tcmalloc`、`protobuf`、`curl`、`zlib`；RPC 部分依赖 `3rdparty/nacos-sdk-cpp` 子模块。

### 编译
```bash
cd project-path

# 拉取 Nacos SDK 子模块
git submodule update --init --recursive

# Release 编译
cmake -S . -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release -j$(nproc)
```

### 运行交互式客户端
```bash
./cmake-build-release/redis_cli
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

### 运行 RPC 服务端
```bash
# 需要先启动 Nacos 默认地址为 127.0.0.1:8848，Nacos 不可用时仅跳过服务注册，本地直连不受影响
./cmake-build-release/rpc_server

# 另开终端运行协议冒烟测试
python3 test/test_rpc/rpc_client.py
```

---

## Redis 客户端

### RESP 协议

命令构建由 `buildRESPCommand` 完成，负责将参数列表拼接为 RESP 数组；响应解析由 `RESP_Parser` 完成，支持 Simple String、Error、Integer、Bulk String、Array、Null 六种类型。数据不足时解析器抛出 `IncompleteRESPException`，调用方捕获后继续等待后续数据，因此可以配合异步 read 回调处理粘包。

### 高层客户端：交互式 Redis CLI

```cpp
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include "app/redis/SimpleRedisClient.h"
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

### 压测结果

**测试环境**：WSL2 Ubuntu 20.04，Ultra7 265k + 32GB 内存，GCC 14，Redis 6.0.16 运行于远程 k8s docker 容器，未开启持久化。

pipeline 压力测试（`test_redis_client_acc`）：SimpleRedisClient 使用 4 条连接，共发送 1,000,000 条 `SET`，发送完所有指令后统一接收返回数据。

| 指标 | 结果                            |
| --- |-------------------------------|
| 总命令数 | 1,000,000                     |
| 成功 | 1,000,000                     |
| 失败 | 0                             |
| 耗时 | 0.851863 s                    |
| 吞吐量 | **≈ 1.1739e+06 ops/s≈ 1.17M** |

异步 SET 压力测试（`test_redis_bench`）：4 连接 + epoll 异步读写，N = 1,000,000。

| 指标 | 结果                              |
| --- |---------------------------------|
| 发送命令 | 1,000,000                       |
| 成功 / 失败 | 1,000,000 / 0                   |
| 耗时 | 0.677078 s                      |
| 吞吐量 | **≈ 1.47693e+06 req/s ≈ 1.47M** |

memtier_benchmark 测试结果，吞吐大约 1.87M：

```bash
memtier_benchmark -s ${redis_ip} -p ${redis_port} -t 4  -c 4 -n 1000000 --ratio=1:0 -d 10 --pipeline=1000

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

---

## RPC 服务端

### 注册并暴露一个服务

```cpp
#include <iostream>
#include "app/rpc/RpcServer.h"
#include "logger/Logger.h"
#include "protoc/hello.pb.h"

struct Hello : RpcService<Hello> {
    void setup(RpcServer& server) override {
        // 服务名同时用于 Nacos 服务注册
        registerServiceName(server, "HelloService");
        // 注册 rpc 函数
        registerFunction(server, "hello", &Hello::hello);
    }

    HelloWorldResponse hello(HelloWorldRequest req) {
        HelloWorldResponse resp;
        resp.set_res(req.msg() == "hello" ? "world" : req.msg());
        return resp;
    }
};

int main() {
    Logger::getInstance().set_log_level(WARNING);
    RpcServer server;
    if (!server.bindAndListen("127.0.0.1", 8891)) {
        std::cout << "bind failed" << std::endl;
    }

    // 向服务器实例注册
    server.registerService(Hello::create());

    server.run(true); // 阻塞运行
    return 0;
}
```

### RPC 协议

自定义二进制协议，整帧以 `0x0a 0x0b` 起始、以 `0x0b 0x0c` 结束，字段定义如下：

| 字段 | 长度 | 说明 |
| --- | --- | --- |
| type | 1 | 1 = REQUEST，2 = RESPONSE，3 = ERROR |
| request_id | 8 | 大端无符号，不能为 0 |
| error_code | 1 | 请求固定为 0，响应携带错误码 |
| service_len | 1 | 服务名长度，不超过 255 |
| service_name | 变长 | UTF-8 编码的服务名 |
| func_len | 1 | 函数名长度，不超过 255 |
| func_name | 变长 | UTF-8 编码的函数名 |
| param_len | 4 | 大端，报文体字节数，错误响应固定为 0 |
| param | 变长 | protobuf 序列化后的参数或返回值 |

错误码定义：

| 错误码 | 枚举 | 含义 |
| --- | --- | --- |
| 1 | `BAD_REQUEST` | 报文解析失败 |
| 3 | `BAD_INTERNAL` | 服务端业务处理异常 |
| 100 | `UNKNOWN_FUNCTION` | 服务名或函数名未注册 |
| 101 | `UNKNOWN_PARAM` | 请求体反序列化失败 |
| 102 | `UNKNOWN_RESPONSE` | 响应体序列化或解析失败 |

`test/test_rpc/rpc_client.py` 是按上述协议写的 Python 客户端，只用于协议冒烟测试。

### 配套 RPC 客户端

[SimpleRpcClient](https://github.com/nagimegesa/SimpleRpcClient) 是基于 Spring Boot 3 与 Netty 实现的完整客户端，与本项目的 `rpc_server` 共用同一套二进制协议，包含 Nacos 服务发现、单连接并发、分层重试与指标埋点等能力，具体用法与实现细节见该仓库的 README。

---

## 网络框架与定时器

### ECHO 服务器

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

    socket->asyncAccept([&clients](const std::shared_ptr<ISocket>& client) {
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
            client->asyncWriteOnce([](bool success) {
                if (!success) LOG(ERR) << "writeOnce failed";
            }, buf.substr(0, size));
            return size;
        });
    });

    context->run(true); // 阻塞运行
}
```

### 定时器

需要定时任务时，可以在 context 或 socket 上注册，关闭定时器还没写：

```cpp
// 不绑定 socket 的定时器
context->addTimer(std::chrono::milliseconds(100), []() {
    LOG(INFO) << "timeout";
});

// 绑定到 socket 的定时器，socket 关闭时会被一起清理
socket->addTimer(std::chrono::milliseconds(100), []() { LOG(INFO) << "timeout"; });
```

---

## 目录结构

```
src/
  app/
      redis/     SimpleRedisClient、RESP 协议
      rpc/       RpcServer、RpcService、Nacos 服务注册
  net/
      context/   EpollContext，事件循环、定时器、读写调度
      socket/    SocketManager、ISocket、LinuxSocket（WindowsSocket 未实现）
  utils/
      logger/    Logger
      thread_pool/  ThreadPool、SpinLock、阻塞队列、无锁队列
  protoc/        RPC 示例的 protobuf 定义与生成代码
  redis_cli.cpp  SimpleRedisClient 版交互式客户端
  rpc_server.cpp 使用 protobuf 与 Nacos 的示例 rpc server
test/
  test_net/             网络框架基本测试、定时器测试、Python echo 压测脚本
  test_queue/           无锁队列性能测试
  test_rpc/             rpc server 的协议冒烟测试（Python 客户端）与多语言生成代码
  test_redis/
      test_redis_bench.cpp         epoll 异步 SET 压力测试，直接使用底层 socket 接口
      test_redis_client_bench.cpp  pipeline 压力测试
      test_redis_parser.cpp        RESP 解析与异步集成正确性测试
  test_thread_pool/
      test_thread_pool_benchmark.cpp  线程池正确性与性能基准
      test_thread_pool_result.cpp     线程池 Result 接口冒烟测试
      test_memory_order.cpp           内存序实验，未加入构建目标
  test_nacos/           Nacos 服务注册冒烟测试
```

---

## 构建目标

| 目标                        | 说明                                                                 |
|---------------------------|--------------------------------------------------------------------|
| `redis_cli`               | 交互式 Redis 客户端                                 |
| `rpc_server`              | 示例 RPC 服务端，protobuf 编解码与 Nacos 服务注册                            |
| `test_redis_bench`        | SET 压力测试，连续 1,000,000 条 SET，和 test_redis_client_acc 区别是不走封装的client |
| `test_redis_client_acc`   | pipeline 压力测试，连续 1,000,000 条 SET                                   |
| `test_redis_acc`          | RESP 解析正确性与异步集成测试                                                  |
| `test_thread_pool`        | 线程池正确性与性能基准                                                        |
| `test_thread_pool_result` | 线程池 Result 接口冒烟测试                                                  |
| `test_spsc_queue`         | 无锁队列性能基准测试                                                         |
| `test_echo_server`        | 简单的EchoServer实现                                                    |
| `test_timer`              | 定时器接口测试                                                            |
| `test_nacos`              | Nacos 服务注册冒烟测试                                                      |

---

## 其他测试与基准结果

### 线程池性能基准

| 场景 | 参数 | 耗时 | 吞吐量 |
| --- | --- | --- | --- |
| 空任务 | 8 线程 × 1,000,000 任务 | 61.21 ms | ~16.3M tasks/s |
| CPU 密集型 | 8 线程 × 1,000,000 任务 × 100 次迭代 | 865.46 ms | ~1.16M tasks/s |
| I/O 模拟（sleep 5ms） | 8 线程 × 100 任务 | 66.48 ms | ~1,504 tasks/s |
| 空任务最佳点 | 2 线程 × 100,000 任务 | 2.34 ms | ~42.8M tasks/s |

Redis 客户端的压测数据见 [Redis 客户端 - 压测结果](#压测结果)。

---

## 已知限制

### 当前限制
- `SimpleRedisClient` **不维护客户端状态**，因此暂不支持 `MULTI`/`EXEC`、`SUBSCRIBE` 等需要上下文状态的命令。
- 网络层目前仅支持 Linux / WSL2，无 Windows 原生支持，`WindowsSocket` 尚未实现。
- `RpcServer` 目前只有服务端，仓库内仅提供 Python 协议冒烟测试，完整客户端见 [SimpleRpcClient](https://github.com/nagimegesa/SimpleRpcClient)；服务发现依赖 Nacos，Nacos 不可用时仅跳过注册，不影响本地直连。
- ~~性能分析显示 `write` 系统调用和智能指针复制是主要瓶颈，有待进一步优化。~~
  1. write 的性能瓶颈集中在 postTask 函数中每一次提交任务都需要唤醒 epoll 线程，所以加入了一个变量判断是否已经唤醒线程，配合 writev，和扩大系统写缓冲区，将瓶颈减轻。
- ~~目前性能分析显示，剩下的主要瓶颈在智能指针以及内存分配，write 的系统调用暂时没有想好怎么改。~~
- 同时由于记录日志的类比较简单，如果日志多起来，性能问题也比较严重，测试的时候只开的 warning 以上，而且直接把日志线程给注释了，所以相对不明显。

最新一版（2026.9.30）profile，热点回到 write，其次是 promise，以及各类小 function 的堆分配：

已经尝试做出的改进：
1. 去掉目前发现的没有必要的智能指针
2. 添加了一个批量写的接口，但是为了保证无锁，还是要利用 EpollContext 里面的 postTask 接口，又导致了 write 函数的频繁调用，目前还没想好怎么改

最终结果就是整体上更加稳定了一点，压测数据差距不大。

```bash
Total: 775 samples
117  15.1%  15.1%      118  15.2% __libc_write (inline)
51   6.6%  21.7%       51   6.6% futex_wake (inline)
49   6.3%  28.0%       49   6.3% epoll_wait
47   6.1%  34.1%       52   6.7% tcmalloc::CentralFreeList::Populate
42   5.4%  39.5%       45   5.8% tcmalloc::ThreadCache::FreeList::TryPop (inline)
37   4.8%  44.3%       39   5.0% __libc_read (inline)
29   3.7%  48.0%       29   3.7% __nss_database_lookup
29   3.7%  51.7%      164  21.2% std::_Sp_counted_base::_M_release (inline)
20   2.6%  54.3%      260  33.5% std::_Destroy_aux::__destroy (inline)
20   2.6%  56.9%       20   2.6% writev
16   2.1%  61.2%      279  36.0% std::vector::operator=
13   1.7%  64.8%      385  49.7% std::__new_allocator::deallocate (inline)
13   1.7%  66.5%       41   5.3% std::__shared_ptr::__shared_ptr (inline)
```

批量写之前的 profile，可见 `shared_ptr` 拷贝占比较高：

```bash
Total: 355 samples
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
```
