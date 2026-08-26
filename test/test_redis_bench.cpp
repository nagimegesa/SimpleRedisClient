#include <iostream>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>
#include <thread>
#include <memory>

#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "context/EpollContext.h"
#include "redis.h"

int main(int argc, char* argv[]) {
    int N = (argc > 1) ? std::stoi(argv[1]) : 200000;

    // 设置为 DEBUG 以便看到框架的 ERR 日志（比如连接关闭、读写错误等）
    Logger::getInstance().set_log_level(DEBUG);

    auto socket = SocketManager::getInstance().getSocket();
    if (!socket->connect("127.0.0.1", 6379)) {
        std::cerr << "Failed to connect to Redis\n";
        return 1;
    }

    EpollContext context;

    std::atomic<int> success_count{0};
    std::atomic<int> fail_count{0};
    std::atomic<int> write_success{0};
    std::atomic<int> write_fail{0};
    std::atomic<bool> all_done{false};

    int count = 0;
    context.registerAsyncRead(socket, [&](const std::string& buf, size_t size) -> size_t {
        size_t pos = 0;
        int parsed = 0;
        while (pos < size) {
            size_t start_pos = pos;  // 记录当前响应的起始位置
            try {
                RESPValue result = RESP_Parser::parse(buf, pos);
                parsed++;
                if (result.type == RESPType::SimpleString && result.as_string() == "OK") {
                    ++success_count;
                } else {
                    LOG(DEBUG) << "Unexpected response: " << result.as_string();
                    ++fail_count;
                }
                if (success_count.load() + fail_count.load() >= N) {
                    all_done = true;
                }
            } catch (const IncompleteRESPException&) {
                // 数据不完整，回退到该响应开头，并停止解析
                pos = start_pos;
                break;
            } catch (const std::exception& e) {
                // 其他解析错误（如数据损坏），尝试跳过当前字符继续解析（避免死循环）
                LOG(ERR) << "Parse error: " << e.what() << ", skipping one byte";
                ++fail_count;
                pos = start_pos + 1;  // 跳过出错的字符
                // 也可直接返回 pos（清空缓冲区），但丢失后续数据风险更大
                if (success_count.load() + fail_count.load() >= N) all_done = true;
            }
        }
        // LOG(DEBUG) << "Read callback: parsed " << parsed
        //            << " responses, consumed " << pos << " bytes, buffer size " << size;
        return pos;
    });
    context.run();  // 启动事件循环线程

    auto start = std::chrono::steady_clock::now();

    // 发送 N 条 SET 命令
    for (int i = 0; i < N; ++i) {
        std::string key = "stress_key_" + std::to_string(i);
        std::string value = "stress_value_" + std::to_string(i);
        std::vector<std::string> args = {"SET", key, value};
        std::string cmd = buildRESPCommand(args);

        context.asyncWriteOnce(socket,
            [&](bool ok) {
                if (ok) {
                    ++write_success;
                } else {
                    ++write_fail;
                    LOG(ERR) << "Write callback failed for command #" << i;
                }
            },
            std::make_shared<std::string>(std::move(cmd)));

        // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // 等待响应，每秒钟打印进度
    int timeout_seconds = 30;
    while (!all_done.load() && timeout_seconds-- > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        LOG(DEBUG) << "Waiting... responses=" << (success_count.load()+fail_count.load())
                   << ", write_success=" << write_success.load()
                   << ", write_fail=" << write_fail.load();
    }

    auto end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    int total_received = success_count.load() + fail_count.load();
    std::cout << "\n===== Stress Test Results =====\n";
    std::cout << "Commands sent:      " << N << "\n";
    std::cout << "Responses received: " << total_received << "\n";
    std::cout << "Success:            " << success_count.load() << "\n";
    std::cout << "Failures:           " << fail_count.load() << "\n";
    std::cout << "Write success:      " << write_success.load() << "\n";
    std::cout << "Write failures:     " << write_fail.load() << "\n";
    std::cout << "Elapsed time:       " << elapsed << " seconds\n";
    std::cout << "Throughput:         " << (elapsed > 0 ? N / elapsed : 0) << " req/s\n";

    socket->close();

    if (total_received != N || fail_count.load() > 0 || write_fail.load() > 0) {
        std::cerr << "Stress test FAILED\n";
        return 1;
    }

    std::cout << "Stress test PASSED\n";
    return 0;
}