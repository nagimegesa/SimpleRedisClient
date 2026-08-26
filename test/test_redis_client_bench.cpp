#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <atomic>
#include <random>
#include "SimpleRedisClient.h"
#include "logger/Logger.h"

// 辅助函数：生成随机字符串
std::string random_string(size_t len = 8) {
    static const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);
    std::string s;
    s.reserve(len);
    for (size_t i = 0; i < len; ++i) s.push_back(alphanum[dis(gen)]);
    return s;
}

void pipeline_stress_test() {
    SimpleRedisClient client;
    if (!client.connect("127.0.0.1", 6379)) {
        std::cerr << "Failed to connect to Redis.\n";
        return;
    }

    // ===== 可调参数 =====
    const size_t BATCH_SIZE = 1000;        // 每批发送的命令数（pipeline 深度）
    const size_t TOTAL_BATCHES = 1000;      // 总批次数（总命令数 = BATCH_SIZE * TOTAL_BATCHES）
    // ====================

    const size_t total_commands = BATCH_SIZE * TOTAL_BATCHES;
    std::cout << "Starting pipeline stress test:\n";
    std::cout << "  Batch size:      " << BATCH_SIZE << "\n";
    std::cout << "  Total batches:   " << TOTAL_BATCHES << "\n";
    std::cout << "  Total commands:  " << total_commands << "\n";

    // 统计
    std::atomic<long long> success{0};
    std::atomic<long long> failures{0};
    auto start = std::chrono::steady_clock::now();

    // 单线程执行（模拟 redis-benchmark 的 -c 1 -P 1000）
    for (size_t batch = 0; batch < TOTAL_BATCHES; ++batch) {
        // 存储本批次所有 future（promise 由 execute 返回）
        std::vector<std::shared_ptr<std::promise<RESPValue>>> promises;
        promises.reserve(BATCH_SIZE);

        // 1. 连续发送 BATCH_SIZE 条 SET 命令，不等待
        for (size_t i = 0; i < BATCH_SIZE; ++i) {
            std::string key = "bench_key_" + std::to_string(batch * BATCH_SIZE + i);
            std::string value = random_string(10);
            // 直接调用 execute，返回 promise，但暂不获取 future
            promises.push_back(client.execute({"SET", key, value}));
        }

        // 2. 统一等待所有响应
        for (auto& p : promises) {
            auto resp = p->get_future().get();
            if (resp.type == RESPType::SimpleString && resp.as_string() == "OK") {
                success++;
            } else {
                failures++;
            }
        }

        // 可选：每 10 批打印一次进度
        if ((batch + 1) % 10 == 0) {
            auto now = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(now - start).count();
            std::cout << "  Batch " << (batch + 1) << "/" << TOTAL_BATCHES
                      << ", commands sent: " << (batch + 1) * BATCH_SIZE
                      << ", elapsed: " << elapsed << " s\n";
        }
    }

    auto end = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end - start).count();
    double throughput = total_commands / elapsed_sec;

    std::cout << "\n========== Pipeline Stress Results ==========\n";
    std::cout << "Total commands:     " << total_commands << "\n";
    std::cout << "Successful:         " << success.load() << "\n";
    std::cout << "Failures:           " << failures.load() << "\n";
    std::cout << "Elapsed time:       " << elapsed_sec << " seconds\n";
    std::cout << "Throughput:         " << throughput << " ops/sec\n";
    std::cout << "=============================================\n";
}

int main() {
    // 关闭调试日志（避免影响性能）
    Logger::getInstance().set_log_level(WARNING);
    pipeline_stress_test();
    return 0;
}