//
// Created by computer on 2026/8/27.
//

#include "thread_pool/queue/LockFreeQueue.h"
#include "thread_pool/queue/CurrentQueue.h"
#include <iostream>
#include <iomanip>

template <typename Queue>
void test_iqueue(Queue& queue, size_t iterations, const std::string& name) {
    auto start = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        for (size_t i = 0; i < iterations; ++i) {
            queue.push(std::make_shared<int>(static_cast<int>(i)));
        }
        queue.shutdown();
    });

    std::thread consumer([&]() {
        size_t count = 0;
        while (true) {
            auto item = queue.pop();
            if (!item) break;
            ++count;
        }
        // 可选：assert(count == iterations);
    });

    producer.join();
    consumer.join();

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    double throughput = static_cast<double>(iterations) / (elapsed / 1000.0);
    std::cout << std::setw(20) << name
              << " | iter = " << std::setw(8) << iterations
              << " | time = " << std::setw(6) << elapsed << " ms"
              << " | throughput = " << std::fixed << std::setprecision(2) << throughput << " ops/s"
              << std::endl;
}

// 单独测试 SPSCQueue（接口不同）
template <typename T, std::size_t Cap>
void test_spsc(LockFreeQueue<T, Cap>& queue, size_t iterations, const std::string& name) {
    auto start = std::chrono::steady_clock::now();

    std::thread producer([&]() {
        for (size_t i = 0; i < iterations; ++i) {
            while (!queue.push(std::make_shared<int>(static_cast<int>(i)))) {
                // 队列满时自旋
            }
        }
    });

    std::thread consumer([&]() {
        size_t count = 0;
        std::shared_ptr<int> item;
        while (count < iterations) {
            if (queue.pop(item)) {
                ++count;
            }
        }
    });

    producer.join();
    consumer.join();

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    double throughput = static_cast<double>(iterations) / (elapsed / 1000.0);
    std::cout << std::setw(20) << name
              << " | iter = " << std::setw(8) << iterations
              << " | time = " << std::setw(6) << elapsed << " ms"
              << " | throughput = " << std::fixed << std::setprecision(2) << throughput << " ops/s"
              << std::endl;
}

template <typename T, std::size_t Cap, size_t Producer = 4>
void test_mpsc(MPSCQueue<T, Cap>& queue, size_t iterations, const std::string& name) {
    std::atomic<bool> start_atomic = false;
    std::vector<std::thread> producers;
    for (int i = 0; i < Producer; ++i) {
        producers.emplace_back([&]() {
            std::size_t producer_iter = iterations / Producer;
            while (!start_atomic.load(std::memory_order_acquire)) {}
            for (size_t j = 0; j < producer_iter; ++j) {
                while (!queue.push(std::make_shared<int>(static_cast<int>(j)))) {
                    // 队列满时自旋
                }
            }
        });
    }

    std::thread consumer([&]() {
        size_t count = 0;
        std::shared_ptr<int> item;
        while (count < iterations) {
            if (queue.pop(item)) {
                ++count;
            }
        }
    });

    auto start = std::chrono::steady_clock::now();
    start_atomic.store(true, std::memory_order_release);
    for (int i = 0; i < Producer; ++i) {
        producers[i].join();
    }

    consumer.join();

    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    double throughput = static_cast<double>(iterations) / (elapsed / 1000.0);
    std::cout << std::setw(20) << name
              << " | iter = " << std::setw(8) << iterations
              << " | time = " << std::setw(6) << elapsed << " ms"
              << " | throughput = " << std::fixed << std::setprecision(2) << throughput << " ops/s"
              << std::endl;
}

// ==================== 主程序 ====================
int main() {
    const size_t N = 100'0000;   // 测试迭代次数（可根据需要调整）

    std::cout << "========== Single Producer / Single Consumer Stress Test ==========\n";
    std::cout << "Elements: " << N << "\n\n";

    // 测试 BlockingQueue
    {
        BlockingQueue<std::shared_ptr<int>> queue;
        test_iqueue(queue, N, "BlockingQueue");
    }

    // 测试 SpinBlockingQueue
    {
        SpinBlockingQueue<std::shared_ptr<int>> queue;
        test_iqueue(queue, N, "SpinBlockingQueue");
    }

    // 测试 SPSCQueue
    {
        LockFreeQueue<std::shared_ptr<int>, 2048> queue;
        test_spsc(queue, N, "SPSCQueue (cap=2048)");
    }


    {
        MPSCQueue<std::shared_ptr<int>, 2048> queue;
        test_mpsc(queue, N, "MPSCQueue (cap=2048)");
    }
    std::cout << "=============================================\n";
    return 0;
}