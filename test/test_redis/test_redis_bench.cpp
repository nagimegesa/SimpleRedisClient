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
    int N = (argc > 1) ? std::stoi(argv[1]) : 1000000;
    int T = (argc > 2) ? std::stoi(argv[2]) : 4;   // 线程数，默认 4
    if (T <= 0) T = 1;
    if (N <= 0) {
        std::cerr << "N must be positive\n";
        return 1;
    }

    const std::string host = "127.0.0.1";
    const unsigned short port = 6379;

    // 全局统计
    std::atomic<int> total_success{0};
    std::atomic<int> total_fail{0};
    std::atomic<int> total_write_success{0};
    std::atomic<int> total_write_fail{0};

    auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(T);

    auto context = std::make_shared<EpollContext>();
    context->run();  // 启动本线程的事件循环线程

    for (int t = 0; t < T; ++t) {
        // 把 N 条命令尽量平均分给 T 个线程
        int per_thread = N / T + (t < N % T ? 1 : 0);
        if (per_thread == 0) continue;

        threads.emplace_back([&, t, per_thread]() {
            // 每个线程独立创建一个事件循环和 socket

            auto socket = SocketManager::getInstance().getSocket(context);

            if (!socket->connect(host.c_str(), port)) {
                std::cerr << "Thread " << t << " failed to connect to Redis\n";
                total_write_fail += per_thread;
                return;
            }

            // 本线程内的统计
            std::atomic<int> success_count{0};
            std::atomic<int> fail_count{0};
            std::atomic<int> write_success{0};
            std::atomic<int> write_fail{0};
            std::atomic<bool> all_done{false};

            // 注册读回调
            context->registerAsyncRead(socket, [&](const std::string& buf, size_t size) -> size_t {
                size_t pos = 0;
                while (pos < size) {
                    size_t start_pos = pos;
                    RESPValue result;
                    try {
                        result = RESP_Parser::parse(buf, pos);
                        ++success_count;

                        if (success_count + fail_count >= per_thread) {
                            all_done = true;
                        }
                    } catch (const IncompleteRESPException&) {
                        pos = start_pos;
                        break;
                    } catch (const std::exception& e) {
                        pos = start_pos + 1;
                        ++fail_count;
                        LOG(ERR) << "Thread " << t << " parse error: " << e.what();
                        if (success_count + fail_count >= per_thread) {
                            all_done = true;
                        }
                        break;
                    }
                }
                return pos;
            });

            // 发送本线程负责的命令
            for (int i = 0; i < per_thread; ++i) {
                std::string key = "stress_key_" + std::to_string(t) + "_" + std::to_string(i);
                std::string value = "stress_value_" + std::to_string(t) + "_" + std::to_string(i);
                std::vector<std::string> args = {"SET", key, value};
                std::string cmd = buildRESPCommand(args);

                context->asyncWriteOnce(socket,
                    [&, i](bool ok) {
                        if (ok) {
                            ++write_success;
                        } else {
                            ++write_fail;
                            ++fail_count;   // 写失败相当于该命令没有响应
                            LOG(ERR) << "Thread " << t << " write failed for command #" << i;
                            if (success_count + fail_count >= per_thread) {
                                LOG(INFO) << "Write success: " << write_success.load() << "\n";
                            }
                        }
                    },
                    std::make_shared<std::string>(std::move(cmd)));
            }

            // 等待本线程所有响应收完
            while (!all_done) {
            }

            // 汇总到全局
            total_success += success_count.load();
            total_fail += fail_count.load();
            total_write_success += write_success.load();
            total_write_fail += write_fail.load();

            socket->close();
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    context->close();

    auto end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();

    int total_received = total_success.load() + total_fail.load();

    std::cout << "\n===== Stress Test Results =====\n";
    std::cout << "Threads:            " << T << "\n";
    std::cout << "Commands sent:      " << N << "\n";
    std::cout << "Responses received: " << total_received << "\n";
    std::cout << "Success:            " << total_success.load() << "\n";
    std::cout << "Failures:           " << total_fail.load() << "\n";
    std::cout << "Write success:      " << total_write_success.load() << "\n";
    std::cout << "Write failures:     " << total_write_fail.load() << "\n";
    std::cout << "Elapsed time:       " << elapsed << " seconds\n";
    std::cout << "Throughput:         " << (elapsed > 0 ? N / elapsed : 0) << " req/s\n";

    if (total_received != N || total_fail.load() > 0 || total_write_fail.load() > 0) {
        std::cerr << "Stress test FAILED\n";
        return 1;
    }

    std::cout << "Stress test PASSED\n";
    return 0;
}