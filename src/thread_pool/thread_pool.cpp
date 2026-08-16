//
// Created by computer on 2026/8/14.
//

#include "thread_pool.h"

#include <atomic>
#include <functional>
#include <iostream>
#include <mutex>
#include <ostream>
#include <queue>
#include <thread>


ThreadPool::ThreadPool(int thread_num): thread_count(thread_num) {
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(&ThreadPool::run, this);
    }
}
ThreadPool::~ThreadPool() {
    join();
}

void ThreadPool::join() {
    joined = true;
    cv.notify_all();
    for (auto &t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}
void ThreadPool::run() {
    while (true) {
        std::function<void()> task = nullptr;
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [this] {
                return !tasks.empty() || joined.load();
            });

            if (joined.load() && tasks.empty()) {
                break;
            }

            task = std::move(tasks.front());
            tasks.pop();
        }

        if (task) {
            try {
                task();
            } catch (std::exception& e) {
                std::cerr << "Exception occured\n" << e.what() << std::endl;
            }
        }
    }
}
