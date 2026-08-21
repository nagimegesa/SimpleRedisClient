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
    cb_thread = std::thread(&ThreadPool::cb_run, this);
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

    cb_stop.store(true);
    cb_condition_variable.notify_all();
    if (cb_thread.joinable()) {
        cb_thread.join();
    }
}

void ThreadPool::enqueue_callback(std::function<void()> func) {
    std::unique_lock lock(cb_mutex);
    cb_queue.emplace(std::move(func));
    // cb_condition_variable.notify_one();
}

void ThreadPool::cb_run() {
    while (true) {
        std::function<void()> func = nullptr;
        {
            std::unique_lock lock(cb_mutex);
            // cb_condition_variable.wait(lock, [this] { return !cb_queue.empty() || cb_stop.load(); });
            if (cb_stop.load() && cb_queue.empty()) {
                return;
            }
            if (!cb_queue.empty()) {
                func = std::move(cb_queue.front());
                cb_queue.pop();
            }
        }

        if (func) {
            try {
                func();
            } catch (...) {
                std::cerr << "Exception occurred\n" << std::endl;
            }
        }
    }
}

void ThreadPool::run() {
    while (true) {
        std::function<void()> task = nullptr;
        {
            std::unique_lock lock(mutex);
            // cv.wait(lock, [this] {
            //     return !tasks.empty() || joined.load();
            // });

            if (joined.load() && tasks.empty()) {
                break;
            }
            if (!tasks.empty()) {
                task = std::move(tasks.front());
                tasks.pop();
            }
        }

        if (task) {
            try {
                task();
            } catch (std::exception& e) {
                std::cerr << "Exception occurred\n" << e.what() << std::endl;
            }
        }
    }
}
