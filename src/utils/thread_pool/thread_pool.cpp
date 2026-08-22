//
// Created by computer on 2026/8/14.
//

#include "thread_pool.h"

#include <atomic>
#include <functional>
#include <future>
#include <iostream>
#include <ostream>
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

    tasks.shutdown();

    joined.store(true, std::memory_order_relaxed);

    for (auto &t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    cb_tasks.shutdown();
    if (cb_thread.joinable()) {
        cb_thread.join();
    }
}

void ThreadPool::enqueue_callback(std::function<void()> func) {
    cb_tasks.push(std::move(func));
}

void ThreadPool::cb_run() {
    while (true) {
        if (auto task = cb_tasks.pop(); task ) {
            try {
                task();
            } catch (std::exception& e) {
                std::cerr << "Exception occurred\n" << e.what() << std::endl;
            }
        } else if (joined.load(std::memory_order_acquire) && cb_tasks.empty()) {
            break;
        }
    }
}

void ThreadPool::run() {
    while (true) {
        if (auto task = tasks.pop(); task ) {
            try {
                task();
            } catch (std::exception& e) {
                std::cerr << "Exception occurred\n" << e.what() << std::endl;
            }
        } else if (joined.load(std::memory_order_acquire) && tasks.empty()) {
            break;
        }
    }
}