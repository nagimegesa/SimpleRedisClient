//
// Created by computer on 2026/8/15.
//

#ifndef DEMO_THREAD_POOL_H
#define DEMO_THREAD_POOL_H
#include <any>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <utility>
#include <vector>
#include <thread>

struct Result {
    std::any result;
    std::exception_ptr exception;
    template <class T>
    T get() {

        if (exception) {
            std::rethrow_exception(exception);
        }

        return any_cast<T>(result);
    }
    Result(std::any result) : result(std::move(result)) {}
    Result(std::any result, std::exception_ptr exception) : result(std::move(result)), exception(std::move(exception)) {}
};

template <class T>
struct is_void {
    static constexpr bool value = false;
};

template <>
struct is_void<void> {
    static constexpr bool value = true;
};

template <class F, class ...Args>
struct is_void_func {
    using value = is_void<std::result_of_t<F(Args...)>>;
};

template <class T, class ...Args>
using is_void_func_t = typename is_void_func<T, Args...>::value;

class ThreadPool {
public:
    explicit ThreadPool(int thread_num);

    ~ThreadPool();

    template <typename F, typename ... Args, typename Callback>
    void put(Callback&& cb, F&& func, Args&&... args) {
        // 使用广义捕获，将参数按值（或移动）存储到 lambda 闭包中
        auto task = [this, func = std::forward<F>(func),
                    ... args = std::forward<Args>(args),
                    cb = std::forward<Callback>(cb)]() mutable -> void {
            // 调用时，args... 是左值（因为它们是 lambda 的成员）
            // 如果 func 接受值或 const 引用，这完全没问题
            // 如果你想减少拷贝，可以改为 std::move(args)...，但会强制右值传递

            if constexpr(is_void_func_t<F, Args...>::value) {
                try {
                    func(args...);
                    if (use_async_cb) {
                        enqueue_callback([cb = std::move(cb)]() { cb(nullptr); });
                    } else {
                        cb(Result(nullptr));
                    }
                } catch (...) {
                    auto exception = std::current_exception();
                    if (use_async_cb) {
                        enqueue_callback([cb = std::move(cb), exception]() { cb(Result(nullptr, exception)); });
                    } else {
                        cb(Result(nullptr, exception));
                    }
                }
            } else {
                try {
                    auto t = func(args...);
                    if (use_async_cb) {
                        enqueue_callback([cb = std::move(cb), t = std::move(t)]() { cb(Result(t)); });
                    } else {
                        cb(Result(t));
                    }
                } catch (...) {
                    auto exception = std::current_exception();
                    if (use_async_cb) {
                        enqueue_callback([cb = std::move(cb), exception]() { cb(Result(nullptr, exception)); });
                    } else {
                        cb(Result(nullptr, exception));
                    }

                }
            }
        };

        {
            std::lock_guard<std::mutex> lock(mutex);
            tasks.emplace(std::move(task));   // 移动 lambda，避免拷贝
        }
        cv.notify_one();
    }

    void join();

private:
    void enqueue_callback(std::function<void()> func);
    void cb_run();
    void run();
protected:

    std::atomic<bool> joined = false;
    std::atomic<int>                  thread_count{};
    std::vector<std::thread>          threads;
    std::queue<std::function<void()>> tasks;
    std::mutex                        mutex;
    std::condition_variable           cv;

    std::thread cb_thread;
    std::mutex cb_mutex;
    std::condition_variable cb_condition_variable;
    std::queue<std::function<void()>> cb_queue;
    std::atomic<bool> cb_stop = false;

    bool use_async_cb = true;
};


#endif //DEMO_THREAD_POOL_H