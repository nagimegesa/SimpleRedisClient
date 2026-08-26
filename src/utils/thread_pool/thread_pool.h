//
// Created by computer on 2026/8/15.
//

#ifndef DEMO_THREAD_POOL_H
#define DEMO_THREAD_POOL_H
#include <any>
#include <atomic>
#include <functional>
#include <utility>
#include <vector>
#include <thread>

#include "queue/CurrentQueue.h"

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

using CallBackNone = std::nullptr_t;

class ThreadPool {
public:
    explicit ThreadPool(int thread_num);

    ~ThreadPool();

    template <typename Callback, typename F, typename ... Args>
    void put(Callback&& cb, F&& func, Args&&... args) {
        // 使用广义捕获，将参数按值（或移动）存储到 lambda 闭包中
        auto task = [this, func = std::forward<F>(func),
                    ... args = std::forward<Args>(args),
                    cb = std::forward<Callback>(cb)]() mutable -> void {
            if constexpr(is_void_func_t<F, Args...>::value) {
                try {
                    func(args...);
                    if constexpr (!std::is_same_v<Callback, CallBackNone>) {
                        if (use_async_cb) {
                            enqueue_callback([cb = std::move(cb)]() { cb(nullptr); });
                        } else {
                            cb(Result(nullptr));
                        }
                    }
                } catch (...) {
                    auto exception = std::current_exception();
                    if constexpr (!std::is_same_v<Callback, CallBackNone>) {
                        if (use_async_cb) {
                            enqueue_callback([cb = std::move(cb), exception]() { cb(Result(nullptr, exception)); });
                        } else {
                            cb(Result(nullptr, exception));
                        }
                    }
                }
            } else {
                try {
                    auto t = func(args...);
                    if constexpr (!std::is_same_v<Callback, CallBackNone>) {
                        if (use_async_cb) {
                            enqueue_callback([cb = std::move(cb), t = std::move(t)]() { cb(Result(t)); });
                        } else {
                            cb(Result(t));
                        }
                    }
                } catch (...) {
                    auto exception = std::current_exception();
                    if constexpr (!std::is_same_v<Callback, CallBackNone>) {
                        if (use_async_cb) {
                            enqueue_callback([cb = std::move(cb), exception]() { cb(Result(nullptr, exception)); });
                        } else {
                            cb(Result(nullptr, exception));
                        }
                    }
                }
            }
        };

        tasks.push(std::move(task));
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
    std::thread cb_thread;


    SpinBlockingQueue<std::function<void()>> tasks;
    SpinBlockingQueue<std::function<void()>> cb_tasks;

    bool use_async_cb = false;
};


#endif //DEMO_THREAD_POOL_H