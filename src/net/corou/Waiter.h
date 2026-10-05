//
// Created by computer on 2026/10/3.
//

#ifndef DEMO_WAITER_H
#define DEMO_WAITER_H

#include <coroutine>

#include "context/EpollContext.h"
#include <memory>
#include <utility>

#include "thread_pool/thread_pool.h"

struct TimerWaiter {
    std::shared_ptr<EpollContext> context{};
    std::chrono::milliseconds ms;
    bool await_ready() { return false; } // 直接挂起，进入 await_suspend
    TimerWaiter(std::shared_ptr<EpollContext> context, std::chrono::milliseconds ms) : context(std::move(context)), ms {ms} {}
    void await_suspend(std::coroutine_handle<> h) { // void 返回值表示挂起
        context->addTimer(ms, [h]() {
            h.resume(); // h 是父协程的句柄，所以 callback 是调用 h.resume(); 也就是恢复父协程
        });
    }

    void await_resume() const noexcept {}
};


struct WriterWaiter {
    std::shared_ptr<ISocket> socket{};
    std::string data;
    bool writeResult = false;
    bool await_ready() { return false; }
    WriterWaiter(std::shared_ptr<ISocket> socket, std::string&& data) : socket(std::move(socket)), data(std::move(data)) {}
    void await_suspend(std::coroutine_handle<> h) {
        if (socket) {
            socket->asyncWriteOnce([this, h](bool success) {
                this->writeResult = success;
                h.resume();
            }, std::move(data));
        } else {
            h.resume();
        }
    }

    bool await_resume() const noexcept {
        return writeResult;
    }
};

template <typename T>
struct ThreadPoolWaiter {
    std::optional<T> result;
    std::exception_ptr exception;
    ThreadPool& pool;
    std::function<T()> func;

    ThreadPoolWaiter(ThreadPool& pool, std::function<T()> func)
        : pool(pool), func(std::move(func)) {}

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) {

        if (!pool.is_joinable()) {
            exception = std::make_exception_ptr(std::runtime_error("ThreadPool thread already close"));
            h.resume();
            return;
        }

        pool.put([this, h](Result res) {
            try {
                res.checkException();
                result.emplace(res.get<T>());
            } catch (...) {
                exception = std::current_exception();
            }
            h.resume();
        }, std::move(func));
    }

    T await_resume() {
        if (exception) std::rethrow_exception(exception);
        return std::move(*result);
    }
};

template <>
struct ThreadPoolWaiter<void> {
    ThreadPool& pool;
    std::function<void()> func;
    std::exception_ptr exception;
    ThreadPoolWaiter(
        ThreadPool& pool, std::function<void()>&& func) : pool(pool), func(std::move(func)) {}

    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> h) {
        pool.put([h, this](Result r) {
            try {
                r.checkException();
            } catch (...) {
                exception = std::current_exception();
            }
            h.resume();
        }, std::move(func));
    }

    void await_resume() const {
        if (exception) std::rethrow_exception(exception);
    }
};

#endif //DEMO_WAITER_H