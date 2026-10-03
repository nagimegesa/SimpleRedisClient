//
// Created by computer on 2026/10/3.
//

#include <iostream>
#include <chrono>
#include <coroutine>
#include <optional>
#include <memory>
#include <thread>

#include "context/EpollContext.h"
#include "rpc/corou/Waiter.h"

template <typename T>
class AsyncGenerator {
public:
    struct promise_type {
        T value_{};
        std::coroutine_handle<> continuation_{};

        AsyncGenerator get_return_object() {
            return AsyncGenerator{
                std::coroutine_handle<promise_type>::from_promise(*this)
            };
        }
        std::suspend_always initial_suspend() noexcept { return {}; }

        struct FinalAwaiter {
            bool await_ready() noexcept { return false; }

            // 这里返回一个句柄，意思是挂起并回到返回句柄的协程（对称转移）
            std::coroutine_handle<>
            await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                auto c = h.promise().continuation_;
                return c ? c : std::noop_coroutine();
            }
            void await_resume() noexcept {}
        };

        FinalAwaiter final_suspend() noexcept { return {}; }

        struct YieldAwaiter {
            std::coroutine_handle<> consumer_;
            bool await_ready() noexcept { return false; }
            std::coroutine_handle<>
            await_suspend(std::coroutine_handle<>) noexcept {
                return consumer_ ? consumer_ : std::noop_coroutine();
            }
            void await_resume() noexcept {}
        };

        YieldAwaiter yield_value(T v) {
            value_ = std::move(v);
            auto c = continuation_;
            continuation_ = nullptr;   // 交接完毕，清空避免重复唤醒
            return YieldAwaiter{ c };
        }

        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };

    std::coroutine_handle<promise_type> handle_{};

    explicit AsyncGenerator(std::coroutine_handle<promise_type> h) : handle_(h) {}
    AsyncGenerator(AsyncGenerator&& o) noexcept : handle_(o.handle_) { o.handle_ = {}; }
    ~AsyncGenerator() { if (handle_) handle_.destroy(); }

    struct NextAwaiter {
        std::coroutine_handle<promise_type> h_;

        bool await_ready() const noexcept { return !h_ || h_.done(); }

        std::coroutine_handle<>
        await_suspend(std::coroutine_handle<> consumer) noexcept {
            // 这里 consumer 是 Task 的句柄
            h_.promise().continuation_ = consumer; // task 的句柄保存到 Generator
            return h_;
        }

        std::optional<T> await_resume() const {
            if (!h_ || h_.done()) return std::nullopt;
            return std::move(h_.promise().value_);
        }
    };

    NextAwaiter next() { return NextAwaiter{ handle_ }; }
};

class Task {
public:
    struct promise_type {
        Task get_return_object() {
            return Task{ std::coroutine_handle<promise_type>::from_promise(*this) };
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend()   noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };

    std::coroutine_handle<promise_type> handle_{};

    explicit Task(std::coroutine_handle<promise_type> h) : handle_(h) {}
    Task(Task&& o) noexcept : handle_(o.handle_) { o.handle_ = {}; }
    ~Task() { if (handle_) handle_.destroy(); }

    bool done() const { return !handle_ || handle_.done(); }
};

std::shared_ptr<EpollContext> context = nullptr;

AsyncGenerator<int> count_for_time(int n, std::chrono::milliseconds ms) {
    for (int i = 0; i < n; i++) {
        // 这里把 AsyncGenerator的句柄传入 TimerWaiter 的 await_suspend, 因为 Timer 的 Ready是 false
        co_await TimerWaiter(context, ms);
        co_yield i;
    }
}

Task consume(AsyncGenerator<int> t) {
    auto now = std::chrono::system_clock::now();
    while (auto i = co_await t.next()) {
        auto diff = std::chrono::system_clock::now() - now;
        std::cout << "[ "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(diff).count()
                  << " ms ] " << *i << std::endl;
    }
    std::cout << "consume done" << std::endl;
}

int main() {
    context = std::make_shared<EpollContext>();
    context->run();
    auto task = consume(count_for_time(10, std::chrono::milliseconds(1000)));
    while (!task.done()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    context->close();
    return 0;
}