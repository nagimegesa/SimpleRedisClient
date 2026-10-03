#ifndef DEMO_GENERATOR_H_
#define DEMO_GENERATOR_H_
#include <coroutine>
#include <optional>

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
            continuation_ = nullptr;
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

template <typename T>
struct Task {
    struct promise_type;
    std::coroutine_handle<promise_type> handle;
    struct promise_type {
        T value;
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_never initial_suspend() const noexcept {
            return {};
        }

        std::suspend_always final_suspend() const noexcept {
            return std::suspend_always();
        }

        void unhandled_exception() {
            std::terminate();
        }

        void return_value(T v) {
            this->value = std::move(v);
        }

    };

    bool await_ready() noexcept {
        return handle.done();
    }

    T await_resume() noexcept {
        return handle.promise().value;
    }

    bool await_suspend(std::coroutine_handle<> parent) noexcept {
        handle.resume();
        return false; // 不挂起自己
    }

    ~Task() {
        if (handle) {
            handle.destroy();
        }
    }
};

template <>
struct Task<void> {
    struct promise_type;
    std::coroutine_handle<promise_type> handle;
    struct promise_type {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void return_void() {
        }
        std::suspend_never initial_suspend() const noexcept {
            return {};
        }

        std::suspend_always final_suspend() const noexcept {
            return std::suspend_always();
        }

        void unhandled_exception() {
            std::terminate();
        }
    };

    bool await_ready() noexcept {
        return handle.done();
    }
    void await_resume() noexcept { }
    bool await_suspend(std::coroutine_handle<> parent) noexcept { return true; }

    ~Task() {
        if (handle) {
            handle.destroy();
        }
    }
};


#endif