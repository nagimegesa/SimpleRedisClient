#ifndef DEMO_GENERATOR_H_
#define DEMO_GENERATOR_H_
#include <coroutine>
#include <deque>
#include <optional>
#include <functional>
#include <exception>
#include <utility>

template <typename T>
class AsyncGenerator {
public:
    struct promise_type {
        T value_{};
        std::coroutine_handle<> continuation_{};
        std::exception_ptr exception_{};

        AsyncGenerator get_return_object() {
            return AsyncGenerator{
                std::coroutine_handle<promise_type>::from_promise(*this)
            };
        }

        std::suspend_always initial_suspend() noexcept { return {}; }

        struct FinalAwaiter {
            bool await_ready() noexcept { return false; }

            std::coroutine_handle<>
            await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                auto c = h.promise().continuation_;
                h.promise().continuation_ = nullptr;
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

        void return_void() noexcept {}

        void unhandled_exception() noexcept {
            exception_ = std::current_exception();
        }
    };

    std::coroutine_handle<promise_type> handle_{};

    explicit AsyncGenerator(std::coroutine_handle<promise_type> h) : handle_(h) {}
    AsyncGenerator(AsyncGenerator&& o) noexcept : handle_(o.handle_) { o.handle_ = {}; }
    AsyncGenerator(const AsyncGenerator&) = delete;
    AsyncGenerator& operator=(const AsyncGenerator&) = delete;
    AsyncGenerator& operator=(AsyncGenerator&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, {});
        }
        return *this;
    }
    ~AsyncGenerator() { if (handle_) handle_.destroy(); }

    struct NextAwaiter {
        std::coroutine_handle<promise_type> h_;

        bool await_ready() const noexcept { return !h_ || h_.done(); }

        std::coroutine_handle<>
        await_suspend(std::coroutine_handle<> consumer) noexcept {
            h_.promise().continuation_ = consumer;
            return h_;
        }

        // 异常在这里 rethrow
        std::optional<T> await_resume() const {
            if (!h_) return std::nullopt;

            if (h_.promise().exception_) {
                auto p = h_.promise().exception_;
                h_.promise().exception_ = nullptr;
                std::rethrow_exception(p);
            }

            if (h_.done()) return std::nullopt;
            return std::move(h_.promise().value_);
        }
    };

    NextAwaiter next() { return NextAwaiter{ handle_ }; }
};

template <typename Promise>
struct TaskPromiseBase {
    bool closed = false;
    std::coroutine_handle<> continuation{};
    std::function<void()> onTaskClosed;
    std::exception_ptr exception_{};

    std::suspend_always initial_suspend() const noexcept { return {}; }

    struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }

        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<Promise> h) const noexcept {
            auto& promise = h.promise();

            if (promise.onTaskClosed) {
                promise.onTaskClosed();
            }
            promise.closed = true;

            return promise.continuation
                ? promise.continuation
                : std::noop_coroutine();
        }

        void await_resume() const noexcept {}
    };

    FinalAwaiter final_suspend() const noexcept { return {}; }

    void unhandled_exception() noexcept {
        exception_ = std::current_exception();
    }
};

template <typename T>
struct Task {
    static_assert(!std::is_reference_v<T>,
                  "Task<T> does not support reference types");

    struct promise_type : TaskPromiseBase<promise_type> {
        std::optional<T> value;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void return_value(T v) {
            value.emplace(std::move(v));
        }
    };

    std::coroutine_handle<promise_type> handle{};

    Task() = default;

    explicit Task(std::coroutine_handle<promise_type> h) noexcept
        : handle(h) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : handle(std::exchange(other.handle, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle) handle.destroy();
            handle = std::exchange(other.handle, {});
        }
        return *this;
    }

    ~Task() {
        if (handle) handle.destroy();
    }

    bool await_ready() const noexcept {
        return !handle || handle.done();
    }

    // 异常在这里 rethrow
    T await_resume() {
        if (handle.promise().exception_) {
            auto p = handle.promise().exception_;
            handle.promise().exception_ = nullptr;
            std::rethrow_exception(p);
        }
        return std::move(*handle.promise().value);
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<> parent) noexcept {
        handle.promise().continuation = parent;
        return handle;
    }

    void registerTaskCloseCallback(std::function<void()> cb) const noexcept {
        if (handle) handle.promise().onTaskClosed = std::move(cb);
    }

    void start() {
        if (handle) handle.resume();
    }

    bool isClosed() const {
        if (handle) return handle.promise().closed;
        return true;
    }
};

template <>
struct Task<void> {
    struct promise_type : TaskPromiseBase<promise_type> {
        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void return_void() noexcept {}
    };

    std::coroutine_handle<promise_type> handle{};

    Task() = default;

    explicit Task(std::coroutine_handle<promise_type> h) noexcept
        : handle(h) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : handle(std::exchange(other.handle, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle) handle.destroy();
            handle = std::exchange(other.handle, {});
        }
        return *this;
    }

    ~Task() {
        if (handle) handle.destroy();
    }

    bool await_ready() const noexcept {
        return !handle || handle.done();
    }

    // 异常在这里 rethrow
    void await_resume() {
        if (handle.promise().exception_) {
            auto p = handle.promise().exception_;
            handle.promise().exception_ = nullptr;
            std::rethrow_exception(p);
        }
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<> parent) noexcept {
        handle.promise().continuation = parent;
        return handle;
    }

    void registerTaskCloseCallback(std::function<void()> cb) const noexcept {
        if (handle) handle.promise().onTaskClosed = std::move(cb);
    }

    void start() {
        if (handle) handle.resume();
    }

    bool isClosed() const {
        if (handle) return handle.promise().closed;
        return true;
    }
};

template <typename T>
struct Channel {
    std::coroutine_handle<> waiter = nullptr;
    std::deque<T>           buffer;
    bool                    closed_ = false;

    struct ChannelWaiter {
        Channel& channel;

        bool await_ready() const noexcept {
            return channel.closed_ || !channel.buffer.empty();
        }
        void await_suspend(std::coroutine_handle<> handle) const noexcept {
            channel.waiter = handle;
        }
        void await_resume() const noexcept {}
    };

    template <typename U>
    void push(U&& item) {
        buffer.push_back(std::forward<U>(item));
        if (waiter) {
            auto h = waiter;
            waiter = nullptr;
            h.resume();
        }
    }

    ChannelWaiter wait() { return ChannelWaiter{*this}; }

    Task<std::optional<T>> next() {
        while (buffer.empty() && !closed_) {
            co_await wait();
        }

        if (buffer.empty()) {
            co_return std::nullopt;
        }

        auto t = std::move(buffer.front());
        buffer.pop_front();
        co_return std::move(t);
    }

    void close() {
        closed_ = true;
        if (waiter) {
            auto h = waiter;
            waiter = nullptr;
            h.resume();
        }
    }

    ~Channel() {
        close();
    }
};

#endif