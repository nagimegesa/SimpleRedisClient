//
// Created by computer on 2026/8/21.
//

#ifndef DEMO_CURRENTQUEUE_H
#define DEMO_CURRENTQUEUE_H
#include <condition_variable>
#include <queue>

#include "thread_pool/lock/SpinLock.h"

template <typename T>
class IQueue {
public:
    virtual ~IQueue() = default;
    virtual void push(T&& item) = 0;
    virtual T pop() = 0;      // 阻塞
    virtual T try_pop() = 0;  // 非阻塞
    virtual bool empty() = 0;
    virtual void shutdown() = 0;
};


template <typename T>
class BlockingQueue : public IQueue<T> {
private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool stopped_ = false;  // 不需要 atomic，因为总在锁内操作

public:
    virtual void push(T&& item) override {
        {
            std::lock_guard lock(mutex_);
            if (stopped_) return; // 已关闭则丢弃
            queue_.emplace(std::forward<T>(item));
        }
        cv_.notify_one(); // 解锁后再通知，减少锁争用
    }

    virtual T pop() override {
        std::unique_lock lock(mutex_);
        // 标准写法，一目了然
        cv_.wait(lock, [this] {
            return !queue_.empty() || stopped_;
        });

        if (stopped_ && queue_.empty()) {
            return nullptr;
        }
        auto item = queue_.front();
        queue_.pop();
        return item;
    }

    virtual void shutdown() override {
        {
            std::lock_guard lock(mutex_);
            stopped_ = true;
        }
        cv_.notify_all();
    }

    // 只需要一个简单的 try_pop（非阻塞）扩展
    virtual T try_pop() override {
        std::lock_guard lock(mutex_);
        if (queue_.empty()) return nullptr;
        auto item = queue_.front();
        queue_.pop();
        return item;
    }

    virtual bool empty() override {
        std::unique_lock lock(mutex_);
        return queue_.empty();
    }
};

template <typename T>
class SpinBlockingQueue : public IQueue<T> {
private:
    alignas(std::hardware_constructive_interference_size)
    std::queue<T> queue_;
    mutable SpinLock mutex_;
    bool stopped_ = false;  // 不需要 atomic，因为总在锁内操作

public:
    virtual void push(T&& item)  {
        // auto ptr = std::make_shared<T>(std::forward<T>(item));
        {
            std::lock_guard lock(mutex_);
            if (stopped_) return; // 已关闭则丢弃
            queue_.emplace(std::forward<T>(item));
        }
    }

    virtual T pop()  {
        while (true) {
            std::unique_lock lock(mutex_);

            if (stopped_ && queue_.empty()) {
                return nullptr;
            }
            if (!queue_.empty()) {
                auto item = std::move(queue_.front());
                queue_.pop();
                return item;
            }
        }
    }

    virtual void shutdown()  {
        {
            std::lock_guard lock(mutex_);
            stopped_ = true;
        }
    }

    // 只需要一个简单的 try_pop（非阻塞）扩展
    virtual T try_pop()  {
        std::lock_guard lock(mutex_);
        if (queue_.empty()) return nullptr;
        auto item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    virtual bool empty()  {
        std::unique_lock lock(mutex_);
        return queue_.empty();
    }
};

#endif //DEMO_CURRENTQUEUE_H