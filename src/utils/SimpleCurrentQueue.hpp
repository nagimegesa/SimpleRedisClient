//
// Created by computer on 2026/8/15.
//

#ifndef DEMO_SIMPLECURRENTQUEUE_H
#define DEMO_SIMPLECURRENTQUEUE_H
#include <mutex>
#include <queue>


template <typename T>
class SimpleConcurrentQueue {
private:
    std::mutex mutex;
    std::queue<T> queue;

public:
    void enqueue(T&& item) {
        std::lock_guard<std::mutex> lock(mutex);
        queue.emplace(std::forward<T>(item));
    }

    bool dequeue(T* ret) {
        std::lock_guard<std::mutex> lock(mutex);

        if (queue.empty()) {
            return false;
        }

        *ret = std::move(queue.front());
        queue.pop();
        return true;
    }

    size_t size() {
        std::lock_guard<std::mutex> lock(mutex);
        return queue.size();
    }
};


#endif //DEMO_SIMPLECURRENTQUEUE_H