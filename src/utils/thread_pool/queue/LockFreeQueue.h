//
// Created by computer on 2026/8/27.
//

#ifndef DEMO_LOCKFREEQUEUE_H
#define DEMO_LOCKFREEQUEUE_H
#include <array>
#include <atomic>


template <typename T, std::size_t Cap = 1024>
class LockFreeQueue {
public:
    template <typename U> requires std::is_same_v<std::remove_cvref_t<U>, T>
    bool push(U&& item) {
        std::size_t head = head_atomic_.load(std::memory_order_acquire);
        std::size_t head_next = (head + 1) % Cap;
        if (head_next == tail_atomic_.load(std::memory_order_acquire)) {
            return false;
        }
        data[head] = std::forward<T>(item);
        head_atomic_.store(head_next, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        std::size_t tail = tail_atomic_.load(std::memory_order_acquire);
        std::size_t head = head_atomic_.load(std::memory_order_acquire);
        if (tail == head) {
            return false;
        }
        item = std::move(data[tail]);
        std::size_t tail_next = (tail + 1) % Cap;
        tail_atomic_.store(tail_next, std::memory_order_release);
        return true;
    }

    bool empty() {
        return head_atomic_.load(std::memory_order_acquire) == tail_atomic_.load(std::memory_order_acquire);
    }

private:
    std::array<T, Cap> data;
    alignas(std::hardware_constructive_interference_size)
    std::atomic<std::size_t> head_atomic_ = 0;
    alignas(std::hardware_constructive_interference_size)
    std::atomic<std::size_t> tail_atomic_ = 0;
};

template <typename T, std::size_t Cap = 1024>
class MPSCQueue {
public:
    template <typename U> requires std::is_same_v<std::remove_cvref_t<U>, T>
    bool push(U&& item) {
        std::size_t head = head_atomic_.load(std::memory_order_acquire);
        std::size_t head_next = 0;
        do {
            head_next = (head + 1) % Cap;
            if (head_next == tail_atomic_.load(std::memory_order_acquire)) {
                return false;
            }
        } while (!head_atomic_.compare_exchange_weak(head, head_next, std::memory_order_release));
        data[head].item = std::forward<T>(item);
        data[head].seq.store(head, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        std::size_t tail = tail_atomic_.load(std::memory_order_acquire);
        std::size_t head = head_atomic_.load(std::memory_order_acquire);
        if (tail == head) {
            return false;
        }

        // std::size_t except = tail;
        // while (!data[tail].seq.compare_exchange_weak(except, INVALID_INDEX, std::memory_order_acq_rel, std::memory_order_acquire)) {
        //     except = tail;
        // }

        if (data[tail].seq.load(std::memory_order_acquire) == INVALID_INDEX) {
            return false;
        }

        item = std::move(data[tail].item);
        std::size_t tail_next = (tail + 1) % Cap;
        data[tail].seq.store(INVALID_INDEX, std::memory_order_release);
        tail_atomic_.store(tail_next, std::memory_order_release);
        return true;
    }

    bool empty() {
        std::size_t tail = tail_atomic_.load(std::memory_order_acquire);
        std::size_t head = head_atomic_.load(std::memory_order_acquire);
        return tail == head;
    }

private:
    static_assert(Cap > 0 && Cap < INT_MAX);
    static constexpr std::size_t INVALID_INDEX = Cap + 1;

    struct Slot {
        alignas(std::hardware_constructive_interference_size)
        std::atomic<std::size_t> seq = INVALID_INDEX;
        T item;
    };

    std::array<Slot, Cap> data;
    alignas(std::hardware_constructive_interference_size)
    std::atomic<std::size_t> head_atomic_ = 0;
    alignas(std::hardware_constructive_interference_size)
    std::atomic<std::size_t> tail_atomic_ = 0;
};


template <typename T, std::size_t Cap = 1024>
class MPMCQueue {
public:
    template <typename U> requires std::is_same_v<std::remove_cvref_t<U>, T>
    bool push(U&& item) {
        std::size_t head = head_atomic_.load(std::memory_order_acquire);
        std::size_t head_next = 0;
        do {
            head_next = (head + 1) % Cap;
            if (head_next == tail_atomic_.load(std::memory_order_acquire)) {
                return false;
            }
        } while (!head_atomic_.compare_exchange_weak(head, head_next, std::memory_order_release));

        while (data[head].seq.load(std::memory_order_acquire) != INVALID_INDEX) {}
        data[head].item = std::forward<T>(item);
        data[head].seq.store(head, std::memory_order_release);

        return true;
    }

    bool pop(T& item) {
        std::size_t tail = tail_atomic_.load(std::memory_order_acquire);
        do {
            std::size_t head = head_atomic_.load(std::memory_order_acquire);
            if (tail == head) {
                return false;
            }
        } while (!tail_atomic_.compare_exchange_weak(tail, (tail + 1) % Cap, std::memory_order_release));
        while (data[tail].seq.load(std::memory_order_acquire) != tail) {}
        item = std::move(data[tail].item);
        data[tail].seq.store(INVALID_INDEX, std::memory_order_release);
        return true;
    }

    bool empty() {
        std::size_t tail = tail_atomic_.load(std::memory_order_acquire);
        std::size_t head = head_atomic_.load(std::memory_order_acquire);
        return tail == head;
    }

private:
    static_assert(Cap > 0 && Cap < INT_MAX);
    static constexpr std::size_t INVALID_INDEX = Cap + 1;

    struct Slot {
        alignas(std::hardware_constructive_interference_size)
        std::atomic<std::size_t> seq = INVALID_INDEX;
        T item;
    };

    std::array<Slot, Cap> data;
    alignas(std::hardware_constructive_interference_size)
    std::atomic<std::size_t> head_atomic_ = 0;
    alignas(std::hardware_constructive_interference_size)
    std::atomic<std::size_t> tail_atomic_ = 0;
};


#endif //DEMO_LOCKFREEQUEUE_H