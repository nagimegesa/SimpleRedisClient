#pragma once

#include <atomic>
class SpinLock {
private:
    // alignas(std::hardware_constructive_interference_size)
    std::atomic<bool> flag = false;
public:
    SpinLock() = default;
    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;
    void lock();
    void unlock();
};