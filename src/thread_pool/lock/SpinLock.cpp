//
// Created by computer on 2026/8/19.
//

#include "lock/SpinLock.h"

#include <thread>
#include <immintrin.h> // for _mm_pause

void SpinLock::lock() {
    int backoff = 0;
    while (true) {
        // 1. 先只读检查 (Relaxed)，避免不必要的 RMW 操作
        if (!flag.load(std::memory_order_relaxed) &&
            !flag.exchange(true, std::memory_order_acquire)) {
            return; // 成功获取锁
            }

        // 2. 自旋退避循环 (不立即 yield)
        for (int i = 0; i < (1 << std::min(backoff, 8)); ++i) {
#if defined(__x86_64__) || defined(_M_X64)
            _mm_pause(); // X86 暂停，省电且提高超线程性能
            // __asm__ volatile("nop");
#elif defined(__arm__) || defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#else
            // 其他架构空转
#endif
        }

        // 3. 如果自旋了多次还没拿到，再让步 (退避指数增长)
        if (backoff < 16) backoff++;
        else std::this_thread::yield();
    }
}

void SpinLock::unlock() {
    flag.store(false, std::memory_order_release);
}