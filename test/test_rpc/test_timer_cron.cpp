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
#include "corou/Waiter.h"

std::shared_ptr<EpollContext> context;

AsyncGenerator<int> count_for_time(int n, std::chrono::milliseconds ms) {
    for (int i = 0; i < n; i++) {
        co_await TimerWaiter{context, ms};
        co_yield i;
    }
}

Task<void> consume(AsyncGenerator<int> t) {
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
    task.start();
    while (!task.isClosed()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    context->close();
    return 0;
}