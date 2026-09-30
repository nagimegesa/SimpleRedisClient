//
// Created by computer on 2026/9/30.
//
#include "context/EpollContext.h"
#include "socket/SocketManager.h"
#include <iostream>
#include <atomic>
#include <chrono>

int main() {
    auto context = std::make_shared<EpollContext>();
    auto socket = SocketManager::getInstance().getSocket(context);

    std::atomic<bool> close = false;
    auto now = std::chrono::system_clock::now();
    context->addTimer(socket, std::chrono::milliseconds(99), [&close, &now]() {
        auto end = std::chrono::system_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(end - now);
        std::cout << "time out after : " << diff << std::endl;
        // close = true;
    });

    context->addTimer(std::chrono::milliseconds(100), [&close, &now]() {
        auto end = std::chrono::system_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(end - now);
        std::cout << "time out after : " << diff << std::endl;
        close = true;
    });

    context->run();

    while (!close) {}

    return 0;
}
