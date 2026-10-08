//
// Created by computer on 2026/9/6.
//

#include <iostream>

#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/LinuxSocket.h"
#include "socket/SocketManager.h"

struct Scope : IEpollContextScope {
    void onRegister(const std::shared_ptr<ISocket>& socket) override {
        socket->asyncRead( [socket](const std::string& buf, int size)->int {
            if (size == 0) {
                LOG(INFO) << "client closed write, try close socket";
                socket->close();
                return 0;
            }

            socket->asyncWriteOnce([](bool success) {
                if (!success) {
                    LOG(ERR) << "writeOnce failed";
                }
            }, buf.substr(0, size));
            return size;
        });
    }

    void onDestroy() override {
    }
};

int main() {

    Logger::getInstance().set_log_level(DEBUG);

    auto context = std::make_shared<EpollContext>();
    auto socket = SocketManager::getInstance().getSocket(context);

    socket->bind("127.0.0.1", 8081);
    socket->listen(ISocket::DEFAULT_BACKLOG);

    socket->asyncAccept([](const std::shared_ptr<ISocket>& client) {
        if (auto c = std::static_pointer_cast<LinuxSocket>(client)) {
            LOG(INFO) << "Client accept fd: " << c->getNative();
        }

        client->registerScope(std::make_shared<Scope>());
    });

    context->run();
    std::string input;
    while (true) {
        std::getline(std::cin, input);
        if (input == "exit") {
            break;
        }

        socket->asyncWriteOnce([](bool success) {
            // std::cout << success << std::endl;
            if (!success) {
                LOG(ERR) << "writeOnce failed";
            }
        }, input);
    }
}