//
// Created by computer on 2026/9/6.
//

#include <iostream>

#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/LinuxSocket.h"
#include "socket/SocketManager.h"

int main() {

    Logger::getInstance().set_log_level(WARNING);

    auto context = std::make_shared<EpollContext>();
    auto socket = SocketManager::getInstance().getSocket(context);

    socket->bind("127.0.0.1", 8081);
    socket->listen(ISocket::DEFAULT_BACKLOG);

    std::vector<std::shared_ptr<ISocket>> clients;

    socket->asyncAccept([&clients](std::shared_ptr<ISocket> client) {
        if (auto c = std::static_pointer_cast<LinuxSocket>(client)) {
            LOG(INFO) << "Client accept fd: " << c->getNative();
        }
        clients.push_back(client);
        client->asyncRead( [client](const std::string& buf, int size)->int {

            if (size == 0) {
                LOG(INFO) << "client closed write, try close socket";
                client->close();
                return 0;
            }

            auto buffer = buf.substr(0, size);
            client->asyncWriteOnce([](bool success) {
                if (!success) {
                    LOG(ERR) << "writeOnce failed";
                }
            }, std::make_shared<std::string>(std::move(buffer)));
            return size;
        });
    });

    context->run();
    std::string input;
    while (true) {
        std::getline(std::cin, input);
        if (input == "exit") {
            break;
        }

        auto write_buf = std::make_shared<std::string>(std::move(input));
        socket->asyncWriteOnce([](bool success) {
            // std::cout << success << std::endl;
            if (!success) {
                LOG(ERR) << "writeOnce failed";
            }
        }, write_buf);
    }
}