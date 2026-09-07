//
// Created by computer on 2026/9/6.
//

#include <iostream>

#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/SocketManager.h"

int main() {
    EpollContext context;
    auto socket = SocketManager::getInstance().getSocket();

    socket->bind("127.0.0.1", 8080);
    socket->listen(ISocket::DEFAULT_BACKLOG);

    std::vector<std::shared_ptr<ISocket>> clients;

    socket->asyncAccept(context, [&clients](const EpollContext& context, std::shared_ptr<ISocket> client) {
        clients.push_back(client);
        client->asyncRead(context, [client, &context](const std::string& buf, int size)->int {
            auto buffer = buf.substr(0, size);
            client->asyncWriteOnce(context, [](bool success) {
                if (!success) {
                    LOG(ERR) << "writeOnce failed";
                }
            }, std::make_shared<std::string>(std::move(buffer)));
            return size;
        });
    });

    context.run();
    std::string input;
    while (true) {
        std::getline(std::cin, input);
        if (input == "exit") {
            break;
        }
        auto write_buf = std::make_shared<std::string>(std::move(input));
        socket->asyncWriteOnce(context, [](bool success) {
            // std::cout << success << std::endl;
            if (!success) {
                LOG(ERR) << "writeOnce failed";
            }
        }, write_buf);
    }
}