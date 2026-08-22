//
// Created by computer on 2026/8/16.
//

#ifndef DEMO_SOCKETMANAGER_H
#define DEMO_SOCKETMANAGER_H
#include <memory>


#include "socket.h"

class SocketManager {
private:
    SocketManager() = default;
public:

    static SocketManager &getInstance();

    std::shared_ptr<ISocket> getSocket();

    ~SocketManager();
};


#endif //DEMO_SOCKETMANAGER_H