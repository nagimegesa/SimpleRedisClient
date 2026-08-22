//
// Created by computer on 2026/8/16.
//


#include "SocketManager.h"

#include "logger/Logger.h"

#ifdef _WIN32

#include "WindowsSocket.h"

#include <iostream>
#include <mutex>
#include <Windows.h>
#include <winsock2.h>
#pragma comment(lib, "Ws2_32.lib")

static bool wsa_init() {
    WORD wVersionRequested;
    WSADATA wsaData;
    int err;
    wVersionRequested = MAKEWORD(2, 2);

    err = WSAStartup(wVersionRequested, &wsaData);
    if (err != 0) {
        // printf("%d\n", err);
        LOG(ERR) << "WSAStartup failed with error: " << err;
        return false;
    }

    if (LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 2) {
        /* Tell the user that we could not find a usable */
        /* WinSock DLL.                                  */
        LOG(ERR) << "Could not find a usable version of Winsock.dll";
        WSACleanup();
        return false;
    }

    return true;
}

static std::once_flag wsa_inited;

#elif __linux__
#include "LinuxSocket.h"

#endif


SocketManager& SocketManager::getInstance() {
    static SocketManager instance;
    return instance;
}

std::shared_ptr<ISocket> SocketManager::getSocket() {
#ifdef _WIN32
    std::call_once(wsa_inited, []() {wsa_init();});
    return std::make_shared<WindowsSocket>();
#elif __linux__
    return std::make_shared<LinuxSocket>();
#endif

    return nullptr;
}

SocketManager::~SocketManager() {
#ifdef _WIN32
    WSACleanup();
#endif
}
