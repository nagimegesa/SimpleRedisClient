//
// Created by computer on 2026/8/16.
//


#include "SocketManager.h"

#include "WindowsSocket.h"
#ifdef _WIN32

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
        printf("WSAStartup failed with error: %d\n", err);
        return false;
    }

    if (LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 2) {
        /* Tell the user that we could not find a usable */
        /* WinSock DLL.                                  */
        printf("Could not find a usable version of Winsock.dll\n");
        WSACleanup();
        return false;
    }

    return true;
}

static std::once_flag wsa_inited;

#endif

SocketManager& SocketManager::getInstance() {
    static SocketManager instance;
    return instance;
}

std::shared_ptr<ISocket> SocketManager::get_socket() {
#ifdef _WIN32
    std::call_once(wsa_inited, []() {wsa_init();});
    return std::make_shared<WindowsSocket>();
#endif

    return nullptr;
}

SocketManager::~SocketManager() {
#ifdef _WIN32
    WSACleanup();
#endif
}
