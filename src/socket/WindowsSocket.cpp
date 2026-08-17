//
// Created by computer on 2026/8/16.
//

#include "socket.h"
#include "WindowsSocket.h"

#ifdef _WIN32

#include <Windows.h>
#include <winsock2.h>
#pragma comment(lib, "Ws2_32.lib")

WindowsSocket::WindowsSocket(SOCKET s): socket_fd(s) {
    if (s != INVALID_SOCKET) {
        is_bound     = true;   // 客户端套接字已连接，可视为“已绑定”以启用后续操作
        is_listening = false;
    }
}

WindowsSocket::WindowsSocket(WindowsSocket&& other) noexcept: socket_fd(other.socket_fd), is_bound(other.is_bound), is_listening(other.is_listening) {
    other.socket_fd    = INVALID_SOCKET;
    other.is_bound     = false;
    other.is_listening = false;
}

WindowsSocket& ::WindowsSocket::operator=(WindowsSocket&& other) noexcept {
    if (this != &other) {
        if (socket_fd != INVALID_SOCKET)
            closesocket(socket_fd);
        socket_fd          = other.socket_fd;
        is_bound           = other.is_bound;
        is_listening       = other.is_listening;
        other.socket_fd    = INVALID_SOCKET;
        other.is_bound     = false;
        other.is_listening = false;
    }
    return *this;
}

WindowsSocket::~WindowsSocket() {
    if (socket_fd != INVALID_SOCKET) {
        closesocket(socket_fd);
        socket_fd = INVALID_SOCKET;
    }
}

bool WindowsSocket::bind(const std::string& ip, unsigned short port) {
    if (socket_fd == INVALID_SOCKET) {
        socket_fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (socket_fd == INVALID_SOCKET) {
            printf("socket creation failed: %d\n", WSAGetLastError());
            return false;
        }
    }

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    // 将 IP 字符串转换为二进制地址
    addr.sin_addr.s_addr = inet_addr(ip.c_str());
    if (addr.sin_addr.s_addr == INADDR_NONE && ip != "0.0.0.0") {
        printf("Invalid IP address: %s\n", ip.c_str());
        closesocket(socket_fd);
        socket_fd = INVALID_SOCKET;
        return false;
    }

    int res = ::bind(socket_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (res == SOCKET_ERROR) {
        printf("bind failed with error: %d\n", WSAGetLastError());
        closesocket(socket_fd);
        socket_fd = INVALID_SOCKET;
        return false;
    }

    is_bound = true;
    return true;
}

bool WindowsSocket::listen(int backlog) {
    if (socket_fd == INVALID_SOCKET || !is_bound) {
        printf("Socket not bound\n");
        return false;
    }

    if (backlog <= 0) {
        backlog = default_backlog;
    }

    int res = ::listen(socket_fd, backlog);
    if (res == SOCKET_ERROR) {
        printf("listen failed with error: %d\n", WSAGetLastError());
        return false;
    }
    is_listening = true;
    return true;
}

std::shared_ptr<ISocket> WindowsSocket::accept() {
    if (socket_fd == INVALID_SOCKET || !is_listening) {
        printf("Socket not listening\n");
        return nullptr;
    }
    SOCKET client = ::accept(socket_fd, nullptr, nullptr);
    if (client == INVALID_SOCKET) {
        printf("accept failed with error: %d\n", WSAGetLastError());
        return nullptr;
    }
    return std::shared_ptr<ISocket>(new WindowsSocket(client));
}

int WindowsSocket::read(char* msg, int len) {
    if (socket_fd == INVALID_SOCKET) {
        printf("invalid socket\n");
        return -1;
    }

    return recv(socket_fd, msg, len, 0);
}

int  WindowsSocket::write(const char* msg, int len) {
    if (socket_fd == INVALID_SOCKET) {
        printf("invalid socket\n");
        return -1;
    }

    return send(socket_fd, msg, len, 0);
}

void WindowsSocket::close() {
    if (socket_fd != INVALID_SOCKET) {
        closesocket(socket_fd);
    }
}

unsigned short ::WindowsSocket::getLocalPort() const {
    if (socket_fd == INVALID_SOCKET) return 0;
    sockaddr_in addr;
    int         len = sizeof(addr);
    if (getsockname(socket_fd, (sockaddr*)&addr, &len) == 0) {
        return ntohs(addr.sin_port);
    }
    return 0;
}
SOCKET WindowsSocket::native_handle() const { return socket_fd; }
#endif
