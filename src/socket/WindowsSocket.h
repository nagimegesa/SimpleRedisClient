//
// Created by computer on 2026/8/16.
//
#pragma once

#include "socket.h"

#ifdef _WIN32
#include <winsock2.h>
#include <Windows.h>

// #pragma comment(lib, "Ws2_32.lib")

class WindowsSocket : public ISocket {
    SOCKET socket_fd = INVALID_SOCKET;
    bool is_bound = false;
    bool is_listening = false;

public:
    WindowsSocket();

    // 用于封装已接受的客户端套接字（由 accept 内部调用）
    explicit WindowsSocket(SOCKET s);

    WindowsSocket(const WindowsSocket&) = delete;
    WindowsSocket& operator=(const WindowsSocket&) = delete;

    // 允许移动（简化资源转移）
    WindowsSocket(WindowsSocket&& other) noexcept;

    WindowsSocket& operator=(WindowsSocket&& other) noexcept;

    ~WindowsSocket() override;

    // 绑定地址和端口（返回 true 表示成功）
    bool bind(const std::string& ip, unsigned short port) override;

    // 开始监听（backlog 默认 SOMAXCONN）
    bool listen(int backlog) override;

    bool connect(const char* ip, unsigned short port) override;

    // 接受连接，返回新的 WindowsSocket 对象（调用者负责 delete）
    std::shared_ptr<ISocket> accept() override;

    int read(char* msg, int len) override;
    int write(const char* msg, int len) override;
    void close() override;

    // 辅助方法：获取实际绑定的本地端口（用于端口自动分配时）
    unsigned short getLocalPort() const;

    // 获取底层 SOCKET 句柄（必要时使用）
    SOCKET native_handle() const;
};

#endif
