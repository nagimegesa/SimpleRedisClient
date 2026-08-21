//
// Created by computer on 2026/8/16.
//
#pragma once
#include <memory>

class SocketManager;

class ISocket {
public:
    virtual      ~ISocket() = default;
    virtual bool bind(const std::string& ip, unsigned short port) = 0;
    virtual bool listen(int backlog) = 0;
    virtual std::shared_ptr<ISocket> accept() = 0;
    virtual int read(char* msg, int len) = 0;
    virtual int write(const char* msg, int len) = 0;
    virtual bool connect(const char* ip, unsigned short port) = 0;
    virtual void close() = 0;

    static constexpr int default_backlog = -1;
};
