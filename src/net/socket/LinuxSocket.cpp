#include "LinuxSocket.h"

#include <unistd.h>
#include <fcntl.h>
#include "logger/Logger.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <memory>
#include <string>
#include <tbb/internal/_template_helpers.h>

#include "context/EpollContext.h"

LinuxSocket::LinuxSocket(const std::shared_ptr<EpollContext>& epoll_context) : ISocket(epoll_context) {
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd == -1) {
        LOG(ERR) << "LinuxSocket::LinuxSocket(): invalid socket";
    }
}

LinuxSocket::LinuxSocket(int fd, const std::shared_ptr<EpollContext>& epoll_context) : ISocket(epoll_context), socket_fd(fd) {
}

LinuxSocket::~LinuxSocket() {
    LinuxSocket::close();
}

bool LinuxSocket::bind(const std::string& ip, unsigned short port) {
    if (socket_fd == -1) {
        LOG(ERR) << "LinuxSocket::bind(): invalid socket";
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        LOG(ERR) << "LinuxSocket::bind(): invalid IP address";
        return false;
    }

    if (::bind(socket_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1) {
        LOG(ERR) << "LinuxSocket::bind(): bind failed";
        return false;
    }
    return true;
}

bool LinuxSocket::listen(int backlog) {
    if (socket_fd == -1) {
        LOG(ERR) << "LinuxSocket::listen(): invalid socket";
        return false;
    }

    // 如果 backlog 为 0 或负数，使用默认值（例如 SOMAXCONN）
    if (backlog == DEFAULT_BACKLOG) {
        backlog = SOMAXCONN;
    }

    if (::listen(socket_fd, backlog) == -1) {
        LOG(ERR) << "LinuxSocket::listen(): listen failed";
        return false;
    }
    return true;
}

std::shared_ptr<ISocket> LinuxSocket::accept() {
    if (socket_fd == -1) {
        LOG(ERR) << "LinuxSocket::accept(): invalid socket";
        return nullptr;
    }

    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);
    int client_fd = ::accept(socket_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
    if (client_fd == -1) {
        LOG(ERR) << "LinuxSocket::accept(): accept failed";
        return nullptr;
    }
    if (auto context = epollContext_.lock()) {
        return std::make_shared<LinuxSocket>(client_fd, context);
    }
    LOG(ERR) << "LinuxSocket::accept(): accept failed";
    return nullptr;
}

int LinuxSocket::read(char* msg, int len) {
    if (socket_fd == -1) {
        LOG(ERR) << "LinuxSocket::read(): invalid socket";
        return -1;
    }
    return ::read(socket_fd, msg, len);
}

int LinuxSocket::write(const char* msg, int len) {
    if (socket_fd == -1) {
        LOG(ERR) << "LinuxSocket::write(): invalid socket";
        return -1;
    }
    return ::write(socket_fd, msg, len);
}

bool LinuxSocket::connect(const char* ip, unsigned short port) {
    if (socket_fd == -1) {
        LOG(ERR) << "LinuxSocket::connect(): invalid socket";
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        LOG(ERR) << "LinuxSocket::connect(): invalid IP address";
        return false;
    }

    if (::connect(socket_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1) {
        LOG(ERR) << "LinuxSocket::connect(): connect failed";
        return false;
    }
    return true;
}

void LinuxSocket::close() {
    if (socket_fd != -1) {
        if (auto context = epollContext_.lock()) {
            context->removeSocket(shared_from_this());
        }
        ::close(socket_fd);
        socket_fd = -1;
    }
}

void LinuxSocket::asyncAccept(const AcceptContextCallback& callback) {
    if (auto context = epollContext_.lock()) {
        context->registerAsyncAccept(shared_from_this(), callback);
    } else {
        LOG(ERR) << "LinuxSocket::asyncAccept(): epollContext is nullptr";
    }
}

void LinuxSocket::asyncRead(const ReadContextCallBack& call_back) {
    if (auto context = epollContext_.lock()) {
        context->registerAsyncRead(shared_from_this(), call_back);
    } else {
        LOG(ERR) << "LinuxSocket::asyncRead(): epollContext is nullptr";
    }
}

void LinuxSocket::asyncWriteOnce(
    const WriteContextCallBack& callback, const std::shared_ptr<std::string>& buf) {
    if (auto context = epollContext_.lock()) {
        context->asyncWriteOnce(shared_from_this(), callback, buf);
    } else {
        LOG(ERR) << "LinuxSocket::asyncWriteOnce: epollContext is nullptr";
    }
}

void LinuxSocket::registerLowLevelCallback(const LowLevelCallback& callback) {
    if (auto context = epollContext_.lock()) {
        context->registerLowLevelCallback(shared_from_this(), callback);
    } else {
        LOG(ERR) << "LinuxSocket::registerLowLevelCallback: epollContext is nullptr";
    }
}

void LinuxSocket::registerHighLevelCallback(const HighLevelCallback& callback) {
    if (auto context = epollContext_.lock()) {
        context->registerHighLevelCallback(shared_from_this(), callback);
    } else {
        LOG(ERR) << "LinuxSocket::registerHighLevelCallback: epollContext is nullptr";
    }
}

void LinuxSocket::setNoBlock() {
    int flags = ::fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1) {
        LOG(ERR) << "fcntl F_GETFL failed";
        return;
    }
    if (::fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        LOG(ERR) << "fcntl F_SETFL failed";
    }

    int enable = 1;
    if (setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable)) < 0) {
        LOG(ERR) <<"setsockopt TCP_NODELAY failed";
    }

    if (setsockopt( socket_fd, SOL_SOCKET, SO_SNDBUF, &ISocket::DEFAULT_SEND_BUFFER_SIZE, sizeof( ISocket::DEFAULT_SEND_BUFFER_SIZE ) )) {
        LOG(ERR) << "setsockopt SO_SNDBUF failed";
    }

    if (setsockopt(socket_fd, SOL_SOCKET, SO_RCVBUF, &ISocket::DEFAULT_RECV_BUFFER_SIZE, sizeof( ISocket::DEFAULT_RECV_BUFFER_SIZE ) )) {
        LOG(ERR) << "setsockopt SO_RCVBUF failed";
    }
}

int LinuxSocket::getNative() const {
    return socket_fd;
}
