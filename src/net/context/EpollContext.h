//
// Created by computer on 2026/8/21.
//

#ifndef DEMO_EPOLLSERVER_H
#define DEMO_EPOLLSERVER_H
#include <memory>


#ifndef __linux__
static_assert(false, "this context is only for linux");
#endif

#include "socket/socket.h"
#include <cstring>
class ISocket;

#ifdef __linux__
struct EpollContextImpl;

class EpollContext {
    std::unique_ptr<EpollContextImpl> impl;
public:
    EpollContext();

    EpollContext(const EpollContext&) = delete;
    EpollContext& operator=(const EpollContext&) = delete;

    void registerAsyncRead(const std::shared_ptr<ISocket>& socket, const ReadContextCallBack& callback) const;
    void asyncWriteOnce(const std::shared_ptr<ISocket>& socket, const WriteContextCallBack& callback, const std::shared_ptr<std::string>& buf) const;
    void close(const std::shared_ptr<ISocket>& socket) const;
    void run() const;

    ~EpollContext();
};

struct SimpleBuffer {
    std::string buffer;
    int data_size_ = 0;

    void resize(size_t size) {
        buffer.resize(size);
    }

    int size() const {
        return buffer.size();
    }

    char* data() {
        return buffer.data();
    }

    int data_size() const {
        return data_size_;
    }

    void clear() {
        data_size_ = 0;
    }

    void read(std::size_t n) {
        if (n >= static_cast<size_t>(data_size_)) {
            clear();              // data_size_ = 0，不清空 buffer
            return;
        }
        // 将剩余有效数据移到开头
        std::memmove(buffer.data(), buffer.data() + n, data_size_ - n);
        data_size_ -= n;
        // 注意：不要改变 buffer.size()，保持原容量
    }
};

#endif

#endif //DEMO_EPOLLSERVER_H