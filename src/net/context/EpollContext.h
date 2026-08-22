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
    void run() const;

    ~EpollContext();
};

struct SimpleBuffer {
    std::string buffer;
    int buffer_size;

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
        return buffer_size;
    }

    void clear() {
        buffer.clear();
    }
};

#endif

#endif //DEMO_EPOLLSERVER_H