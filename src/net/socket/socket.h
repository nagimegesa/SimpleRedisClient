//
// Created by computer on 2026/8/16.
//
#ifndef SOCKET_HPP_
#define SOCKET_HPP_

#include <memory>
#include <functional>

class SocketManager;
class EpollContext;

using ReadContextCallBack = std::function<bool(const std::string&)>;
using WriteContextCallBack = std::function<void(bool)>;

class ISocket : public std::enable_shared_from_this<ISocket> {
public:
    virtual      ~ISocket() = default;
    virtual bool bind(const std::string& ip, unsigned short port) = 0;
    virtual bool listen(int backlog) = 0;
    virtual std::shared_ptr<ISocket> accept() = 0;
    virtual int read(char* msg, int len) = 0;
    virtual int write(const char* msg, int len) = 0;
    virtual bool connect(const char* ip, unsigned short port) = 0;
    virtual void close() = 0;
    virtual void setNoBlock() = 0;
    virtual void asyncRead(const EpollContext& context, const ReadContextCallBack& call_back) = 0;
    virtual void asyncWriteOnce(
        const EpollContext&                 context,
        const WriteContextCallBack&         callback,
        const std::shared_ptr<std::string>& buf
    ) = 0;

    static constexpr int default_backlog = -1;
};

#endif
