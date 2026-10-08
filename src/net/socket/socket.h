//
// Created by computer on 2026/8/16.
//
#ifndef SOCKET_HPP_
#define SOCKET_HPP_

#include <memory>
#include <functional>
#include <chrono>


class SocketManager;
class EpollContext;
class IEpollContextScope;
struct Connection;
class ISocket;

using AcceptContextCallback = std::function<void(const std::shared_ptr<ISocket>& client)>;
using ReadContextCallBack = std::function<std::size_t(const std::string&, int size)>;
using HighLevelCallback = std::function<void(const std::shared_ptr<ISocket>& client)>;
using LowLevelCallback = std::function<void(const std::shared_ptr<ISocket>& client)>;
using ClosingCallback = std::function<void(const std::shared_ptr<ISocket>& client)>;
using WriteContextCallBack = std::function<void(bool)>;
using BatchWriteContextCallback = std::function<void(bool, int)>;

class ISocket : public std::enable_shared_from_this<ISocket> {

#ifdef __linux__
public:
using SocketHandler = int;
    constexpr static SocketHandler ERROR_SOCKET = - 1;
#elif _WIN32
public:
using SockHandle = SOCKET;
    constexpr static SocketHandler ERROR_SOCKET = INVALID_SOCKET;
#else
#endif

protected:
    std::weak_ptr<EpollContext> epollContext_;
    std::weak_ptr<Connection> connection_;

public:
    explicit                         ISocket(const std::shared_ptr<EpollContext>& epollContext) : epollContext_(epollContext) {};
    virtual                          ~ISocket() = default;
    virtual bool                     bind(const std::string& ip, unsigned short port) = 0;
    virtual bool                     listen(int backlog) = 0;
    virtual std::shared_ptr<ISocket> accept() = 0;

    virtual int   read(char* msg, int len) = 0;
    virtual int   write(const char* msg, int len) = 0;
    virtual bool  connect(const char* ip, unsigned short port) = 0;
    virtual void  close() = 0;
    virtual void  setNoBlock() = 0;
    virtual void  asyncAccept(const AcceptContextCallback& callback) = 0;
    virtual void  asyncRead(const ReadContextCallBack& callback) = 0;
    virtual void  asyncWriteOnce(const WriteContextCallBack& callback, std::string buf) = 0;
    virtual void  asyncWriteBatch(const BatchWriteContextCallback& callback, const std::vector<std::string>& buf) = 0;
    virtual void  asyncWriteBatch(const BatchWriteContextCallback& callback, std::vector<std::string>&& buf) = 0;

    virtual SocketHandler addTimer(std::chrono::milliseconds ms, std::function<void()> callback) = 0;

    virtual SocketHandler getNative() const = 0;

    virtual void registerHighLevelCallback(const HighLevelCallback& callback) = 0;
    virtual void registerLowLevelCallback(const LowLevelCallback& callback) = 0;
    virtual void registerScope(const std::shared_ptr<IEpollContextScope>& scope) = 0;

    // !! 注意这里 EpollContext 只适配了 linux
    std::shared_ptr<EpollContext> getContext() const {
        if (!epollContext_.expired()) {
            return epollContext_.lock();
        }

        throw std::runtime_error("context 的周期一定比 socket 长，如果使用正确，不会执行到这里");
    }

    // 外面不会调用这个方法
    void setConnection(const std::shared_ptr<Connection>& connection) {
        connection_ = connection;
    }

    std::shared_ptr<Connection> getConnection() const {
        return connection_.lock();
    }


    static constexpr int DEFAULT_BACKLOG = -1;
    static constexpr int DEFAULT_SEND_BUFFER_SIZE = 1024 * 8;
    static constexpr int DEFAULT_RECV_BUFFER_SIZE = 1024 * 8;
    static constexpr int DEFAULT_HIGH_LEVEL_SIZE = 1024 * 512;
    static constexpr int DEFAULT_LOW_LEVEL_SIZE = 1024 * 8;
};

#endif
