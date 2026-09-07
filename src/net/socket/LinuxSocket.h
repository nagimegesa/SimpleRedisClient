#ifndef LINUX_SOCKET_
#define LINUX_SOCKET_

#include "socket.h"

#ifdef __linux__

class LinuxSocket: public ISocket {
    int socket_fd = -1;

public:
    LinuxSocket(const std::shared_ptr<EpollContext>& epoll_context);
    explicit LinuxSocket(int fd, const std::shared_ptr<EpollContext>& epoll_context);

    LinuxSocket(const LinuxSocket& sock) = delete;
    LinuxSocket& operator=(const LinuxSocket& sock) = delete;

    ~LinuxSocket() override;
    bool                     bind(const std::string& ip, unsigned short port) override;
    bool                     listen(int backlog) override;
    std::shared_ptr<ISocket> accept() override;
    int                      read(char* msg, int len) override;
    int                      write(const char* msg, int len) override;
    bool                     connect(const char* ip, unsigned short port) override;
    void                     close() override;
    void                     asyncAccept(const AcceptContextCallback& callback) override;
    void                     asyncRead(const ReadContextCallBack& call_back) override;
    void                     asyncWriteOnce(
        const WriteContextCallBack&         callback,
        const std::shared_ptr<std::string>& buf
    ) override;
    void setNoBlock() override;
    int getNative() const;
};


#endif

#endif