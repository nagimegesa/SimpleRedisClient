#ifndef LINUX_SOCKET_
#define LINUX_SOCKET_

#include "socket.h"

#ifdef __linux__

class LinuxSocket: public ISocket {
    int socket_fd = -1;

public:
    LinuxSocket();
    LinuxSocket(int fd);

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
    void                     asyncRead(const EpollContext& context, const ReadContextCallBack& call_back);
    void                     asyncWriteOnce(
        const EpollContext&                 context,
        const WriteContextCallBack&         callback,
        const std::shared_ptr<std::string>& buf
    );
    void setNoBlock() override;
    int getNative() const;
};


#endif

#endif