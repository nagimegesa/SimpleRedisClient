//
// Created by computer on 2026/8/22.
//

#include "EpollContext.h"

#include <mutex>
#include <queue>
#include <atomic>
#include <thread>
#include <vector>
#include <unordered_map>
#include <functional>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

#include "logger/Logger.h"
#include "queue/LockFreeQueue.h"
#include "socket/LinuxSocket.h"

// 写元信息（原代码中的 WriteMetaInfo）
struct WriteMetaInfo {
    std::shared_ptr<ISocket> socket;
    std::shared_ptr<std::string> buffer;
    size_t offset = 0;
    WriteContextCallBack callback;

    WriteMetaInfo(std::shared_ptr<ISocket> s, WriteContextCallBack cb, std::shared_ptr<std::string> buf)
        : socket(std::move(s)), callback(std::move(cb)), buffer(std::move(buf)) {}
};

// ------------------------- 内部结构定义 -------------------------

// 连接对象，封装一个 fd 的所有状态（仅在事件循环线程中访问，无需加锁）
struct Connection {
    std::shared_ptr<ISocket> socket;          // 原始 socket（可能是 LinuxSocket）
    ReadContextCallBack read_cb;              // 读回调
    SimpleBuffer read_buffer;                 // 读缓冲区
    std::queue<WriteMetaInfo> write_queue;    // 写队列
    bool read_registered = false;             // 是否已注册 EPOLLIN
    bool write_registered = false;            // 是否已注册 EPOLLOUT
    bool peer_closed_ = false;                // 对端是否已关闭写端（收到 FIN）

    Connection(std::shared_ptr<ISocket> s, ReadContextCallBack cb)
        : socket(std::move(s)), read_cb(std::move(cb)) {
        read_buffer.resize(DEFAULT_BUFFER_SIZE);
    }

    constexpr static int DEFAULT_BUFFER_SIZE = 2048;
};

// 单个事件循环线程的实现
class EventLoopThread {
public:
    EventLoopThread() : epoll_fd_(-1), wakeup_fd_(-1), running_(false) {
        epoll_fd_ = ::epoll_create(1);
        if (epoll_fd_ == -1) {
            LOG(ERR) << "EventLoopThread: epoll_create failed";
            return;
        }
        wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeup_fd_ == -1) {
            LOG(ERR) << "EventLoopThread: eventfd failed";
            ::close(epoll_fd_);
            epoll_fd_ = -1;
            return;
        }
        // 注册 wakeup_fd 到 epoll
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = wakeup_fd_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &ev);
    }

    ~EventLoopThread() {
        stop();
        if (epoll_fd_ != -1) ::close(epoll_fd_);
        if (wakeup_fd_ != -1) ::close(wakeup_fd_);
    }

    // 启动事件循环（在独立线程中运行）
    void start() {
        running_ = true;
        thread_ = std::thread([this] { run(); });
    }

    // 停止事件循环（线程安全）
    void stop() {
        if (running_.exchange(false)) {
            // 唤醒 epoll_wait
            uint64_t one = 1;
            ::write(wakeup_fd_, &one, sizeof(one));
            if (thread_.joinable()) {
                thread_.join();
            }
        }
    }

    // 提交任务到本线程的事件循环（线程安全）
    void postTask(std::function<void()> task) {
        {
            // std::lock_guard<std::mutex> lock(task_mutex_);
            task_queue_.push(std::move(task));
        }
        // 唤醒 epoll_wait
        uint64_t one = 1;
        ::write(wakeup_fd_, &one, sizeof(one));
    }

private:
    void run() {
        epoll_event events[MAX_EVENTS];
        while (running_) {
            int n = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, -1);
            if (n == -1) {
                if (errno == EINTR) continue;
                LOG(ERR) << "EventLoopThread: epoll_wait error";
                break;
            }
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (fd == wakeup_fd_) {
                    // 处理唤醒：清空 eventfd 并执行所有待处理任务
                    uint64_t val;
                    while (::read(wakeup_fd_, &val, sizeof(val)) > 0) {}
                    drainTasks();
                    continue;
                }
                // 处理普通事件
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    LOG(DEBUG) << "EventLoopThread: close connection because EPOLLERR | EPOLLHUP";
                    closeConnection(fd);
                    continue;
                }
                if (events[i].events & EPOLLIN) {
                    handleRead(fd);
                }
                if (events[i].events & EPOLLOUT) {
                    handleWrite(fd);
                }
            }
        }
    }

    void drainTasks() {
        // std::queue<std::function<void()>> tasks;
        // {
        //     std::lock_guard<std::mutex> lock(task_mutex_);
        //     tasks.swap(task_queue_);
        // }
        // while (!tasks.empty()) {
        //     tasks.front()();
        //     tasks.pop();
        // }

        std::function<void()> task;
        while (task_queue_.pop(task) || (!running_ && !task_queue_.empty())) {
            task();
        }
    }

    void handleRead(int fd) {
        auto it = connections_.find(fd);
        if (it == connections_.end()) return;
        auto& conn = it->second;

        int data_size = conn->read_buffer.data_size();
        int capacity = conn->read_buffer.size();
        char* buf = conn->read_buffer.data() + data_size;
        int ret = conn->socket->read(buf, capacity - data_size);

        if (ret > 0) {
            // 正常读取，调用回调
            if (conn->read_cb) {
                conn->read_buffer.data_size_ += ret;
                size_t consumed = conn->read_cb(conn->read_buffer.buffer, conn->read_buffer.data_size());
                // 防止 consumed 大于 data_size_ 导致负数
                if (consumed > conn->read_buffer.data_size()) {
                    consumed = conn->read_buffer.data_size();
                }
                conn->read_buffer.read(consumed);
            } else {
                conn->read_buffer.clear();
            }
        } else if (ret == 0) {
            // 对端关闭写端（半关闭），设置标志并调整事件
            LOG(DEBUG) << "EventLoopThread: peer closed write side on fd " << fd;
            conn->peer_closed_ = true;
            conn->read_registered = false;      // 对端不再发送数据，无需监听读

            if (!conn->write_queue.empty()) {
                // 仍有数据待发送，注册写事件
                if (!conn->write_registered) {
                    conn->write_registered = true;
                    modifyEpollEvents(fd, EPOLLOUT);
                } else {
                    // 之前可能同时监听了读写，改为只监听写
                    modifyEpollEvents(fd, EPOLLOUT);
                }
            } else {
                // 无待写数据，移除所有事件监听，但保留连接对象
                conn->write_registered = false;
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            }
        } else { // ret < 0
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 暂时无数据，忽略，等待下次 epoll 触发
                return;
            }
            LOG(ERR) << "EventLoopThread: read error on fd " << fd << " error code " << errno;
            closeConnection(fd);
        }
    }

    void handleWrite(int fd) {
        auto it = connections_.find(fd);
        if (it == connections_.end()) return;

        auto& conn = it->second;

        while (!conn->write_queue.empty()) {
            auto& meta = conn->write_queue.front();
            char* data = meta.buffer->data() + meta.offset;
            size_t remaining = meta.buffer->size() - meta.offset;
            int n = meta.socket->write(data, remaining);

            if (n > 0) {
                meta.offset += n;
                if (meta.offset == meta.buffer->size()) {
                    if (meta.callback) meta.callback(true);
                    conn->write_queue.pop();
                }
            } else if (n == 0) {
                LOG(DEBUG) << "EventLoopThread: handling write get write returns 0";
                // 对端关闭，应视为错误
                failAllPendingWrites(conn);
                closeConnection(fd);
                return;
            } else { // n < 0
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    LOG(DEBUG) << "EventLoopThread: write error: errno " << errno;
                    // 写缓冲区满，等待下次 EPOLLOUT，停止本次处理
                    return;
                } else {
                    LOG(ERR) << "EventLoopThread: write error on fd " << fd <<  " error code " << errno;
                    failAllPendingWrites(conn);
                    closeConnection(fd);
                    return;
                }
            }
        }

        // 写队列已空，更新事件监听状态
        conn->write_registered = false;
        if (conn->peer_closed_) {
            // 对端已半关闭，且数据已发送完毕，移除所有事件，等待外部关闭
            LOG(DEBUG) << "EventLoopThread: peer half-closed and all data sent, remove fd from epoll, fd " << fd;
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        } else {
            // 对端未关闭，根据读注册状态调整事件
            if (conn->read_registered) {
                modifyEpollEvents(fd, EPOLLIN);
            } else {
                // 无读事件，移除所有事件（保持连接，等待外部后续操作）
                LOG(DEBUG) << "EventLoopThread: write queue empty and no read registered, remove fd from epoll, fd " << fd;
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            }
        }
    }

    void failAllPendingWrites(const std::shared_ptr<Connection>& conn) {
        LOG(DEBUG) << "EventLoopThread: all writes failed";
        while (!conn->write_queue.empty()) {
            auto& meta = conn->write_queue.front();
            if (meta.callback) {
                meta.callback(false);
            }
            conn->write_queue.pop();
        }
    }

    void closeConnection(int fd) {
        LOG(DEBUG) << "EventLoopThread: closing connection " << fd;
        auto it = connections_.find(fd);
        if (it == connections_.end()) return;
        auto conn = it->second; // 拷贝 shared_ptr 以便在 map 外使用

        // 从 epoll 中删除（可能之前已被移除，忽略错误）
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        // 从 map 中移除
        connections_.erase(it);

        // 通知所有待写请求失败
        failAllPendingWrites(conn);
    }

    void modifyEpollEvents(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
    }

public:
    // 以下为通过 postTask 调用的函数（在事件循环线程执行）
    void doRegisterRead(int fd, const std::shared_ptr<ISocket>& socket, const ReadContextCallBack& callback) {
        // 如果连接已存在，更新读回调和事件；否则新建
        auto it = connections_.find(fd);
        if (it != connections_.end()) {
            auto& conn = it->second;
            conn->read_cb = callback;
            // 如果对端已半关闭，则不再注册读事件
            if (conn->peer_closed_) {
                LOG(DEBUG) << "EventLoopThread: ignore read registration because peer already closed, fd " << fd;
                return;
            }
            if (!conn->read_registered) {
                conn->read_registered = true;
                if (conn->write_registered) {
                    modifyEpollEvents(fd, EPOLLIN | EPOLLOUT);
                } else {
                    modifyEpollEvents(fd, EPOLLIN);
                }
            }
            return;
        }

        // 新建连接
        auto conn = std::make_shared<Connection>(socket, callback);
        conn->read_registered = true;
        connections_[fd] = conn;

        // 设置 socket 非阻塞（假设在外部已设置，这里再次确保）
        if (auto sc = std::static_pointer_cast<LinuxSocket>(socket)) {
            sc->setNoBlock();
        }

        // 注册到 epoll
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
    }

    void doAsyncWriteOnce(int fd, const std::shared_ptr<ISocket>& socket,
                          const WriteContextCallBack& callback, const std::shared_ptr<std::string>& buf) {
        // 如果连接不存在，则创建一个只写连接
        auto it = connections_.find(fd);
        if (it == connections_.end()) {
            auto conn = std::make_shared<Connection>(socket, nullptr);
            conn->write_registered = true;
            connections_[fd] = conn;

            epoll_event ev{};
            ev.events = EPOLLOUT;
            ev.data.fd = fd;
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);

            it = connections_.find(fd); // 重新获取
        }

        // 添加写请求
        auto& conn = it->second;
        conn->write_queue.emplace(socket, callback, buf);

        // 如果尚未注册写事件，则修改 epoll
        if (!conn->write_registered) {
            conn->write_registered = true;
            if (conn->read_registered) {
                modifyEpollEvents(fd, EPOLLIN | EPOLLOUT);
            } else {
                modifyEpollEvents(fd, EPOLLOUT);
            }
        }
    }

    void doClose(int fd) {
        closeConnection(fd);
    }

private:
    int epoll_fd_;
    int wakeup_fd_;
    std::atomic<bool> running_{false};
    std::thread thread_;

    std::unordered_map<int, std::shared_ptr<Connection>> connections_; // 仅在事件循环线程访问
    // std::queue<std::function<void()>> task_queue_;                     // 待处理任务队列
    // std::mutex task_mutex_;                                            // 保护任务队列
    MPSCQueue<std::function<void()>, 4096> task_queue_;

    constexpr static int MAX_EVENTS = 2048;
};

// ------------------------- EpollContext::Impl -------------------------
struct EpollContextImpl {
    static constexpr int loop_size = 4;
public:
    EpollContextImpl() {
        // 创建两个事件循环线程（可根据需要调整）
        loops_.reserve(loop_size);
        for (int i = 0; i < loop_size; ++i) {
            loops_.emplace_back(std::make_unique<EventLoopThread>());
        }
    }

    ~EpollContextImpl() {
        // 停止所有事件循环
        for (auto& loop : loops_) {
            loop->stop();
        }
    }

    void registerRead(const std::shared_ptr<ISocket>& socket, const ReadContextCallBack& callback) {
        if (auto sc = std::static_pointer_cast<LinuxSocket>(socket)) {
            int fd = sc->getNative();
            if (fd == -1) {
                LOG(ERR) << "EpollContext: invalid socket fd";
                return;
            }
            // 根据 fd 哈希选择事件循环线程
            size_t index = std::hash<int>{}(fd) % loops_.size();
            // 提交任务到对应线程
            loops_[index]->postTask([this, index, fd, socket, callback] {
                loops_[index]->doRegisterRead(fd, socket, callback);
            });
        } else {
            LOG(ERR) << "EpollContext is only for linux socket";
        }
    }

    void asyncWriteOnce(const std::shared_ptr<ISocket>& socket, const WriteContextCallBack& callback,
                        const std::shared_ptr<std::string>& buf) {
        if (auto sc = std::static_pointer_cast<LinuxSocket>(socket)) {
            int fd = sc->getNative();
            if (fd == -1) {
                LOG(ERR) << "EpollContext: invalid socket fd";
                if (callback) callback(false);
                return;
            }
            size_t index = std::hash<int>{}(fd) % loops_.size();
            loops_[index]->postTask([this, index, fd, socket, callback, buf] {
                loops_[index]->doAsyncWriteOnce(fd, socket, callback, buf);
            });
        } else {
            LOG(ERR) << "EpollContext is only for linux socket";
            if (callback) callback(false);
        }
    }

    void close(const std::shared_ptr<ISocket>& socket) {
        if (auto sc = std::static_pointer_cast<LinuxSocket>(socket)) {
            int fd = sc->getNative();
            if (fd == -1) {
                LOG(ERR) << "EpollContext: invalid socket fd";
                return;
            }
            size_t index = std::hash<int>{}(fd) % loops_.size();
            loops_[index]->postTask([this, index, fd] {
                loops_[index]->doClose(fd);
            });
        } else {
            LOG(ERR) << "EpollContext is only for linux socket";
        }
    }

    void run() {
        // 启动所有事件循环线程
        for (auto& loop : loops_) {
            loop->start();
        }
    }

private:
    std::vector<std::unique_ptr<EventLoopThread>> loops_;
};

EpollContext::EpollContext() : impl(std::make_unique<EpollContextImpl>()) {}

EpollContext::~EpollContext() = default;

void EpollContext::registerAsyncRead(const std::shared_ptr<ISocket>& socket, const ReadContextCallBack& callback) const {
    impl->registerRead(socket, callback);
}

void EpollContext::asyncWriteOnce(const std::shared_ptr<ISocket>& socket, const WriteContextCallBack& callback,
                                  const std::shared_ptr<std::string>& buf) const {
    impl->asyncWriteOnce(socket, callback, buf);
}

void EpollContext::close(const std::shared_ptr<ISocket>& socket) const {
    impl->close(socket);
}

void EpollContext::run() const {
    impl->run();
}