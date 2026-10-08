//
// Created by computer on 2026/8/22.
//

#include "EpollContext.h"

#include <mutex>
#include <queue>
#include <atomic>
#include <cassert>
#include <thread>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <random>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/uio.h>

#include "logger/Logger.h"
#include "thread_pool/queue/LockFreeQueue.h"
#include "socket/LinuxSocket.h"

class EventLoopThread;

struct Event {
    enum class Kind : uint8_t { Connection, Accept, Timer, Wakeup } kind;
    int fd;
protected:
    explicit Event(Kind k, int fd) : kind(k), fd(fd) {}
};

// 写元信息
struct WriteMetaInfo {
    std::string buffer;
    size_t offset = 0;
    WriteContextCallBack callback;
    WriteMetaInfo(WriteContextCallBack&& cb, std::string&& buf) : buffer(std::move(buf)), callback(std::move(cb)) {}
};

// ------------------------- 内部结构定义 -------------------------

// 连接对象，封装一个 fd 的所有状态
struct Connection : public Event, std::enable_shared_from_this<Connection> {
    std::size_t id;
    std::shared_ptr<ISocket> socket;          // 原始 socket
    std::shared_ptr<IEpollContextScope> scope_;
    EventLoopThread* loop_ = nullptr;
    ReadContextCallBack read_cb;              // 读回调
    SimpleBuffer read_buffer;                 // 读缓冲区
    // std::queue<WriteMetaInfo> write_queue;    // 写队列
    std::deque<WriteMetaInfo> write_queue;
    int in_queue_write_buffer_byte_size = 0;
    HighLevelCallback high_level_callback;
    LowLevelCallback low_level_callback;
    ClosingCallback closing_callback;
    bool high_level_callback_set = false;
    bool read_registered = false;             // 是否已注册 EPOLLIN
    bool write_registered = false;            // 是否已注册 EPOLLOUT
    bool peer_closed_ = false;                // 对端是否已关闭写端（收到 FIN）
    bool closing = false;

    Connection(
        std::size_t id,
        std::shared_ptr<ISocket> socket,
        std::shared_ptr<IEpollContextScope> scope)
        : Event(Event::Kind::Connection, socket->getNative()), id(id), socket(std::move(socket)), scope_(std::move(scope)) {
        read_buffer.resize(DEFAULT_BUFFER_SIZE);
    }

    Connection(Connection&&)  noexcept = default;
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    ~Connection() = default;

    static std::atomic<std::size_t> connection_id;
    constexpr static int DEFAULT_BUFFER_SIZE = 4096;
};

std::atomic<std::size_t> Connection::connection_id = 0;

struct AcceptContext : public Event {
    std::shared_ptr<ISocket> socket;
    AcceptContextCallback accept_cb;

    AcceptContext(std::shared_ptr<ISocket> s, AcceptContextCallback cb) :
        Event(Event::Kind::Accept, s->getNative()), socket(std::move(s)), accept_cb(std::move(cb)) {
    }
};

struct TimerContext : public Event {
    std::function<void()> callback;
    TimerContext(std::function<void()> func, int fd) : Event(Event::Kind::Timer, fd), callback(std::move(func)) {}
};

struct AwakeContext : public Event {
    AwakeContext() : Event(Event::Kind::Wakeup, -1) {}
};

// 单个事件循环线程的实现
class EventLoopThread {
public:
    EventLoopThread() : epoll_fd_(-1), wakeup_fd_(-1), running_(false) { // 注意这里 构造函数里面不要使用 epoll_context 否则出现未定义行为
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
        ev.data.ptr = &awake_context_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &ev);
    }

    ~EventLoopThread() {
        stop();
        // stop() 在事件循环线程内部调用时刻意不 join（见下），这里兜底回收：
        // 否则 std::thread 析构时仍 joinable 会直接 std::terminate
        if (thread_.joinable()) {
            if (inLoopThread()) {
                thread_.detach(); // 析构发生在事件循环线程里，不能 join 自己
            } else {
                thread_.join();
            }
        }
        if (epoll_fd_ != -1) ::close(epoll_fd_);
        if (wakeup_fd_ != -1) ::close(wakeup_fd_);
    }

    // 启动事件循环
    void start() {
        if (!running_) {
            running_ = true;
            thread_ = std::thread([this] {
                loop_id_ = std::this_thread::get_id(); // 由本线程自己写入，避免与 start() 竞争
                run();
            });
        }
    }

    // 停止事件循环
    void stop() {
        if(running_.exchange(false)) {
            // 唤醒 epoll_wait
            uint64_t one = 1;
            ::write(wakeup_fd_, &one, sizeof(one));
        }

        // 在任一事件循环线程里都不做 join：
        //  1) 事件循环线程里调用 close()（例如 RedisConnection::onDestroy -> client->close()）
        //     会 join 当前线程自己 -> std::system_error: Resource deadlock avoided;
        //  2) 两个事件循环互相 stop 时也会互相 join 而死锁。
        // 线程回收交给外部线程：之后的 stop()（running_ 已为 false 也会走到这里）或析构函数。
        if (thread_.joinable() && !inLoopThread()) {
            thread_.join();
        }
    }

    void postTask(std::function<void()>&& task) { // 这里实际上只会接收右值
        // postTask里面再PostTask走本地队列
        if (std::this_thread::get_id() == loop_id_) {
            local_tasks_.emplace_back(std::move(task));
            return;
        }

        while (!task_queue_.push(std::move(task))) { // push 可能返回 False 需要不断尝试
            std::this_thread::yield();               // 让出 CPU：和消费者对转纯属浪费
        }

        if (!awake.exchange(true, std::memory_order_release)) { // 如果 exchange 返回 false, 之前就是 false
            uint64_t one = 1;
            ::write(wakeup_fd_, &one, sizeof(one));
        }
    }

    void postTaskBatch(std::vector<std::function<void()>>&& task_batch) {
        const bool self = (std::this_thread::get_id() == loop_id_);

        for (auto& t : task_batch) {
            if (self) { // 同 postTask：自投递不能走有界队列
                local_tasks_.emplace_back(std::move(t));
                continue;
            }
            while (!task_queue_.push(std::move(t))) { // push 可能返回 False 需要不断尝试
                std::this_thread::yield();
            }
        }

        if (!awake.exchange(true, std::memory_order_release)) { // 如果 exchange 返回 false, 之前就是 false
            uint64_t one = 1;
            ::write(wakeup_fd_, &one, sizeof(one));
        }
    }

    ISocket::SocketHandler addTimer(std::chrono::milliseconds ms, const std::function<void()>& cb) {
        int timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
        if (timerfd < 0) {
            LOG(ERR) << "EpollContext::addTimer: timerfd_create failed";
            return ISocket::ERROR_SOCKET;
        }

        ::itimerspec spec{};

        auto sec = std::chrono::duration_cast<std::chrono::seconds>(ms);
        auto ns = std::chrono::nanoseconds(ms - sec);

        spec.it_value.tv_sec = sec.count();
        spec.it_value.tv_nsec = ns.count();

        if (timerfd_settime(timerfd, 0, &spec, nullptr) < 0) {
            ::close(timerfd);
            LOG(ERR) << "EpollContext::addTimer: timerfd_settime failed";
            return ISocket::ERROR_SOCKET;;
        }
        auto context = std::make_shared<TimerContext>(cb, timerfd);
        const auto ptr = context.get();
        timer_fds_.emplace(timerfd, std::move(context));

        ::epoll_event ev{};
        ev.data.ptr = ptr;
        ev.events = EPOLLIN | EPOLLONESHOT;

        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timerfd, &ev) < 0) {
            ::close(timerfd);
            timer_fds_.erase(timerfd);
            LOG(ERR) << "EpollContext::addTimer: epoll_ctl failed";

            return ISocket::ERROR_SOCKET;
        }

        return timerfd;
    }


private:
    void run() {
        // 标记当前线程正处于事件循环中：stop() 依赖它判断"能不能 join"
        struct InLoopScope {
            InLoopScope()  { in_loop_thread_ = true; }
            ~InLoopScope() { in_loop_thread_ = false; }
        } in_loop_scope;

        epoll_event events[MAX_EVENTS];
        while (running_) {
            drainLocalTasks();
            if (!running_) break;

            int n = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, -1);
            if (n == -1) {
                if (errno == EINTR) continue;
                LOG(ERR) << "EventLoopThread: epoll_wait error";
                break;
            }
            for (int i = 0; i < n; ++i) {
                auto* event = static_cast<Event*>(events[i].data.ptr);
                switch (event->kind) {
                    case Event::Kind::Wakeup:
                        uint64_t val;
                        while (::read(wakeup_fd_, &val, sizeof(val)) > 0) {}
                        drainTasks();
                        break;
                    case Event::Kind::Accept:
                        handleAccept(static_cast<AcceptContext*>(event));
                        break;
                    case Event::Kind::Timer:
                        handleTimer(static_cast<TimerContext*>(event));
                        break;
                    case Event::Kind::Connection: {
                        auto* conn = static_cast<Connection*>(event);
                        if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                            closeConnection(conn); break;
                        }
                        if (events[i].events & EPOLLIN)
                            handleRead(conn);
                        if (events[i].events & EPOLLOUT)
                            handleWrite(conn);
                        break;
                    }
                }
            }
        }

        // 关闭的时候清空任务（先本地自投递任务，再全局队列）
        drainLocalTasks();
        std::function<void()> task;
        while (!task_queue_.empty()) {
            if (task_queue_.pop(task)) {
                task();
            }
        }

        LOG(DEBUG) << "EventLoopThread: closed";
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

        for (;;) {
            if (!local_tasks_.empty()) {
                task = std::move(local_tasks_.front());
                local_tasks_.pop_front();
            } else if (!task_queue_.pop(task)) { // 如果 pop 失败，下次在处理，这里不能保证 queue 是空
                break;
            }
            task();
        }

        awake.store(false, std::memory_order_release);
        // 如果 post task 提交了一个任务，在 store false 前，where 循环后，这个任务可能被丢失，所以这里再检查一次
        if (!task_queue_.empty() || !local_tasks_.empty()) {
            if (!awake.exchange(true, std::memory_order_release)) {
                uint64_t one = 1;
                ::write(wakeup_fd_, &one, sizeof(one));
            }
        }
    }

    void drainLocalTasks() {
        while (!local_tasks_.empty()) {
            auto task = std::move(local_tasks_.front());
            local_tasks_.pop_front();
            task();
        }
    }

    void handleAccept(AcceptContext* context) const {
        auto client = context->socket->accept();
        if (client == nullptr) {
            LOG(ERR) << "EpollContext::handleAccept: accept failed";
            return;
        }
        if (context->accept_cb) {
            context->accept_cb(client);
        }

        LOG(DEBUG) << "Accept a new socket";
    }

    void handleTimer(TimerContext* context) {
        if (context->callback) {
            context->callback();
        }
        close(context->fd);
        timer_fds_.erase(context->fd); // timer_fd 设置了 oneshot epoll 会自动移除
    }

    void handleRead(Connection* conn) {

        int fd = conn->socket->getNative();

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
                    modifyEpollEvents(conn, EPOLLOUT);
                } else {
                    // 之前可能同时监听了读写，改为只监听写
                    modifyEpollEvents(conn, EPOLLOUT);
                }
            } else {
                // 无待写数据，移除所有事件监听，但保留连接对象
                conn->write_registered = false;
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            }
            // 通知上层应用是否处理对端关闭
            // 因为上层可能直接关闭连接所以最后调用
            if (conn->read_cb) {
                conn->read_cb(conn->read_buffer.buffer, 0);
            }
        } else { // ret < 0
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 暂时无数据，忽略，等待下次 epoll 触发
                return;
            }
            LOG(ERR) << "EventLoopThread: read error on fd " << fd << " error code " << errno;
            closeConnection(conn);
        }
    }

    void handleWrite(Connection* conn) {

        assert(conn != nullptr); // 如果模型正确不可能到这里

        int fd = conn->socket->getNative();
        int write_count = std::min(static_cast<int>(conn->write_queue.size()), IOV_MAX);

        std::vector<iovec> iovs;
        iovs.reserve(write_count);

        for (int i = 0; i < write_count; ++i) {
            ::iovec iov{};
            auto& data = conn->write_queue[i];
            iov.iov_base = data.buffer.data() + data.offset;
            iov.iov_len = data.buffer.size() - data.offset;
            if (iov.iov_len > 0) {
                iovs.emplace_back(iov);
            }
        }

        if (iovs.empty()) {
            return;
        }

        ssize_t n = ::writev(fd, iovs.data(), static_cast<int>(iovs.size()));
        if (n <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 写缓冲区满，等待下次 EPOLLOUT，停止本次处理
                LOG(DEBUG) << "EventLoopThread: write buffer full";
                return;
            }
            LOG(ERR) << "EventLoopThread: write error, error code " << errno;
            failAllPendingWrites(conn);
            closeConnection(conn);
            return;
        }

        auto remaining = static_cast<size_t>(n);

        conn->in_queue_write_buffer_byte_size -= static_cast<int>(remaining);
        if (conn->in_queue_write_buffer_byte_size < 0) {
            LOG(WARNING) << "EventLoopThread: in_queue_write_buffer_byte_size " << conn->in_queue_write_buffer_byte_size;
            conn->in_queue_write_buffer_byte_size = 0;
        }

        while (!conn->write_queue.empty() && remaining > 0) {
            auto& meta = conn->write_queue.front();
            size_t unSent = meta.buffer.size() - meta.offset;
            if (remaining >= unSent) {
                remaining -= unSent;
                if (meta.callback) meta.callback(true);
                conn->write_queue.pop_front();
            } else {
                meta.offset += remaining;
                remaining = 0;
            }
        }

        if (conn->write_queue.empty()) {
            // 写队列已空，更新事件监听状态
            conn->write_registered = false;
            if (conn->peer_closed_) {
                // 对端已半关闭，且数据已发送完毕，移除所有事件，等待外部关闭
                LOG(DEBUG) << "EventLoopThread: peer half-closed and all data sent, remove fd from epoll, fd " << fd;
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            } else {
                // 对端未关闭，根据读注册状态调整事件
                if (conn->read_registered) {
                    modifyEpollEvents(conn, EPOLLIN);
                } else {
                    // 无读事件，移除所有事件（保持连接，等待外部后续操作）
                    LOG(DEBUG) << "EventLoopThread: write queue empty and no read registered, remove fd from epoll, fd " << fd;
                    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                }
            }
        }

        // 低水位回调
        if (conn->high_level_callback_set && conn->in_queue_write_buffer_byte_size < ISocket::DEFAULT_LOW_LEVEL_SIZE) {
            if (conn->low_level_callback) {
                conn->high_level_callback_set = false;
                conn->low_level_callback(conn->socket);
            }
        }
    }

    void failAllPendingWrites(Connection* conn) {
        if (!conn->write_queue.empty()) {
            LOG(DEBUG) << "EventLoopThread: all writes failed";
            while (!conn->write_queue.empty()) {
                auto& meta = conn->write_queue.front();
                if (meta.callback) {
                    meta.callback(false);
                }
                conn->write_queue.pop_front();
            }
            conn->in_queue_write_buffer_byte_size = 0;
        }
    }

    void closeFd(int fd) {
        LOG(DEBUG) << "EventLoopThread: close fd " << fd;
        if (accept_socket_set_.contains(fd)) {
            accept_socket_set_.erase(fd);
            ::close(fd);
            return;
        }

        if (timer_fds_.contains(fd)) {
            timer_fds_.erase(fd);
            ::close(fd);
            return;
        }

        LOG(ERR) << "EventLoopThread: 如果模型正确，不可能走到这里" << fd;
        assert(false);
    }

    void closeConnection(Connection* connection) {
        std::shared_ptr<Connection> conn = connection->shared_from_this();
        postTask([conn, this]() {
            assert(conn != nullptr); // 如果使用正确 conn 不可能是 nullptr

            if (conn->closing) {
                return;
            }

            conn->closing = true;

            if (conn->closing_callback) {
                conn->closing_callback(conn->socket);
            }

            conn->scope_->onDestroy(); // 通知上层进行销毁


            int fd = conn->socket->getNative();
            LOG(DEBUG) << "EventLoopThread: closing connection " << fd;

            // 从 epoll 中删除
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);

            // 通知所有待写请求失败
            failAllPendingWrites(conn.get());

            // 从 map 中移除, 这里可能会触发socket 析构，或者 conn 里面还有数据没写完，所以放在最后
            connections_.erase(conn->id);
        });
    }

    void modifyEpollEvents(Connection* conn, uint32_t events) {
        epoll_event ev{};
        ev.events = events;
        ev.data.ptr = conn;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn->socket->getNative(), &ev);
    }

public:
    void doRegisterAccept(int fd, const std::shared_ptr<ISocket>& socket, const AcceptContextCallback& callback) {
        if (accept_socket_set_.contains(fd)) {
            LOG(WARNING) << "EventLoopThread: already register accept, fd " << fd;
            accept_socket_set_[fd]->accept_cb = callback;
            return;
        }

        auto context = std::make_shared<AcceptContext>(socket, callback);

        accept_socket_set_[fd] = context;
        socket->setNoBlock();

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.ptr = context.get();
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
    }

    void doRegisterRead(const std::shared_ptr<Connection>& conn, const ReadContextCallBack& callback) {

        if (conn->closing) {
            LOG(WARNING) << "EventLoopThread: connection is closing, register read failed";
            return;
        }

        if (conn->read_registered) {
            LOG(WARNING) << "EventLoopThread: already register read, fd " << conn->socket->getNative();
            conn->read_cb = callback;
            return;
        }

        if (conn->peer_closed_) { // 对端已经关闭写
            LOG(DEBUG) << "EventLoopThread: ignore read registration because peer already closed, fd " << conn->socket->getNative();
            return;
        }

        conn->read_cb = callback;
        auto& socket = conn->socket;
        int fd = socket->getNative();



        conn->read_registered = true;
        // 注册过写函数
        if (conn->write_registered) {
            modifyEpollEvents(conn.get(), EPOLLIN | EPOLLOUT);
            return;
        }

        // 只注册读
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.ptr = conn.get();
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
    }

    void doAsyncWriteOnce(const std::shared_ptr<Connection>& conn ,
                          WriteContextCallBack&& callback, std::string&& buf) {


        if (conn->closing) {
            LOG(WARNING) << "EventLoopThread: connection is closing, write failed";
            if (callback) {
                callback(false);
            }
            return;
        }

        int size = static_cast<int>(buf.size());
        // conn->write_queue.emplace_back(socket, callback, buf);
        conn->write_queue.emplace_back(std::move(callback), std::move(buf));
        // 高水位背压
        conn->in_queue_write_buffer_byte_size += size;
        if (conn->in_queue_write_buffer_byte_size > ISocket::DEFAULT_HIGH_LEVEL_SIZE) {
            if (conn->high_level_callback && !conn->high_level_callback_set) {
                conn->high_level_callback_set = true;
                conn->high_level_callback(conn->socket);
            }
        }

        // 如果尚未注册写事件，则修改 epoll
        if (!conn->write_registered) {
            conn->write_registered = true;
            if (conn->read_registered) {
                modifyEpollEvents(conn.get(), EPOLLIN | EPOLLOUT);
            } else {
                // modifyEpollEvents(conn.get(), EPOLLOUT);
                // 没有注册过写和读需要添加 fd
                epoll_event ev{};
                ev.events = EPOLLOUT;
                ev.data.ptr = conn.get();
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, conn->socket->getNative(), &ev);
            }
        }
    }

    void doAsyncWriteBatch(const std::shared_ptr<Connection>& conn,
                       BatchWriteContextCallback&& callbacks,
                       std::vector<std::string>&& bufs) {
        if (conn->closing) {
            LOG(WARNING) << "EventLoopThread: connection is closing, write failed";
            if (callbacks) {
                callbacks(false, -1);
            }
            return;
        }

        for (int i = 0; i < bufs.size(); ++i) {
            conn->in_queue_write_buffer_byte_size += static_cast<int>(bufs[i].size());
            conn->write_queue.emplace_back([i, callbacks](bool r) { callbacks(r, i); }, std::move(bufs[i]));
        }

        if (conn->in_queue_write_buffer_byte_size > ISocket::DEFAULT_HIGH_LEVEL_SIZE) {
            if (conn->high_level_callback && !conn->high_level_callback_set) {
                conn->high_level_callback_set = true;
                conn->high_level_callback(conn->socket);
            }
        }

        if (!conn->write_registered) {
            conn->write_registered = true;
            if (conn->read_registered) {
                modifyEpollEvents(conn.get(), EPOLLIN | EPOLLOUT);
            } else {
                epoll_event ev{};
                ev.events = EPOLLOUT;
                ev.data.ptr = conn.get();
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, conn->socket->getNative(), &ev);
            }
        }
    }

    void doRegisterScope(const std::shared_ptr<ISocket>& socket, const std::shared_ptr<IEpollContextScope>& scope) {
        LOG(DEBUG) << "EpollLoopThread: RegisterScope " << socket->getNative();
        std::size_t id = Connection::connection_id.fetch_add(1);
        auto connection = std::make_shared<Connection>(id, socket, scope);
        connection->loop_ = this;
        socket->setConnection(connection);
        socket->setNoBlock();
        connections_.emplace(id, connection);

        scope->postTask = [this](std::function<void()> callback) {
            this->postTask(std::move(callback));
        };

        scope->onRegister(socket);
    }

    void join() {
        thread_.join();
    }

private:
    int epoll_fd_;
    int wakeup_fd_;
    std::atomic<bool> running_{false};
    alignas(std::hardware_constructive_interference_size)
    std::atomic<bool> awake{false};
    std::thread thread_;
    std::thread::id loop_id_{};                      // 本事件循环线程的 id（由该线程自己写入）
    std::deque<std::function<void()>> local_tasks_;  // 自投递任务：无界，仅本事件循环线程访问
    static thread_local bool in_loop_thread_;        // 当前线程是否正运行在某个事件循环里

    static bool inLoopThread() { return in_loop_thread_; }

    std::unordered_map<std::size_t, std::shared_ptr<Connection>> connections_; // 保留 connection 防止被销毁
    std::unordered_map<int, std::shared_ptr<AcceptContext>> accept_socket_set_;
    std::unordered_map<int, std::shared_ptr<TimerContext>> timer_fds_;
    // std::queue<std::function<void()>> task_queue_;                     // 待处理任务队列
    // std::mutex task_mutex_;                                            // 保护任务队列
    MPSCQueue<std::function<void()>, 512> task_queue_;

    AwakeContext awake_context_;

    constexpr static int MAX_EVENTS = 2048;
    friend EpollContextImpl;
};

thread_local bool EventLoopThread::in_loop_thread_ = false;

// ------------------------- EpollContext::Impl -------------------------
struct EpollContextImpl {

    static constexpr int LOOP_SIZE = 4;
    std::vector<std::unique_ptr<EventLoopThread>> loops_;

    static bool checkSocket(const std::shared_ptr<ISocket>& socket, int& fd) {
        if (socket == nullptr) {
            LOG(ERR) << "EpollContext: socket is nullptr";
            return false;
        }

        fd = socket->getNative();
        if (fd == ISocket::ERROR_SOCKET) {
            LOG(ERR) << "EpollContext: invalid socket fd";
            return false;
        }
        return true;
    }

    std::shared_ptr<Connection> checkSocketWithConnection(const std::shared_ptr<ISocket>& socket) {
        if (socket == nullptr) {
            LOG(ERR) << "EpollContext: socket is nullptr";
            return nullptr;
        }

        if (socket->getNative() == ISocket::ERROR_SOCKET) {
            LOG(ERR) << "EpollContext: invalid socket fd";
            return nullptr;
        }

        auto conn = socket->getConnection();
        if (conn == nullptr) {
            LOG(ERR) << "EpollContext: connection is nullptr";
            return nullptr;
        }

        if (conn->closing) {
            LOG(ERR) << "EpollContext: connection is closing";
            return nullptr;
        }

        return conn;
    };

public:
    EpollContextImpl() {
        loops_.reserve(LOOP_SIZE);
        for (int i = 0; i < LOOP_SIZE; ++i) {
            loops_.emplace_back(std::make_unique<EventLoopThread>());
        }
    }

    ~EpollContextImpl() {
        // 停止所有事件循环
        for (auto& loop : loops_) {
            loop->stop();
        }
    }

    void registerAccept(const std::shared_ptr<ISocket>& socket, const AcceptContextCallback& callback) {
        if (int fd = ISocket::ERROR_SOCKET; checkSocket(socket, fd)) {
            // 根据 fd 哈希选择事件循环线程
            std::size_t index = std::hash<int>{}(fd) % loops_.size();
            // 提交任务到对应线程
            loops_[index]->postTask([this, index, fd, socket, callback] {
                loops_[index]->doRegisterAccept(fd, socket, callback);
            });
        } else {
            LOG(ERR) << "registerAccept failed";
        }
    }

    void registerRead(const std::shared_ptr<ISocket>& socket, const ReadContextCallBack& callback) {
        if (auto conn = checkSocketWithConnection(socket)) {
            conn->loop_->postTask([callback, conn] {
                conn->loop_->doRegisterRead(conn, callback);
            });
        } else {
            LOG(ERR) << "registerRead failed";
        }
    }

    void asyncWriteOnce(const std::shared_ptr<ISocket>& socket, const WriteContextCallBack& callback,
                        std::string&& buf) {
        asyncWriteOnceImpl(socket, callback, buf);
    }

    void asyncWriteOnce(const std::shared_ptr<ISocket>& socket, const WriteContextCallBack& callback,
                        std::string& buf) {
        // if (int fd = -1; checkSocket(socket, fd)) {
        //     size_t index = std::hash<int>{}(fd) % loops_.size();
        //     loops_[index]->postTask([this, index, fd, socket, callback, buf] {
        //         loops_[index]->doAsyncWriteOnce(fd, socket, callback, buf);
        //     });
        // } else {
        //     LOG(ERR) << "async write failed";
        //     if (callback) callback(false);
        // }

        // 使用拷贝，后面尽可能的 move, 这里的buf 在最上层已经拷贝了一次，可以安全移动
        asyncWriteOnceImpl(socket, callback, buf);
    }

    void asyncWriteOnceImpl(const std::shared_ptr<ISocket>& socket, WriteContextCallBack callback,
                        std::string& buf) {

        struct WriteContext {
            std::shared_ptr<Connection> conn;
            WriteContextCallBack callback;
            std::string buf;

            WriteContext(std::shared_ptr<Connection>&& conn,
                WriteContextCallBack&& callback, std::string&& buf)
                : conn(std::move(conn)), callback(std::move(callback)), buf(std::move(buf)) {}
        };

        if (auto conn = checkSocketWithConnection(socket)) {

            auto loops = conn->loop_;
            // 使用 WriteContext 优化 std::function 的堆分配，但是会多一次 shared_ptr, 后面可以改成 unique_ptr
            std::shared_ptr<WriteContext> context = std::make_shared<WriteContext>(
                std::move(conn), std::move(callback), std::move(buf)
            );

            loops->postTask([context = std::move(context)] {
                context->conn->loop_->doAsyncWriteOnce(context->conn, std::move(context->callback), std::move(context->buf));
            });

        } else {
            LOG(ERR) << "async write failed";
            if (callback) callback(false);
        }
    }

    void asyncWriteBatch(const std::shared_ptr<ISocket>& socket,
            const BatchWriteContextCallback& callback, const std::vector<std::string>& buf) {
        asyncWriteBatchImpl(socket, callback, buf);
    }

    void asyncWriteBatch(const std::shared_ptr<ISocket>& socket,
        const BatchWriteContextCallback& callback, std::vector<std::string>&& buf) {
        asyncWriteBatchImpl(socket, callback, std::move(buf));
    }

    void asyncWriteBatchImpl(const std::shared_ptr<ISocket>& socket,
                         BatchWriteContextCallback callback,
                         std::vector<std::string> bufs) {
        struct WriteContext {
            std::shared_ptr<Connection> conn;
            BatchWriteContextCallback callback;
            std::vector<std::string> bufs;

            WriteContext(std::shared_ptr<Connection>&& conn,
                         BatchWriteContextCallback&& cbs,
                         std::vector<std::string>&& b)
                : conn(std::move(conn)), callback(std::move(cbs)), bufs(std::move(b)) {}
        };

        if (auto conn = checkSocketWithConnection(socket)) {
            auto loops = conn->loop_;

            auto context = std::make_shared<WriteContext>(
                std::move(conn), std::move(callback), std::move(bufs));

            loops->postTask([context = std::move(context)] {
                context->conn->loop_->doAsyncWriteBatch(
                    context->conn,
                    std::move(context->callback),
                    std::move(context->bufs));
            });
        } else {
            LOG(ERR) << "asyncWriteBatch failed";
            if (callback) callback(false, -1); // -1 表示全部失败
        }
    }

    void removeSocket(const std::shared_ptr<ISocket>& socket) {
        if (auto conn = checkSocketWithConnection(socket)) {
            conn->loop_->closeConnection(conn.get());
        } else {
            LOG(ERR) << "removeSocket failed";
        }
    }

    void run(bool block) {
        // 启动所有事件循环线程
        for (auto& loop : loops_) {
            loop->start();
        }

        if (!block)
            return;

        for (auto& loop : loops_) {
            loop->join();
        }
    }

    void close() {
        for (auto& loop : loops_) {
            loop->stop();
        }
    }

    void registerHighLevel(const std::shared_ptr<ISocket>& socket, const HighLevelCallback& callback) {
        if (auto conn = checkSocketWithConnection(socket)) {
            conn->loop_->postTask([conn, callback] {
                conn->high_level_callback = callback;
            });
        } else {
            LOG(ERR) << "registerHighLevel failed";
        }
    }

    void registerLowLevel(const std::shared_ptr<ISocket>& socket, const LowLevelCallback& callback) {
        if (auto conn = checkSocketWithConnection(socket)) {
            conn->loop_->postTask([conn, callback] {
                conn->low_level_callback = callback;
            });
        } else {
            LOG(ERR) << "registerLowLevel failed";
        }
    }

    void postTask(const std::shared_ptr<ISocket>& socket, std::function<void()> function) {
        if (int fd = ISocket::ERROR_SOCKET; checkSocket(socket, fd)) {
            std::size_t index = std::hash<int>{}(fd) % loops_.size();
            loops_[index]->postTask(std::move(function));
        } else {
            LOG(ERR) << "postTask failed";
        }
    }

    ISocket::SocketHandler addTimer(const std::shared_ptr<ISocket>& socket, std::chrono::milliseconds duration, const std::function<void()>& callback) {
        if (int fd = ISocket::ERROR_SOCKET; checkSocket(socket, fd)) {
            return addTimer(fd, duration, callback);
        } else {
            LOG(ERR) << "addTimer failed";
        }

        return ISocket::ERROR_SOCKET;
    }

    ISocket::SocketHandler addTimer(std::chrono::milliseconds duration,
                                              const std::function<void()>& callback) {
        static std::atomic<int> next_id{0};
        return addTimer(++next_id, duration, callback);
    }

    ISocket::SocketHandler addTimer(int fd, std::chrono::milliseconds duration, const std::function<void()>& callback) {
        std::size_t index = std::hash<int>{}(fd) % loops_.size();
        return loops_[index]->addTimer(duration, callback);
    }

    void registerScope(const std::shared_ptr<ISocket>& socket, const std::shared_ptr<IEpollContextScope>& scope) {
        if (int fd = ISocket::ERROR_SOCKET; checkSocket(socket, fd)) {
            std::size_t index = std::hash<int>{}(fd) % loops_.size();
            loops_[index]->postTask([this, index, socket, scope] {
                loops_[index]->doRegisterScope(socket, scope);
            });
        }
    }
};

EpollContext::EpollContext() : impl(std::make_unique<EpollContextImpl>()) {}

EpollContext::~EpollContext() = default;

void EpollContext::registerScope(
    const std::shared_ptr<ISocket>& socket,
    const std::shared_ptr<IEpollContextScope>& scope
) const {
    impl->registerScope(socket, scope);
}

void EpollContext::registerAsyncAccept(const std::shared_ptr<ISocket>& socket, const AcceptContextCallback& callback) const {
    impl->registerAccept(socket, callback);
}

void EpollContext::registerAsyncRead(const std::shared_ptr<ISocket>& socket, const ReadContextCallBack& callback) const {
    impl->registerRead(socket, callback);
}

void EpollContext::asyncWriteOnce(const std::shared_ptr<ISocket>& socket, const WriteContextCallBack& callback,
                                  std::string& buf) const {
    impl->asyncWriteOnce(socket, callback, buf);
}

void EpollContext::asyncWriteOnce(const std::shared_ptr<ISocket>& socket, const WriteContextCallBack& callback,
                                  std::string&& buf) const {
    impl->asyncWriteOnce(socket, callback, std::move(buf));
}

void EpollContext::asyncWriteBatch(const std::shared_ptr<ISocket>& socket, const BatchWriteContextCallback& callback,
                                  const std::vector<std::string>& buf) const {
    impl->asyncWriteBatch(socket, callback, buf);
}

void EpollContext::asyncWriteBatch(const std::shared_ptr<ISocket>& socket, const BatchWriteContextCallback& callback,
                                  std::vector<std::string>&& buf) const {
    impl->asyncWriteBatch(socket, callback, std::move(buf));
}

void EpollContext::removeSocket(const std::shared_ptr<ISocket>& socket) const {
    impl->removeSocket(socket);
}

void EpollContext::registerHighLevelCallback(
    const std::shared_ptr<ISocket>& socket,
    const HighLevelCallback& callback
) const {
    impl->registerHighLevel(socket, callback);
}

void EpollContext::registerLowLevelCallback(
    const std::shared_ptr<ISocket>& socket,
    const LowLevelCallback& callback
) const {
    impl->registerLowLevel(socket, callback);
}

void EpollContext::postTask(const std::shared_ptr<ISocket>& socket, const std::function<void()>& function) const {
    impl->postTask(socket, function);
}

ISocket::SocketHandler EpollContext::addTimer(
    const std::shared_ptr<ISocket>&  socket,
    const std::chrono::milliseconds& milliseconds,
    const std::function<void()>&     callback
) const {
    return impl->addTimer(socket, milliseconds, callback);
}

ISocket::SocketHandler EpollContext::addTimer(
    const std::chrono::milliseconds& milliseconds,
    const std::function<void()>&     callback
) {
    return impl->addTimer(milliseconds, callback);
}

void EpollContext::run(bool block) const {
    impl->run(block);
}

void EpollContext::close() const {
    impl->close();
}