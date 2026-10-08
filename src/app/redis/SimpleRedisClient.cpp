//
// Created by computer on 2026/8/23.
//

#include "SimpleRedisClient.h"

#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "thread_pool/lock/SpinLock.h"

#include <queue>
#include <utility>

struct SimpleRedisClient::ClientImpl {

    struct RedisConnection : IEpollContextScope {
        std::queue<std::shared_ptr<std::promise<RESPValue>>> promises;
        std::deque<std::shared_ptr<std::promise<RESPValue>>> writing_promises;
        std::vector<std::string> writing_cmds;

        SimpleRedisClient::ClientImpl* client = nullptr;

        bool flush_time = false;
        bool closing = false;
        std::atomic<bool> registered = false;

        ISocket* socket = nullptr; // socket 在 destroy 前一定存在

        // SpinLock lock; // 自旋锁比 mutex 好一点，但是不多
        // std::mutex lock;

        std::atomic<bool> highLevel = {false };
        RedisConnection(ClientImpl* impl, ISocket* socket) : client(impl), socket(socket) {};

        RedisConnection(RedisConnection&& r) noexcept : promises(std::move(r.promises)),
            writing_promises(std::move(r.writing_promises)), writing_cmds(std::move(r.writing_cmds)) {
            highLevel = r.highLevel.load();
            client = r.client;
            r.client = nullptr;
        };

        void onDestroy() override {
            closing = true;
            socket = nullptr;

            if (client != nullptr) {
                client->onConnectionClosed();
            }
        }

        void onRegister(const std::shared_ptr<ISocket>& socket) override {
            socket->asyncRead([this](const std::string& buf, std::size_t sz) {
                return client->read(*this, buf, sz);
            });

            registered = true;

            // socket->registerHighLevelCallback([this](const std::shared_ptr<ISocket>& _) {
            //     LOG(WARNING) << "SimpleRedisClient: HighLevelCallback is call, stop for write";
            //     highLevel = true;
            // });
            //
            // socket->registerLowLevelCallback([this](const std::shared_ptr<ISocket>& _) {
            //     highLevel = false;
            // });
        }
    };

    std::shared_ptr<EpollContext> epollContext;
    std::vector<std::shared_ptr<RedisConnection>> clients;
    std::atomic<bool> connection_closed_{false}; // 连接失效标记（可能由事件循环线程写）
    static thread_local std::size_t counter;

    void onConnectionClosed() {
        connection_closed_.store(true, std::memory_order_release);
    }

    constexpr static size_t WRITE_BATCH_SIZE = 1000;

    ClientImpl() {
        epollContext = std::make_shared<EpollContext>();
    }

    ~ ClientImpl() {
        close();
    }

    bool connect(const std::string& host, const unsigned short& port) {
        bool res = true;
        epollContext->run();

        for (int i = 0; i < DEFAULT_CLIENT_COUNT; i++) {
            auto socket = SocketManager::getInstance().getSocket(epollContext);
            res &= socket->connect(host.data(), port);
            auto connection = std::make_shared<RedisConnection>(this, socket.get());
            socket->registerScope(connection);
            clients.emplace_back(connection);
        }

        if (!res) {
            LOG(ERR) << "SimpleRedisClient::connect() failed";
            close();
            return false;
        }

        return true;
    }

    void close() {
        clients.clear();
        epollContext->close();
    }

    std::shared_ptr<std::promise<RESPValue>> execute1(std::string args) {
        std::size_t which_sock = std::hash<std::size_t>{}(++SimpleRedisClient::ClientImpl::counter) % clients.size();

        auto p = std::make_shared<std::promise<RESPValue>>();
        if (clients[which_sock]->highLevel) {
            p->set_exception(std::make_exception_ptr(std::runtime_error("to many bytes to write")));
            return p;
        }

        auto& conn = clients[which_sock];
        conn->postTask([conn, args = std::move(args), promise = p]() mutable {

            if (conn->closing) {
                promise->set_exception(std::make_exception_ptr(std::runtime_error("connection is closing")));
                return;
            }

            // 原本就是想省掉这个postTask, 但是又绕回去了，是只是整体更稳定一点
            conn->writing_cmds.emplace_back(std::move(args));
            conn->writing_promises.emplace_back(std::move(promise));

            auto batch_write_callback = [conn](bool success, int index) {
                auto promise = std::move(conn->writing_promises.front());
                conn->writing_promises.pop_front();
                if (!success) {
                    LOG(ERR) << "SimpleRedisClient::execute() failed";
                    promise->set_exception(std::make_exception_ptr(std::runtime_error("write error")));
                    return;
                }
                conn->promises.push(std::move(promise));
            };

            auto flush = [conn, batch_write_callback]() {
                if (!conn->writing_cmds.empty()) {
                    std::vector<std::string> cmds;
                    cmds.swap(conn->writing_cmds);
                    conn->socket->asyncWriteBatch(batch_write_callback, std::move(cmds));
                }
            };

            if (conn->writing_cmds.size() >= WRITE_BATCH_SIZE) {
                flush();
            } else if (!conn->flush_time) {
                ISocket::SocketHandler handler =
                    conn->socket->addTimer(std::chrono::milliseconds(10),[conn, flush]() {
                        flush();
                        conn->flush_time = false;
                });

                if (handler != ISocket::ERROR_SOCKET) {
                    conn->flush_time = true;
                    flush();
                }
            }
        });
        return p;
    }

    std::shared_ptr<std::promise<RESPValue>> execute(std::string cmd) { // C++ 17 保证传入右值不会拷贝

        auto p = std::make_shared<std::promise<RESPValue>>();

        if (clients.empty() || connection_closed_.load(std::memory_order_acquire)) {
            p->set_exception(std::make_exception_ptr(std::runtime_error("client is closed")));
            return p;
        }

        std::size_t which_sock = std::hash<std::size_t>{}(++SimpleRedisClient::ClientImpl::counter) % clients.size();
        if (clients[which_sock]->highLevel) {
            p->set_exception(std::make_exception_ptr(std::runtime_error("to many bytes to write")));
            return p;
        }

        auto& conn = clients[which_sock];
        while (!conn->registered) {}    // 没有注册前自旋等一下，很快，没必要阻塞

        conn->postTask([conn, p, cmd = std::move(cmd)]() mutable {
            if (conn->closing) {
                p->set_exception(std::make_exception_ptr(std::runtime_error("connection is closing")));
                return;
            }
            conn->socket->asyncWriteOnce([conn, p](bool success) {
               if (!success) {
                   LOG(ERR) << "SimpleRedisClient::execute() failed";
                   p->set_exception(std::make_exception_ptr(std::runtime_error("write error")));
               } else { // 写失败不入队
                   // 这里 一个socket会对应唯一的一个 epoll context, context 是单线程的，保证先写入的先调用回调
                   conn->promises.push(p);
               }
           },
           std::move(cmd));
        });
        return p;
    }

    std::size_t read(RedisConnection& connection, const std::string& buf, std::size_t size) {
        size_t pos = 0;
        while (pos < size) {
            size_t start_pos = pos;  // 记录当前响应的起始位置
            RESPValue result;
            try {
                result = RESP_Parser::parse(buf, pos);
            } catch (const IncompleteRESPException&) {
                pos = start_pos;
                break;
            } catch (const std::exception& e) {
                pos = start_pos + 1;
                LOG(ERR) << "Parse error: " << e.what() << ", skipping one byte";
                break;
            }

            std::shared_ptr<std::promise<RESPValue>> promise = nullptr;

            {
                // std::lock_guard lock(connection.lock);
                // read 只会被 epoll 线程调用，保证不会出现线程安全问题
                if (!connection.promises.empty()) {
                    promise = std::move(connection.promises.front());
                    connection.promises.pop();
                }
            }

            if (promise == nullptr) {
                LOG(ERR) << "SimpleRedisClient::read() get nullptr. impossible";
                return pos;
            }
            LOG(DEBUG) << "SimpleRedisClient::read() " << formatResponse(result);
            promise->set_value(result);
        }
        return pos;
    }
};

thread_local std::size_t SimpleRedisClient::ClientImpl::counter = 0;
SimpleRedisClient::~SimpleRedisClient() = default;
SimpleRedisClient::SimpleRedisClient() : impl(std::make_unique<ClientImpl>()){}

bool SimpleRedisClient::connect(const std::string& host, const unsigned short& port) {
    return impl->connect(host, port);
}

std::shared_ptr<std::promise<RESPValue>> SimpleRedisClient::execute(const std::vector<std::string>& args) {
    return impl->execute(buildRESPCommand(args));
}

std::shared_ptr<std::promise<RESPValue>> SimpleRedisClient::execute(const std::string& cmd) {
    std::istringstream iss(cmd);

    std::vector<std::string> args;

    std::string word;
    while (iss >> word) {
        args.push_back(std::move(word));
    }

    return execute(args);
}

void SimpleRedisClient::close() {
    impl->close();
}

