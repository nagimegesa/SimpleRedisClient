//
// Created by computer on 2026/8/23.
//

#include "SimpleRedisClient.h"

#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "lock/SpinLock.h"

#include <queue>


struct RedisConnection {
    std::shared_ptr<ISocket> socket;

    std::queue<std::shared_ptr<std::promise<RESPValue>>> promises;

    // SpinLock lock; // 自旋锁比 mutex 好一点，但是不多
    // std::mutex lock;

    RedisConnection() = default;
    RedisConnection(RedisConnection&&)  noexcept {}
};

struct SimpleRedisClient::ClientImpl {
    std::unique_ptr<EpollContext> epollContext;
    std::vector<RedisConnection> clients;
    static thread_local std::size_t counter;

    ClientImpl() {
        clients.resize(DEFAULT_CLIENT_COUNT);
        for (int i = 0; i < DEFAULT_CLIENT_COUNT; i++) {
            clients[i].socket = SocketManager::getInstance().getSocket();
        }

        epollContext = std::make_unique<EpollContext>();
    }


    ~ ClientImpl() = default;

    bool connect(const std::string& host, const unsigned short& port) {
        bool res = true;
        for (const auto& client : clients) {
            res &= client.socket->connect(host.c_str(), port);
        }

        if (!res) {
            LOG(ERR) << "SimpleRedisClient::connect() failed";
            close();
            return false;
        }

        for (auto& client : clients) {
            client.socket->asyncRead(*epollContext, [this, &client](const std::string& buf, std::size_t sz) {
                return this->read(client, buf, sz);
            });
        }
        epollContext->run();
        return true;
    }

    void close() {
        for (const auto& client : clients) {
            if (client.socket != nullptr) {
                epollContext->close(client.socket);
                client.socket->close();
            }
        }
    }

    std::shared_ptr<std::promise<RESPValue>> execute(std::string cmd) {
        int which_sock = std::hash<std::size_t>{}(++SimpleRedisClient::ClientImpl::counter) % clients.size();
        auto p = std::make_shared<std::promise<RESPValue>>();
        // {
        //     std::lock_guard _(clients[which_sock].lock);
        //     clients[which_sock].socket->asyncWriteOnce(*epollContext,
        //         [p](bool success) {
        //             if (!success) {
        //                 // TODO: 这里或许需要 把失败的 promises 删除。或者设置异常，但是没想好怎么写
        //                 // TODO: 但是其实大部分情况下不会出现写错误，这里暂时忽略
        //                 LOG(ERR) << "SimpleRedisClient::execute() failed";
        //                 return;
        //             }
        //         },
        //         std::make_shared<std::string>(std::move(cmd)));
        //     clients[which_sock].promises.push(p);
        // }

        clients[which_sock].socket->asyncWriteOnce(*epollContext,
            [p, this, which_sock](bool success) {
                if (!success) {
                    LOG(ERR) << "SimpleRedisClient::execute() failed";
                    p->set_exception(std::make_exception_ptr(std::runtime_error("write error")));
                }
                // 这里 一个socket会对应唯一的一个 epoll context, context 是单线程的，保证先写入的先调用回调
                clients[which_sock].promises.push(p);
            },
            std::make_shared<std::string>(std::move(cmd)));

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

