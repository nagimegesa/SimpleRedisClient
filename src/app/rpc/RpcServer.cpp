//
// Created by computer on 2026/9/15.
//
#include <string>

#include "RpcServer.h"

#include "thread_pool.h"
#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/SocketManager.h"

using RpcHandler = std::function<std::string(std::string)>;

/*
 * 0x0a 0x0b    // 2
 * type 1 字节  1 = REQUEST 2 = RESPONSE 3 = ERROR server端只会接收到 1 // 3
 * request_id   8 字节 大端 无符号，不能为 0 // 11
 * name_len 1 字节 // 12
 * function name for name_len // 12 + name_len
 * parma_len  4 字节 大端  // 16 + name_len
 * serialize parma for parma_len // 16 + name_len + parma_len
 * 0x0b 0x0c // 18 + name_len + parma_len
 */

enum ParserStatus {
    SOA, SOB,
    TYPE,
    REQUEST_ID,
    FUNC_LEN,
    FUNC_NAME,
    PARMA_LEN,
    PARMA_STRING,
    EOB, EOC,
};

enum ParserResult {
    COMPLETE,
    WAITING,
    ERROR,
};

enum ErrorCode {
    ERR_BAD_REQUEST = 1,
    ERR_BAD_INTERNAL = 3,

    ERR_UNKNOWN_FUNCTION = 100,
    ERR_UNKNOWN_PARAM = 101,
    ERR_UNKNOWN_RESPONSE = 102,
};

enum RequestType : char {
    REQUEST = 1,
    RESPONSE = 2,
    REQ_ERROR = 3,
};

struct RpcContext {
    ParserStatus status;
    ParserResult result;
    std::uint64_t request_id;
    std::string name;
    std::string parms;
    std::vector<char> buffer;

    RpcContext() : status(SOA), result(WAITING), request_id(0), name(), parms(), buffer() {}
};

namespace {

    inline void append_u64_be(std::string& out, std::uint64_t v) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.push_back(static_cast<char>((v >> shift) & 0xff));
        }
    }

    inline void append_u32_be(std::string& out, std::uint32_t v) {
        out.push_back(static_cast<char>((v >> 24) & 0xff));
        out.push_back(static_cast<char>((v >> 16) & 0xff));
        out.push_back(static_cast<char>((v >> 8)  & 0xff));
        out.push_back(static_cast<char>( v        & 0xff));
    }

}  // namespace

// 0x0a 0x0b | type(3) | request_id(8) | error_code(1) | param_len(4)=0 | 0x0b 0x0c
// 共 18 字节
std::string buildErrorResponse(std::uint64_t request_id, char error_code) {
    std::string res;
    res.reserve(18);

    res.push_back(0x0a);
    res.push_back(0x0b);
    res.push_back(static_cast<char>(RequestType::REQ_ERROR));  // 3
    append_u64_be(res, request_id);
    res.push_back(error_code);        // 复用 name_len 那 1 字节
    append_u32_be(res, 0);            // param_len = 0
    res.push_back(0x0b);
    res.push_back(0x0c);

    return res;
}

// 0x0a 0x0b | type(2) | request_id(8) | name_len(1)=0 | param_len(4) | body | 0x0b 0x0c
// 共 18 + body.size() 字节
std::string buildResponse(std::string&& body, std::uint64_t request_id) {
    const std::size_t body_len = body.size();

    std::string res;
    res.reserve(18 + body_len);

    res.push_back(0x0a);
    res.push_back(0x0b);
    res.push_back(static_cast<char>(RequestType::RESPONSE));   // 2
    append_u64_be(res, request_id);
    res.push_back(0x00);              // name_len = 0
    append_u32_be(res, static_cast<std::uint32_t>(body_len));
    res.append(body);
    res.push_back(0x0b);
    res.push_back(0x0c);

    return res;
}

void parse(
    const std::string& buffer,
    std::size_t start,
    std::size_t end,
    std::size_t& parsed,
    RpcContext& context) {

    parsed = 0;
    if (start >= end) {
        context.result = WAITING;
        return;
    }

    const char* data = buffer.data();
    std::size_t i = start;
    context.result = WAITING;

    auto u8 = [](char c) -> unsigned char {
        return static_cast<unsigned char>(c);
    };

    auto set_error = [&]() {
        context.result = ERROR;
        context.status = SOA;
        context.request_id = 0;
        context.name.clear();
        context.parms.clear();
        context.buffer.clear();
        parsed = i - start;
    };

    while (i < end) {
        switch (context.status) {
        case SOA: {
            // 查找起始字节 0x0a
            const void* p = std::memchr(data + i, 0x0a, end - i);
            if (p == nullptr) {
                i = end;
                parsed = i - start;
                context.result = WAITING;
                return;
            }
            i = static_cast<const char*>(p) - data + 1;
            context.status = SOB;
            break;
        }

        case SOB: {
            if (u8(data[i]) != 0x0b) {
                set_error();
                return;
            }
            ++i;

            context.request_id = 0;
            context.name.clear();
            context.parms.clear();
            context.buffer.clear();

            context.status = TYPE;
            break;
        }

        case TYPE: {
            if (i >= end) {
                parsed = i - start;
                context.result = WAITING;
                return;
            }

            const unsigned char type = u8(data[i++]);

            // server 端只接收 REQUEST
            if (type != REQUEST) {
                set_error();
                return;
            }

            context.buffer.clear();
            context.status = REQUEST_ID;
            break;
        }

        case REQUEST_ID: {
            constexpr std::size_t need = 8;

            // 如果有 8 个字节
            if (context.buffer.empty() && (end - i) >= need) {
                std::uint64_t id = 0;
                for (std::size_t k = 0; k < need; ++k) {
                    id = (id << 8) | u8(data[i + k]);
                }
                i += need;

                if (id == 0) {
                    set_error();
                    return;
                }

                context.request_id = id;
                context.status = FUNC_LEN;
                break;
            }

            // 跨包暂存
            while (context.buffer.size() < need && i < end) {
                context.buffer.push_back(data[i++]);
            }

            if (context.buffer.size() < need) {
                parsed = i - start;
                context.result = WAITING;
                return;
            }

            std::uint64_t id = 0;
            for (std::size_t k = 0; k < need; ++k) {
                id = (id << 8) | u8(context.buffer[k]);
            }

            if (id == 0) {  // id 不能为 0
                set_error();
                return;
            }

            context.request_id = id;
            context.buffer.clear();
            context.status = FUNC_LEN;
            break;
        }

        case FUNC_LEN: {
            if (i >= end) {
                parsed = i - start;
                context.result = WAITING;
                return;
            }

            const unsigned char name_len = u8(data[i++]);

            context.name.clear();
            context.name.reserve(name_len);

            context.buffer.clear();
            context.buffer.push_back(static_cast<char>(name_len));

            context.status = FUNC_NAME;
            break;
        }

        case FUNC_NAME: {
            if (context.buffer.empty()) {
                set_error();
                return;
            }

            const std::size_t name_len = u8(context.buffer[0]);

            if (context.name.size() > name_len) {
                set_error();
                return;
            }

            const std::size_t remain = name_len - context.name.size();
            if (remain > 0) {
                const std::size_t avail = end - i;
                const std::size_t take = remain < avail ? remain : avail;

                context.name.append(data + i, take);
                i += take;

                if (context.name.size() < name_len) {
                    parsed = i - start;
                    context.result = WAITING;
                    return;
                }
            }

            context.buffer.clear();
            context.status = PARMA_LEN;
            break;
        }

        case PARMA_LEN: {
            constexpr std::size_t need = 4;

            while (context.buffer.size() < need && i < end) {
                context.buffer.push_back(data[i++]);
            }

            if (context.buffer.size() < need) {
                parsed = i - start;
                context.result = WAITING;
                return;
            }

            context.parms.clear();
            context.status = PARMA_STRING;
            break;
        }

        case PARMA_STRING: {
            if (context.buffer.size() < 4) {
                set_error();
                return;
            }

            // 大端
            const std::uint32_t param_len =
                (static_cast<std::uint32_t>(u8(context.buffer[0])) << 24) |
                (static_cast<std::uint32_t>(u8(context.buffer[1])) << 16) |
                (static_cast<std::uint32_t>(u8(context.buffer[2])) << 8)  |
                (static_cast<std::uint32_t>(u8(context.buffer[3])));

            if (context.parms.size() > param_len) {
                set_error();
                return;
            }

            if (context.parms.capacity() < param_len) {
                context.parms.reserve(param_len);
            }

            const std::size_t remain = param_len - context.parms.size();
            if (remain > 0) {
                const std::size_t avail = end - i;
                const std::size_t take = remain < avail ? remain : avail;

                context.parms.append(data + i, take);
                i += take;

                if (context.parms.size() < param_len) {
                    parsed = i - start;
                    context.result = WAITING;
                    return;
                }
            }

            context.buffer.clear();
            context.status = EOB;
            break;
        }

        case EOB: {
            if (i >= end) {
                parsed = i - start;
                context.result = WAITING;
                return;
            }

            if (u8(data[i]) != 0x0b) {
                set_error();
                return;
            }

            ++i;
            context.status = EOC;
            break;
        }

        case EOC: {
            if (i >= end) {
                parsed = i - start;
                context.result = WAITING;
                return;
            }

            if (u8(data[i]) != 0x0c) {
                set_error();
                return;
            }

            ++i;
            context.status = SOA;
            context.result = COMPLETE;
            parsed = i - start;
            return;
        }
        }
    }

    parsed = end - start;
    context.result = WAITING;
}

struct RpcServer::RpcServerImpl {
private:
    std::shared_ptr<EpollContext> context_;
    std::set<std::unique_ptr<RpcServiceBase>> services_; // 保留Service防止被析构
    std::unordered_map<std::string, RpcHandler> handlers_;

    std::shared_ptr<ISocket> socket_;

    ThreadPool threadPool_;

public:
    RpcServerImpl(): context_(std::make_shared<EpollContext>()), threadPool_(2) {
        socket_ = SocketManager::getInstance().getSocket(context_);
        socket_->asyncAccept([this](const std::shared_ptr<ISocket>& client) {
            client->getContext()->postTask(client, [this, client]() {
                acceptClient(client);
            });
        });
    }

    void acceptClient(const std::shared_ptr<ISocket>& client) {
        LOG(DEBUG) << "RpcServer:: accept a new client";

        auto context = std::make_shared<RpcContext>(); // 这个 context 会保存到 close
        client->asyncRead([this, client, context](const std::string& buffer, std::size_t size) {
            LOG(DEBUG) << "RpcServer: read " << " size " << size << " fd " << client->getNative();
            if (size == 0) {
                client->close();
                return size;
            }

            std::size_t offset = 0;
            while (offset < size) {
                std::size_t parsed = 0;
                parse(buffer, offset, size, parsed, *context);
                offset += parsed;
                if (context->result == COMPLETE || context->result == ERROR) {
                    RpcContext local = std::move(*context);
                    *context = RpcContext{};
                    // 扔给线程池避免阻塞 epoll 线程
                    threadPool_.put([](const Result&) {},
                        &RpcServerImpl::dispatch, this, std::move(local), client);
                    continue;
                }

                if (context->result == WAITING) {
                    return offset;
                }

                // 不可能执行到这里
                assert(false);
            }
            return offset;
        });

        client->registerCloseCallback([](const std::shared_ptr<ISocket>& client) {
            LOG(DEBUG) << "RpcServer: close " << client->getNative();
        });
    }

    void dispatch(RpcContext&& context, const std::shared_ptr<ISocket>& client) {

        assert(context.result != WAITING);
        std::string response;

        if (context.result == ERROR) {
            response = buildErrorResponse(context.request_id, ERR_BAD_REQUEST);
        } else if (context.result == COMPLETE) {
            if (handlers_.contains(context.name)) {
                LOG(DEBUG) << "Rpc server: dispatch function " << context.name;
                try {
                    std::string res = handlers_[context.name](std::move(context.parms));
                    response = buildResponse(std::move(res), context.request_id);
                } catch (BadParamException& e) {
                    response = buildErrorResponse(context.request_id, ERR_UNKNOWN_PARAM);
                } catch (BadResponseException& e) {
                    response = buildErrorResponse(context.request_id, ERR_UNKNOWN_RESPONSE);
                } catch (...) {
                    response = buildErrorResponse(context.request_id, ERR_BAD_INTERNAL);
                }
            } else {
                response = buildErrorResponse(context.request_id, ERR_UNKNOWN_FUNCTION);
            }
        }

        client->asyncWriteOnce(
            [](bool success) {},
            std::make_shared<std::string>(std::move(response))
        );
    }

    void run(bool block) {
        context_->run(block);
    }

    void close() {
        context_->close();
    }

    void save(std::unique_ptr<RpcServiceBase> service) {
        services_.insert(std::move(service));
    }

    void addHandler(const std::string& name, RpcHandler&& handler) {
        handlers_[name] = std::move(handler);
    }

    bool bindAndListen(const char* ip, short port) {
        if (socket_->bind(ip, port) &&
            socket_->listen(ISocket::DEFAULT_BACKLOG)) {
            return true;
        }
        return false;
    }
};

void RpcServer::addHandler(const std::string& name, std::function<std::string(std::string buffer)> func) {
    impl->addHandler(name, std::move(func));
}

void RpcServer::registerService(std::unique_ptr<RpcServiceBase> service) {
    service->setup(*this);
    impl->save(std::move(service));
}

void RpcServer::run(bool block) {
    impl->run(block);
}
void RpcServer::close() {
    impl->close();
}

bool RpcServer::bindAndListen(const char* ip, short port) {
    return impl->bindAndListen(ip, port);
}

RpcServer::RpcServer() : impl(std::make_unique<RpcServerImpl>()){}

RpcServer::~RpcServer() = default;
