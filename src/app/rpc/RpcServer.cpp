//
// Created by computer on 2026/9/15.
//

#include "RpcServer.h"
#include <string>
#include <future>
#include "corou/Generator.h"
#include "thread_pool/thread_pool.h"
#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "Nacos.h"
#include "corou/Waiter.h"

/*
 * 0x0a 0x0b    // 2
 * type 1 字节  1 = REQUEST 2 = RESPONSE 3 = ERROR server端只会接收到 1 // 3
 * request_id   8 字节 大端 无符号，不能为 0 // 11
 * error_code  1 字节，server 端只会接收到 0 // 12
 * service_len 1 字节 // 13
 * service_name for service_len
 * func_len 1 字节 // 14 + service_len
 * function name for func_len // 14 + service_len + func_len
 * parma_len  4 字节 大端  // 18 + service_len + func_len
 * serialize parma for parma_len // 18 + service_len + func_len + parma_len
 * 0x0b 0x0c // 20 + service_len + func_len + parma_len
 */


/* region 解析协议 */

enum ParserStatus {
    SOA, SOB,
    TYPE,
    REQUEST_ID,
    ERROR_CODE,
    SERVICE_LEN,
    SERVICE_NAME,
    FUNC_LEN,
    FUNC_NAME,
    PARAM_LEN,
    PARAM_STRING,
    EOB, EOC,
};

enum ParserResult {
    COMPLETE,
    WAITING,
    ERROR,
};

enum ErrorCode : char {
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
    std::string service_name;
    std::string func_name;
    std::string params;
    std::vector<char> buffer;

    RpcContext() : status(SOA), result(WAITING), request_id(0) {}

    void reset_message() {
        request_id = 0;
        service_name.clear();
        func_name.clear();
        params.clear();
        buffer.clear();
    }
};

namespace {

constexpr unsigned char kMagic0 = 0x0a;
constexpr unsigned char kMagic1 = 0x0b;
constexpr unsigned char kMagic2 = 0x0c;

inline unsigned char u8(char c) {
    return static_cast<unsigned char>(c);
}

inline void append_u64_be(std::string& out, std::uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((v >> shift) & 0xff));
    }
}

inline void append_u32_be(std::string& out, std::uint32_t v) {
    out.push_back(static_cast<char>((v >> 24) & 0xff));
    out.push_back(static_cast<char>((v >> 16) & 0xff));
    out.push_back(static_cast<char>((v >> 8) & 0xff));
    out.push_back(static_cast<char>(v & 0xff));
}

inline std::uint64_t read_u64_be(const char* p) {
    std::uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

inline std::uint32_t read_u32_be(const char* p) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

inline void append_magic_begin(std::string& out) {
    out.push_back(static_cast<char>(kMagic0));
    out.push_back(static_cast<char>(kMagic1));
}

inline void append_magic_end(std::string& out) {
    out.push_back(static_cast<char>(kMagic1));
    out.push_back(static_cast<char>(kMagic2));
}

}  // namespace

// 0x0a 0x0b | type(3) | request_id(8) | error_code(1) | param_len(4)=0 | 0x0b 0x0c
std::string buildErrorResponse(std::uint64_t request_id, char error_code) {
    std::string res;
    res.reserve(18);

    append_magic_begin(res);
    res.push_back(static_cast<char>(RequestType::REQ_ERROR));
    append_u64_be(res, request_id);
    res.push_back(error_code);
    append_u32_be(res, 0); // parma_len 0
    append_magic_end(res);

    return res;
}

// 0x0a 0x0b | type(2) | request_id(8) | error_code(1)=0 |
// param_len(4) | body | 0x0b 0x0c
std::string buildResponse(const std::string& body, std::uint64_t request_id) {
    std::string res;
    res.reserve(18 + body.size());

    append_magic_begin(res);
    res.push_back(static_cast<char>(RequestType::RESPONSE));
    append_u64_be(res, request_id);
    res.push_back(0);   // error code 0
    append_u32_be(res, static_cast<std::uint32_t>(body.size()));
    res.append(body);
    append_magic_end(res);

    return res;
}

std::string buildStreamResponse(const std::string& body, std::uint64_t request_id) {
    // TODO: 实习这个函数，这里暂时使用 buildResponse替代
    return buildResponse(body, request_id);
}

inline void parse(
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

    auto set_waiting = [&]() {
        parsed = i - start;
        context.result = WAITING;
    };

    auto set_error = [&]() {
        context.result = ERROR;
        context.status = SOA;
        context.reset_message();
        parsed = i - start;
    };

    // 从 buffer + data 中凑齐 need 个字节，放入 context.buffer
    auto read_fixed = [&](std::size_t need) -> bool {
        while (context.buffer.size() < need && i < end) {
            context.buffer.push_back(data[i++]);
        }

        if (context.buffer.size() < need) {
            set_waiting();
            return false;
        }

        return true;
    };

    // 读取变长字段
    auto read_var = [&](std::size_t len, std::string& out) -> bool {
        if (out.size() > len) {
            set_error();
            return false;
        }

        if (out.capacity() < len) {
            out.reserve(len);
        }

        const std::size_t remain = len - out.size();
        if (remain > 0) {
            const std::size_t avail = end - i;
            const std::size_t take = std::min(remain, avail);

            out.append(data + i, take);
            i += take;

            if (out.size() < len) {
                set_waiting();
                return false;
            }
        }

        return true;
    };

    while (i < end) {
        switch (context.status) {
        case SOA: {
            const void* p = std::memchr(data + i, kMagic0, end - i);
            if (p == nullptr) {
                i = end;
                set_waiting();
                return;
            }

            i = static_cast<const char*>(p) - data + 1;
            context.status = SOB;
            break;
        }

        case SOB: {
            if (u8(data[i]) != kMagic1) {
                set_error();
                return;
            }

            ++i;
            context.reset_message();
            context.status = TYPE;
            break;
        }

        case TYPE: {
            if (i >= end) {
                set_waiting();
                return;
            }

            const unsigned char type = u8(data[i++]);

            // server 端只接收 REQUEST
            if (type != static_cast<unsigned char>(RequestType::REQUEST)) {
                set_error();
                return;
            }

            context.status = REQUEST_ID;
            break;
        }

        case REQUEST_ID: {
            if (!read_fixed(8)) {
                return;
            }

            const std::uint64_t id = read_u64_be(context.buffer.data());
            if (id == 0) {
                set_error();
                return;
            }

            context.request_id = id;
            context.buffer.clear();
            context.status = ERROR_CODE;
            break;
        }

        case ERROR_CODE: {
            if (i >= end) {
                set_waiting();
                return;
            }

            const unsigned char error_code = u8(data[i++]);

            // server 端收到的 REQUEST，error_code 必须为 0
            if (error_code != 0) {
                set_error();
                return;
            }

            context.status = SERVICE_LEN;
            break;
        }

        case SERVICE_LEN: {
            if (i >= end) {
                set_waiting();
                return;
            }

            const unsigned char len = u8(data[i++]);

            context.service_name.clear();
            context.service_name.reserve(len);

            context.buffer.clear();
            context.buffer.push_back(static_cast<char>(len));

            context.status = SERVICE_NAME;
            break;
        }

        case SERVICE_NAME: {
            if (context.buffer.empty()) {
                set_error();
                return;
            }

            const std::size_t len = u8(context.buffer[0]);
            if (!read_var(len, context.service_name)) {
                return;
            }

            context.buffer.clear();
            context.status = FUNC_LEN;
            break;
        }

        case FUNC_LEN: {
            if (i >= end) {
                set_waiting();
                return;
            }

            const unsigned char len = u8(data[i++]);

            context.func_name.clear();
            context.func_name.reserve(len);

            context.buffer.clear();
            context.buffer.push_back(static_cast<char>(len));

            context.status = FUNC_NAME;
            break;
        }

        case FUNC_NAME: {
            if (context.buffer.empty()) {
                set_error();
                return;
            }

            const std::size_t len = u8(context.buffer[0]);
            if (!read_var(len, context.func_name)) {
                return;
            }

            context.buffer.clear();
            context.status = PARAM_LEN;
            break;
        }

        case PARAM_LEN: {
            if (!read_fixed(4)) {
                return;
            }

            context.params.clear();
            context.status = PARAM_STRING;
            break;
        }

        case PARAM_STRING: {
            if (context.buffer.size() < 4) {
                set_error();
                return;
            }

            const std::uint32_t param_len = read_u32_be(context.buffer.data());
            if (!read_var(static_cast<std::size_t>(param_len), context.params)) {
                return;
            }

            context.buffer.clear();
            context.status = EOB;
            break;
        }

        case EOB: {
            if (i >= end) {
                set_waiting();
                return;
            }

            if (u8(data[i]) != kMagic1) {
                set_error();
                return;
            }

            ++i;
            context.status = EOC;
            break;
        }

        case EOC: {
            if (i >= end) {
                set_waiting();
                return;
            }

            if (u8(data[i]) != kMagic2) {
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

    set_waiting();
}

/* endregion */

struct Channel {
    std::coroutine_handle<> waiter = nullptr;
    std::deque<std::string> buffer;
    bool closed_ = false;

    struct ChannelWaiter {
        Channel& channel;

        bool await_ready() const noexcept { return channel.closed_  || !channel.buffer.empty(); }
        bool await_suspend(std::coroutine_handle<> handle) const noexcept {
            channel.waiter = handle;
        }
        void await_resume() const noexcept { }
    };

    void push(std::string string) {
        buffer.push_back(std::move(string));
        if (waiter) { // 这里这样写是为了防止 在写错调用期间 waiter 被覆盖，或者销毁（其实这里还没太搞懂, deepseek 教的）
            auto h = waiter;
            waiter = nullptr;
            h.resume();
        }
    }

    ChannelWaiter wait() {
        return ChannelWaiter{*this};
    }

    Task<std::optional<std::string>> next() {
        while (buffer.empty() && !closed_) {
            co_await wait();
        }

        if (buffer.empty()) {
            co_return std::nullopt;
        }

        auto t = std::move(buffer.front());
        buffer.pop_front();
        co_return std::move(t);
    }

    void close() {
        closed_ = true;
        if (waiter) {
            auto h = waiter;
            waiter = nullptr;
            h.resume(); // close 的时候需要让 next 知道，然后返回 nullopt
        }
    }
};

struct RpcSession {
    std::shared_ptr<ISocket> socket;
    Channel channel;
    std::shared_ptr<RpcContext> context;
};

using RpcHandler = std::function<std::string(std::string)>;
using RpcStreamHandler = std::function<AsyncGenerator<std::string>(AsyncGenerator<std::string> bufferStream)>;

struct RpcServer::RpcWriter::Impl {

    std::uint64_t request_id = 0;
    std::shared_ptr<ISocket> socket = nullptr;

    bool write(const ::google::protobuf::Message& message) const {
        std::string out;
        if (message.SerializeToString(&out)) {
            out = buildErrorResponse(request_id, ERR_UNKNOWN_RESPONSE);
        }

        if (socket) {
            std::promise<bool> promise;
            socket->asyncWriteOnce([&promise](bool success) {
                promise.set_value(success);
            }, buildStreamResponse(out, request_id));

            return promise.get_future().get();
        }

        return false;
    }
};

bool RpcServer::RpcWriter::write(::google::protobuf::Message& message) const {
    return impl->write(message);
}

RpcServer::RpcWriter::RpcWriter() : impl(std::make_unique<Impl>()) {}

struct RpcServer::RpcServerImpl {
private:

    struct PairStringHash {
        std::size_t operator()(const std::pair<std::string, std::string>& p) const {
            const std::hash<std::string> hasher;
            return hasher(p.first) ^ hasher(p.second);
        }
    };

    std::shared_ptr<EpollContext> context_;
    std::set<std::unique_ptr<RpcServiceBase>> services_; // 保留Service防止被析构
    std::unordered_map<std::pair<std::string, std::string>, RpcHandler, PairStringHash> handlers_;
    std::unordered_map<std::pair<std::string, std::string>, RpcStreamHandler, PairStringHash> streamHandlers_;
    std::shared_ptr<ISocket> socket_;
    ThreadPool threadPool_;
    std::unique_ptr<nacos::NamingService> namingService_;

    std::string server_ip;
    short server_port = 0;
    bool use_nacos = true;

    static constexpr std::string NACOS_SERVER_ADDRESS = "127.0.0.1:8848";
    static constexpr std::string NACOS_USER_NAME = "nacos";
    static constexpr std::string NACOS_PASSWORD = "nacos";


public:
    RpcServerImpl(): context_(std::make_shared<EpollContext>()), threadPool_(2), namingService_(nullptr) {
        socket_ = SocketManager::getInstance().getSocket(context_);
        socket_->asyncAccept([this](const std::shared_ptr<ISocket>& client) {
            client->getContext()->postTask(client, [this, client]() {
                acceptClient(client);
            });
        });
    }

    Task<void> acceptClient(const std::shared_ptr<ISocket>& client) {
        LOG(DEBUG) << "RpcServer:: accept a new client";

        auto context = std::make_shared<RpcContext>(); // 这个 context 会保存到 close

        auto session = std::make_shared<RpcSession>();
        session->socket = client;
        session->context = context;
        session->channel = Channel{};

        client->getContext()->postTask(client, [this, session]() {
            dispatch(session);
        });

        // session 会跟着 socket 销毁
        client->asyncRead([client, session](const std::string& buffer, std::size_t size) {
            LOG(DEBUG) << "RpcServer: read " << " size " << size << " fd " << client->getNative();
            if (size == 0) {
                client->close();
                return size;
            }

            session->channel.push(buffer);
            return size;
        });

        client->registerCloseCallback([](const std::shared_ptr<ISocket>& client) {
            LOG(DEBUG) << "RpcServer: close " << client->getNative();
        });

        co_return;
    }

    /**
     * TODO: 这个 dispatch 因为还没有修改二进制协议，同时 一个 session 只能支持一个流，后面改了协议可以解决这个问题
     * @param session
     * @return
     */
    Task<void> dispatch(std::shared_ptr<RpcSession> session) {
        std::string buffer;
        while (auto t_buffer = co_await session->channel.next()) {
            buffer.append(*t_buffer);
            auto& session_context = session->context;
            std::size_t offset = 0;
            while (offset < buffer.size()) {
                std::size_t parsed = 0;
                parse(buffer, offset, buffer.size(), parsed, *session_context);
                offset += parsed;
                if (session_context->result == COMPLETE) {

                    RpcContext context(std::move(*session_context));
                    *session_context = RpcContext{}; // 清空掉原来的 context

                    auto key = std::pair{context.service_name, context.func_name};
                    if (handlers_.contains(key)) {
                        auto& func = handlers_[key];
                        std::string response;
                        try {
                            response = co_await ThreadPoolWaiter<std::string>(threadPool_,
                                [&func, &context]() -> std::string {
                                return func(context.params);
                            });
                        } catch (RpcBadParamException& e) {
                            response = buildErrorResponse(context.request_id, ERR_UNKNOWN_PARAM);
                        } catch (RpcBadResponseException& e) {
                            // 这个异常应该是服务端的问题，但是为了确保服务不崩溃，没法通知上层应用，只能打个 log 交给对面处理了
                            LOG(ERR) << "Rpc server: dispatch " << context.service_name << " " << context.func_name << ": " << e.what();
                            response = buildErrorResponse(context.request_id, ERR_UNKNOWN_RESPONSE);
                        } catch (std::exception& e) {
                            // 走到这里应该也是服务端的问题，但是为了确保服务不崩溃，没法通知上层应用，只能打个 log 交给对面处理了
                            LOG(ERR) << "Rpc server: dispatch " << context.service_name << " " << context.func_name << ": " << e.what();
                            response = buildErrorResponse(context.request_id, ERR_BAD_INTERNAL);
                        }
                        co_await WriterWaiter{session->socket, buildResponse(response, context.request_id)};
                    } else if (streamHandlers_.contains(key)) {
                        auto& func = streamHandlers_[key];
                        auto getStringStream = [](RpcSession& session)-> AsyncGenerator<std::string> {
                            while (auto s = co_await session.channel.next()) {
                                co_yield std::move(*s);
                            }
                        };
                        std::string errorResponse;
                        try {
                            AsyncGenerator<std::string> stringStream = func(getStringStream(*session));
                            while (auto s = co_await stringStream.next()) {
                                co_await WriterWaiter{session->socket, buildStreamResponse(*s, context.request_id)};
                            }
                        } catch (RpcBadParamException& e) {
                            errorResponse = buildErrorResponse(context.request_id, ERR_UNKNOWN_PARAM);
                        } catch (RpcBadResponseException& e) {
                            // 这个异常应该是服务端的问题，但是为了确保服务不崩溃，没法通知上层应用，只能打个 log 交给对面处理了
                            LOG(ERR) << "Rpc server: dispatch " << context.service_name << " " << context.func_name << ": " << e.what();
                            errorResponse = buildErrorResponse(context.request_id, ERR_UNKNOWN_RESPONSE);
                        } catch (std::exception& e) {
                            // 走到这里应该也是服务端的问题，但是为了确保服务不崩溃，没法通知上层应用，只能打个 log 交给对面处理了
                            LOG(ERR) << "Rpc server: dispatch " << context.service_name << " " << context.func_name << ": " << e.what();
                            errorResponse = buildErrorResponse(context.request_id, ERR_BAD_INTERNAL);
                        }
                        co_await WriterWaiter{session->socket, buildResponse(errorResponse, context.request_id)};
                    } else {
                        co_await WriterWaiter{session->socket, buildErrorResponse(context.request_id, ERR_UNKNOWN_FUNCTION)};
                    }
                    buffer.erase(0, offset);
                    offset = 0;
                    continue;
                }

                if (session_context->result == ERROR) {
                    RpcContext context(std::move(*session_context));
                    *session_context = RpcContext{}; // 清空掉原来的 context
                    co_await WriterWaiter{session->socket, buildErrorResponse(context.request_id, ERR_BAD_REQUEST)};
                    buffer.erase(0, offset); // 移除有问题的字节
                    offset = 0;
                    continue;
                }

                if (session_context->result == WAITING) {
                    // WAITING 什么都不用做
                }
            }
        }
        LOG(INFO) << "RpcServer:: dispatch complete";
    }

    // void dispatch(RpcContext&& context, const std::shared_ptr<ISocket>& client) {
    //     assert(context.result != WAITING);
    //     std::string response;
    //     if (context.result == ERROR) {
    //         response = buildErrorResponse(context.request_id, ERR_BAD_REQUEST);
    //     } else if (context.result == COMPLETE) {
    //         bool stream_find = streamHandlers_.contains(std::pair{context.service_name, context.func_name});
    //         bool common_find = handlers_.contains(std::pair{context.service_name, context.func_name});
    //
    //         if (stream_find || common_find) {
    //             LOG(DEBUG) << "Rpc server: dispatch function " << context.func_name;
    //             try {
    //                 std::string res;
    //                 if (common_find) {
    //                     res = handlers_[std::pair{context.service_name, context.func_name}](std::move(context.params));
    //                 } else {
    //                     assert(false); // 这个是旧的写法，暂时放在这里
    //                 }
    //                 response = buildResponse(res, context.request_id);
    //             } catch (RpcBadParamException& e) {
    //                 response = buildErrorResponse(context.request_id, ERR_UNKNOWN_PARAM);
    //             } catch (RpcBadResponseException& e) {
    //                 // 这个异常应该是服务端的问题，但是为了确保服务不崩溃，没法通知上层应用，只能打个 log 交给对面处理了
    //                 LOG(ERR) << "Rpc server: dispatch " << context.service_name << " " << context.func_name << ": " << e.what();
    //                 response = buildErrorResponse(context.request_id, ERR_UNKNOWN_RESPONSE);
    //             } catch (std::exception& e) {
    //                 // 走到这里应该也是服务端的问题，但是为了确保服务不崩溃，没法通知上层应用，只能打个 log 交给对面处理了
    //                 LOG(ERR) << "Rpc server: dispatch " << context.service_name << " " << context.func_name << ": " << e.what();
    //                 response = buildErrorResponse(context.request_id, ERR_BAD_INTERNAL);
    //             }
    //         } else {
    //             response = buildErrorResponse(context.request_id, ERR_UNKNOWN_FUNCTION);
    //         }
    //     }
    //
    //     client->asyncWriteOnce(
    //         [&client](bool success) {
    //             if (!success) {
    //                 LOG(WARNING) << "RpcServer: write error, close it " << client->getNative();
    //                 client->close();
    //             }
    //         },
    //         std::move(response)
    //     );
    // }


    void run(bool block) {
        context_->run(block);
    }

    void close() {
        context_->close();
    }

    void save(std::unique_ptr<RpcServiceBase> service) {
        services_.insert(std::move(service));
    }

    void addHandler(const std::string& group, const std::string& name, RpcHandler&& handler) {
        LOG(DEBUG) << "RpcServer:: addHandler " << group << " " << name;
        handlers_[std::pair{group, name}] = std::move(handler);
    }

    void addStreamHandler(const std::string& group, const std::string& name, RpcStreamHandler&& handler) {
        LOG(DEBUG) << "RpcServer:: addStreamHandler " << group << " " << name;
        streamHandlers_[std::pair{group, name}] = std::move(handler);
    }

    bool bindAndListen(const char* ip, short port) {
        using namespace nacos;
        if (socket_->bind(ip, port) &&
            socket_->listen(ISocket::DEFAULT_BACKLOG)) {

            server_ip = ip;
            server_port = port;
            try {
                Properties props;
                props[PropertyKeyConst::SERVER_ADDR] = NACOS_SERVER_ADDRESS;
                props[PropertyKeyConst::AUTH_PASSWORD] = NACOS_PASSWORD;
                props[PropertyKeyConst::AUTH_USERNAME] = NACOS_USER_NAME;
                auto *factory = nacos::NacosFactoryFactory::getNacosFactory(props);

                ResourceGuard _(factory);
                NamingService* nameService = factory->CreateNamingService();
                this->namingService_ = std::unique_ptr<NamingService>(nameService);
            } catch (NacosException& e) {
                LOG(WARNING) << "Rpc Server: create nacos discovery service failed";
                use_nacos = false;
            }
            return true;
        }
        return false;
    }

    void registerDiscovery(const std::string& name) {
        try {
            if (use_nacos) {
                this->namingService_->registerInstance(name, server_ip, server_port);
            }
        } catch (nacos::NacosException& e) {
            LOG(DEBUG) << "Rpc server: failed to register a server to nacos " << name;
        }
    }
};

void RpcServer::addHandler(const std::string& group, const std::string& name, std::function<std::string(std::string buffer)> func) const {
    impl->addHandler(group, name, std::move(func));
}

void RpcServer::addStreamHandler(
    const std::string& group,
    const std::string& name,
    std::function<AsyncGenerator<std::string>(AsyncGenerator<std::string> bufferStream)> func
) const {
    impl->addStreamHandler(group, name, std::move(func));
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

void RpcServer::registerServiceName(const std::string& name) {
    impl->registerDiscovery(name);
}

RpcServer::RpcServer() : impl(std::make_unique<RpcServerImpl>()){}

RpcServer::~RpcServer() = default;
