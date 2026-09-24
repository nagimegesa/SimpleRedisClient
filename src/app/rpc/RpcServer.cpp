//
// Created by computer on 2026/9/15.
//
#include <string>

#include "RpcServer.h"

#include "thread_pool/thread_pool.h"
#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "Nacos.h"

using RpcHandler = std::function<std::string(std::string)>;

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

struct RpcServer::RpcServerImpl {
private:

    struct PairStringHash {
        std::size_t operator()(const std::pair<std::string, std::string>& p) const {
            std::hash<std::string> hasher;
            return hasher(p.first) ^ hasher(p.second);
        }
    };

    std::shared_ptr<EpollContext> context_;
    std::set<std::unique_ptr<RpcServiceBase>> services_; // 保留Service防止被析构
    std::unordered_map<std::pair<std::string, std::string>, RpcHandler, PairStringHash> handlers_;
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
            if (handlers_.contains(std::pair{context.service_name, context.func_name})) {
                LOG(DEBUG) << "Rpc server: dispatch function " << context.func_name;
                try {
                    std::string res = handlers_[std::pair{context.service_name, context.func_name}](std::move(context.params));
                    response = buildResponse(res, context.request_id);
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

    void addHandler(const std::string& group, const std::string& name, RpcHandler&& handler) {
        LOG(DEBUG) << "RpcServer:: addHandler " << group << " " << name;
        handlers_[std::pair{group, name}] = std::move(handler);
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

void RpcServer::addHandler(const std::string& group, const std::string& name, std::function<std::string(std::string buffer)> func) {
    impl->addHandler(group, name, std::move(func));
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
