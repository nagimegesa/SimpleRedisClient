//
// Created by computer on 2026/9/15.
//

#include "RpcServer.h"
#include <string>
#include <future>
#include <bits/fs_fwd.h>
#include <sys/socket.h>

#include "corou/Generator.h"
#include "thread_pool/thread_pool.h"
#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "Nacos.h"
#include "corou/Waiter.h"

// 这个是旧的协议
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

// 这个是新的协议
/*
 * 0x0a 0x0b    // 2
 * type 1 字节  1 = REQUEST 2 = RESPONSE 3 = STREAM_REQUEST 4 = STREAM_RESPONSE 5 = STREAM_END 255 = ERROR server端只会接收到 1 3 5 // 3
 * request_id 8 字节 大端 无符号，不能为 0 // 11
 * stream_id 8 字节 大端 无符号，若是 STREAM_xxx 或者 STREAM_ERROR 不能为 0， 否则为 0 // 19
 * error_code  1 字节，server 端只会接收到 0 // 20
 * service_len 1 字节 // 21
 * func_len 1 字节 // 22
 * parma_len  2 字节 大端 // 24
 * service_name for service_len
 * function name for func_len // service_len + func_len
 * serialize parma for parma_len // service_len + func_len + parma_len
 * 0x0b 0x0c // 26 + service_len + func_len + parma_len
 */


/* region 解析协议 */

enum ParserStatus {
    SOA,          // 找 0x0a
    SOB,          // 校验 0x0b
    HEADER,       // 读固定头 22 字节并统一校验
    SERVICE_NAME, // 读 service_name
    FUNC_NAME,    // 读 func_name
    PARAM_STRING, // 读 params
    EOB,          // 校验 0x0b
    EOC,          // 校验 0x0c，完成
};

enum ParserResult {
    COMPLETE,
    WAITING,
    ERROR,
};

enum ErrorCode : char {
    ERR_BAD_INTERNAL = 0,
    ERR_BAD_REQUEST  = 1,
    ERR_BAD_STREAM_ID = 2,
    ERR_BAD_STREAM_TYPE = 3,

    ERR_UNKNOWN_FUNCTION = 100,
    ERR_UNKNOWN_PARAM    = 101,
    ERR_UNKNOWN_RESPONSE = 102,
};

enum RequestType : unsigned char {
    REQUEST          = 1,
    RESPONSE         = 2,
    STREAM_REQUEST   = 3,
    STREAM_RESPONSE  = 4,
    STREAM_END       = 5,
    REQ_ERROR        = 255,
};

struct RpcContext {
    ParserStatus status;
    ParserResult result;

    RequestType type;
    std::uint64_t request_id;
    std::uint64_t stream_id;

    std::string service_name;
    std::string func_name;
    std::string params;
    std::vector<char> buffer;

    RpcContext()
        : status(SOA),
          result(WAITING),
          type(REQUEST),
          request_id(0),
          stream_id(0) {}

    void resetMessage() {
        // 故意不清 type / request_id / stream_id，便于错误处理时回错误响应。
        service_name.clear();
        func_name.clear();
        params.clear();
        buffer.clear();
    }

    bool isStream() const {
        return (type == STREAM_REQUEST || type == STREAM_END) && stream_id != 0;
    }

    bool isRequest() const {
        return type == REQUEST && request_id != 0;
    }

    RpcContext(RpcContext&& context) noexcept
        : status(context.status),
          result(context.result),
          type(context.type),
          request_id(context.request_id),
          stream_id(context.stream_id),
          service_name(std::move(context.service_name)),
          func_name(std::move(context.func_name)),
          params(std::move(context.params)),
          buffer(std::move(context.buffer)) {
        context.status = SOA;
        context.result = WAITING;
        context.type = REQUEST;
        context.request_id = 0;
        context.stream_id = 0;
    }

    RpcContext& operator=(RpcContext&& context) noexcept {
        if (this != &context) {
            status = context.status;
            result = context.result;
            type = context.type;
            request_id = context.request_id;
            stream_id = context.stream_id;
            service_name = std::move(context.service_name);
            func_name = std::move(context.func_name);
            params = std::move(context.params);
            buffer = std::move(context.buffer);

            context.status = SOA;
            context.result = WAITING;
            context.type = REQUEST;
            context.request_id = 0;
            context.stream_id = 0;
        }
        return *this;
    }
};

namespace {
    constexpr unsigned char kMagic0 = 0x0a;
    constexpr unsigned char kMagic1 = 0x0b;
    constexpr unsigned char kMagic2 = 0x0c;

    // 固定头长度：type(1)+request_id(8)+stream_id(8)+error_code(1)
    //            +service_len(1)+func_len(1)+param_len(2) = 22
    constexpr std::size_t kFixedHeaderSize = 22;

    // 固定头内部偏移
    constexpr std::size_t kOffsetType       = 0;
    constexpr std::size_t kOffsetRequestId  = 1;
    constexpr std::size_t kOffsetStreamId   = 9;
    constexpr std::size_t kOffsetErrorCode  = 17;
    constexpr std::size_t kOffsetServiceLen = 18;
    constexpr std::size_t kOffsetFuncLen    = 19;
    constexpr std::size_t kOffsetParamLen   = 20;

    inline unsigned char u8(char c) { return static_cast<unsigned char>(c); }

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

    inline void append_u16_be(std::string& out, std::uint16_t v) {
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

    inline std::uint16_t read_u16_be(const char* p) {
        return static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(static_cast<unsigned char>(p[0])) << 8) |
            static_cast<std::uint16_t>(static_cast<unsigned char>(p[1]))
        );
    }

    inline void append_magic_begin(std::string& out) {
        out.push_back(static_cast<char>(kMagic0));
        out.push_back(static_cast<char>(kMagic1));
    }

    inline void append_magic_end(std::string& out) {
        out.push_back(static_cast<char>(kMagic1));
        out.push_back(static_cast<char>(kMagic2));
    }
} // namespace

// 请求级错误：stream_id = 0
std::string buildErrorResponse(std::uint64_t request_id, char error_code) {
    std::string res;
    res.reserve(26);

    append_magic_begin(res);
    res.push_back(static_cast<char>(REQ_ERROR)); // 255
    append_u64_be(res, request_id);
    append_u64_be(res, 0); // stream_id = 0，请求级错误
    res.push_back(error_code);
    res.push_back(0);
    res.push_back(0);
    append_u16_be(res, 0);
    append_magic_end(res);

    return res;
}

// 流级错误：stream_id != 0
std::string buildErrorResponse(
    std::uint64_t request_id,
    std::uint64_t stream_id,
    char error_code
) {
    std::string res;
    res.reserve(26);

    append_magic_begin(res);
    res.push_back(static_cast<char>(REQ_ERROR)); // 255
    append_u64_be(res, request_id);
    append_u64_be(res, stream_id);
    res.push_back(error_code);
    res.push_back(0);
    res.push_back(0);
    append_u16_be(res, 0);
    append_magic_end(res);

    return res;
}

// 非流式响应：stream_id = 0
std::string buildResponse(const std::string& body, std::uint64_t request_id) {
    if (body.size() > 0xffffu) {
        return buildErrorResponse(
            request_id,
            static_cast<char>(ErrorCode::ERR_BAD_INTERNAL)
        );
    }

    std::string res;
    res.reserve(26 + body.size());

    append_magic_begin(res);
    res.push_back(static_cast<char>(RequestType::RESPONSE));
    append_u64_be(res, request_id);
    append_u64_be(res, 0); // stream_id = 0，非流式
    res.push_back(0);
    res.push_back(0);
    res.push_back(0);
    append_u16_be(res, static_cast<std::uint16_t>(body.size()));
    res.append(body);
    append_magic_end(res);

    return res;
}

// 流式响应数据帧：STREAM_RESPONSE，第一帧即 begin + data
std::string buildStreamResponse(
    const std::string& body,
    std::uint64_t request_id,
    std::uint64_t stream_id
) {
    if (stream_id == 0) {
        // 没有 stream_id，构造不出合法流帧，只能回请求级错误。
        return buildErrorResponse(
            request_id,
            static_cast<char>(ErrorCode::ERR_BAD_REQUEST)
        );
    }

    if (body.size() > 0xffffu) {
        // 超出 param_len 范围，回该流的流级错误。
        return buildErrorResponse(
            request_id,
            stream_id,
            static_cast<char>(ErrorCode::ERR_BAD_REQUEST)
        );
    }

    std::string res;
    res.reserve(26 + body.size());

    append_magic_begin(res);
    res.push_back(static_cast<char>(RequestType::STREAM_RESPONSE));
    append_u64_be(res, request_id);
    append_u64_be(res, stream_id);
    res.push_back(0); // error_code
    res.push_back(0); // service_len
    res.push_back(0); // func_len
    append_u16_be(res, static_cast<std::uint16_t>(body.size()));
    res.append(body);
    append_magic_end(res);

    return res;
}

// 流式正常结束：STREAM_END，param_len = 0
std::string buildStreamEnd(std::uint64_t request_id, std::uint64_t stream_id) {
    if (stream_id == 0) {
        return buildErrorResponse(
            request_id,
            static_cast<char>(ErrorCode::ERR_BAD_REQUEST)
        );
    }

    std::string res;
    res.reserve(26);

    append_magic_begin(res);
    res.push_back(static_cast<char>(RequestType::STREAM_END));
    append_u64_be(res, request_id);
    append_u64_be(res, stream_id);
    res.push_back(0);
    res.push_back(0);
    res.push_back(0);
    append_u16_be(res, 0);
    append_magic_end(res);

    return res;
}

inline void parse(
    const std::string& buffer,
    std::size_t start,
    std::size_t end,
    std::size_t& parsed,
    RpcContext& context
) {
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

    // 出错时保留 type / request_id / stream_id，便于上层回错误响应。
    auto set_error = [&]() {
        LOG(DEBUG) << "parse ERROR at i=" << i << " start=" << start
           << " end=" << end
           << " status=" << static_cast<int>(context.status)
           << " buf.size=" << context.buffer.size();
        context.result = ERROR;
        context.status = SOA;
        context.resetMessage();
        parsed = i - start;
    };

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
            if (i >= end) {
                set_waiting();
                return;
            }

            if (u8(data[i]) != kMagic1) {
                set_error();
                return;
            }

            ++i;
            context.resetMessage();
            context.type = RequestType::REQUEST;
            context.request_id = 0;
            context.stream_id = 0;
            context.status = HEADER;
            break;
        }

        case HEADER: {
            if (!read_fixed(kFixedHeaderSize)) {
                return;
            }

            const char* h = context.buffer.data();

            const unsigned char type       = u8(h[kOffsetType]);
            const std::uint64_t request_id = read_u64_be(h + kOffsetRequestId);
            const std::uint64_t stream_id  = read_u64_be(h + kOffsetStreamId);
            const unsigned char error_code = u8(h[kOffsetErrorCode]);

            LOG(DEBUG) << "HEADER buf.size=" << context.buffer.size()
           << " svc_len=" << (int)u8(h[kOffsetServiceLen])
           << " fn_len="  << (int)u8(h[kOffsetFuncLen])
           << " param_len=" << read_u16_be(h + kOffsetParamLen);

            context.type       = static_cast<RequestType>(type);
            context.request_id = request_id;
            context.stream_id  = stream_id;

            if (type != static_cast<unsigned char>(RequestType::REQUEST) &&
                type != static_cast<unsigned char>(RequestType::STREAM_REQUEST) &&
                type != static_cast<unsigned char>(RequestType::STREAM_END)) {
                set_error();
                return;
            }

            if (request_id == 0) {
                set_error();
                return;
            }

            if (type == static_cast<unsigned char>(RequestType::REQUEST) &&
                stream_id != 0) {
                set_error();
                return;
            }

            if (type == static_cast<unsigned char>(RequestType::STREAM_REQUEST) &&
                stream_id == 0) {
                set_error();
                return;
            }

            if (error_code != 0) {
                set_error();
                return;
            }

            context.service_name.clear();
            context.func_name.clear();
            context.params.clear();
            context.status = SERVICE_NAME;
            break;
        }

        case SERVICE_NAME: {
            const std::size_t service_len = u8(context.buffer[kOffsetServiceLen]);
            if (!read_var(service_len, context.service_name)) {
                return;
            }
            context.status = FUNC_NAME;
            break;
        }

        case FUNC_NAME: {
            const std::size_t func_len = u8(context.buffer[kOffsetFuncLen]);
            if (!read_var(func_len, context.func_name)) {
                return;
            }
            context.status = PARAM_STRING;
            break;
        }

        case PARAM_STRING: {
            const std::uint16_t param_len =
                read_u16_be(context.buffer.data() + kOffsetParamLen);
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
            LOG(DEBUG) << "EOB check i=" << i
                       << " byte=0x" << std::hex << (int)u8(data[i])
                       << std::dec << " buf.size=" << context.buffer.size();
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

struct RpcChannel;

struct RpcSession {
    std::uint64_t request_id = 0;
    std::uint64_t send_stream_id = 0;
    std::uint64_t recv_stream_id = 0;
    RpcChannel& channel;
    Channel<RpcContext> context_channel; // 一帧的完整数据
    std::unique_ptr<Channel<std::string>> inChannel; // 帧数据提取出来的输入
    std::unique_ptr<AsyncGenerator<std::string>> outGenerator; // 输出
    Task<void> recvTask;
    Task<void> sendTask;
    RpcSession(uint64_t request_id, RpcChannel& channel) : request_id(request_id), channel(channel) {}
    RpcSession(RpcSession&& other)  noexcept = default;
    RpcSession& operator=(RpcSession&& other) = delete;

    ~RpcSession() {
        context_channel.close();
        if (inChannel) inChannel->close();
    }
};

// 一个socket 映射 一个 Rpc Channel
struct RpcChannel {
    std::weak_ptr<ISocket> socket;
    Task<void> dispatchTask;
    Channel<std::string> channel;
    // 一个 channel 可以有很多 session
    std::unordered_map<std::uint64_t, std::shared_ptr<RpcSession>> sessions_;

    bool closed = false;

    ~RpcChannel() {
        closed = true;
        channel.close();
    }
};

using RpcHandler = std::function<std::string(std::string)>;
using RpcStreamHandler = std::function<AsyncGenerator<std::string>(AsyncGenerator<std::string> bufferStream)>;

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
    bool use_nacos = false;

    static constexpr std::string NACOS_SERVER_ADDRESS = "127.0.0.1:8848";
    static constexpr std::string NACOS_USER_NAME = "nacos";
    static constexpr std::string NACOS_PASSWORD = "nacos";

public:
    RpcServerImpl() : context_(std::make_shared<EpollContext>()), threadPool_(2), namingService_(nullptr) {
        socket_ = SocketManager::getInstance().getSocket(context_);
        socket_->asyncAccept([this](const std::shared_ptr<ISocket>& client) {
                client->getContext()->postTask(client, [this, client]() { acceptClient(client); });
            }
        );
    }

    void acceptClient(const std::shared_ptr<ISocket>& client) {
        LOG(DEBUG) << "RpcServer:: accept a new client";

        // 一个 socket 对应唯一的 channel, channel 管理和分发 session
        std::shared_ptr<RpcChannel> channel = std::make_shared<RpcChannel>();
        channel->socket = client;

        // client->getContext()->postCoroutineTask(client,
        //                                         [channel, this]() -> Task<void> {
        //                                             return dispatch(channel);
        //                                         }
        // );

        client->getContext()->postTask(client, [this, channel]() mutable {
            channel->dispatchTask = dispatch(*channel);
            channel->dispatchTask.start();
        });

        client->asyncRead([client, channel](const std::string& buffer, std::size_t size) {
                LOG(DEBUG) << "RpcServer: read " << " size " << size << " fd " << client->getNative();
                if (size == 0) {
                    client->close();
                    return size;
                }
                channel->channel.push(buffer.substr(0, size));
                return size;
            }
        );

        client->registerCloseCallback([channel](const std::shared_ptr<ISocket>& client) {
            channel->channel.close();
            LOG(DEBUG) << "RpcServer: close " << client->getNative();
        });
    }

    Task<void> dispatch(RpcChannel& channel) { // 这里可以用 const 因为 channel 的 生命周期绑定在了上面的lambda
        auto socket = channel.socket.lock();
        if (socket == nullptr) {
            LOG(DEBUG) << "RpcServer: socket is nullptr";
            co_return;
        }

        std::string buffer;
        RpcContext session_context;
        std::size_t offset = 0;
        while (auto t_buffer = co_await channel.channel.next()) {
            buffer.append(*t_buffer);
            while (offset < buffer.size()) {
                std::size_t parsed = 0;
                parse(buffer, offset, buffer.size(), parsed, session_context);
                offset += parsed;
                if (session_context.result == COMPLETE) {
                    RpcContext context(std::move(session_context)); // move 的时候清空了 session_context
                    auto key = std::pair{context.service_name, context.func_name};
                    if (context.isStream()) {
                        co_await handleStreamRequest(key, std::move(context), channel);
                    } else if (context.isRequest()) {
                        co_await handleCommonRequest(key, context, channel.socket);
                    } else {
                        assert (false); // 这个分支实际上是 session_context.result == ERROR，如果代码没写错就是不可能执行到这里
                    }

                    buffer.erase(0, offset);
                    offset = 0;
                    continue;
                }

                if (session_context.result == ERROR) {
                    LOG(DEBUG) << "RpcServer:: dispatch error " << session_context.result << " ";
                    RpcContext context(std::move(session_context));
                    co_await WriterWaiter{
                        socket,
                        buildErrorResponse(context.request_id, ERR_BAD_REQUEST)
                    };
                    buffer.erase(0, offset); // 移除有问题的字节
                    offset = 0;
                    continue;
                }

                if (session_context.result == WAITING) {
                    break;
                }
            }
        }
        LOG(INFO) << "RpcServer:: dispatch complete";
    }

    Task<void> handleCommonRequest(const auto& key, const RpcContext& context, const std::weak_ptr<ISocket> socket) {

        if (!handlers_.contains(key)) {
            co_await WriterWaiter{ socket.lock(),
                buildErrorResponse(context.request_id, ERR_UNKNOWN_FUNCTION)};
            co_return;
        }

        auto& func = handlers_[key];
        std::string response;
        try {
            std::string res = co_await ThreadPoolWaiter<std::string>(threadPool_,
                                                              [&func, &context]() -> std::string {
                                                                  return func(context.params);
                                                              }
            );
            response = buildResponse(res, context.request_id);
        } catch (RpcBadParamException& e) {
            LOG(ERR) << "Rpc server: dispatch " << context.service_name << " " << context.func_name << ": " << e.what();
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
        co_await WriterWaiter{socket.lock(), std::move(response)};
    }

    Task<void> handleStreamRequest(const auto& key, RpcContext&& context, RpcChannel& channel) {

        std::shared_ptr<ISocket> socket = channel.socket.lock();
        if (socket == nullptr) {
            LOG(ERR) << "Rpc Server: bad socket";
            co_return;
        }

        auto& sessions_ = channel.sessions_;

        if (!streamHandlers_.count(key)) {
            co_await WriterWaiter{socket,
                buildErrorResponse(context.request_id, context.stream_id, ERR_UNKNOWN_FUNCTION)};
            co_return;
        }

        auto& handler = streamHandlers_[key];
        auto id = context.request_id;
        std::shared_ptr<RpcSession> session = nullptr;
        if (sessions_.contains(id)) {
            session = sessions_[id];
        } else {
            if (context.type == STREAM_END) { // 第一帧不能是STREAM_END
                co_await WriterWaiter{socket,
                buildErrorResponse(context.request_id, context.stream_id, ERR_BAD_STREAM_TYPE) };
                co_return;
            }

            if (context.stream_id != 1) { // stream_id 只能从1开始依次递增
                co_await WriterWaiter{socket,
                    buildErrorResponse(context.request_id, context.stream_id, ERR_BAD_STREAM_ID) };
                co_return;
            }
            session = std::make_shared<RpcSession>(id, channel);
            sessions_[id] = session;
            session->request_id = context.request_id;
            // socket->getContext()->postCoroutineTask(socket,
            //     [this, handler, session]() -> Task<void> {
            //     return sessionRecvLoop(handler, session);
            // });

            socket->getContext()->postTask(socket,
                [this, handler, session]() {
                    session->recvTask = sessionRecvLoop(handler, *session);
                    session->recvTask.start();
            });
        }

        if (context.stream_id != session->recv_stream_id + 1) { // stream_id 只能从1开始依次递增
            co_await WriterWaiter{socket,
                buildErrorResponse(context.request_id, context.stream_id, ERR_BAD_STREAM_ID) };
            session->context_channel.close();                   // 写错了说明客户端异常，直接关闭
            co_return;
        }

        if (context.type == STREAM_END) {                       // 收到 STREAM END 关闭读通道
            LOG(DEBUG) << "RpcServer: recv STREAM END for stream " << context.request_id << " " << context.stream_id;
            session->context_channel.close();
            co_return;
        }

        session->recv_stream_id = context.stream_id;
        session->context_channel.push(std::move(context));
        co_return;
    }

    Task<void> sessionRecvLoop(RpcStreamHandler handler, RpcSession& session) {
        std::shared_ptr<ISocket> socket = session.channel.socket.lock();
        if (socket == nullptr) {
            LOG(ERR) << "Rpc Server: bad socket";
            co_return;
        }

        auto context2StringStream = [&session]() -> AsyncGenerator<std::string> {
            while (auto s = co_await session.inChannel->next()) {
                co_yield *s;
            }
        };

        session.inChannel = std::make_unique<Channel<std::string>>();
        session.outGenerator = std::make_unique<AsyncGenerator<std::string>>(
            handler(context2StringStream())
        );

        socket->getContext()->postTask(socket,
            [this, &session]() {
                session.sendTask = sessionSendLoop(session, *(session.outGenerator));
                session.sendTask.registerTaskCloseCallback([&session]() {
                    // 这里销毁了 std::shared_ptr<RpcSession>, 写关闭的时候销毁 session, 和 sessionLoop
                    session.context_channel.close();        // 服务端写完了也会关闭读通道，这个时候客户端再写会收到 BAD_STREAM_ID
                    session.channel.sessions_.erase(session.request_id);
                });
                session.sendTask.start();
        });

        while (std::optional<RpcContext> context = co_await session.context_channel.next()) {
            std::string errorResponse;
            session.inChannel->push(std::move(context->params)); // push 的异常会被抛出到 sessionSendLoop
        }
        session.inChannel->close();
        LOG(DEBUG) << "RpcServer:: dispatch stream complete " << session.request_id;
    }

    Task<void> sessionSendLoop(RpcSession& session, AsyncGenerator<std::string>& respGenerator) {
        std::shared_ptr<ISocket> socket = session.channel.socket.lock();
        if (socket == nullptr) {
            LOG(ERR) << "Rpc Server: bad socket";
            co_return;
        }

        std::string errorResponse;
        try {
            while (auto s = co_await respGenerator.next()) {
                LOG(DEBUG) << "RpcServer: Try send stream " << session.request_id << " " << session.send_stream_id + 1;
                session.send_stream_id += 1;
                bool writeRes = co_await WriterWaiter{socket,
                    buildStreamResponse(*s, session.request_id, session.send_stream_id)};

                if (writeRes == false) {
                    LOG(WARNING) << "RpcServer: send stream failed" << session.request_id << " " << session.send_stream_id + 1;
                    break;
                }

                LOG(DEBUG) << "RpcServer: stream sent " << session.send_stream_id;
            }
        }  catch (RpcBadResponseException& e) {
            LOG(ERR) << "Rpc server: dispatch error " << e.what();
            errorResponse = buildErrorResponse(
                session.request_id,
                session.send_stream_id + 1,
                ERR_UNKNOWN_RESPONSE
            );
        } catch (RpcBadParamException& e) {
            // 走到这里应该也是服务端的问题，但是为了确保服务不崩溃，没法通知上层应用，只能打个 log 交给对面处理了
            LOG(ERR) << "Rpc server: dispatch error " << e.what();
            errorResponse = buildErrorResponse(
                session.request_id,
                session.send_stream_id + 1,
                ERR_UNKNOWN_PARAM
            );
        } catch (...) { // 不知道发生了什么异常
            auto exptr = std::current_exception();
            LOG(ERR) << "Rpc server: dispatch error " << session.request_id;
            errorResponse = buildErrorResponse(
                session.request_id,
                session.send_stream_id + 1,
                ERR_BAD_INTERNAL
            );
        }
        if (!errorResponse.empty()) {
            session.send_stream_id += 1;    // error 的帧号
            co_await WriterWaiter{socket, std::move(errorResponse) };
        }
        session.send_stream_id += 1; // streamEnd 的帧号
        co_await WriterWaiter{socket, buildStreamEnd(session.request_id, session.send_stream_id)};
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


    void run(bool block) { context_->run(block); }

    void close() { context_->close(); }

    void save(std::unique_ptr<RpcServiceBase> service) { services_.insert(std::move(service)); }

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
            if (use_nacos) {
                try {
                    Properties props;
                    props[PropertyKeyConst::SERVER_ADDR] = NACOS_SERVER_ADDRESS;
                    props[PropertyKeyConst::AUTH_PASSWORD] = NACOS_PASSWORD;
                    props[PropertyKeyConst::AUTH_USERNAME] = NACOS_USER_NAME;
                    auto* factory = nacos::NacosFactoryFactory::getNacosFactory(props);

                    ResourceGuard _(factory);
                    NamingService* nameService = factory->CreateNamingService();
                    this->namingService_ = std::unique_ptr<NamingService>(nameService);
                } catch (NacosException& e) {
                    LOG(WARNING) << "Rpc Server: create nacos discovery service failed";
                    use_nacos = false;
                }
            }
            return true;
        }
        return false;
    }

    void registerDiscovery(const std::string& name) {
        try { if (use_nacos) { this->namingService_->registerInstance(name, server_ip, server_port); } } catch (
            nacos::NacosException& e) { LOG(DEBUG) << "Rpc server: failed to register a server to nacos " << name; }
    }
};

void RpcServer::addHandler(
    const std::string& group,
    const std::string& name,
    std::function<std::string(std::string buffer)> func
) const { impl->addHandler(group, name, std::move(func)); }

void RpcServer::addStreamHandler(
    const std::string& group,
    const std::string& name,
    std::function<AsyncGenerator<std::string>(AsyncGenerator<std::string> bufferStream)> func
) const { impl->addStreamHandler(group, name, std::move(func)); }

void RpcServer::registerService(std::unique_ptr<RpcServiceBase> service) {
    service->setup(*this);
    impl->save(std::move(service));
}

void RpcServer::run(bool block) { impl->run(block); }
void RpcServer::close() { impl->close(); }

bool RpcServer::bindAndListen(const char* ip, short port) { return impl->bindAndListen(ip, port); }

void RpcServer::registerServiceName(const std::string& name) { impl->registerDiscovery(name); }

RpcServer::RpcServer() : impl(std::make_unique<RpcServerImpl>()) {}

RpcServer::~RpcServer() = default;
