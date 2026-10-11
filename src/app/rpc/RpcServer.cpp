//
// Created by computer on 2026/9/15.
//

#include "RpcServer.h"

#include <string>
#include <sys/socket.h>

#include "protocol/RpcProtocol.h"
#include "corou/Generator.h"
#include "thread_pool/thread_pool.h"
#include "context/EpollContext.h"
#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "Nacos.h"
#include "corou/Waiter.h"

using namespace rpc;

using RpcHandler = std::function<std::string(std::string)>;
using RpcStreamHandler = std::function<AsyncGenerator<std::string>(AsyncGenerator<std::string> bufferStream)>;

struct RpcServer::RpcServerImpl {

    struct RpcSession;

    // 一个socket 映射 一个 Rpc Channel
    struct RpcChannel : public IEpollContextScope, public std::enable_shared_from_this<RpcChannel> {
        RpcServerImpl& server;
        Task<void> dispatchTask;
        Channel<std::string> channel;
        ISocket* socket = nullptr;
        std::unordered_map<std::uint64_t, std::shared_ptr<RpcSession>> sessions_;

        bool closed = false;

        void onDestroy() override {
            socket = nullptr;
            closed = true;
            channel.close();
        }

        void onRegister(const std::shared_ptr<ISocket>& sc) override {
            postTask([this]() mutable {
                dispatchTask = server.dispatch(*this);
                dispatchTask.start();
            });

            sc->asyncRead([this, sc](const std::string& buffer, std::size_t size) {
                    LOG(DEBUG) << "RpcServer: read " << " size " << size << " fd " << socket->getNative();
                    if (size == 0) {
                        socket->close();
                        return size;
                    }
                    channel.push(buffer.substr(0, size));
                    return size;
                }
            );
        }

        RpcChannel(RpcServerImpl& server, ISocket* socket) : server(server), socket(socket) {}
        virtual ~RpcChannel() override = default;
    };

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
        std::shared_ptr<RpcChannel> channel = std::make_shared<RpcChannel>(*this, client.get());
        client->registerScope(channel);
    }

    Task<void> dispatch(RpcChannel& channel) { // 这里可以用 const 因为 channel 的 生命周期绑定在了上面的lambda

        if (channel.closed || channel.socket == nullptr) {
            LOG(DEBUG) << "RpcServer:: closed";
            co_return;
        }

        std::string buffer;
        RpcContext session_context;
        std::size_t offset = 0;
        while (auto t_buffer = co_await channel.channel.next()) {
            buffer.append(*t_buffer);
            while (offset < buffer.size()) {
                if (channel.closed || channel.socket == nullptr) {
                    LOG(DEBUG) << "RpcServer:: channel closed while dispatching, drop "
                               << (buffer.size() - offset) << " bytes";
                    co_return;
                }

                std::size_t parsed = 0;
                parse(buffer, offset, buffer.size(), parsed, session_context);
                offset += parsed;
                if (session_context.result == COMPLETE) {
                    RpcContext context(std::move(session_context)); // move 的时候清空了 session_context
                    auto key = std::pair{context.service_name, context.func_name};
                    if (context.isStream()) {
                        co_await handleStreamRequest(key, std::move(context), channel);
                    } else if (context.isRequest()) {
                        co_await handleCommonRequest(key, context, channel);
                    } else {
                        // 协议解析已经保证 REQUEST 必是 request、STREAM_REQUEST/STREAM_END 必是 stream，
                        // 正常不会走到这里。真走到了说明校验和分发不一致, 回一个错误响应，
                        LOG(ERR) << "RpcServer:: dispatch unexpected frame"
                                 << " type=" << static_cast<int>(context.type)
                                 << " request_id=" << context.request_id
                                 << " stream_id=" << context.stream_id;
                        co_await WriterWaiter{
                            channel.socket->shared_from_this(),
                            buildErrorResponse(context.request_id, ERR_BAD_REQUEST)
                        };
                    }

                    buffer.erase(0, offset);
                    offset = 0;
                    continue;
                }

                if (session_context.result == ERROR) {
                    LOG(DEBUG) << "RpcServer:: dispatch error " << session_context.result << " ";
                    RpcContext context(std::move(session_context));
                    co_await WriterWaiter{
                        channel.socket->shared_from_this(),
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

    Task<void> handleCommonRequest(const auto& key, const RpcContext& context, RpcChannel& channel) {

        if (channel.closed || channel.socket == nullptr) {
            LOG(DEBUG) << "RpcServer:: channel is closed";
            co_return;
        }

        auto socket = channel.socket->shared_from_this();

        if (!handlers_.contains(key)) {
            co_await WriterWaiter{ socket,
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
        co_await WriterWaiter{socket, std::move(response)};
    }

    Task<void> handleStreamRequest(const auto& key, RpcContext&& context, RpcChannel& channel) {

        if (channel.closed || channel.socket == nullptr) {
            LOG(DEBUG) << "RpcServer:: channel is closed";
            co_return;
        }

        auto& sessions_ = channel.sessions_;

        if (!streamHandlers_.count(key)) {
            co_await WriterWaiter{channel.socket->shared_from_this(),
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
                co_await WriterWaiter{channel.socket->shared_from_this(),
                buildErrorResponse(context.request_id, context.stream_id, ERR_BAD_STREAM_TYPE) };
                co_return;
            }

            if (context.stream_id != 1) { // stream_id 只能从1开始依次递增
                co_await WriterWaiter{channel.socket->shared_from_this(),
                    buildErrorResponse(context.request_id, context.stream_id, ERR_BAD_STREAM_ID) };
                co_return;
            }
            session = std::make_shared<RpcSession>(id, channel);
            sessions_[id] = session;
            session->request_id = context.request_id;

            // 这个任务按值持有 session，所以 channel 若先被销毁，session 会变成一个
            // 引用悬垂 channel 的孤儿；用弱引用确认 channel 还活着再启动收流协程。
            std::weak_ptr<RpcChannel> weakChannel = channel.weak_from_this();
            channel.postTask([this, handler, session, weakChannel]() {
                    if (weakChannel.expired()) {
                        LOG(DEBUG) << "RpcServer:: channel destroyed before stream started";
                        return;
                    }
                    session->recvTask = sessionRecvLoop(handler, *session);
                    session->recvTask.start();
            });
        }

        if (context.stream_id != session->recv_stream_id + 1) { // stream_id 只能从1开始依次递增
            co_await WriterWaiter{channel.socket->shared_from_this(),
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
        if (session.channel.closed || session.channel.socket == nullptr) {
            LOG(DEBUG) << "RpcServer:: channel is closed";
            co_return;
        }

        std::shared_ptr<ISocket> socket = session.channel.socket->shared_from_this();

        auto context2StringStream = [&session]() -> AsyncGenerator<std::string> {
            while (auto s = co_await session.inChannel->next()) {
                co_yield *s;
            }
        };

        session.inChannel = std::make_unique<Channel<std::string>>();
        session.outGenerator = std::make_unique<AsyncGenerator<std::string>>(
            handler(context2StringStream())
        );

        std::weak_ptr<RpcChannel> weakChannel = session.channel.weak_from_this();
        const std::uint64_t requestId = session.request_id;
        RpcChannel& sessionChannel = session.channel;

        socket->getContext()->postTask(socket,
            [this, &session, weakChannel, requestId, &sessionChannel]() {
                session.sendTask = sessionSendLoop(session, *(session.outGenerator));
                session.sendTask.registerTaskCloseCallback(
                    [&session, weakChannel, requestId, &sessionChannel]() {
                        // 服务端写完了也会关闭读通道，这个时候客户端再写会收到 BAD_STREAM_ID
                        session.context_channel.close();
                        sessionChannel.postTask([weakChannel, requestId]() {
                            if (auto channel = weakChannel.lock()) {
                                channel->sessions_.erase(requestId);
                            }
                        });
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

        if (session.channel.closed || session.channel.socket == nullptr) {
            LOG(DEBUG) << "RpcServer:: channel is closed";
            co_return;
        }
        std::shared_ptr<ISocket> socket = session.channel.socket->shared_from_this();

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
        // 收尾的两帧写失败说明连接已经不可用, 不能再无条件往下写，
        // 否则帧留在待写队列里、回调可能永远不触发，send 循环会永久挂住。
        if (!errorResponse.empty()) {
            session.send_stream_id += 1;    // error 的帧号
            if (!(co_await WriterWaiter{socket, std::move(errorResponse)})) {
                LOG(DEBUG) << "RpcServer: send stream error frame failed " << session.request_id;
                co_return;
            }
        }
        session.send_stream_id += 1; // streamEnd 的帧号
        if (!(co_await WriterWaiter{socket, buildStreamEnd(session.request_id, session.send_stream_id)})) {
            LOG(DEBUG) << "RpcServer: send stream end failed " << session.request_id;
            co_return;
        }
    }

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
