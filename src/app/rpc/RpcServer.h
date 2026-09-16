//
// Created by computer on 2026/9/15.
//

#ifndef DEMO_RPCSERVER_H
#define DEMO_RPCSERVER_H

#include <functional>
#include <memory>
#include <string>
#include <google/protobuf/message.h>

/*
 * struct HelloRpc : public RpcService<HelloRpc> {
 *     virtual void setup(RpcServer& server) override {
 *         registerFunction(server, "hello", &HelloRpc::hello);
 *     }
 *
 *     HelloWorldResponse hello(HelloWorldRequest request) {
 *         HelloWorldResponse response;
 *         response.set_res("world");
 *         return response;
 *     }
 * }
 *
 * RpcServer server;
 * server.registerRpc(HelloRpc::create());
 * service.run();
 * ...
 * service.close();
 */

struct RpcServiceBase;

class RpcServer {
private:
    struct RpcServerImpl;
    std::unique_ptr<RpcServerImpl> impl;
    void addHandler(const std::string&name, std::function<std::string(std::string buffer)> func);
    template <typename Derived> friend struct RpcService;

public:
    void registerService(std::unique_ptr<RpcServiceBase> base);
    void run(bool block=false);
    void close();
    bool bindAndListen(const char* ip, short port);
    RpcServer();
    ~RpcServer();
};

struct RpcServiceBase {
    virtual      ~RpcServiceBase() = default;
    virtual void setup(RpcServer& server) = 0;
};

class BadParamException : public std::runtime_error {
public:
    explicit BadParamException(const std::string& msg) : std::runtime_error(msg) {}
};

class BadResponseException : public std::runtime_error {
public:
    explicit BadResponseException(const std::string& msg) : std::runtime_error(msg) {}
};

template<typename Derived>
struct RpcService : RpcServiceBase {
    template <typename Ret, typename Arg> requires std::is_base_of_v<::google::protobuf::Message, Ret>
    && std::is_base_of_v<::google::protobuf::Message, Arg>
    void registerFunction(RpcServer& server, const std::string& name, Ret(Derived::*func)(Arg arg)) {
        server.addHandler(name, [func, this](std::string buffer) -> std::string {
            Arg req;
            if (!req.ParseFromString(buffer)) {
                throw BadParamException("bad parameters");
            }
            Ret ret = (static_cast<Derived*>(this)->*func)(std::move(req));
            std::string out;
            if (!ret.SerializeToString(&out)) {
                throw BadResponseException("bad response");
            }
            return out;
        });
    }

    static std::unique_ptr<Derived> create() {
        return std::make_unique<Derived>();
    }

protected:
    RpcService()                   = default;
    virtual ~RpcService() override = default;
};

#endif //DEMO_RPCSERVER_H