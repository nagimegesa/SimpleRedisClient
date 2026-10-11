//
// Created by computer on 2026/9/15.
//

#ifndef DEMO_RPCSERVER_H
#define DEMO_RPCSERVER_H

#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <google/protobuf/message.h>

#include "corou/Generator.h"
#include "socket/socket.h"
#include "protocol/RpcCodec.h"

/*
 * struct HelloRpc : public RpcService<HelloRpc> {
 *     virtual void setup(RpcServer& server) override {
 *         registerServiceName(server, "hello");
 *         registerFunction(server, "hello", &HelloRpc::hello);
 *         registerStreamFunction(server, "helloStream", *HellpRpc::helloStream");
 *     }
 *
 *     HelloWorldResponse hello(HelloWorldRequest request) {
 *         HelloWorldResponse response;
 *         response.set_res("world");
 *         return response;
 *     }
 *
 *     AsyncGenerator<HelloWorldResponse> helloStream(AsyncGenerator<HelloWorldRequest> request) {
 *          while(auto req = co_await request.next()) {
 *              HelloWorldResponse res;
 *              res.set_res(req->msg());
 *              for(int i = 0; i < 4; ++i) {
 *                  co_yield res;
 *              }
 *          }
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
public:
    class RpcWriter;

private:
    struct RpcServerImpl;
    std::unique_ptr<RpcServerImpl> impl;
    void                           addHandler(const std::string& group, const std::string& name, std::function<std::string(std::string buffer)> func) const;
    void                           addStreamHandler(
        const std::string&                                                                 group,
        const std::string&                                                                 name,
        std::function<AsyncGenerator<std::string>(AsyncGenerator<std::string> bufferStream)> func
    ) const;
    template <typename Derived> friend struct RpcService;

public:
    void registerService(std::unique_ptr<RpcServiceBase> base);
    void run(bool block=false);
    void close();
    bool bindAndListen(const char* ip, short port);
    void registerServiceName(const std::string& name);
    RpcServer();
    ~RpcServer();
};

using RpcWriter = RpcServer::RpcWriter;

struct RpcServiceBase {
    virtual      ~RpcServiceBase() = default;
    virtual void setup(RpcServer& server) = 0;
};

template<typename Derived>
struct RpcService : RpcServiceBase {

    std::string serviceName;
    void registerServiceName(RpcServer& server, const std::string& name);

    template <typename Ret, typename Arg> requires std::is_base_of_v<::google::protobuf::Message, Ret>
    && std::is_base_of_v<::google::protobuf::Message, Arg>
    void registerFunction(RpcServer& server, const std::string& name, Ret(Derived::*func)(Arg arg)) {
        server.addHandler(serviceName, name,
                          [func, this](const std::string& buffer) -> std::string {
                              Arg req = rpc::RpcCodec::decode<Arg>(buffer);
                              Ret ret = (static_cast<Derived*>(this)->*func)(std::move(req));
                              return rpc::RpcCodec::encode(ret);
                          }
        );
    }

    template<typename Ret> requires std::is_base_of_v<::google::protobuf::Message, Ret>
    void registerFunction(RpcServer& server, const std::string& name, Ret(Derived::*func)()) {
        server.addHandler(serviceName, name,
                          [func, this](const std::string&) -> std::string {
                              Ret ret = (static_cast<Derived*>(this)->*func)();
                              return rpc::RpcCodec::encode(ret);
                          }
        );
    }

    template <typename Arg> requires std::is_base_of_v<::google::protobuf::Message, Arg>
    void registerFunction(RpcServer& server, const std::string& name, void(Derived::*func)(Arg arg)) {
        server.addHandler(serviceName, name,
                          [func, this](const std::string& buffer) -> std::string {
                              Arg req = rpc::RpcCodec::decode<Arg>(buffer);
                              (static_cast<Derived*>(this)->*func)(std::move(req));
                              return "";
                          }
        );
    }

    void registerFunction(RpcServer& server, const std::string& name, void(Derived::*func)()) {
        server.addHandler(serviceName, name,
                          [func, this](const std::string& buffer) -> std::string {
                              (static_cast<Derived*>(this)->*func)();
                              return "";
                          }
        );
    }

    template <typename Ret, typename Arg> requires std::is_base_of_v<::google::protobuf::Message, Ret>
        && std::is_base_of_v<::google::protobuf::Message, Arg>
    void registerStreamFunction(RpcServer& server, const std::string& name, AsyncGenerator<Ret>(Derived::*func)(AsyncGenerator<Arg>)) {
        server.addStreamHandler(serviceName, name,
                                [func, this](AsyncGenerator<std::string> stringStream) -> AsyncGenerator<std::string> {
                                    return rpc::RpcCodec::encodeStream(
                                        (static_cast<Derived*>(this)->*func)(
                                            rpc::RpcCodec::decodeStream<Arg>(std::move(stringStream))
                                        )
                                    );
                                }
        );
    }

    static std::unique_ptr<Derived> create() {
        return std::make_unique<Derived>();
    }

protected:
    RpcService()                   = default;
    virtual ~RpcService() override = default;
};

template <typename Derived>
void RpcService<Derived>::registerServiceName(RpcServer& server, const std::string& name) {
    serviceName = name;
    server.registerServiceName(name);
}


#endif //DEMO_RPCSERVER_H
