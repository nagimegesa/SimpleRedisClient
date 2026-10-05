//
// Created by computer on 2026/9/16.
//


#include "app/rpc/RpcServer.h"
#include "logger/Logger.h"
#include "protoc/hello.pb.h"

struct Hello : RpcService<Hello> {
    virtual void setup(RpcServer& server) override {
        registerServiceName(server, "HelloService");
        registerFunction(server, "hello", &Hello::hello);
        registerStreamFunction(server, "helloStream", &Hello::helloStream);
    }

    HelloWorldResponse hello(HelloWorldRequest req) {
        HelloWorldResponse resp;
        resp.set_res(req.msg());

        return resp;
    }

    AsyncGenerator<HelloWorldResponse> helloStream(AsyncGenerator<HelloWorldRequest> req) {
        while (auto r = co_await req.next()) {
            LOG(DEBUG) << "recv hello stream: " << r->msg();
            HelloWorldResponse resp;
            resp.set_res(r->msg());
            co_yield resp;
        }

        LOG(DEBUG) << "recv hello stream end";
    }
};

int main() {
    Logger::getInstance().set_log_level(DEBUG);
    RpcServer server;
    if (!server.bindAndListen("127.0.0.1", 8891)) {
        std::cout << "bind failed" << std::endl;
        return 0;
    }
    std::cout << "bind at 127.0.0.1:8891" << std::endl;
    server.registerService(Hello::create());

    server.run(true); // 阻塞运行

    return 0;
}