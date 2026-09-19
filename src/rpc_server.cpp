//
// Created by computer on 2026/9/16.
//


#include "app/rpc/RpcServer.h"
#include "logger/Logger.h"
#include "protoc/hello.pb.h"

struct Hello : RpcService<Hello> {
    virtual void setup(RpcServer& server) override {
        registerFunction(server, "HelloService", "hello", &Hello::hello);
    }

    HelloWorldResponse hello(HelloWorldRequest req) {
        HelloWorldResponse resp;
        if (req.msg() == "hello") {
            resp.set_res("world");
        } else {
            resp.set_res(req.msg());
        }
        return resp;
    }
};

int main() {
    Logger::getInstance().set_log_level(WARNING);
    RpcServer server;
    if (!server.bindAndListen("127.0.0.1", 8891)) {
        std::cout << "bind failed" << std::endl;
    }
    std::cout << "bind at 127.0.0.1:8891" << std::endl;
    server.registerService(Hello::create());

    server.run(true); // 阻塞运行

    return 0;
}