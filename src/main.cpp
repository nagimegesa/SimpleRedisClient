#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <memory>
#include <thread>

#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "context/EpollContext.h"
#include "app/redis.h"

// 异步读取回调函数（提取出来，使 main 更简洁）
bool onRedisResponse(const std::string& buf) {
    try {
        RESPValue result = RESP_Parser::parse(buf);
        std::cout << formatResponse(result) << std::flush;
    } catch (const IncompleteRESPException&) {
        return false;
    } catch (const std::exception& e) {
        LOG(ERR) << "Parse error: " << e.what();
        LOG(ERR) << "Raw response: " << buf;
    }

    return true;
}

int main() {
    // ---------- 初始化日志 ----------
    Logger::getInstance().set_log_file("./logs/app.log");
    // Logger::getInstance().set_log_level(DEBUG);

    // ---------- 连接 Redis ----------
    auto socket = SocketManager::getInstance().getSocket();
    if (!socket->connect("127.0.0.1", 6379)) {
        LOG(ERR) << "connect redis failed";
        return 1;
    }
    LOG(INFO) << "Connected to Redis at 127.0.0.1:6379";
    LOG(INFO) << "Type 'quit' or 'exit' to leave.";

    // ---------- 设置异步读取（Epoll） ----------
    EpollContext context;
    context.registerAsyncRead(socket, onRedisResponse);

    // ---------- 启动 Epoll 事件循环线程 ----------
    context.run();
    // ---------- 主交互循环 ----------
    std::string input;
    while (true) {
        std::cout << "redis> " << std::flush;

        if (!std::getline(std::cin, input)) {
            // 遇到 EOF 或错误
            break;
        }

        // 去除首尾空白
        size_t start = input.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;
        size_t end = input.find_last_not_of(" \t\r\n");
        input = input.substr(start, end - start + 1);

        // 退出命令
        if (input == "quit" || input == "exit" || input == "q") {
            std::cout << "Bye!" << std::endl;
            break;
        }

        // 解析命令参数
        std::istringstream iss(input);
        std::vector<std::string> args;
        std::string arg;
        while (iss >> arg) {
            args.push_back(arg);
        }
        if (args.empty()) continue;

        // 构建 RESP 协议命令
        std::string cmd = buildRESPCommand(args);
        LOG(DEBUG) << "cmd: " << cmd;

        socket->asyncWriteOnce(context, [](bool res) {
            if (res == true) {
                LOG(INFO) << "redis response success";
            } else {
                LOG(INFO) << "redis response failed";
            }
        }, std::make_shared<std::string>(std::move(cmd)));
    }

    return 0;
}