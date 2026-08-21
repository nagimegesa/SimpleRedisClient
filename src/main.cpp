#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <memory>

#include "Logger.h"
#include "SocketManager.h"
#include "app/redis.h"


int main() {
    Logger::getInstance().set_log_file("./logs/app.log");
    // 若 Logger 同时输出控制台，可临时关闭以保持交互清晰
    Logger::getInstance().set_log_level(ERR); // 可选

    auto socket = SocketManager::getInstance().getSocket();
    if (!socket->connect("127.0.0.1", 6379)) {
        LOG(ERR) << "connect redis failed";
        return 1;
    }
    LOG(INFO) << "Connected to Redis at 127.0.0.1:6379";
    LOG(INFO) << "Type 'quit' or 'exit' to leave.";

    std::string input;
    while (true) {
        std::cout << "redis> " << std::flush;

        if (!std::getline(std::cin, input)) break;

        size_t start = input.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;
        size_t end = input.find_last_not_of(" \t\r\n");
        input = input.substr(start, end - start + 1);

        if (input == "quit" || input == "exit" || input == "q") {
            std::cout << "Bye!" << std::endl;
            break;
        }

        std::istringstream iss(input);
        std::vector<std::string> args;
        std::string arg;
        while (iss >> arg) args.push_back(arg);
        if (args.empty()) continue;

        // ---------- 构建 RESP 命令 ----------
        std::string cmd = buildRESPCommand(args);
        LOG(DEBUG) << "cmd: " << cmd;

        // ---------- 循环发送，确保完整 ----------
        const char* data = cmd.c_str();
        size_t total = cmd.size();
        ssize_t sent = 0;
        while (sent < total) {
            ssize_t n = socket->write(data + sent, total - sent);
            if (n <= 0) {
                LOG(ERR) << "send failed, sent=" << sent << ", errno=" << errno;
                break;
            }
            sent += n;
        }
        if (sent != total) {
            LOG(ERR) << "Incomplete send, abort command";
            continue; // 跳过本次命令，等待下一轮输入
        }

        // ---------- 接收响应 ----------
        std::string response;
        char buffer[4096];
        bool done = false;
        while (!done) {
            ssize_t n = socket->read(buffer, sizeof(buffer) - 1);
            if (n < 0) {
                LOG(ERR) << "read error, errno=" << errno;
                break;
            } else if (n == 0) {
                LOG(ERR) << "connection closed by peer";
                break;
            }
            buffer[n] = '\0';
            response.append(buffer, n);

            try {
                RESPValue result = RESP_Parser::parse(response);
                std::cout << formatResponse(result) << std::flush;
                done = true;
            } catch (const IncompleteRESPException&) {
                continue;
            } catch (const std::exception& e) {
                LOG(ERR) << "Parse error: " << e.what();
                LOG(ERR) << "Raw response: " << response;
                done = true; // 或 break
                break;
            }
        }
    }
    return 0;
}
