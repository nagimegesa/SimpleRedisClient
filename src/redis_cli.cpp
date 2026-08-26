//
// Created by computer on 2026/8/26.
// 交互式 Redis 客户端
//
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include "app/SimpleRedisClient.h"
#include "logger/Logger.h"

std::vector<std::string> splitArgs(const std::string& line) {
    std::vector<std::string> args;
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
        args.push_back(std::move(token));
    }
    return args;
}

int main() {
    Logger::getInstance().set_log_level(WARNING);
    SimpleRedisClient client;
    if (!client.connect("127.0.0.1", 6379)) {
        LOG(ERR) << "Connect failed";
        return 1;
    }

    std::cout << "Connected to Redis at 127.0.0.1:6379" << std::endl;
    std::cout << "Type 'quit' or 'exit' to disconnect." << std::endl;

    std::string input;
    while (true) {
        std::cout << "redis> ";
        if (!std::getline(std::cin, input)) {
            std::cout << std::endl;
            break;
        }

        if (input.empty()) continue;

        if (input == "quit" || input == "exit") {
            break;
        }

        auto args = splitArgs(input);
        if (args.empty()) continue;

        auto res = client.execute(args);
        auto reply = res->get_future().get();

        std::cout << formatResponse(reply) << std::endl;
    }

    std::cout << "Bey." << std::endl;
    return 0;
}