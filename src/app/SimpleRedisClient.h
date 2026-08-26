//
// Created by computer on 2026/8/23.
//

#ifndef DEMO_SIMPLEREDISCLIENT_H
#define DEMO_SIMPLEREDISCLIENT_H
#include <future>

#include "redis.h"

class SimpleRedisClient {
    struct ClientImpl;
    std::unique_ptr<ClientImpl> impl;

public:
    ~SimpleRedisClient();
    SimpleRedisClient();
    bool                                     connect(const std::string& host, const unsigned short& port);
    std::shared_ptr<std::promise<RESPValue>> execute(const std::vector<std::string>& args);
    void                                     close();
    std::shared_ptr<std::promise<RESPValue>> execute(const std::string& cmd);
    static constexpr int                     DEFAULT_CLIENT_COUNT = 4;
};

#endif //DEMO_SIMPLEREDISCLIENT_H