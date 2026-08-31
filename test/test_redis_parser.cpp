#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <atomic>
#include <chrono>
#include <thread>
#include <cassert>

#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "context/EpollContext.h"
#include "redis.h"

// 简单测试宏
#define CHECK(cond, msg) \
    do { \
        if (!(cond)) { \
            std::cerr << "[FAIL] " << msg << std::endl; \
            failed++; \
        } else { \
            std::cout << "[PASS] " << msg << std::endl; \
            passed++; \
        } \
    } while(0)

static int passed = 0;
static int failed = 0;

// ---------- 1. 测试 buildRESPCommand ----------
void test_build_command() {
    std::cout << "\n== Testing buildRESPCommand ==\n";

    std::vector<std::string> args1 = {"PING"};
    CHECK(buildRESPCommand(args1) == "*1\r\n$4\r\nPING\r\n", "Build PING command");

    std::vector<std::string> args2 = {"SET", "key", "value"};
    CHECK(buildRESPCommand(args2) == "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
          "Build SET key value");

    std::vector<std::string> args3 = {};
    CHECK(buildRESPCommand(args3) == "*0\r\n", "Build empty command");

    std::vector<std::string> args4 = {"SET", "key", "val\r\nue"};
    CHECK(buildRESPCommand(args4) == "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$7\r\nval\r\nue\r\n",
          "Build binary-safe command");
}

// ---------- 2. 测试 RESP_Parser::parse ----------
void test_parse_resp() {
    std::cout << "\n== Testing RESP_Parser::parse ==\n";

    // 简单字符串
    RESPValue v1 = RESP_Parser::parse("+OK\r\n");
    CHECK(v1.type == RESPType::SimpleString && v1.as_string() == "OK", "Parse simple string");

    // 错误
    RESPValue v2 = RESP_Parser::parse("-ERR unknown command\r\n");
    CHECK(v2.type == RESPType::Error && v2.as_string() == "ERR unknown command", "Parse error");

    // 整数
    RESPValue v3 = RESP_Parser::parse(":1000\r\n");
    CHECK(v3.type == RESPType::Integer && v3.as_integer() == 1000, "Parse integer");

    // 批量字符串
    RESPValue v4 = RESP_Parser::parse("$5\r\nhello\r\n");
    CHECK(v4.type == RESPType::BulkString && v4.as_string() == "hello", "Parse bulk string");

    // 空批量字符串
    RESPValue v5 = RESP_Parser::parse("$-1\r\n");
    CHECK(v5.type == RESPType::Null, "Parse null bulk string");

    // 数组
    RESPValue v6 = RESP_Parser::parse("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    CHECK(v6.type == RESPType::Array &&
          v6.as_array().size() == 2 &&
          v6.as_array()[0].as_string() == "foo" &&
          v6.as_array()[1].as_string() == "bar",
          "Parse array");

    // 不完整数据
    bool threw = false;
    try {
        RESP_Parser::parse("$5\r\nhel");
    } catch (const IncompleteRESPException&) {
        threw = true;
    }
    CHECK(threw, "Throw IncompleteRESPException on incomplete data");
}

// ---------- 3. 异步网络集成测试（需要真实 Redis 运行） ----------
void test_async_redis_basic() {
    std::cout << "\n== Testing async Redis basic commands ==\n";

    auto socket = SocketManager::getInstance().getSocket();
    if (!socket->connect("127.0.0.1", 6379)) {
        CHECK(false, "Connect to Redis");
        return;
    }

    EpollContext context;
    std::atomic<int> response_count{0};
    std::atomic<bool> got_pong{false};
    std::atomic<bool> got_ok{false};
    std::atomic<bool> got_value{false};

    // 累积缓冲区，处理可能的分包
    std::string pending;
    context.registerAsyncRead(socket, [&](const std::string& buf, int size) -> std::size_t {
        pending += buf;
        std::size_t pos = 0;
        try {
            RESPValue result = RESP_Parser::parse(pending, pos);
            pending.clear();

            // 根据响应内容判断
            if (result.type == RESPType::SimpleString && result.as_string() == "PONG") {
                got_pong = true;
            } else if (result.type == RESPType::SimpleString && result.as_string() == "OK") {
                got_ok = true;
            } else if (result.type == RESPType::BulkString && result.as_string() == "testvalue") {
                got_value = true;
            }
            response_count++;
        } catch (const IncompleteRESPException&) {
            return pos; // 数据不完整，继续接收
        } catch (const std::exception& e) {
            std::cerr << "Parse error in test: " << e.what() << std::endl;
            pending.clear();
        }
        return pos;
    });

    context.run();

    // 发送 PING
    std::string cmd1 = buildRESPCommand({"PING"});
    socket->asyncWriteOnce(context, [](bool ok) {
        if (!ok) std::cerr << "Write failed for PING" << std::endl;
    }, std::make_shared<std::string>(std::move(cmd1)));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // 发送 SET
    std::string cmd2 = buildRESPCommand({"SET", "testkey", "testvalue"});
    socket->asyncWriteOnce(context, [](bool ok) {
        if (!ok) std::cerr << "Write failed for SET" << std::endl;
    }, std::make_shared<std::string>(std::move(cmd2)));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // 发送 GET
    std::string cmd3 = buildRESPCommand({"GET", "testkey"});
    socket->asyncWriteOnce(context, [](bool ok) {
        if (!ok) std::cerr << "Write failed for GET" << std::endl;
    }, std::make_shared<std::string>(std::move(cmd3)));

    // 等待最多 2 秒
    auto start = std::chrono::steady_clock::now();
    while (response_count.load() < 3) {
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    CHECK(got_pong.load(), "Received PONG");
    CHECK(got_ok.load(), "Received OK after SET");
    CHECK(got_value.load(), "Received testvalue after GET");
    CHECK(response_count.load() == 3, "Received 3 responses in total");

    // context.stop();
    socket->close();
}

int main() {
    Logger::getInstance().set_log_level(INFO); // 减少日志干扰

    test_build_command();
    test_parse_resp();
    test_async_redis_basic();

    std::cout << "\n===================\n";
    std::cout << "Passed: " << passed << ", Failed: " << failed << std::endl;
    return failed == 0 ? 0 : 1;
}