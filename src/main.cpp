//
// Created by computer on 2026/8/14.
//

#include <iostream>

#include "logger/Logger.h"
#include "socket/SocketManager.h"
#include "thread_pool/thread_pool.h"
using namespace std;

int add(int a, int b) {
    return a + b;
}

void add_res(Result result) {
    int res = result.get<int>();
    LOG(DEBUG) << "Add result: " << res;
    LOG(WARNING) << "Add result: " << add(res, res);
    LOG(INFO) << "Add result: " << add(res, res);
    LOG(ERROR) << "Add result: " << add(res, res);
}

int main() {
    Logger::getInstance().set_log_file("./logs/app.log");
    // Logger::getInstance().set_log_level(WARNING);
    ThreadPool pool(2);
    pool.put(add_res, add, 1, 2);

    // auto socket = SocketManager::getInstance().get_socket();
    // socket->bind("127.0.0.1", 8080);
    // socket->listen(ISocket::default_backlog);
    //
    // auto client = socket->accept();
    // LOG(DEBUG) << "Accepted: " << client;
    //
    // client->write("Hello", 5);
    // client->write("World!", 6);
    //
    // char buf[512];
    // int ret = client->read(buf, sizeof(buf));
    //
    // LOG(DEBUG) << "Received: " << buf << "ret: " << ret;

    return 0;
}