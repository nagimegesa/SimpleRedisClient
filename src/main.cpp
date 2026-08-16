//
// Created by computer on 2026/8/14.
//

#include <iostream>

#include "logger/Logger.h"
#include "thread_pool/thread_pool.h"
using namespace std;

int add(int a, int b) {
    return a + b;
}

void add_res(Result result) {
    int res = result.get<int>();
    LOG(DEBUG) << "Add result: " << res;
}
int main() {
    ThreadPool pool(2);
    pool.put(add_res, add, 1, 2);
    return 0;
}