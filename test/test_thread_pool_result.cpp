//
// Created by computer on 2026/8/15.
//

#include <iostream>

#include "thread_pool.h"

int add(int a, int b) {
    return a + b;
}

void int_result(Result result) {
    std::cout << "result: " << result.get<int>() << std::endl;
}

int main () {
    ThreadPool pool(2);
    pool.put(int_result, add, 1, 2);
    pool.join();
}