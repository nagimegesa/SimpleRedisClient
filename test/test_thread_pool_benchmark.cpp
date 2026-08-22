//
// 测试：新接口 (Result) 的正确性与性能 (Release 安全版)
// 编译：g++ -std=c++17 -pthread -O2 test_new_interface.cpp -o test_new_interface
// 运行：./test_new_interface
//
// 注意：需要链接 thread_pool 库，确保 thread_pool.h 和 Result 类型已定义。
//

#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include <future>
#include <stdexcept>
#include <thread>
#include <atomic>
#include <iomanip>
#include <functional>
#include <cstdlib>
#include "thread_pool.h"

using namespace std;
using namespace chrono;

// ---------- 自定义断言（永不消失） ----------
#define MY_ASSERT(expr) \
    do { \
        if (!(expr)) { \
            cerr << "Assertion failed: " #expr << " at " << __FILE__ << ":" << __LINE__ << endl; \
            abort(); \
        } \
    } while (false)

// ---------- 阻止编译器优化的工具 ----------
template<typename T>
static inline void DoNotOptimize(T&& value) {
    // GCC/Clang 内联汇编，标记变量已被使用
    asm volatile("" : "+r"(value));
}

// ---------- 辅助函数 ----------
template<typename T>
void check_result(const Result& r, const T& expected, const string& desc) {
    T val = r.get<T>();
    cout << desc << " expected=" << expected << ", actual=" << val;
    MY_ASSERT(val == expected);
    cout << " OK" << endl;
}

// ---------- 正确性测试 ----------

// 1. 基本类型 (int)
void test_basic_int() {
    cout << "[Test] basic int add..." << endl;
    ThreadPool pool(2);
    pool.put([](Result r) {
        int val = r.get<int>();
        cout << "int result: " << val << endl;
        MY_ASSERT(val == 3);
    }, [](int a, int b) { return a + b; }, 1, 2);
    pool.join();
    cout << "PASSED\n" << endl;
}

// 2. 浮点数
void test_double() {
    cout << "[Test] double multiplication..." << endl;
    ThreadPool pool(2);
    pool.put([](Result r) {
        double val = r.get<double>();
        cout << "double result: " << val << endl;
        MY_ASSERT(abs(val - 6.28) < 1e-9);
    }, [](double a, double b) { return a * b; }, 3.14, 2.0);
    pool.join();
    cout << "PASSED\n" << endl;
}

// 3. 字符串
void test_string() {
    cout << "[Test] string concatenation..." << endl;
    ThreadPool pool(2);
    pool.put([](Result r) {
        string val = r.get<string>();
        cout << "string result: " << val << endl;
        MY_ASSERT(val == "HelloWorld");
    }, [](string a, string b) { return a + b; }, string("Hello"), string("World"));
    pool.join();
    cout << "PASSED\n" << endl;
}

// 4. 多个参数和复杂类型 (vector)
void test_vector() {
    cout << "[Test] vector construction..." << endl;
    ThreadPool pool(2);
    pool.put([](Result r) {
        vector<int> v = r.get<vector<int>>();
        cout << "vector size: " << v.size() << ", elements: ";
        for (int x : v) cout << x << " ";
        cout << endl;
        MY_ASSERT(v.size() == 3 && v[0] == 1 && v[1] == 2 && v[2] == 3);
    }, [](int a, int b, int c) {
        return vector<int>{a, b, c};
    }, 1, 2, 3);
    pool.join();
    cout << "PASSED\n" << endl;
}

// 5. 异常处理 (任务抛出异常，Result捕获)
void test_exception() {
    cout << "[Test] exception handling..." << endl;
    ThreadPool pool(2);
    bool exception_caught = false;
    pool.put([&exception_caught](Result r) {
        try {
            r.get<int>(); // 期望抛出异常
        } catch (const std::exception& e) {
            cout << "Caught exception: " << e.what() << endl;
            exception_caught = true;
        }
    }, []() -> int {
        throw runtime_error("Task failed");
    });
    pool.join();
    MY_ASSERT(exception_caught);
    cout << "PASSED\n" << endl;
}

// 6. 多个任务并发，收集结果
void test_concurrent_results() {
    cout << "[Test] concurrent results..." << endl;
    ThreadPool pool(4);
    const int N = 100;
    vector<int> results(N, -1);
    for (int i = 0; i < N; ++i) {
        pool.put([i, &results](Result r) {
            int val = r.get<int>();
            results[i] = val;
        }, [i]() { return i * i; });
    }
    pool.join();
    for (int i = 0; i < N; ++i) {
        MY_ASSERT(results[i] == i * i);
    }
    cout << "All " << N << " results correct." << endl;
    cout << "PASSED\n" << endl;
}

// 7. 边界：0、负数、空字符串
void test_edge_values() {
    cout << "[Test] edge values..." << endl;
    ThreadPool pool(2);
    pool.put([](Result r) {
        int v = r.get<int>();
        cout << "zero: " << v << endl;
        MY_ASSERT(v == 0);
    }, []() { return 0; });
    pool.put([](Result r) {
        int v = r.get<int>();
        cout << "negative: " << v << endl;
        MY_ASSERT(v == -42);
    }, []() { return -42; });
    pool.put([](Result r) {
        string s = r.get<string>();
        cout << "empty string: '" << s << "'" << endl;
        MY_ASSERT(s.empty());
    }, []() { return string(); });
    pool.join();
    cout << "PASSED\n" << endl;
}

// ---------- 性能基准测试 ----------

class Timer {
public:
    void start() {
        // 防止编译器将代码重排到计时之外
        asm volatile("" ::: "memory");
        begin = high_resolution_clock::now();
    }
    double elapsed_ms() {
        auto end = high_resolution_clock::now();
        asm volatile("" ::: "memory");
        return duration<double, milli>(end - begin).count();
    }
private:
    high_resolution_clock::time_point begin;
};

// 空任务 (纯调度)
void bench_empty(int threads, int tasks) {
    cout << "\n[Bench] Empty tasks: threads=" << threads << ", tasks=" << tasks << endl;
    ThreadPool pool(threads);
    atomic<int> done{0};
    Timer timer;
    timer.start();
    for (int i = 0; i < tasks; ++i) {
        pool.put([](Result r) {
            // 不做任何事
        }, []() { return 0; });
    }
    pool.join();
    double ms = timer.elapsed_ms();
    cout << "  Total: " << fixed << setprecision(2) << ms << " ms" << endl;
    cout << "  Throughput: " << fixed << setprecision(0) << (tasks / (ms/1000.0)) << " tasks/s" << endl;
    cout << "  Avg per task: " << fixed << setprecision(3) << (ms / tasks) << " ms" << endl;
}

// CPU 密集型 (累加) —— 阻止循环优化
void bench_cpu(int threads, int tasks, int iter) {
    cout << "\n[Bench] CPU tasks: threads=" << threads << ", tasks=" << tasks << ", iter=" << iter << endl;
    ThreadPool pool(threads);
    atomic<long long> sum{0};
    Timer timer;
    timer.start();
    for (int i = 0; i < tasks; ++i) {
        pool.put([&sum](Result r) {
            long long local = r.get<long long>();
            sum.fetch_add(local);
        }, [iter]() -> long long {
            volatile long long s = 0;
            for (int j = 0; j < iter; ++j) s += j;
            DoNotOptimize(s);   // 强制编译器认为 s 被使用
            return s;
        });
    }
    pool.join();
    double ms = timer.elapsed_ms();
    cout << "  Total: " << fixed << setprecision(2) << ms << " ms" << endl;
    cout << "  Throughput: " << fixed << setprecision(0) << (tasks / (ms/1000.0)) << " tasks/s" << endl;
    cout << "  Sum result: " << sum.load() << endl;
}

// I/O 模拟 (sleep)
void bench_io(int threads, int tasks, int sleep_ms) {
    cout << "\n[Bench] I/O tasks: threads=" << threads << ", tasks=" << tasks << ", sleep=" << sleep_ms << "ms" << endl;
    ThreadPool pool(threads);
    Timer timer;
    timer.start();
    for (int i = 0; i < tasks; ++i) {
        pool.put([](Result r) {
            // 结果无关紧要
        }, [sleep_ms]() {
            this_thread::sleep_for(milliseconds(sleep_ms));
            return 0;
        });
    }
    pool.join();
    double ms = timer.elapsed_ms();
    cout << "  Total: " << fixed << setprecision(2) << ms << " ms" << endl;
    cout << "  Throughput: " << fixed << setprecision(0) << (tasks / (ms/1000.0)) << " tasks/s" << endl;
    cout << "  Avg per task: " << fixed << setprecision(3) << (ms / tasks) << " ms" << endl;
}

// ---------- 主函数 ----------
int main() {
    cout << "===== ThreadPool (Result接口) 测试 (Release 安全) =====" << endl;
    cout << "硬件并发: " << thread::hardware_concurrency() << " cores\n" << endl;

    // ----- 正确性测试 -----
    test_basic_int();
    test_double();
    test_string();
    test_vector();
    test_exception();
    test_concurrent_results();
    test_edge_values();

    // ----- 性能基准 -----
    cout << "\n===== 性能基准 =====\n";

    // 1. 空任务，变化线程数
    const int TASKS = 100000;
    for (int t : {1, 2, 4, 8, 16}) {
        bench_empty(t, TASKS);
    }

    // 2. CPU 密集
    for (int t : {1, 2, 4, 8, 16}) {
        bench_cpu(t, TASKS, 1000);
    }

    // 3. I/O 模拟
    bench_io(4, 100, 5);
    bench_io(8, 100, 5);

    // 4. 大压力
    bench_empty(8, 1000000);
    bench_cpu(8, 1000000, 100);

    // 5. 边界：线程多于任务
    cout << "\n[Bench] Edge: 8 threads, 3 tasks (CPU)" << endl;
    ThreadPool pool(8);
    atomic<int> cnt{0};
    Timer timer;
    timer.start();
    for (int i = 0; i < 3; ++i) {
        pool.put([](Result r) {
            // 结果无所谓
        }, [&cnt]() {
            this_thread::sleep_for(milliseconds(10));
            cnt++;
            return 0;
        });
    }
    pool.join();
    double ms = timer.elapsed_ms();
    cout << "  Total time: " << fixed << setprecision(2) << ms << " ms" << endl;
    cout << "  Counter: " << cnt.load() << " (should be 3)" << endl;

    cout << "\n===== 所有测试完成 =====" << endl;
    return 0;
}