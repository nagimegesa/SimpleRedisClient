//
// Queue Benchmark
//
// Test:
//   1. int
//   2. std::shared_ptr<int>
//
// Concurrency:
//   1P1C
//   4P1C
//   4P4C
//
// Queue interfaces are NOT unified.
// BlockingQueue / SpinBlockingQueue:
//     push(value)
//     pop() -> value
//
// LockFreeQueue / MPSCQueue / MPMCQueue:
//     push(value) -> bool
//     pop(value) -> bool
//

#include "thread_pool/queue/LockFreeQueue.h"
#include "thread_pool/queue/BlockQueue.h"

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <numeric>


// ============================================================
// Configuration
// ============================================================

constexpr std::size_t TOTAL_OPERATIONS = 1'000'000;

constexpr std::size_t WARMUP_ROUNDS = 1;

constexpr std::size_t BENCHMARK_ROUNDS = 5;


// ============================================================
// Result
// ============================================================

struct BenchmarkResult {

    std::string name;

    std::string data_type;

    std::size_t producers;

    std::size_t consumers;

    std::size_t operations;

    double avg_time_us;

    double min_time_us;

    double max_time_us;

    double throughput_ops;
};


// ============================================================
// Start Barrier
// ============================================================

class StartBarrier {

public:

    explicit StartBarrier(std::size_t thread_count)
        : ready_(0),
          total_(thread_count),
          start_(false)
    {
    }


    void arrive_and_wait()
    {
        ready_.fetch_add(
            1,
            std::memory_order_release
        );

        while (
            ready_.load(
                std::memory_order_acquire
            ) < total_
        ) {
            std::this_thread::yield();
        }

        while (
            !start_.load(
                std::memory_order_acquire
            )
        ) {
            std::this_thread::yield();
        }
    }


    void start()
    {
        while (
            ready_.load(
                std::memory_order_acquire
            ) < total_
        ) {
            std::this_thread::yield();
        }

        start_.store(
            true,
            std::memory_order_release
        );
    }


private:

    std::atomic<std::size_t> ready_;

    std::size_t total_;

    std::atomic<bool> start_;
};


// ============================================================
// Producer workload distribution
// ============================================================

static std::size_t get_producer_operations(
    std::size_t producer_id,
    std::size_t producer_count,
    std::size_t total_operations)
{
    const std::size_t base =
        total_operations / producer_count;

    const std::size_t remainder =
        total_operations % producer_count;

    return base +
           (producer_id < remainder ? 1 : 0);
}


// ============================================================
// Result calculation
// ============================================================

static BenchmarkResult make_result(
    const std::string& name,
    const std::string& data_type,
    std::size_t producers,
    std::size_t consumers,
    std::size_t operations,
    const std::vector<double>& times)
{
    const double sum =
        std::accumulate(
            times.begin(),
            times.end(),
            0.0
        );

    const double avg =
        sum / static_cast<double>(times.size());

    const double min_time =
        *std::min_element(
            times.begin(),
            times.end()
        );

    const double max_time =
        *std::max_element(
            times.begin(),
            times.end()
        );

    const double throughput =
        static_cast<double>(operations) /
        (avg / 1'000'000.0);

    return {
        name,
        data_type,
        producers,
        consumers,
        operations,
        avg,
        min_time,
        max_time,
        throughput
    };
}


// ============================================================
// Print one result
// ============================================================

static void print_result(
    const BenchmarkResult& r)
{
    std::cout
        << std::left
        << std::setw(28)
        << r.name

        << " | "
        << std::setw(12)
        << r.data_type

        << " | "
        << r.producers
        << "P"
        << r.consumers
        << "C"

        << " | avg = "

        << std::fixed
        << std::setprecision(2)
        << std::setw(12)
        << r.avg_time_us
        << " us"

        << " | throughput = "

        << std::setw(14)
        << r.throughput_ops
        << " ops/s"

        << std::endl;
}


// ============================================================
// Print benchmark header
// ============================================================

static void print_test_header(
    const std::string& name,
    const std::string& data_type,
    std::size_t producers,
    std::size_t consumers,
    std::size_t operations)
{
    std::cout
        << "\n"
        << "============================================================\n"
        << name << "\n"
        << "------------------------------------------------------------\n"
        << "Data type : " << data_type << "\n"
        << "Producers : " << producers << "\n"
        << "Consumers : " << consumers << "\n"
        << "Operations: " << operations << "\n"
        << "============================================================\n";
}


// ################################################################
// #                                                              #
// #              Part 1: BlockingQueue family                  #
// #                                                              #
// ################################################################


// ============================================================
// BlockingQueue
//
// Interface:
//
// queue.push(value)
// queue.pop() -> value
//
// ============================================================

template <
    typename Queue,
    typename T,
    typename MakeValue,
    typename IsStop,
    std::size_t Producer,
    std::size_t Consumer
>
double run_blocking_once(
    Queue& queue,
    std::size_t iterations,
    MakeValue make_value,
    IsStop is_stop)
{
    StartBarrier barrier(
        Producer + Consumer
    );

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;

    producers.reserve(Producer);
    consumers.reserve(Consumer);


    // ========================================================
    // Producers
    // ========================================================

    for (
        std::size_t producer_id = 0;
        producer_id < Producer;
        ++producer_id
    ) {

        producers.emplace_back(
            [&, producer_id]()
            {
                const std::size_t count =
                    get_producer_operations(
                        producer_id,
                        Producer,
                        iterations
                    );

                barrier.arrive_and_wait();

                for (
                    std::size_t j = 0;
                    j < count;
                    ++j
                ) {

                    queue.push(
                        make_value(j)
                    );
                }
            }
        );
    }


    // ========================================================
    // Consumers
    // ========================================================

    for (
        std::size_t consumer_id = 0;
        consumer_id < Consumer;
        ++consumer_id
    ) {

        consumers.emplace_back(
            [&]()
            {
                barrier.arrive_and_wait();

                while (true) {

                    T item = queue.pop();

                    if (is_stop(item)) {
                        break;
                    }
                }
            }
        );
    }


    // ========================================================
    // Start
    // ========================================================

    barrier.start();

    const auto start =
        std::chrono::steady_clock::now();


    // ========================================================
    // Wait for producers
    // ========================================================

    for (auto& thread : producers) {
        thread.join();
    }


    // ========================================================
    // Send STOP
    //
    // All normal tasks are already in queue because
    // all producers have finished.
    // ========================================================

    for (
        std::size_t i = 0;
        i < Consumer;
        ++i
    ) {

        queue.push(
            make_value(
                static_cast<std::size_t>(-1)
            )
        );
    }


    // ========================================================
    // Wait for consumers
    // ========================================================

    for (auto& thread : consumers) {
        thread.join();
    }


    const auto end =
        std::chrono::steady_clock::now();


    return std::chrono::duration<double, std::micro>(
        end - start
    ).count();
}


// ============================================================
// Blocking benchmark
// ============================================================

template <
    typename Queue,
    typename T,
    typename MakeValue,
    typename IsStop,
    std::size_t Producer,
    std::size_t Consumer
>
BenchmarkResult benchmark_blocking(
    const std::string& name,
    const std::string& data_type,
    std::size_t iterations,
    MakeValue make_value,
    IsStop is_stop)
{
    print_test_header(
        name,
        data_type,
        Producer,
        Consumer,
        iterations
    );


    // --------------------------------------------------------
    // Warmup
    // --------------------------------------------------------

    for (
        std::size_t round = 0;
        round < WARMUP_ROUNDS;
        ++round
    ) {

        Queue queue;

        run_blocking_once<
            Queue,
            T,
            MakeValue,
            IsStop,
            Producer,
            Consumer
        >(
            queue,
            iterations,
            make_value,
            is_stop
        );
    }


    // --------------------------------------------------------
    // Benchmark
    // --------------------------------------------------------

    std::vector<double> times;

    times.reserve(
        BENCHMARK_ROUNDS
    );


    for (
        std::size_t round = 0;
        round < BENCHMARK_ROUNDS;
        ++round
    ) {

        Queue queue;

        const double elapsed =
            run_blocking_once<
                Queue,
                T,
                MakeValue,
                IsStop,
                Producer,
                Consumer
            >(
                queue,
                iterations,
                make_value,
                is_stop
            );

        times.push_back(elapsed);
    }


    return make_result(
        name,
        data_type,
        Producer,
        Consumer,
        iterations,
        times
    );
}


// ################################################################
// #                                                              #
// #              Part 2: Lock-Free Queue family                #
// #                                                              #
// ################################################################


// ============================================================
// Lock-Free Queue
//
// Interface:
//
// queue.push(value) -> bool
// queue.pop(value)  -> bool
//
// ============================================================

template <
    typename Queue,
    typename T,
    typename MakeValue,
    typename IsStop,
    std::size_t Producer,
    std::size_t Consumer
>
double run_lockfree_once(
    Queue& queue,
    std::size_t iterations,
    MakeValue make_value,
    IsStop is_stop)
{
    StartBarrier barrier(
        Producer + Consumer
    );

    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;

    producers.reserve(Producer);
    consumers.reserve(Consumer);


    // ========================================================
    // Producers
    // ========================================================

    for (
        std::size_t producer_id = 0;
        producer_id < Producer;
        ++producer_id
    ) {

        producers.emplace_back(
            [&, producer_id]()
            {
                const std::size_t count =
                    get_producer_operations(
                        producer_id,
                        Producer,
                        iterations
                    );

                barrier.arrive_and_wait();

                for (
                    std::size_t j = 0;
                    j < count;
                    ++j
                ) {

                    T item =
                        make_value(j);

                    while (
                        !queue.push(item)
                    ) {

                        std::this_thread::yield();
                    }
                }
            }
        );
    }


    // ========================================================
    // Consumers
    // ========================================================

    for (
        std::size_t consumer_id = 0;
        consumer_id < Consumer;
        ++consumer_id
    ) {

        consumers.emplace_back(
            [&]()
            {
                barrier.arrive_and_wait();

                T item;

                while (true) {

                    if (queue.pop(item)) {

                        if (is_stop(item)) {
                            break;
                        }
                    }
                    else {

                        std::this_thread::yield();
                    }
                }
            }
        );
    }


    // ========================================================
    // Start
    // ========================================================

    barrier.start();

    const auto start =
        std::chrono::steady_clock::now();


    // ========================================================
    // Producers finish
    // ========================================================

    for (auto& thread : producers) {
        thread.join();
    }


    // ========================================================
    // STOP
    // ========================================================

    for (
        std::size_t i = 0;
        i < Consumer;
        ++i
    ) {

        T stop =
            make_value(
                static_cast<std::size_t>(-1)
            );

        while (!queue.push(stop)) {

            std::this_thread::yield();
        }
    }


    // ========================================================
    // Consumers finish
    // ========================================================

    for (auto& thread : consumers) {
        thread.join();
    }


    const auto end =
        std::chrono::steady_clock::now();


    return std::chrono::duration<double, std::micro>(
        end - start
    ).count();
}


// ============================================================
// Lock-Free benchmark
// ============================================================

template <
    typename Queue,
    typename T,
    typename MakeValue,
    typename IsStop,
    std::size_t Producer,
    std::size_t Consumer
>
BenchmarkResult benchmark_lockfree(
    const std::string& name,
    const std::string& data_type,
    std::size_t iterations,
    MakeValue make_value,
    IsStop is_stop)
{
    print_test_header(
        name,
        data_type,
        Producer,
        Consumer,
        iterations
    );


    // --------------------------------------------------------
    // Warmup
    // --------------------------------------------------------

    for (
        std::size_t round = 0;
        round < WARMUP_ROUNDS;
        ++round
    ) {

        Queue queue;

        run_lockfree_once<
            Queue,
            T,
            MakeValue,
            IsStop,
            Producer,
            Consumer
        >(
            queue,
            iterations,
            make_value,
            is_stop
        );
    }


    // --------------------------------------------------------
    // Benchmark
    // --------------------------------------------------------

    std::vector<double> times;

    times.reserve(
        BENCHMARK_ROUNDS
    );


    for (
        std::size_t round = 0;
        round < BENCHMARK_ROUNDS;
        ++round
    ) {

        Queue queue;

        const double elapsed =
            run_lockfree_once<
                Queue,
                T,
                MakeValue,
                IsStop,
                Producer,
                Consumer
            >(
                queue,
                iterations,
                make_value,
                is_stop
            );

        times.push_back(elapsed);
    }


    return make_result(
        name,
        data_type,
        Producer,
        Consumer,
        iterations,
        times
    );
}


// ################################################################
// #                                                              #
// #                         Main                                #
// #                                                              #
// ################################################################

int main()
{
    const std::size_t N =
        TOTAL_OPERATIONS;


    std::cout
        << "============================================================\n"
        << "                    Queue Benchmark\n"
        << "============================================================\n"
        << "Operations       : " << N << "\n"
        << "Warmup rounds    : " << WARMUP_ROUNDS << "\n"
        << "Benchmark rounds : " << BENCHMARK_ROUNDS << "\n"
        << "============================================================\n";


    std::vector<BenchmarkResult> results;


    // ========================================================
    // Value generators
    // ========================================================

    // --------------------------------------------------------
    // int
    //
    // 正常数据：
    //     0, 1, 2, ...
    //
    // STOP：
    //     -1
    // --------------------------------------------------------

    auto make_int =
        [](std::size_t value) -> int
        {
            if (value == static_cast<std::size_t>(-1)) {
                return -1;
            }

            return static_cast<int>(value);
        };


    auto is_stop_int =
        [](const int& value) -> bool
        {
            return value == -1;
        };


    // --------------------------------------------------------
    // shared_ptr<int>
    // --------------------------------------------------------

    auto make_shared_int =
        [](std::size_t value)
            -> std::shared_ptr<int>
        {
            if (value == static_cast<std::size_t>(-1)) {

                return std::make_shared<int>(-1);
            }

            return std::make_shared<int>(
                static_cast<int>(value)
            );
        };


    auto is_stop_shared_int =
        [](const std::shared_ptr<int>& value)
            -> bool
        {
            return value &&
                   *value == -1;
        };


    // ========================================================
    //                    1P1C
    // ========================================================

    std::cout
        << "\n\n"
        << "######################## 1P1C ########################\n";


    // --------------------------------------------------------
    // BlockingQueue<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            BlockingQueue<int>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            1,
            1
        >(
            "BlockingQueue",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // SpinBlockingQueue<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            SpinBlockingQueue<int>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            1,
            1
        >(
            "SpinBlockingQueue",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // SPSC<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_lockfree<
            LockFreeQueue<int, 4096>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            1,
            1
        >(
            "SPSCQueue(cap=4096)",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // BlockingQueue<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            BlockingQueue<std::shared_ptr<int>>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            1,
            1
        >(
            "BlockingQueue",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // --------------------------------------------------------
    // SpinBlockingQueue<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            SpinBlockingQueue<std::shared_ptr<int>>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            1,
            1
        >(
            "SpinBlockingQueue",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // --------------------------------------------------------
    // SPSC<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_lockfree<
            LockFreeQueue<std::shared_ptr<int>, 4096>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            1,
            1
        >(
            "SPSCQueue(cap=4096)",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // ========================================================
    //                    4P1C
    // ========================================================

    std::cout
        << "\n\n"
        << "######################## 4P1C ########################\n";


    // --------------------------------------------------------
    // BlockingQueue<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            BlockingQueue<int>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            4,
            1
        >(
            "BlockingQueue",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // SpinBlockingQueue<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            SpinBlockingQueue<int>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            4,
            1
        >(
            "SpinBlockingQueue",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // MPSC<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_lockfree<
            MPSCQueue<int, 4096>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            4,
            1
        >(
            "MPSCQueue(cap=4096)",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // BlockingQueue<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            BlockingQueue<std::shared_ptr<int>>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            4,
            1
        >(
            "BlockingQueue",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // --------------------------------------------------------
    // SpinBlockingQueue<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            SpinBlockingQueue<std::shared_ptr<int>>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            4,
            1
        >(
            "SpinBlockingQueue",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // --------------------------------------------------------
    // MPSC<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_lockfree<
            MPSCQueue<std::shared_ptr<int>, 4096>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            4,
            1
        >(
            "MPSCQueue(cap=4096)",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // ========================================================
    //                    4P4C
    // ========================================================

    std::cout
        << "\n\n"
        << "######################## 4P4C ########################\n";


    // --------------------------------------------------------
    // BlockingQueue<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            BlockingQueue<int>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            4,
            4
        >(
            "BlockingQueue",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // SpinBlockingQueue<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            SpinBlockingQueue<int>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            4,
            4
        >(
            "SpinBlockingQueue",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // MPMC<int>
    // --------------------------------------------------------

    results.push_back(
        benchmark_lockfree<
            MPMCQueue<int, 4096>,
            int,
            decltype(make_int),
            decltype(is_stop_int),
            4,
            4
        >(
            "MPMCQueue(cap=4096)",
            "int",
            N,
            make_int,
            is_stop_int
        )
    );


    // --------------------------------------------------------
    // BlockingQueue<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            BlockingQueue<std::shared_ptr<int>>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            4,
            4
        >(
            "BlockingQueue",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // --------------------------------------------------------
    // SpinBlockingQueue<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_blocking<
            SpinBlockingQueue<std::shared_ptr<int>>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            4,
            4
        >(
            "SpinBlockingQueue",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // --------------------------------------------------------
    // MPMC<shared_ptr<int>>
    // --------------------------------------------------------

    results.push_back(
        benchmark_lockfree<
            MPMCQueue<std::shared_ptr<int>, 4096>,
            std::shared_ptr<int>,
            decltype(make_shared_int),
            decltype(is_stop_shared_int),
            4,
            4
        >(
            "MPMCQueue(cap=4096)",
            "shared_ptr<int>",
            N,
            make_shared_int,
            is_stop_shared_int
        )
    );


    // ========================================================
    // Summary
    // ========================================================

    std::cout
        << "\n\n"
        << "============================================================\n"
        << "                         SUMMARY\n"
        << "============================================================\n";


    std::cout
        << std::left
        << std::setw(28)
        << "Queue"

        << " | "
        << std::setw(14)
        << "Data Type"

        << " | "
        << std::setw(6)
        << "Mode"

        << " | "
        << std::setw(14)
        << "Avg(us)"

        << " | "
        << std::setw(16)
        << "Throughput"

        << "\n";


    std::cout
        << "------------------------------------------------------------\n";


    for (const auto& r : results) {

        std::string mode =
            std::to_string(r.producers)
            + "P"
            + std::to_string(r.consumers)
            + "C";


        std::cout
            << std::left
            << std::setw(28)
            << r.name

            << " | "
            << std::setw(14)
            << r.data_type

            << " | "
            << std::setw(6)
            << mode

            << " | "
            << std::fixed
            << std::setprecision(2)
            << std::setw(14)
            << r.avg_time_us

            << " | "
            << std::setw(14)
            << r.throughput_ops
            << " ops/s"

            << "\n";
    }


    std::cout
        << "============================================================\n";


    return 0;
}