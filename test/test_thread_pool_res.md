---------------------

最原始的实现。使用 std::mutex + std::queue

===== ThreadPool Performance Benchmark =====
Hardware concurrency: 20 cores

[Empty Task] threads=1, tasks=10000
  Total time: 1.99 ms
  Throughput: 5021845 tasks/sec
  Avg task time: 0.000 ms

[Empty Task] threads=2, tasks=10000
  Total time: 2.17 ms
  Throughput: 4608932 tasks/sec
  Avg task time: 0.000 ms

[Empty Task] threads=4, tasks=10000
  Total time: 4.81 ms
  Throughput: 2079305 tasks/sec
  Avg task time: 0.000 ms

[Empty Task] threads=8, tasks=10000
  Total time: 12.96 ms
  Throughput: 771742 tasks/sec
  Avg task time: 0.001 ms

[Empty Task] threads=16, tasks=10000
  Total time: 27.45 ms
  Throughput: 364340 tasks/sec
  Avg task time: 0.003 ms

[CPU Task] threads=1, tasks=10000, iter/task=1000
  Total time: 14.41 ms
  Throughput: 694088 tasks/sec
  Avg task time: 0.001 ms
  Sum result (verify): 4995000000

[CPU Task] threads=2, tasks=10000, iter/task=1000
  Total time: 8.08 ms
  Throughput: 1238344 tasks/sec
  Avg task time: 0.001 ms
  Sum result (verify): 4995000000

[CPU Task] threads=4, tasks=10000, iter/task=1000
  Total time: 5.88 ms
  Throughput: 1700044 tasks/sec
  Avg task time: 0.001 ms
  Sum result (verify): 4995000000

[CPU Task] threads=8, tasks=10000, iter/task=1000
  Total time: 9.04 ms
  Throughput: 1106550 tasks/sec
  Avg task time: 0.001 ms
  Sum result (verify): 4995000000

[CPU Task] threads=16, tasks=10000, iter/task=1000
  Total time: 29.50 ms
  Throughput: 338947 tasks/sec
  Avg task time: 0.003 ms
  Sum result (verify): 4995000000

[I/O Task] threads=4, tasks=100, sleep=5ms
  Total time: 394.21 ms
  Throughput: 254 tasks/sec
  Avg task time: 3.942 ms

[I/O Task] threads=8, tasks=100, sleep=5ms
  Total time: 200.03 ms
  Throughput: 500 tasks/sec
  Avg task time: 2.000 ms

[Empty Task] threads=8, tasks=100000
  Total time: 81.19 ms
  Throughput: 1231674 tasks/sec
  Avg task time: 0.001 ms

[CPU Task] threads=8, tasks=100000, iter/task=100
  Total time: 77.58 ms
  Throughput: 1288974 tasks/sec
  Avg task time: 0.001 ms
  Sum result (verify): 495000000

[Edge: threads > tasks] threads=8, tasks=3 (CPU task)
  Total time: 20.88 ms
  Counter: 3 (should be 3)

===== Benchmark Finished =====
-----------