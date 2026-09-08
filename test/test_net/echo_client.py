#!/usr/bin/env python3
"""
Echo Server 压测工具（支持批量流水线）
用法示例：
    python echo_bench_batch.py --concurrency 100 --total 1000000 --data-size 64 --batch 100
"""

import asyncio
import argparse
import time
import random
import sys
from statistics import mean, median

# ---------- 统计工具 ----------
def percentile(data, p):
    if not data:
        return 0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100.0)
    f = int(k)
    c = k - f
    if f + 1 < len(sorted_data):
        return sorted_data[f] + c * (sorted_data[f + 1] - sorted_data[f])
    return sorted_data[f]

def format_duration(seconds):
    if seconds < 1:
        return f"{seconds*1000:.2f} ms"
    return f"{seconds:.2f} s"

def print_stats(total_requests, errors, latencies, total_time, data_size):
    success = total_requests - errors
    qps = success / total_time if total_time > 0 else 0

    print("\n========== 压测结果 ==========")
    print(f"总请求数:        {total_requests}")
    print(f"成功数:          {success}")
    print(f"失败数:          {errors}")
    print(f"成功率:          {success/total_requests*100:.2f}%")
    print(f"总耗时:          {format_duration(total_time)}")
    print(f"QPS:             {qps:.2f} req/s")
    if data_size:
        throughput = (success * data_size) / total_time / (1024 * 1024)
        print(f"吞吐量:          {throughput:.2f} MB/s")

    if latencies:
        print("\n延迟统计 (秒):")
        print(f"  最小值:        {min(latencies):.6f}")
        print(f"  平均值:        {mean(latencies):.6f}")
        print(f"  中位数:        {median(latencies):.6f}")
        print(f"  最大值:        {max(latencies):.6f}")
        for p in [50, 90, 95, 99]:
            print(f"  P{p:2d}:          {percentile(latencies, p):.6f}")
    print("================================\n")

# ---------- 工作协程（支持批量） ----------
async def worker(worker_id, host, port, data, num_requests, timeout, verify, batch):
    """每个 worker 使用批量发送模式"""
    success = 0
    errors = 0
    latencies = []  # 存储每个请求的延迟（批量时取平均）

    # 建立连接
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except Exception as e:
        return 0, num_requests, []

    try:
        # 按批次处理
        for i in range(0, num_requests, batch):
            current_batch = min(batch, num_requests - i)

            # 构建批量数据（拼接）
            batch_data = data * current_batch

            start = time.perf_counter()
            try:
                # 发送
                writer.write(batch_data)
                await asyncio.wait_for(writer.drain(), timeout=timeout)

                # 接收等长的响应
                response = await asyncio.wait_for(
                    reader.readexactly(len(batch_data)),
                    timeout=timeout
                )
                end = time.perf_counter()

                # 可选校验
                if verify and response != batch_data:
                    errors += current_batch
                else:
                    success += current_batch
                    # 记录该批次的平均单次延迟
                    latencies.append((end - start) / current_batch)

            except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError) as e:
                errors += current_batch
                # 尝试重连（简化处理：断开并重新连接）
                try:
                    writer.close()
                    await writer.wait_closed()
                except:
                    pass
                try:
                    reader, writer = await asyncio.open_connection(host, port)
                except:
                    # 重连失败，后续请求全部失败
                    remaining = num_requests - (success + errors)
                    errors += remaining
                    break

    finally:
        writer.close()
        await writer.wait_closed()   # 正确写法！

    return success, errors, latencies

async def run_benchmark(host, port, data_size, concurrency, total_requests,
                        timeout, verify, random_payload, batch):
    # 准备 payload
    if random_payload:
        data = bytes(random.getrandbits(8) for _ in range(data_size))
    else:
        data = b'X' * data_size

    # 分配请求数
    base = total_requests // concurrency
    remainder = total_requests % concurrency
    per_worker = [base + (1 if i < remainder else 0) for i in range(concurrency)]

    print(f"启动 {concurrency} 个并发连接，总请求数 {total_requests}，数据大小 {data_size} 字节，批量大小 {batch}")
    start_time = time.perf_counter()

    tasks = []
    for i, n in enumerate(per_worker):
        if n > 0:
            tasks.append(worker(i, host, port, data, n, timeout, verify, batch))

    results = await asyncio.gather(*tasks)

    end_time = time.perf_counter()
    total_time = end_time - start_time

    total_success = 0
    total_errors = 0
    all_latencies = []
    for succ, err, lat in results:
        total_success += succ
        total_errors += err
        all_latencies.extend(lat)

    total_requests_sent = total_success + total_errors
    print_stats(total_requests_sent, total_errors, all_latencies, total_time, data_size)

def parse_args():
    parser = argparse.ArgumentParser(description="Echo Server 批量压测工具")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--concurrency", type=int, default=100, help="并发连接数")
    parser.add_argument("--total", type=int, default=10000, help="总请求数")
    parser.add_argument("--data-size", type=int, default=1024, help="每个请求的数据大小 (字节)")
    parser.add_argument("--timeout", type=float, default=5.0, help="每个请求的超时时间 (秒)")
    parser.add_argument("--verify", action="store_true", help="启用 payload 校验")
    parser.add_argument("--random", action="store_true", help="使用随机 payload")
    parser.add_argument("--batch", type=int, default=1, help="每次批量发送的请求数 (默认 1, 即无批量)")
    return parser.parse_args()

def main():
    args = parse_args()
    try:
        asyncio.run(run_benchmark(
            host=args.host,
            port=args.port,
            data_size=args.data_size,
            concurrency=args.concurrency,
            total_requests=args.total,
            timeout=args.timeout,
            verify=args.verify,
            random_payload=args.random,
            batch=args.batch
        ))
    except KeyboardInterrupt:
        print("\n压测被用户中断")
        sys.exit(1)

if __name__ == "__main__":
    main()