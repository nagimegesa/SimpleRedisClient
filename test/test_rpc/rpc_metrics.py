#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RpcServer 指标测试客户端（QPS / 延迟分布 / 吞吐 / 并发扩展 / 流式帧率 / 错误路径）。

这是 `test_rpc_client.py`（冒烟）、`test_client_error.py`（协议一致性）之外的
第三类脚本：不改动服务端，只用 `rpc_client.py` 里的裸协议客户端从**客户端侧**
测服务端在各类负载下的指标。

场景
----
============  ==================================================  ==========================
场景           负载                                                指标
============  ==================================================  ==========================
latency       单连接顺序 unary                                      QPS、延迟 min/avg/p50/p90/p95/p99/max
pipeline      单连接滑动窗口（始终 N 个在途，1/8/32/128）          在途深度 → QPS / 延迟曲线
concurrency   C 条连接 × 每连接 W 个在途（滑动窗口）                  聚合 QPS、单连接 QPS、扩展比、饱和点
payload       报文大小扫描（16B/1KB/60KB）                            小/大包 QPS、网络吞吐 MB/s
stream_pingpong 单流逐帧请求-响应                                   帧 RTT 分布、帧率
stream_burst  单流一次性灌 N 帧后收完                                帧率、首帧时延、整条流耗时
stream_parallel S 条连接各跑一条流并行                                聚合帧率、吞吐
stream_mux    单连接多路复用 F 条流                                  单连接聚合帧率、每流耗时
errors        错误路径（未知函数 100 / 坏 protobuf 101 / 坏魔数 1）    错误处理 QPS、错误码分布、正确率
connect       短连接（connect + 1 次 unary + close）                 建连速率 conn/s、全程延迟
mixed         unary 连接与 stream 连接同时压                        混合负载下各自吞吐
============  ==================================================  ==========================

用法
----
    python3 rpc_metrics.py                                  # 全部场景（默认 127.0.0.1:8891）
    python3 rpc_metrics.py --scenarios latency,concurrency,stream_mux
    python3 rpc_metrics.py --total 5000 --connections 1,2,4,8,16,32,64
    python3 rpc_metrics.py --payload-sizes 16,1024,8000,60000 --json metrics.json
    RPC_PORT=8899 python3 rpc_metrics.py

    # 打 main 分支的老协议服务端（无 stream_id，hello -> world 语义）
    python3 rpc_metrics.py --protocol legacy --check world --scenarios pipeline,concurrency

说明
----
* 速率 = 完成数 / 场景总耗时，计时从**建连之前**开始（各场景口径一致）；延迟只统计
  send → recv 的往返部分，所以速率与延迟不是严格倒数关系。
* 和「N 线程 / N 个在途」的客户端对比时，要用 `--windows`（单连接）或
  `--concurrency-window`（多连接）把在途数对齐；`pipeline` 是滑动窗口（常量在途），
  不是「批发送再批排空」。单进程 Python 客户端大约只能吃掉 1 个核（实测 ~20µs CPU/请求），
  要压出服务端上限请并行跑多个进程。
* 服务端 `src/rpc_server.cpp` 把日志级别设成 DEBUG，打印会明显拉低测量值；想要
  干净的数据请先把日志级别调成 WARNING 再跑。
* 连接被 RST / 超时等异常路径不会中断整轮：未完成的请求按数量计入失败，仍然会打印
  汇总并写出 JSON，最后以退出码 1 结束。
* 客户端是单进程 asyncio，`--total` 很大时客户端自身也可能成为瓶颈（CPU 打满），
  看结果时注意结合 `--connections` 与每秒 QPS 抖动判断。
* 退出码：0 全部场景无失败；1 有场景出现失败 / 内容不一致；2 前置健康检查不通过。
"""

import argparse
import asyncio
import json
import os
import platform
import sys
import time
from collections import OrderedDict

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from rpc_client import (  # noqa: E402
    FUNC, HOST, PORT, PROTOCOL_LEGACY, PROTOCOL_NEW, REQUEST, REQ_ERROR, RESPONSE,
    SERVICE, STREAM_END, STREAM_FUNC, STREAM_RESPONSE, TIMEOUT, AsyncRpcClient,
    ProtocolError, QpsTimeline, RpcError, StreamMux, build_frame,
    build_request_frame, build_stream_frames, error_name, latency_summary,
    make_request, next_request_id, parse_response, percentile,
)


# ================================================================ 统计容器


class Bench:
    """一个场景（一次测量）的统计容器。"""

    def __init__(self, name, title, unit="req", **params):
        self.name = name
        self.title = title
        self.unit = unit          # 计量单位：req（请求）或 frame（流式帧）
        self.params = params
        self.count = 0          # 成功完成的请求 / 帧
        self.failed = 0         # RPC 错误帧 / 超时 / 协议错误
        self.mismatch = 0       # 有响应但内容或错误码不符
        self.error_codes = {}   # 错误码 -> 次数
        self.latencies = []     # 单请求 / 单帧延迟（秒）
        self.durations = []     # 会话级耗时（流式整条流、短连接全程）
        self.timeline = QpsTimeline()
        self.socket_sent = 0
        self.socket_recv = 0
        self.extra = {}
        self.notes = []
        self.planned = None     # 计划完成的请求 / 帧数（用于校验记账是否遗漏）
        self.t0 = None
        self.t1 = None

    # ---------- 采集 ----------

    def start(self):
        self.t0 = time.perf_counter()
        self.timeline = QpsTimeline(self.t0)
        return self

    def stop(self):
        self.t1 = time.perf_counter()
        return self

    def ok(self, latency=None, ts=None):
        self.count += 1
        if latency is not None:
            self.latencies.append(latency)
        self.timeline.add(ts)

    def frames(self, n, ts=None):
        """流式场景：n 帧到达。"""
        self.count += n
        self.timeline.add_many(n, ts)

    def duration(self, seconds):
        self.durations.append(seconds)

    def fail(self, code=None, count=1):
        """记失败；count > 1 用于「一批请求因连接异常整体未完成」的情况。"""
        self.failed += count
        key = error_name(code) if code is not None else "NO_RESPONSE"
        self.error_codes[key] = self.error_codes.get(key, 0) + count

    def bad(self):
        self.mismatch += 1

    def io(self, sent=0, recv=0):
        self.socket_sent += sent
        self.socket_recv += recv

    def note(self, text):
        if text not in self.notes:
            self.notes.append(text)

    # ---------- 汇总 ----------

    @property
    def wall(self):
        if self.t0 is None:
            return 0.0
        return (self.t1 if self.t1 is not None else time.perf_counter()) - self.t0

    @property
    def attempts(self):
        return self.count + self.failed

    def to_dict(self):
        wall = self.wall
        lat = latency_summary(self.latencies)
        out = {
            "name": self.name,
            "title": self.title,
            "unit": self.unit,
            "params": self.params,
            "count": self.count,
            "failed": self.failed,
            "mismatch": self.mismatch,
            "attempts": self.attempts,
            "accounted": self.count + self.failed,
            "planned": self.planned,
            "error_codes": self.error_codes,
            "wall_seconds": wall,
            "qps": (self.count / wall) if wall > 0 else 0.0,
            "attempt_rate": (self.attempts / wall) if wall > 0 else 0.0,
            "latency_ms": ({k: v * 1000 for k, v in lat.items() if k != "count"}
                           if self.latencies else None),
            "durations_ms": ([d * 1000 for d in self.durations] if self.durations else None),
            "timeline": self.timeline.summary(),
            "socket_bytes_sent": self.socket_sent,
            "socket_bytes_recv": self.socket_recv,
            "throughput_mbps": ((self.socket_sent + self.socket_recv) / wall / 1e6)
                               if wall > 0 else 0.0,
            "extra": self.extra,
            "notes": self.notes,
        }
        if self.planned is not None:
            out["unaccounted"] = self.planned - (self.count + self.failed)
        return out


def fmt_sec(s):
    if s < 1:
        return "%.1f ms" % (s * 1000)
    return "%.3f s" % s


def fmt_mb(nbytes):
    if nbytes >= 1024 * 1024:
        return "%.2f MB" % (nbytes / 1024.0 / 1024.0)
    if nbytes >= 1024:
        return "%.1f KB" % (nbytes / 1024.0)
    return "%d B" % nbytes


def fmt_lat(lat_ms, key):
    if not lat_ms or key not in lat_ms:
        return "-"
    return "%.3f" % lat_ms[key]


# ================================================================ 负载工具


def make_msg(size, seq=0):
    """生成恰好 size 字节（ASCII）的请求体，便于做大小扫描与内容校验。"""
    if size <= 0:
        return ""
    base = "m%d" % seq
    return (base + "x" * size)[:size]


def payload_and_expect(cfg, index):
    """返回 (请求体, 期望响应)。`--check world` 用来对齐 main 分支的 hello 语义。"""
    if cfg.check == "world":
        return "hello", "world"
    msg = make_msg(cfg.payload_size, index)
    return msg, (None if cfg.check == "off" else msg)


def new_client(cfg):
    """按 --protocol 建连接。"""
    return AsyncRpcClient.connect(cfg.host, cfg.port, cfg.timeout, cfg.protocol)


async def recv_matching(client, request_ids, timeout=None):
    """读到 request_id 属于 request_ids 的一帧；其余帧丢弃（本脚本每连接只跑一类负载）。"""
    while True:
        frame = await client.recv_frame(timeout)
        if frame.request_id in request_ids:
            return frame


async def read_error_async(client, timeout=None):
    """读第一帧 REQ_ERROR（跳过它之前的其它帧）。"""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + (client.timeout if timeout is None else timeout)
    while True:
        remain = deadline - loop.time()
        if remain <= 0:
            raise TimeoutError("没有收到错误帧")
        frame = await client.recv_frame(remain)
        if frame.type == REQ_ERROR:
            return frame


async def http_alive(cfg):
    """健康检查：一次 echo。返回 (ok, rtt_seconds, 响应内容)。"""
    client = await new_client(cfg)
    try:
        t0 = time.perf_counter()
        resp = await client.call(make_request("alive"))
        return resp.res == "alive", time.perf_counter() - t0, resp.res
    finally:
        await client.aclose()


# ================================================================ unary 负载


async def unary_worker(cfg, bench, n_requests, window=1):
    """一条连接上的 unary 负载。

    window == 1：顺序调用（发一个等一个），延迟就是真实往返时延。
    window > 1 ：**滑动窗口**，始终维持 window 个请求在途：每收回一个响应立刻补发
                 下一个。这一点很关键——批发送再批排空（barrier）会在排空阶段留下
                 「一个请求都没在途」的气泡，测出来的 QPS 明显偏低，也和外层
                 「N 线程并发 / N 个在途」的客户端不可比。
    单请求延迟用各自发送时刻到响应到达时刻计算（含排队时间）。

    发送与接收都在同一个 try 里：连接被 RST / 超时 / 协议错误时，这条连接上
    本应完成但没完成的请求全部按数量记失败，绝不把异常抛回 gather 之外。
    """
    window = max(1, window)
    client = await new_client(cfg)
    sent0, recv0 = client.bytes_sent, client.bytes_recv
    try:
        pending = {}       # request_id -> (发时刻, 请求体, 期望响应)
        next_index = 0
        processed = 0

        async def send_one(index):
            msg, expect = payload_and_expect(cfg, index)
            request_id, frame = build_request_frame(make_request(msg),
                                                    protocol=cfg.protocol)
            t0 = time.perf_counter()
            await client.send_frames([frame])
            pending[request_id] = (t0, msg, expect)

        try:
            # 填满窗口
            while next_index < n_requests and len(pending) < window:
                await send_one(next_index)
                next_index += 1

            while pending:
                frame = await recv_matching(client, pending)
                t0, msg, expect = pending.pop(frame.request_id)
                t1 = time.perf_counter()
                processed += 1
                if frame.type == REQ_ERROR:
                    bench.fail(frame.error_code)
                elif frame.type != RESPONSE:
                    bench.fail(None)
                    bench.note("期望 RESPONSE，收到 type=%s" % frame.type)
                else:
                    bench.ok(t1 - t0, t1)
                    if expect is not None and parse_response(frame).res != expect:
                        bench.bad()
                # 收回一个就补发一个，窗口始终填满（无气泡）
                if next_index < n_requests:
                    await send_one(next_index)
                    next_index += 1
        except (TimeoutError, ConnectionError, OSError, ProtocolError, RpcError) as e:
            missing = n_requests - processed
            bench.fail(None, count=missing)
            bench.note("连接中断，%d 个请求未完成（其中 %d 个未及发出）: %s: %s"
                       % (missing, missing - len(pending), type(e).__name__, e))
            return
    finally:
        bench.io(client.bytes_sent - sent0, client.bytes_recv - recv0)
        await client.aclose()


async def run_unary_bench(cfg, bench, connections, total=None, window=1):
    """在 connections 条连接上并发跑 unary，n_requests 均分。

    注意：`bench.start()` 在建立连接之前，所以 QPS 的分母（wall）含建连时间，
    各场景口径一致。
    """
    window = max(1, window)
    total = cfg.total if total is None else total
    per = max(1, total // max(1, connections))
    actual = per * connections
    bench.params.update({"connections": connections, "window": window,
                         "requests_per_conn": per, "payload_size": cfg.payload_size})
    bench.planned = actual
    bench.start()
    await asyncio.gather(*[unary_worker(cfg, bench, per, window)
                           for _ in range(connections)])
    bench.stop()
    bench.extra["requests"] = actual
    # Little's law：平均在途请求数 ≈ QPS × 平均延迟，用来解释「吞吐 = 在途 / 延迟」
    if bench.latencies and bench.wall > 0:
        mean_lat = sum(bench.latencies) / len(bench.latencies)
        bench.extra["in_flight_est"] = (bench.count / bench.wall) * mean_lat
    return bench


# ================================================================ 各场景


async def scenario_latency(cfg, ctx):
    """单连接顺序 unary：时延基线 + QPS 基线。"""
    b = Bench("latency", "单连接顺序 unary")
    await run_unary_bench(cfg, b, connections=1)
    lat = latency_summary(b.latencies)
    if lat.get("count"):
        b.note("p99/p50 = %.2fx" % (lat["p99"] / lat["p50"] if lat["p50"] else 0))
    ctx["base_qps"] = b.to_dict()["qps"]
    return [b]


async def scenario_pipeline(cfg, ctx):
    """单连接流水线：观察窗口深度对 QPS / 延迟的影响。"""
    out = []
    for window in cfg.windows:
        b = Bench("pipeline_%d" % window, "单连接流水线 window=%d" % window)
        await run_unary_bench(cfg, b, connections=1, window=window)
        if ctx.get("base_qps"):
            ratio = b.to_dict()["qps"] / ctx["base_qps"]
            b.note("相对顺序调用 QPS %.2fx（服务端同一连接内串行 dispatch，流水线主要省 RTT）"
                   % ratio)
        out.append(b)
    return out


async def scenario_concurrency(cfg, ctx):
    """多连接并发 unary：聚合 QPS、扩展比、饱和点。"""
    out = []
    for connections in cfg.connections:
        b = Bench("concurrency_%d" % connections, "%d 连接并发 unary" % connections)
        await run_unary_bench(cfg, b, connections=connections,
                              window=cfg.concurrency_window)
        out.append(b)

    # 扩展比基线：优先用 latency 场景的单连接 QPS，其次用本轮 connections=1 的结果
    base = ctx.get("base_qps") or 0.0
    if not base:
        for b in out:
            if b.params.get("connections") == 1:
                base = b.to_dict()["qps"]
                break
    for b in out:
        d = b.to_dict()
        connections = b.params.get("connections", 1)
        b.extra["qps_per_conn"] = d["qps"] / max(1, connections)
        if base:
            b.extra["speedup_vs_1conn"] = d["qps"] / base
            b.note("相对单连接 QPS %.2fx，单连接均值 %.0f req/s"
                   % (d["qps"] / base, d["qps"] / max(1, connections)))

    if out:
        # 饱和点：找到「再加连接，QPS 提升不足 10%」的最小连接数
        levels = [(b.params.get("connections", 1), b.to_dict()["qps"]) for b in out]
        levels.sort()
        best_c, best_qps = max(levels, key=lambda kv: kv[1])
        sat_c = None
        for i in range(len(levels) - 1):
            c, q = levels[i]
            if q > 0 and levels[i + 1][1] / q < 1.10:
                sat_c = c
                break
        if sat_c is not None:
            ctx["notes"].append(
                "并发扩展：%d 连接起 QPS 基本饱和（%.0f req/s，最高在 %d 连接 %.0f req/s）；"
                "可能受服务端线程池默认 2 线程（RpcServerImpl 构造 threadPool_(2)）限制。"
                % (sat_c, dict(levels)[sat_c], best_c, best_qps))
        else:
            ctx["notes"].append(
                "并发扩展：测试范围内 QPS 随连接数持续上升，最高 %d 连接 %.0f req/s。"
                % (best_c, best_qps))
    return out


async def scenario_payload(cfg, ctx):
    """报文大小扫描：小包看 QPS，大包看吞吐。"""
    out = []
    for size in cfg.payload_sizes:
        # 大包单次成本高，请求数按大小收敛，避免默认参数跑太久
        total = cfg.total if size <= 1024 else max(50, cfg.total // 10)
        b = Bench("payload_%d" % size, "单连接 unary payload=%dB" % size)
        old = cfg.payload_size
        cfg.payload_size = size
        try:
            await run_unary_bench(cfg, b, connections=1, total=total)
        finally:
            cfg.payload_size = old
        d = b.to_dict()
        b.extra["per_payload_mbps"] = (b.count * size * 2) / b.wall / 1e6 if b.wall else 0.0
        ctx["notes"].append(
            "payload %dB：QPS %.0f，网络吞吐 %.2f MB/s，p99 %.3f ms"
            % (size, d["qps"], d["throughput_mbps"], (d["latency_ms"] or {}).get("p99", 0)))
        out.append(b)
    return out


async def stream_pingpong(cfg, bench):
    """单流逐帧请求-响应：帧 RTT 分布。"""
    client = await new_client(cfg)
    sent0, recv0 = client.bytes_sent, client.bytes_recv
    request_id = next_request_id()
    frames = cfg.stream_frames
    bench.planned = frames
    t_start = time.perf_counter()
    recorded = 0
    try:
        for i in range(1, frames + 1):
            msg, _expect = payload_and_expect(cfg, i)
            t0 = time.perf_counter()
            await client.send_stream_request(request_id, i, make_request(msg))
            frame = await recv_matching(client, {request_id})
            t1 = time.perf_counter()
            if frame.type == REQ_ERROR:
                bench.fail(frame.error_code, count=max(1, frames - recorded))
                return
            if frame.type != STREAM_RESPONSE or frame.stream_id != i:
                bench.fail(None, count=max(1, frames - recorded))
                bench.note("期望 STREAM_RESPONSE(stream_id=%d)，收到 type=%s stream_id=%s"
                           % (i, frame.type, frame.stream_id))
                return
            bench.ok(t1 - t0, t1)
            recorded += 1
            if parse_response(frame).res != msg:
                bench.bad()

        t_end = time.perf_counter()
        await client.send_stream_end(request_id, frames + 1)
        frame = await recv_matching(client, {request_id})
        t_close = time.perf_counter()
        if frame.type != STREAM_END:
            bench.fail(frame.error_code if frame.type == REQ_ERROR else None)
        else:
            bench.extra["stream_end_rtt_ms"] = (t_close - t_end) * 1000
            bench.duration(t_close - t_start)
    except (TimeoutError, ConnectionError, OSError, ProtocolError, RpcError) as e:
        bench.fail(getattr(e, "code", None), count=max(1, frames - recorded))
        bench.note("逐帧流中断（已记账 %d/%d 帧）: %s: %s"
                   % (recorded, frames, type(e).__name__, e))
    finally:
        bench.io(client.bytes_sent - sent0, client.bytes_recv - recv0)
        await client.aclose()


async def stream_burst_conn(cfg, bench):
    """一条连接上：一次性发 N 帧 + STREAM_END，再收完。"""
    client = await new_client(cfg)
    sent0, recv0 = client.bytes_sent, client.bytes_recv
    frames = cfg.stream_frames
    msgs = [payload_and_expect(cfg, i)[0] for i in range(1, frames + 1)]
    request_id, wire = build_stream_frames([make_request(m) for m in msgs])
    bench.planned = (bench.planned or 0) + frames
    got = 0
    first = None
    try:
        t0 = time.perf_counter()
        await client.send_frames(wire)
        while True:
            frame = await recv_matching(client, {request_id})
            t = time.perf_counter()
            if frame.type == REQ_ERROR:
                bench.fail(frame.error_code)
                return
            if frame.type == STREAM_RESPONSE:
                got += 1
                if first is None:
                    first = t
                bench.frames(1, t)
                if frame.stream_id != got or parse_response(frame).res != msgs[got - 1]:
                    bench.bad()
            elif frame.type == STREAM_END:
                bench.duration(t - t0)
                if first is not None:
                    bench.extra["ttfb_ms"] = (first - t0) * 1000
                return
            else:
                bench.fail(None)
                bench.note("未预期的帧类型 %s" % frame.type)
                return
    except (TimeoutError, ConnectionError, OSError, ProtocolError, RpcError) as e:
        # 断流时把这条流上「已发出但没回帧」的请求按失败计数
        bench.fail(getattr(e, "code", None), count=max(1, frames - got))
        bench.note("突发流中断（已收 %d/%d 帧）: %s: %s"
                   % (got, frames, type(e).__name__, e))
    finally:
        bench.io(client.bytes_sent - sent0, client.bytes_recv - recv0)
        await client.aclose()


async def scenario_stream(cfg, ctx):
    """流式的四个子场景：逐帧 / 单流突发 / 多连接并行 / 单连接多路复用。"""
    out = []

    b1 = Bench("stream_pingpong", "单流逐帧请求-响应", unit="frame")
    b1.params.update({"frames": cfg.stream_frames, "payload_size": cfg.payload_size})
    b1.start()
    await stream_pingpong(cfg, b1)
    b1.stop()
    b1.extra["frames_per_conn"] = cfg.stream_frames
    out.append(b1)

    b2 = Bench("stream_burst", "单流突发 N 帧", unit="frame")
    b2.params.update({"frames": cfg.stream_frames, "payload_size": cfg.payload_size})
    b2.start()
    await stream_burst_conn(cfg, b2)
    b2.stop()
    out.append(b2)

    b3 = Bench("stream_parallel", "%d 连接并行流" % cfg.stream_connections, unit="frame")
    b3.params.update({"connections": cfg.stream_connections, "frames": cfg.stream_frames,
                      "payload_size": cfg.payload_size})
    b3.start()
    await asyncio.gather(*[stream_burst_conn(cfg, b3)
                           for _ in range(cfg.stream_connections)])
    b3.stop()
    if b3.durations:
        b3.extra["avg_session_ms"] = sum(b3.durations) / len(b3.durations) * 1000
    out.append(b3)

    b4 = Bench("stream_mux", "单连接多路复用 %d 条流" % cfg.stream_multiplex, unit="frame")
    b4.params.update({"streams": cfg.stream_multiplex, "frames": cfg.stream_frames,
                      "payload_size": cfg.payload_size})
    await stream_mux(cfg, b4)
    out.append(b4)

    ctx["notes"].append(
        "流式：单流突发 %.0f 帧/s；%d 连接并行 %.0f 帧/s；单连接 %d 路复用 %.0f 帧/s。"
        % (b2.to_dict()["qps"], cfg.stream_connections, b3.to_dict()["qps"],
           cfg.stream_multiplex, b4.to_dict()["qps"]))
    return out


async def stream_mux(cfg, bench):
    """单连接上多路复用 F 条流（每条 stream_id 独立递增）。"""
    frames = cfg.stream_frames
    bench.start()   # 先计时再建连，与 unary 场景的 QPS 分母口径一致（含建连）
    bench.planned = (bench.planned or 0) + cfg.stream_multiplex * frames
    client = await new_client(cfg)
    sent0, recv0 = client.bytes_sent, client.bytes_recv
    mux = StreamMux(client, SERVICE)
    t0 = bench.t0
    try:
        await mux.start()

        async def one_stream():
            session = mux.open(STREAM_FUNC, frames)
            msgs = [payload_and_expect(cfg, i)[0] for i in range(1, frames + 1)]
            received = 0
            try:
                for i in range(1, frames + 1):
                    await session.send_request(i, make_request(msgs[i - 1]))
                await session.send_end(frames + 1)
                responses = await session.collect()
                received = len(responses)
                now = time.perf_counter()
                if [r.res for r in responses] != msgs:
                    bench.bad()
                bench.frames(len(responses), now)
                if session.first_frame_at is not None:
                    bench.extra.setdefault("ttfb_ms", (session.first_frame_at - t0) * 1000)
                bench.duration(now - t0)
            except (RpcError, ProtocolError, TimeoutError, ConnectionError, OSError) as e:
                bench.fail(getattr(e, "code", None), count=max(1, frames - received))
                bench.note("多路复用流失败（已收 %d/%d 帧）: %s: %s"
                           % (received, frames, type(e).__name__, e))

        await asyncio.gather(*[one_stream() for _ in range(cfg.stream_multiplex)])
        bench.extra["stray_frames"] = mux.stray_frames
        # 每秒速率按「整条流收完」的时刻记账，粒度粗于逐帧到达
        bench.note("每秒速率按整条流收完时刻记账，抖动列不与逐帧场景直接可比")
    finally:
        bench.stop()
        await mux.stop()
        if mux.error is not None:
            # 帧的缺口已经由各条流的 collect 失败按数量记账，这里只记录 reader 退出的原因，
            # 不再额外加一次失败（否则会重复计数）。
            bench.note("多路复用 reader 退出: %s: %s"
                       % (type(mux.error).__name__, mux.error))
        bench.io(client.bytes_sent - sent0, client.bytes_recv - recv0)
        await client.aclose()


async def scenario_errors(cfg, ctx):
    """错误路径：吞吐 + 错误码分布 + 服务端是否存活。"""
    b = Bench("errors", "错误路径吞吐与错误码", total=cfg.errors)
    b.planned = cfg.errors
    b.start()   # 同上：QPS 分母含建连
    client = await new_client(cfg)
    sent0, recv0 = client.bytes_sent, client.bytes_recv
    try:
        for i in range(cfg.errors):
            kind = i % 3
            if kind == 0:      # 未知函数 -> 100
                expect = 100
                _, wire = build_request_frame(make_request("x"), func="no_such_func",
                                              protocol=cfg.protocol)
            elif kind == 1:    # 非法 protobuf -> 101
                expect = 101
                request_id = next_request_id()
                wire = build_frame(REQUEST, request_id, 0, SERVICE, FUNC, b"\x0a\xff",
                                   protocol=cfg.protocol)
            else:              # 坏起始魔数 -> 1
                expect = 1
                wire = b"\x0a\x00"

            t0 = time.perf_counter()
            try:
                await client.send_frames([wire])
                frame = await read_error_async(client)
            except (TimeoutError, ConnectionError, OSError, ProtocolError) as e:
                b.fail(None, count=cfg.errors - i)
                b.note("错误路径无响应，剩余 %d 个请求未完成: %s: %s"
                       % (cfg.errors - i, type(e).__name__, e))
                break
            t1 = time.perf_counter()
            if frame.error_code == expect:
                b.ok(t1 - t0, t1)
            else:
                b.fail(frame.error_code)
                b.note("期望错误码 %s(%s)，实际 %s(%s)"
                       % (expect, error_name(expect), frame.error_code,
                          error_name(frame.error_code)))
    finally:
        b.io(client.bytes_sent - sent0, client.bytes_recv - recv0)

    b.stop()   # 压测到此结束，后面的存活检查不计入速率分母

    # 错误路径之后服务端必须还能正常服务
    try:
        resp = await client.call(make_request("alive"))
        b.extra["alive_after_errors"] = (resp.res == "alive")
        if not b.extra["alive_after_errors"]:
            b.bad()
    except Exception as e:  # noqa: BLE001
        b.extra["alive_after_errors"] = False
        b.bad()
        b.note("错误路径后服务端不可用: %s: %s" % (type(e).__name__, e))
    finally:
        await client.aclose()
    b.extra["error_rate"] = b.failed / b.attempts if b.attempts else 0.0
    return [b]


async def scenario_connect(cfg, ctx):
    """短连接：connect + 1 次 unary + close 的速率与全程延迟。"""
    b = Bench("connect", "短连接 connect+call+close")
    b.params.update({"count": cfg.connect_count, "payload_size": cfg.payload_size})
    b.planned = cfg.connect_count
    b.start()
    for i in range(cfg.connect_count):
        msg, expect = payload_and_expect(cfg, i)
        t0 = time.perf_counter()
        try:
            client = await new_client(cfg)
            try:
                resp = await client.call(make_request(msg))
                t1 = time.perf_counter()
                if expect is not None and resp.res != expect:
                    b.bad()
                b.ok(None, t1)   # 拿到合法响应就算完成，内容不符单独记 mismatch
                b.duration(t1 - t0)
            finally:
                b.io(client.bytes_sent, client.bytes_recv)
                await client.aclose()
        except (OSError, TimeoutError, ProtocolError, RpcError) as e:
            b.fail(getattr(e, "code", None))
            b.note("短连接失败: %s: %s" % (type(e).__name__, e))
    b.stop()
    b.extra["conn_per_sec"] = b.count / b.wall if b.wall else 0.0
    if b.durations:
        b.extra["avg_full_ms"] = sum(b.durations) / len(b.durations) * 1000
    return [b]


async def scenario_mixed(cfg, ctx):
    """unary 连接与 stream 连接同时压，看混合负载下各自吞吐。"""
    u_conns = max(1, cfg.mixed_connections // 2)
    s_conns = max(1, cfg.mixed_connections - u_conns)

    bu = Bench("mixed_unary", "混合负载中的 unary (%d 连接)" % u_conns)
    bs = Bench("mixed_stream", "混合负载中的 stream (%d 连接)" % s_conns, unit="frame")
    bu.params.update({"connections": u_conns, "payload_size": cfg.payload_size})
    bs.params.update({"connections": s_conns, "frames": cfg.stream_frames,
                      "payload_size": cfg.payload_size})
    bu.planned = u_conns * max(1, cfg.total // u_conns)
    bu.start()
    bs.start()
    await asyncio.gather(
        *[unary_worker(cfg, bu, max(1, cfg.total // u_conns)) for _ in range(u_conns)],
        *[stream_burst_conn(cfg, bs) for _ in range(s_conns)],
    )
    bu.stop()
    bs.stop()
    combined = (bu.count + bs.count) / max(bu.wall, bs.wall)
    ctx["notes"].append(
        "混合负载：unary %.0f req/s（%d 连接）+ stream %.0f 帧/s（%d 连接），合计 %.0f 次/s。"
        % (bu.to_dict()["qps"], u_conns, bs.to_dict()["qps"], s_conns, combined))
    return [bu, bs]


SCENARIOS = OrderedDict([
    ("latency", scenario_latency),
    ("pipeline", scenario_pipeline),
    ("concurrency", scenario_concurrency),
    ("payload", scenario_payload),
    ("stream", scenario_stream),
    ("errors", scenario_errors),
    ("connect", scenario_connect),
    ("mixed", scenario_mixed),
])


# ================================================================ 报告


def add_cross_notes(results, ctx):
    """跨场景结论：比较单连接流水线与多连接并发的最高 QPS。"""
    pipe = [b for b in results if b.name.startswith("pipeline_")]
    conc = [b for b in results if b.name.startswith("concurrency_")]
    if not pipe or not conc:
        return
    bp = max(pipe, key=lambda b: b.to_dict()["qps"])
    bc = max(conc, key=lambda b: b.to_dict()["qps"])
    bp_qps, bc_qps = bp.to_dict()["qps"], bc.to_dict()["qps"]
    bp_c = bp.params.get("window", 1)
    bc_c = bc.params.get("connections", 1)
    if bc_qps and bp_qps >= bc_qps:
        ctx["notes"].append(
            "单连接流水线 window=%d 的 QPS（%.0f）已不低于 %d 连接并发（%.0f）："
            "服务端同一连接内串行 dispatch，流水线把往返时延藏起来后，瓶颈更多在"
            "每请求处理开销与线程池，而不是连接数。"
            % (bp_c, bp_qps, bc_c, bc_qps))
    else:
        ctx["notes"].append(
            "单连接流水线 window=%d QPS %.0f，%d 连接并发 %.0f（%.2fx）：并发连接仍有收益。"
            % (bp_c, bp_qps, bc_c, bc_qps, bp_qps / bc_qps if bc_qps else 0.0))


def print_bench(b):
    d = b.to_dict()
    print("-" * 96)
    print("[%s] %s" % (d["name"], d["title"]))
    if d["params"]:
        print("  参数: " + ", ".join("%s=%s" % (k, v) for k, v in d["params"].items()))
    print("  完成 %d 次  失败 %d  内容/错误码不符 %d  耗时 %s"
          % (d["count"], d["failed"], d["mismatch"], fmt_sec(d["wall_seconds"])))
    if d.get("unaccounted"):
        print("  警告: 计划 %d，已记账 %d，有 %d 个请求未进入统计"
              % (d["planned"], d["accounted"], d["unaccounted"]))
    print("  速率 %.1f %s/s" % (d["qps"], d["unit"]))
    if d["latency_ms"]:
        lat = d["latency_ms"]
        print("  延迟 ms: min %.3f  avg %.3f  p50 %.3f  p90 %.3f  p95 %.3f  p99 %.3f  max %.3f  std %.3f"
              % (lat["min"], lat["mean"], lat["p50"], lat["p90"], lat["p95"],
                 lat["p99"], lat["max"], lat["std"]))
    if d["durations_ms"]:
        ds = d["durations_ms"]
        print("  会话耗时 ms: n %d  avg %.3f  p99 %.3f  max %.3f"
              % (len(ds), sum(ds) / len(ds), percentile(ds, 99), max(ds)))
    tl = d["timeline"]
    if tl.get("seconds", 0) >= 2:
        print("  每秒速率: min %d  avg %.0f  max %d  抖动 %.2fx"
              % (tl["min_qps"], tl["avg_qps"], tl["max_qps"], tl["jitter"]))
    if d["error_codes"]:
        print("  错误码: " + ", ".join("%s=%d" % kv for kv in sorted(d["error_codes"].items())))
    if d["socket_bytes_sent"] or d["socket_bytes_recv"]:
        print("  网络吞吐 %.2f MB/s (sent %s, recv %s)"
              % (d["throughput_mbps"], fmt_mb(d["socket_bytes_sent"]),
                 fmt_mb(d["socket_bytes_recv"])))
    if d["extra"]:
        print("  指标: " + ", ".join("%s=%.3f" % (k, v) if isinstance(v, float)
                                     else "%s=%s" % (k, v)
                                     for k, v in d["extra"].items()))
    for note in d["notes"]:
        print("  备注: " + note)


def print_summary(results, health_before, health_after, ctx):
    print("=" * 96)
    print("汇总")
    print("=" * 96)
    header = "%-22s %8s %8s %12s %9s %9s %7s %7s" % (
        "场景", "完成", "失败", "速率/s", "p50(ms)", "p99(ms)", "抖动", "吞吐MB/s")
    print(header)
    print("-" * 96)
    for b in results:
        d = b.to_dict()
        lat = d["latency_ms"] or {}
        print("%-22s %8d %8d %9.1f %-2s %9s %9s %7s %7.2f" % (
            d["name"], d["count"], d["failed"], d["qps"], d["unit"],
            fmt_lat(lat, "p50"), fmt_lat(lat, "p99"),
            ("%.2fx" % d["timeline"]["jitter"])
            if d["timeline"].get("seconds", 0) >= 2 else "-",
            d["throughput_mbps"]))
    failures = sum(b.failed + b.mismatch for b in results)
    long_runs = [b for b in results if b.to_dict()["timeline"].get("seconds", 0) >= 2]
    if results and not long_runs:
        print("提示: 各场景耗时都不足 2 秒，『抖动』列与每秒速率要用更大的 --total 才有意义。")
    print("-" * 96)
    print("健康检查: 前置 %s (%.3f ms)  后置 %s (%.3f ms)"
          % ("OK" if health_before["ok"] else "FAIL", health_before["rtt_ms"],
             "OK" if health_after["ok"] else "FAIL", health_after["rtt_ms"]))
    print("失败合计: %d" % failures)
    if ctx["notes"]:
        print("结论:")
        for note in ctx["notes"]:
            print("  * " + note)
    print("=" * 96)
    return failures


# ================================================================ 参数与入口


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="RpcServer 指标测试客户端（QPS / 延迟 / 吞吐 / 并发 / 流式 / 错误路径）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default=HOST)
    p.add_argument("--port", type=int, default=PORT)
    p.add_argument("--timeout", type=float, default=TIMEOUT, help="单次收发超时（秒）")
    p.add_argument("--total", type=int, default=1000, help="每个一元场景的请求数")
    p.add_argument("--payload-size", type=int, default=64, help="默认报文大小（字节）")
    p.add_argument("--connections", default="1,2,4,8,16,32", help="并发场景的连接数列表")
    p.add_argument("--concurrency-window", type=int, default=1,
                   help="并发场景每条连接的在途请求数（>1 即滑动窗口，用来对齐多线程客户端）")
    p.add_argument("--windows", default="1,8,32", help="流水线窗口大小列表")
    p.add_argument("--payload-sizes", default="16,1024,60000", help="报文大小扫描列表")
    p.add_argument("--stream-frames", type=int, default=200, help="每条流的帧数")
    p.add_argument("--stream-connections", type=int, default=4, help="并行流的连接数")
    p.add_argument("--stream-multiplex", type=int, default=8, help="单连接多路复用的流数")
    p.add_argument("--mixed-connections", type=int, default=8, help="混合负载总连接数")
    p.add_argument("--connect-count", type=int, default=200, help="短连接次数")
    p.add_argument("--errors", type=int, default=300, help="错误路径请求数")
    p.add_argument("--warmup", type=int, default=50, help="预热请求数（不计入指标）")
    p.add_argument("--protocol", choices=[PROTOCOL_NEW, PROTOCOL_LEGACY],
                   default=PROTOCOL_NEW,
                   help="new=当前分支协议（带 stream_id）；legacy=main 分支老协议"
                        "（无 stream_id，param_len 4 字节，不支持流式）")
    p.add_argument("--check", choices=["echo", "world", "off"], default="echo",
                   help="响应内容校验：echo=发什么期望回什么；world=固定发 hello 期望 world"
                        "（对齐 main 分支的 hello 语义）；off=不校验")
    p.add_argument("--scenarios", default="all",
                   help="逗号分隔的场景名，或 all。可选: " + ",".join(SCENARIOS))
    p.add_argument("--json", default=None, help="把结果写成 JSON 文件")
    p.add_argument("--quiet", action="store_true", help="只打印汇总")
    args = p.parse_args(argv)

    def ints(text):
        return [int(x) for x in text.split(",") if x.strip()]

    args.connections = ints(args.connections)
    args.windows = ints(args.windows)
    args.payload_sizes = ints(args.payload_sizes)

    # 参数校验：0 / 负数会让场景空跑或死循环（window=0 时批次不推进且循环内没有 await）
    def need_positive(name, values, upper=None):
        if not values:
            p.error("%s 不能为空" % name)
        for v in values:
            if v < 1:
                p.error("%s 必须 >= 1（收到 %s）" % (name, v))
            if upper is not None and v > upper:
                p.error("%s 必须 <= %s（收到 %s）" % (name, upper, v))

    if args.total < 1:
        p.error("--total 必须 >= 1")
    if args.timeout <= 0:
        p.error("--timeout 必须 > 0")
    if args.warmup < 0:
        p.error("--warmup 必须 >= 0")
    if not (1 <= args.payload_size <= 0xFFFF):
        p.error("--payload-size 必须在 1~65535（param_len 是 2 字节）")
    need_positive("--connections", args.connections)
    need_positive("--concurrency-window", [args.concurrency_window])
    need_positive("--windows", args.windows)
    need_positive("--payload-sizes", args.payload_sizes, upper=0xFFFF)
    need_positive("--stream-frames", [args.stream_frames])
    need_positive("--stream-connections", [args.stream_connections])
    need_positive("--stream-multiplex", [args.stream_multiplex])
    need_positive("--mixed-connections", [args.mixed_connections])
    need_positive("--connect-count", [args.connect_count])
    need_positive("--errors", [args.errors])

    if args.scenarios.strip() == "all":
        args.selected = list(SCENARIOS)
    else:
        args.selected = [s.strip() for s in args.scenarios.split(",") if s.strip()]
        unknown = [s for s in args.selected if s not in SCENARIOS]
        if unknown:
            p.error("未知场景 %s；可选: %s" % (unknown, ",".join(SCENARIOS)))

    if args.protocol == PROTOCOL_LEGACY:
        need_stream = [s for s in args.selected if s in ("stream", "mixed")]
        if need_stream:
            p.error("main 分支老协议没有 stream_id，不支持流式场景: %s" % ",".join(need_stream))
    return args


async def main_async(args):
    env = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "target": "%s:%d" % (args.host, args.port),
        "scenarios": args.selected,
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    print("RpcServer 指标测试  目标 %s  Python %s" % (env["target"], env["python"]))
    print("场景: %s" % ", ".join(args.selected))

    try:
        ok, rtt, res = await http_alive(args)
    except Exception as e:  # noqa: BLE001
        print("前置健康检查失败：无法访问 %s（%s: %s）" % (env["target"], type(e).__name__, e))
        print("请先启动 rpc_server： ./cmake-build-release/src/rpc_server")
        return 2
    health_before = {"ok": ok, "rtt_ms": rtt * 1000, "echo": res}
    print("前置健康检查: %s (%.3f ms)" % ("OK" if ok else "FAIL", rtt * 1000))
    if not ok:
        print("服务端 echo 结果异常: %r" % res)
        return 2

    # 预热
    if args.warmup > 0:
        try:
            client = await AsyncRpcClient.connect(args.host, args.port, args.timeout,
                                                  args.protocol)
            try:
                for i in range(args.warmup):
                    await client.call(make_request(make_msg(args.payload_size, i)))
            finally:
                await client.aclose()
        except Exception as e:  # noqa: BLE001
            print("预热失败: %s: %s" % (type(e).__name__, e))
            return 2

    ctx = {"notes": [], "base_qps": 0.0}
    results = []
    for name in args.selected:
        try:
            bench_list = await SCENARIOS[name](args, ctx)
        except Exception as e:  # noqa: BLE001
            # 兜底：任何未处理的异常也变成一条失败记录，保证仍然有汇总和 JSON
            failed = Bench(name, "场景异常退出: %s" % type(e).__name__)
            failed.start()
            failed.stop()
            failed.fail(None)
            failed.note("场景抛出未处理异常: %s: %s" % (type(e).__name__, e))
            bench_list = [failed]
        results.extend(bench_list)
        if not args.quiet:
            for b in bench_list:
                print_bench(b)

    try:
        ok_after, rtt_after, res_after = await http_alive(args)
    except Exception as e:  # noqa: BLE001
        ok_after, rtt_after, res_after = False, 0.0, "健康检查失败: %s: %s" % (type(e).__name__, e)
    health_after = {"ok": ok_after, "rtt_ms": rtt_after * 1000, "echo": res_after}

    add_cross_notes(results, ctx)
    failures = print_summary(results, health_before, health_after, ctx)

    if args.json:
        payload = {
            "env": env,
            "args": {k: v for k, v in vars(args).items() if k != "selected"},
            "health_before": health_before,
            "health_after": health_after,
            "notes": ctx["notes"],
            "results": [b.to_dict() for b in results],
        }
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print("JSON 结果已写入 %s" % args.json)

    if not health_after["ok"]:
        return 1
    return 1 if failures else 0


def main(argv=None):
    args = parse_args(argv)
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\n被用户中断")
        return 130


if __name__ == "__main__":
    sys.exit(main())
