#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RpcServer 测试共享客户端库（协议 + 同步客户端 + 异步客户端 + 统计工具）。

原来协议实现与客户端在两份测试脚本里各写了一份（`test_rpc_client.py`、
`test_client_error.py`），另有 `test_rpc_client_error.py` 是 `test_client_error.py`
的旧副本。这里把它们收敛到唯一一处：

1. 协议常量与组帧 / 解帧：`build_frame` / `build_stream_frames` / `parse_frame`；
2. 同步客户端 `RpcClient`：协议一致性测试用，可控制 TCP 分段、多路复用流；
3. 异步客户端 `AsyncRpcClient` + `StreamMux`：指标 / 压测客户端用（asyncio）；
4. 统计工具：`percentile` / `latency_summary` / `QpsTimeline`。

协议（见 `src/app/rpc/protocol/RpcProtocol.h` 顶部注释）::

    0x0a 0x0b | 固定头 22 字节 | service | func | params | 0x0b 0x0c

    固定头：type(1) request_id(8) stream_id(8) error_code(1)
            service_len(1) func_len(1) param_len(2)
    整帧长度 = 26 + service_len + func_len + param_len

type：1 REQUEST / 2 RESPONSE / 3 STREAM_REQUEST / 4 STREAM_RESPONSE /
5 STREAM_END / 255 ERROR（服务端只会收到 1、3、5）。
"""

import asyncio
import itertools
import math
import os
import socket
import struct
import sys
import time
from collections import namedtuple
from contextlib import suppress

HERE = os.path.dirname(os.path.abspath(__file__))
if os.path.join(HERE, "protoc") not in sys.path:
    sys.path.insert(0, os.path.join(HERE, "protoc"))

import hello_pb2  # noqa: E402  （protoc 生成代码，路径见上）

# ---------------------------------------------------------------- 默认参数

HOST = os.environ.get("RPC_HOST", "127.0.0.1")
PORT = int(os.environ.get("RPC_PORT", "8891"))
TIMEOUT = float(os.environ.get("RPC_TIMEOUT", "5.0"))

RECV_SIZE = 65536

# ---------------------------------------------------------------- 协议版本

#: 当前分（featrue/rpc_stream）：22 字节固定头 + stream_id + param_len(u16)
PROTOCOL_NEW = "new"
#: main 分支的老协议：16 字节固定头，没有 stream_id，param_len 是 4 字节，
#: 错误帧的 type 是 3 而不是 255。解析时会把它归一成 REQ_ERROR，便于上层代码统一处理。
PROTOCOL_LEGACY = "legacy"

# ---------------------------------------------------------------- 协议常量

START = b"\x0a\x0b"
END = b"\x0b\x0c"

REQUEST = 1
RESPONSE = 2
STREAM_REQUEST = 3
STREAM_RESPONSE = 4
STREAM_END = 5
REQ_ERROR = 255

# type(1) + request_id(8) + stream_id(8) + error_code(1)
# + service_len(1) + func_len(1) + param_len(2)
FIXED_HEADER = 22

# 老协议 magic 之后的固定前缀：type(1) + request_id(8) + error_code(1)；
# 请求帧在此之后是 svc_len|svc|func_len|func，响应/错误帧直接是 param_len(4)
LEGACY_PREFIX = 10
LEGACY_REQUEST = 1
LEGACY_RESPONSE = 2
LEGACY_REQ_ERROR = 3

SERVICE = "HelloService"
FUNC = "hello"
STREAM_FUNC = "helloStream"

ERROR_NAMES = {
    0: "ERR_BAD_INTERNAL",
    1: "ERR_BAD_REQUEST",
    2: "ERR_BAD_STREAM_ID",
    3: "ERR_BAD_STREAM_TYPE",
    100: "ERR_UNKNOWN_FUNCTION",
    101: "ERR_UNKNOWN_PARAM",
    102: "ERR_UNKNOWN_RESPONSE",
}

_id_counter = itertools.count(1)


def next_request_id() -> int:
    """单调递增的非 0 request_id（进程内全局）。"""
    return next(_id_counter)


def error_name(code: int) -> str:
    return ERROR_NAMES.get(code, "UNKNOWN(%d)" % code)


def ensure_stream_supported(protocol):
    """main 分支的老协议没有 stream_id 字段，流式接口在老协议下直接拒绝。"""
    if protocol == PROTOCOL_LEGACY:
        raise ProtocolError("老协议（main 分支）不支持流式：帧里没有 stream_id 字段")


# ---------------------------------------------------------------- 组帧 / 解帧

Frame = namedtuple(
    "Frame",
    ["type", "request_id", "stream_id", "error_code", "service", "func", "params"],
)


class RpcError(Exception):
    """服务端返回了错误帧。"""

    def __init__(self, code: int):
        self.code = code
        super().__init__("RPC error %d: %s" % (code, error_name(code)))


class ProtocolError(Exception):
    """服务端违反了协议（帧尾魔数、stream_id 编号、响应类型等）。"""


def build_frame(type_, request_id, stream_id=0, service="", func="",
                params=b"", error_code=0, start=START, tail=END,
                protocol=PROTOCOL_NEW) -> bytes:
    """按协议组一帧。start/tail 可覆写，用于构造非法帧。

    protocol=PROTOCOL_LEGACY 时按 main 分支的老协议组帧（无 stream_id，
    param_len 是 4 字节，错误帧 type 用 3）。
    """
    svc = service.encode() if isinstance(service, str) else service
    fn = func.encode() if isinstance(func, str) else func
    if len(svc) > 0xFF or len(fn) > 0xFF:
        raise ValueError("service/func 名字过长")

    out = bytearray()
    out += start

    if protocol == PROTOCOL_LEGACY:
        if len(params) > 0xFFFFFFFF:
            raise ValueError("params 超过 param_len(4 字节) 上限")
        if type_ == REQ_ERROR:
            type_ = LEGACY_REQ_ERROR   # 老协议错误帧 type=3
        # 老协议的变长区是交错的：svc_len | svc | func_len | func | param_len(4) | params；
        # 只有请求帧带 service/func，响应/错误帧直接是 param_len(4) | params
        out.append(type_ & 0xFF)
        out += struct.pack(">Q", request_id)
        out.append(error_code & 0xFF)
        if type_ == LEGACY_REQUEST:
            out.append(len(svc))
            out += svc
            out.append(len(fn))
            out += fn
        out += struct.pack(">I", len(params))
        out += params
        out += tail
        return bytes(out)

    out.append(type_ & 0xFF)
    out += struct.pack(">Q", request_id)
    if len(params) > 0xFFFF:
        raise ValueError("params 超过 param_len(2 字节) 上限")
    out += struct.pack(">Q", stream_id)
    out.append(error_code & 0xFF)
    out.append(len(svc))
    out.append(len(fn))
    out += struct.pack(">H", len(params))
    out += svc + fn + params
    out += tail
    return bytes(out)


def build_request_frame(request, request_id=None, service=SERVICE, func=FUNC,
                        protocol=PROTOCOL_NEW):
    """组一帧非流式 REQUEST，返回 (request_id, frame)。"""
    if request_id is None:
        request_id = next_request_id()
    return request_id, build_frame(REQUEST, request_id, 0, service, func,
                                   request.SerializeToString(), protocol=protocol)


def build_stream_frames(requests, service=SERVICE, func=STREAM_FUNC, request_id=None):
    """返回 (request_id, frames)：STREAM_REQUEST 的 stream_id 从 1 递增，末帧为 STREAM_END。"""
    if request_id is None:
        request_id = next_request_id()
    frames = []
    stream_id = 0
    for req in requests:
        stream_id += 1
        frames.append(build_frame(STREAM_REQUEST, request_id, stream_id, service, func,
                                  req.SerializeToString()))
    stream_id += 1
    frames.append(build_frame(STREAM_END, request_id, stream_id, service, func))
    return request_id, frames


def build_stream_request(request_id, stream_id, msg="x",
                         service=SERVICE, func=STREAM_FUNC):
    return build_frame(STREAM_REQUEST, request_id, stream_id, service, func,
                       make_request(msg).SerializeToString())


def build_stream_end(request_id, stream_id, service=SERVICE, func=STREAM_FUNC):
    return build_frame(STREAM_END, request_id, stream_id, service, func)


def parse_frame(buf: bytearray, protocol=PROTOCOL_NEW):
    """从 buf 头部解一帧：数据不足返回 None，帧尾魔数错误抛 ProtocolError。

    解析成功时会从 buf 中消费掉该帧的字节。

    protocol=PROTOCOL_LEGACY 时按 main 分支的老协议解析（无 stream_id，
    param_len 4 字节），并把错误帧的 type(3) 归一成 REQ_ERROR，上层代码无需分叉。

    注意：这里用 `find(START)` 做重同步，遇到「0x0a 后面不是 0x0b」会跳过该字节继续
    找（服务端的解析状态机是直接判 ERROR）。客户端更宽松是为了在垃圾前缀后仍能恢复，
    帧的字节格式本身与服务端一致。
    """
    pos = buf.find(START)
    if pos < 0:
        # 丢掉不可能构成 START 的尾巴，避免缓冲区无限增长
        if buf and buf[-1] == START[0]:
            del buf[:-1]
        else:
            buf.clear()
        return None
    if pos > 0:
        del buf[:pos]

    if protocol == PROTOCOL_LEGACY:
        # 老协议没有统一固定头，按状态机顺序读：
        #   magic | type(1) | request_id(8) | error_code(1)
        #   请求帧再跟 svc_len(1) | svc | func_len(1) | func（响应/错误帧没有这两个字段）
        #   然后 param_len(4) | params | magic
        if len(buf) < 2 + LEGACY_PREFIX + 4:
            return None
        off = 2
        type_ = buf[off]; off += 1
        request_id = struct.unpack(">Q", buf[off:off + 8])[0]; off += 8
        error_code = buf[off]; off += 1

        svc = fn = b""
        if type_ == LEGACY_REQUEST:
            if len(buf) < off + 1:
                return None
            service_len = buf[off]; off += 1
            if len(buf) < off + service_len + 1:
                return None
            svc = bytes(buf[off:off + service_len]); off += service_len
            func_len = buf[off]; off += 1
            if len(buf) < off + func_len + 4:
                return None
            fn = bytes(buf[off:off + func_len]); off += func_len

        param_len = struct.unpack(">I", buf[off:off + 4])[0]; off += 4
        if len(buf) < off + param_len + 2:
            return None

        params = bytes(buf[off:off + param_len]); off += param_len
        if bytes(buf[off:off + 2]) != END:
            raise ProtocolError(
                "帧尾魔数错误：期望 %s，实际 %s（type=%s request_id=%s）"
                % (END.hex(" "), bytes(buf[off:off + 2]).hex(" "), type_, request_id))
        del buf[:off + 2]
        if type_ == LEGACY_REQ_ERROR:
            type_ = REQ_ERROR           # 归一化，上层只认 REQ_ERROR
        return Frame(type_, request_id, 0, error_code, svc, fn, params)

    if len(buf) < 2 + FIXED_HEADER + 2:
        return None

    hdr = buf[2:2 + FIXED_HEADER]
    type_ = hdr[0]
    request_id = struct.unpack(">Q", hdr[1:9])[0]
    stream_id = struct.unpack(">Q", hdr[9:17])[0]
    error_code = hdr[17]
    service_len = hdr[18]
    func_len = hdr[19]
    param_len = struct.unpack(">H", hdr[20:22])[0]

    total = 2 + FIXED_HEADER + service_len + func_len + param_len + 2
    if len(buf) < total:
        return None

    off = 2 + FIXED_HEADER
    svc = bytes(buf[off:off + service_len]); off += service_len
    fn = bytes(buf[off:off + func_len]); off += func_len
    params = bytes(buf[off:off + param_len]); off += param_len
    if bytes(buf[off:off + 2]) != END:
        raise ProtocolError(
            "帧尾魔数错误：期望 %s，实际 %s（type=%s request_id=%s stream_id=%s）"
            % (END.hex(" "), bytes(buf[off:off + 2]).hex(" "), type_, request_id, stream_id))
    del buf[:total]
    return Frame(type_, request_id, stream_id, error_code, svc, fn, params)


def make_request(msg: str):
    req = hello_pb2.HelloWorldRequest()
    req.msg = msg
    return req


def parse_response(frame: Frame):
    resp = hello_pb2.HelloWorldResponse()
    resp.ParseFromString(frame.params)
    return resp


def probe(host=HOST, port=PORT, timeout=2.0) -> bool:
    """探测服务端是否可连接（测试脚本 setUpModule 用）。"""
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


# ---------------------------------------------------------------- 同步客户端


class RpcClient:
    """裸协议同步客户端；cuts 参数用来控制发送侧的分段方式。"""

    def __init__(self, host=HOST, port=PORT, timeout=TIMEOUT, protocol=PROTOCOL_NEW):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self._buf = bytearray()
        self._timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    # ---------- 发送 ----------

    def send_frames(self, frames, cuts=None, delay=0.01):
        """发送一组帧。

        cuts=None      整批发一次（默认）
        cuts="frame"   每帧一次 sendall
        cuts="byte"    逐字节发送
        cuts=int       每 cuts 字节发一次
        cuts=[...]     在这些字节偏移处切开
        """
        if cuts == "frame":
            for frame in frames:
                self.sock.sendall(frame)
                time.sleep(delay)
            return

        data = b"".join(frames)
        if cuts is None:
            self.sock.sendall(data)
            return

        if cuts == "byte":
            for i in range(len(data)):
                self.sock.sendall(data[i:i + 1])
                time.sleep(0.002)
            return

        if isinstance(cuts, int):
            parts = [data[i:i + cuts] for i in range(0, len(data), cuts)]
        else:
            prev, parts = 0, []
            for cut in sorted(cuts):
                parts.append(data[prev:cut])
                prev = cut
            parts.append(data[prev:])

        for part in parts:
            self.sock.sendall(part)
            time.sleep(delay)

    # ---------- 接收 ----------

    def recv_frame(self, timeout=None) -> Frame:
        self.sock.settimeout(self._timeout if timeout is None else timeout)
        while True:
            frame = parse_frame(self._buf, self.protocol)
            if frame is not None:
                return frame
            chunk = self.sock.recv(RECV_SIZE)
            if not chunk:
                raise ConnectionError("服务端关闭了连接")
            self._buf.extend(chunk)

    def read_error(self, timeout=None) -> Frame:
        """读第一帧 REQ_ERROR（跳过它之前的其它帧）。"""
        deadline = time.monotonic() + (self._timeout if timeout is None else timeout)
        while True:
            remain = deadline - time.monotonic()
            if remain <= 0:
                raise TimeoutError("没有收到错误帧")
            frame = self.recv_frame(remain)
            if frame.type == REQ_ERROR:
                return frame

    # ---------- 非流式 ----------

    def call_unary(self, request, service=SERVICE, func=FUNC, timeout=None):
        request_id, frame = build_request_frame(request, service=service, func=func,
                                                protocol=self.protocol)
        self.send_frames([frame])
        resp = self.recv_frame(timeout)
        if resp.request_id != request_id:
            raise ProtocolError("request_id 不匹配：期望 %s，实际 %s"
                                % (request_id, resp.request_id))
        if resp.type == REQ_ERROR:
            raise RpcError(resp.error_code)
        if resp.type != RESPONSE:
            raise ProtocolError("期望 RESPONSE，实际 type=%s" % resp.type)
        if resp.stream_id != 0:
            raise ProtocolError("非流式响应的 stream_id 必须为 0，实际 %s" % resp.stream_id)
        return parse_response(resp)

    def call(self, request, service=SERVICE, func=FUNC, timeout=None):
        """`call_unary` 的别名，冒烟测试里读起来更顺。"""
        return self.call_unary(request, service=service, func=func, timeout=timeout)

    def echo(self, msg, service=SERVICE, func=FUNC, timeout=None):
        """发一条 HelloWorldRequest，返回响应里的 res 字符串。"""
        return self.call_unary(make_request(msg), service=service, func=func,
                               timeout=timeout).res

    # ---------- 流式 ----------

    def collect(self, plan, timeout=None):
        """plan: {request_id: 'unary'|'stream'}；按 request_id 分发响应帧并校验顺序。"""
        deadline = time.monotonic() + (self._timeout if timeout is None else timeout)
        states = {rid: {"kind": kind, "expected": 0, "responses": [],
                        "error": None, "done": False}
                  for rid, kind in plan.items()}
        while not all(s["done"] for s in states.values()):
            remain = deadline - time.monotonic()
            if remain <= 0:
                pending = [rid for rid, s in states.items() if not s["done"]]
                raise TimeoutError("等待响应超时，未完成 request_id=%s" % pending)

            frame = self.recv_frame(remain)
            state = states.get(frame.request_id)
            if state is None or state["done"]:
                continue

            if frame.type == REQ_ERROR:
                state["error"] = frame.error_code
                state["done"] = True
                continue

            if state["kind"] == "unary":
                if frame.type != RESPONSE:
                    raise ProtocolError("期望 RESPONSE，实际 type=%s" % frame.type)
                if frame.stream_id != 0:
                    raise ProtocolError("非流式响应 stream_id 必须为 0，实际 %s" % frame.stream_id)
                state["responses"].append(parse_response(frame))
                state["done"] = True
                continue

            if frame.type == STREAM_RESPONSE:
                state["expected"] += 1
                if frame.stream_id != state["expected"]:
                    raise ProtocolError(
                        "STREAM_RESPONSE stream_id 应为 %s，实际 %s"
                        % (state["expected"], frame.stream_id))
                state["responses"].append(parse_response(frame))
            elif frame.type == STREAM_END:
                if frame.stream_id != state["expected"] + 1:
                    raise ProtocolError(
                        "STREAM_END stream_id 应为 %s，实际 %s"
                        % (state["expected"] + 1, frame.stream_id))
                state["done"] = True
            else:
                raise ProtocolError("未预期的帧类型 %s" % frame.type)
        return states

    def call_stream(self, requests, service=SERVICE, func=STREAM_FUNC,
                    timeout=None, cuts=None):
        """一次性收完整条流，返回 HelloWorldResponse 列表。"""
        ensure_stream_supported(self.protocol)
        request_id, frames = build_stream_frames(requests, service, func)
        self.send_frames(frames, cuts=cuts)
        state = self.collect({request_id: "stream"}, timeout)[request_id]
        if state["error"] is not None:
            raise RpcError(state["error"])
        return state["responses"]

    def iter_stream(self, requests, service=SERVICE, func=STREAM_FUNC,
                    timeout=None, cuts=None):
        """按到达顺序逐帧产出聚合后的 body（bytes），用于边收边处理。"""
        ensure_stream_supported(self.protocol)
        request_id, frames = build_stream_frames(requests, service, func)
        self.send_frames(frames, cuts=cuts)
        expected = 0
        while True:
            frame = self.recv_frame(timeout)
            if frame.request_id != request_id:
                continue
            if frame.type == REQ_ERROR:
                raise RpcError(frame.error_code)
            if frame.type == STREAM_END:
                return
            if frame.type != STREAM_RESPONSE:
                raise ProtocolError("未预期的帧类型 %s" % frame.type)
            expected += 1
            if frame.stream_id != expected:
                raise ProtocolError("STREAM_RESPONSE stream_id 应为 %s，实际 %s"
                                    % (expected, frame.stream_id))
            yield frame.params

    def call_streams(self, batches, service=SERVICE, func=STREAM_FUNC, timeout=None):
        """同一连接上多路复用：各批的帧轮转交错发送，响应按 request_id 归一。"""
        ensure_stream_supported(self.protocol)
        plans, per_stream = {}, []
        for batch in batches:
            request_id, frames = build_stream_frames(batch, service, func)
            plans[request_id] = "stream"
            per_stream.append(frames)

        interleaved = []
        for i in range(max(len(f) for f in per_stream)):
            for frames in per_stream:
                if i < len(frames):
                    interleaved.append(frames[i])
        self.send_frames(interleaved)

        states = self.collect(plans, timeout)
        for state in states.values():
            if state["error"] is not None:
                raise RpcError(state["error"])
        return [states[rid]["responses"] for rid in plans]


def assert_error(test, frame: Frame, code: int, request_id=None, stream_id=None):
    """校验错误帧的 error_code（以及可选的 request_id / stream_id）。"""
    want = error_name(code)
    test.assertEqual(frame.error_code, code,
                     "期望错误码 %s(%s)，实际 %s(%s)"
                     % (code, want, frame.error_code, error_name(frame.error_code)))
    if request_id is not None:
        test.assertEqual(frame.request_id, request_id, "错误帧 request_id 不符")
    if stream_id is not None:
        test.assertEqual(frame.stream_id, stream_id, "错误帧 stream_id 不符")


# ---------------------------------------------------------------- 异步客户端


class AsyncRpcClient:
    """asyncio 版裸协议客户端，指标 / 压测用。

    与 `RpcClient` 的协议实现完全一致，只是把阻塞 socket 换成
    `asyncio.open_connection`，并额外统计收发字节数。
    """

    def __init__(self, host=HOST, port=PORT, timeout=TIMEOUT, protocol=PROTOCOL_NEW):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.protocol = protocol
        self.reader = None
        self.writer = None
        self._buf = bytearray()
        self.bytes_sent = 0
        self.bytes_recv = 0

    @classmethod
    async def connect(cls, host=HOST, port=PORT, timeout=TIMEOUT, protocol=PROTOCOL_NEW):
        self = cls(host, port, timeout, protocol)
        await self.open()
        return self

    async def open(self):
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout)
        except asyncio.TimeoutError:
            # Python 3.8 上 asyncio.TimeoutError 不是内建 TimeoutError 的子类，
            # 统一转成内建异常，调用方一个 except TimeoutError 就能同时覆盖 3.8/3.11。
            raise TimeoutError("连接 %s:%s 超时（%.3fs）"
                               % (self.host, self.port, self.timeout)) from None

    def close(self):
        if self.writer is not None:
            with suppress(Exception):
                self.writer.close()
            self.writer = None

    async def aclose(self):
        if self.writer is not None:
            with suppress(Exception):
                self.writer.close()
                await self.writer.wait_closed()
            self.writer = None

    def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()

    # ---------- 收发 ----------

    async def send_frames(self, frames):
        data = b"".join(frames)
        self.writer.write(data)
        await self.writer.drain()
        self.bytes_sent += len(data)
        return len(data)

    async def recv_frame(self, timeout=None) -> Frame:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + (self.timeout if timeout is None else timeout)
        while True:
            frame = parse_frame(self._buf, self.protocol)
            if frame is not None:
                return frame
            remain = deadline - loop.time()
            if remain <= 0:
                raise TimeoutError("等待响应超时")
            try:
                chunk = await asyncio.wait_for(self.reader.read(RECV_SIZE), timeout=remain)
            except asyncio.TimeoutError:
                raise TimeoutError("等待响应超时（%.3fs）" % remain) from None
            if not chunk:
                raise ConnectionError("服务端关闭了连接")
            self._buf.extend(chunk)
            self.bytes_recv += len(chunk)

    # ---------- 非流式 ----------

    async def call(self, request, service=SERVICE, func=FUNC, timeout=None):
        resp, _, _ = await self.call_timed(request, service=service, func=func, timeout=timeout)
        return resp

    async def call_timed(self, request, service=SERVICE, func=FUNC, timeout=None):
        """返回 (HelloWorldResponse, 往返秒数, 发送字节数)。"""
        request_id, frame = build_request_frame(request, service=service, func=func,
                                                protocol=self.protocol)
        t0 = time.perf_counter()
        sent = await self.send_frames([frame])
        resp = await self.recv_frame(timeout)
        t1 = time.perf_counter()

        if resp.request_id != request_id:
            raise ProtocolError("request_id 不匹配：期望 %s，实际 %s"
                                % (request_id, resp.request_id))
        if resp.type == REQ_ERROR:
            raise RpcError(resp.error_code)
        if resp.type != RESPONSE:
            raise ProtocolError("期望 RESPONSE，实际 type=%s" % resp.type)
        if resp.stream_id != 0:
            raise ProtocolError("非流式响应的 stream_id 必须为 0，实际 %s" % resp.stream_id)
        return parse_response(resp), t1 - t0, sent

    async def echo(self, msg, service=SERVICE, func=FUNC, timeout=None):
        return (await self.call(make_request(msg), service=service, func=func,
                                timeout=timeout)).res

    # ---------- 流式 ----------

    async def send_stream_request(self, request_id, stream_id, request,
                                  service=SERVICE, func=STREAM_FUNC):
        ensure_stream_supported(self.protocol)
        return await self.send_frames([
            build_frame(STREAM_REQUEST, request_id, stream_id, service, func,
                        request.SerializeToString())])

    async def send_stream_end(self, request_id, stream_id,
                              service=SERVICE, func=STREAM_FUNC):
        ensure_stream_supported(self.protocol)
        return await self.send_frames([build_stream_end(request_id, stream_id, service, func)])

    async def call_stream(self, requests, service=SERVICE, func=STREAM_FUNC,
                          timeout=None, validate=True):
        """发完整条流再收完，返回 HelloWorldResponse 列表。

        validate=True 时校验 STREAM_RESPONSE / STREAM_END 的 stream_id 严格递增，
        服务端违反协议抛 ProtocolError（指标脚本会把这类异常记为失败）。
        """
        ensure_stream_supported(self.protocol)
        request_id, frames = build_stream_frames(requests, service, func)
        await self.send_frames(frames)

        responses = []
        expected = 0
        while True:
            frame = await self.recv_frame(timeout)
            if frame.request_id != request_id:
                continue
            if frame.type == REQ_ERROR:
                raise RpcError(frame.error_code)
            if frame.type == STREAM_RESPONSE:
                expected += 1
                if validate and frame.stream_id != expected:
                    raise ProtocolError("STREAM_RESPONSE stream_id 应为 %s，实际 %s"
                                        % (expected, frame.stream_id))
                responses.append(parse_response(frame))
            elif frame.type == STREAM_END:
                if validate and frame.stream_id != expected + 1:
                    raise ProtocolError("STREAM_END stream_id 应为 %s，实际 %s"
                                        % (expected + 1, frame.stream_id))
                return responses
            else:
                raise ProtocolError("未预期的帧类型 %s" % frame.type)


class AsyncStreamSession:
    """`StreamMux` 里的一条流。"""

    def __init__(self, mux, request_id, func, frame_count):
        self.mux = mux
        self.request_id = request_id
        self.func = func
        self.frame_count = frame_count
        self.queue = asyncio.Queue()
        self.first_frame_at = None
        self.end_at = None
        self.error = None
        self.done = False

    async def send_request(self, stream_id, request):
        await self.mux.client.send_stream_request(self.request_id, stream_id, request,
                                                  self.mux.service, self.func)

    async def send_end(self, stream_id):
        await self.mux.client.send_stream_end(self.request_id, stream_id,
                                              self.mux.service, self.func)

    async def collect(self, timeout=None):
        """读到自己这条流的 STREAM_END，返回 HelloWorldResponse 列表。"""
        timeout = self.mux.client.timeout if timeout is None else timeout
        responses = []
        expected = 0
        while True:
            try:
                frame = await asyncio.wait_for(self.queue.get(), timeout)
            except asyncio.TimeoutError:
                self.done = True
                self.mux._sessions.pop(self.request_id, None)   # 放弃这条流，迟到帧算 stray
                raise TimeoutError("等待流 %s 的帧超时（已收到 %d 帧）"
                                   % (self.request_id, expected))
            if frame.type == REQ_ERROR:
                self.error = frame.error_code
                self.done = True
                raise RpcError(frame.error_code)
            if frame.type == STREAM_RESPONSE:
                expected += 1
                if frame.stream_id != expected:
                    raise ProtocolError("STREAM_RESPONSE stream_id 应为 %s，实际 %s"
                                        % (expected, frame.stream_id))
                if self.first_frame_at is None:
                    self.first_frame_at = time.perf_counter()
                responses.append(parse_response(frame))
            elif frame.type == STREAM_END:
                if frame.stream_id != expected + 1:
                    raise ProtocolError("STREAM_END stream_id 应为 %s，实际 %s"
                                        % (expected + 1, frame.stream_id))
                self.end_at = time.perf_counter()
                self.done = True
                self.mux._sessions.pop(self.request_id, None)
                return responses
            else:
                raise ProtocolError("未预期的帧类型 %s" % frame.type)


class StreamMux:
    """在一条连接上跑多条流：单个 reader 任务按 request_id 分发响应帧。

    用于度量服务端「同连接多路复用流」的能力（每个 request_id 对应服务端一个
    RpcSession）。
    """

    def __init__(self, client, service=SERVICE):
        self.client = client
        self.service = service
        self._sessions = {}
        self._reader_task = None
        self.stray_frames = 0
        self.error = None      # reader 任务因超时 / 连接异常退出时记录原因

    def open(self, func=STREAM_FUNC, frame_count=0):
        request_id = next_request_id()
        session = AsyncStreamSession(self, request_id, func, frame_count)
        self._sessions[request_id] = session
        return session

    async def start(self):
        self._reader_task = asyncio.ensure_future(self._read_loop())

    async def _read_loop(self):
        while True:
            frame = await self.client.recv_frame()
            session = self._sessions.get(frame.request_id)
            if session is None or session.done:
                # 没有对应流（或流已经结束）的帧：不静默丢弃，计入 stray 便于发现服务端多发帧
                self.stray_frames += 1
                continue
            session.queue.put_nowait(frame)

    async def stop(self):
        """停掉 reader；reader 若因连接异常已经死亡，异常只记到 self.error，不再往外抛。"""
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            except Exception as e:  # noqa: BLE001  连接被 RST / 超时等
                self.error = e
            self._reader_task = None


# ---------------------------------------------------------------- 统计工具


def percentile(data, p):
    """线性插值百分位（p 取 0~100），data 不必有序。"""
    if not data:
        return 0.0
    ordered = sorted(data)
    k = (len(ordered) - 1) * (p / 100.0)
    f = int(math.floor(k))
    c = k - f
    if f + 1 < len(ordered):
        return ordered[f] + c * (ordered[f + 1] - ordered[f])
    return ordered[f]


def latency_summary(latencies) -> dict:
    """延迟（秒）汇总：count/min/max/mean/median/p90/p95/p99/std。"""
    if not latencies:
        return {"count": 0}
    n = len(latencies)
    mean = sum(latencies) / n
    var = sum((x - mean) ** 2 for x in latencies) / n
    return {
        "count": n,
        "min": min(latencies),
        "max": max(latencies),
        "mean": mean,
        "median": percentile(latencies, 50),
        "p50": percentile(latencies, 50),
        "p90": percentile(latencies, 90),
        "p95": percentile(latencies, 95),
        "p99": percentile(latencies, 99),
        "std": math.sqrt(var),
    }


class QpsTimeline:
    """按完成时刻分桶，统计每秒完成的请求数（用于观察稳定性）。"""

    def __init__(self, start=None):
        self.start = time.perf_counter() if start is None else start
        self._buckets = {}

    def add(self, ts=None):
        ts = time.perf_counter() if ts is None else ts
        self._buckets[int(ts - self.start)] = self._buckets.get(int(ts - self.start), 0) + 1

    def add_many(self, count, ts=None):
        ts = time.perf_counter() if ts is None else ts
        self._buckets[int(ts - self.start)] = self._buckets.get(int(ts - self.start), 0) + count

    def summary(self) -> dict:
        if not self._buckets:
            return {"seconds": 0, "min_qps": 0, "avg_qps": 0, "max_qps": 0,
                    "jitter": 0, "series": []}
        # 补零对齐到真实时间轴：中间有停顿的秒也要算 0，否则 seconds 会低估总时长
        span = max(self._buckets) + 1
        values = [self._buckets.get(i, 0) for i in range(span)]
        # 末尾不足 1 秒的桶不参与 min/avg/max，避免把收尾的半秒当成抖动
        full = values[:-1] if span > 1 else values
        lo, hi = min(full), max(full)
        return {
            "seconds": span,
            "min_qps": lo,
            "avg_qps": sum(full) / len(full),
            "max_qps": hi,
            "jitter": (hi / lo) if lo else 0.0,
            "series": [[i, v] for i, v in enumerate(values)],
        }


# ---------------------------------------------------------------- 库自检


def _self_check():
    """起服务端后 `python3 rpc_client.py` 做一次最小往返，覆盖库的两套客户端。"""
    if not probe():
        print("服务端未启动（%s:%s），跳过自检" % (HOST, PORT))
        return 2

    with RpcClient() as client:
        one = client.echo("selfcheck-unary")
        assert one == "selfcheck-unary", one
        # 逐帧流式：边收边处理（iter_stream 产出的是帧 body）
        bodies = list(client.iter_stream([make_request("selfcheck-iter")]))
        assert len(bodies) == 1, bodies
        resp = hello_pb2.HelloWorldResponse()
        resp.ParseFromString(bodies[0])
        assert resp.res == "selfcheck-iter", resp.res
    print("同步客户端 OK（unary + iter_stream）")

    async def async_part():
        client = await AsyncRpcClient.connect()
        try:
            assert await client.echo("selfcheck-async") == "selfcheck-async"
            responses = await client.call_stream([make_request("s0"), make_request("s1")])
            assert [r.res for r in responses] == ["s0", "s1"], responses
        finally:
            await client.aclose()

    asyncio.run(async_part())
    print("异步客户端 OK（echo + call_stream）")
    return 0


if __name__ == "__main__":
    sys.exit(_self_check())
