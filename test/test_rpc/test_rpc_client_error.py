#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RpcServer 协议一致性测试。

覆盖范围
--------
1. 非流式：echo、连续调用、流水线、未知函数、非法 protobuf
2. 流式：stream_id 递增、STREAM_END 编号、多路复用、与 unary 混用
3. 错误码：1 ERR_BAD_REQUEST / 2 ERR_BAD_STREAM_ID / 3 ERR_BAD_STREAM_TYPE
          / 100 ERR_UNKNOWN_FUNCTION / 101 ERR_UNKNOWN_PARAM
4. TCP 分段：逐字节、逐帧、逐个切分点、随机切分
5. 健壮性：大量短连接、半帧后 RST、垃圾前缀后重新同步
6. 并发：多线程 unary / stream
7. 大负载：60KB 单帧、1000 帧流

运行
----
    python3 test_client_error.py                    # 默认 127.0.0.1:8891
    RPC_PORT=8899 python3 test_client_error.py      # 换端口
    python3 test_client_error.py -k ErrorCode       # 只跑名字匹配的用例
    python3 test_client_error.py -v

协议（见 src/app/rpc/RpcServer.cpp 顶部注释）
--------------------------------------------
    0x0a 0x0b | type(1) | request_id(8) | stream_id(8) | error_code(1)
              | service_len(1) | func_len(1) | param_len(2)
              | service | func | params | 0x0b 0x0c
"""

import itertools
import os
import random
import socket
import struct
import sys
import threading
import time
import unittest
from collections import namedtuple

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "protoc"))

import hello_pb2  # noqa: E402

HOST = os.environ.get("RPC_HOST", "127.0.0.1")
PORT = int(os.environ.get("RPC_PORT", "8891"))
TIMEOUT = float(os.environ.get("RPC_TIMEOUT", "5.0"))

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
    return next(_id_counter)


# ---------------------------------------------------------------- 组帧 / 解帧

Frame = namedtuple(
    "Frame",
    ["type", "request_id", "stream_id", "error_code", "service", "func", "params"],
)


class RpcError(Exception):
    """服务端返回了错误帧。"""

    def __init__(self, code: int):
        self.code = code
        super().__init__(f"RPC error {code}: {ERROR_NAMES.get(code, 'UNKNOWN')}")


class ProtocolError(Exception):
    """服务端违反了协议。"""


def build_frame(type_, request_id, stream_id=0, service="", func="",
                params=b"", error_code=0, start=START, tail=END) -> bytes:
    """按协议组一帧。start/tail 可覆写，用于构造非法帧。"""
    svc = service.encode() if isinstance(service, str) else service
    fn = func.encode() if isinstance(func, str) else func
    if len(svc) > 0xFF or len(fn) > 0xFF:
        raise ValueError("service/func 名字过长")
    if len(params) > 0xFFFF:
        raise ValueError("params 超过 param_len(2 字节) 上限")

    out = bytearray()
    out += start
    out.append(type_ & 0xFF)
    out += struct.pack(">Q", request_id)
    out += struct.pack(">Q", stream_id)
    out.append(error_code & 0xFF)
    out.append(len(svc))
    out.append(len(fn))
    out += struct.pack(">H", len(params))
    out += svc + fn + params
    out += tail
    return bytes(out)


def build_stream_frames(requests, service=SERVICE, func=STREAM_FUNC):
    """返回 (request_id, frames)：STREAM_REQUEST 的 stream_id 从 1 递增，末帧为 STREAM_END。"""
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


def build_stream_request(request_id, stream_id, msg="x"):
    return build_frame(STREAM_REQUEST, request_id, stream_id, SERVICE, STREAM_FUNC,
                       make_request(msg).SerializeToString())


def parse_frame(buf: bytearray):
    """从 buf 头部解一帧：数据不足返回 None，帧尾魔数错误抛 ProtocolError。"""
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
            f"帧尾魔数错误：期望 {END.hex(' ')}，实际 {bytes(buf[off:off + 2]).hex(' ')}"
            f"（type={type_} request_id={request_id} stream_id={stream_id}）")
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


# ---------------------------------------------------------------- 客户端

class RpcClient:
    """裸协议客户端；cuts 参数用来控制发送侧的分段方式。"""

    def __init__(self, host=HOST, port=PORT, timeout=TIMEOUT):
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
            frame = parse_frame(self._buf)
            if frame is not None:
                return frame
            chunk = self.sock.recv(65536)
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
        request_id = next_request_id()
        self.send_frames([build_frame(REQUEST, request_id, 0, service, func,
                                      request.SerializeToString())])
        frame = self.recv_frame(timeout)
        if frame.request_id != request_id:
            raise ProtocolError(f"request_id 不匹配：期望 {request_id}，实际 {frame.request_id}")
        if frame.type == REQ_ERROR:
            raise RpcError(frame.error_code)
        if frame.type != RESPONSE:
            raise ProtocolError(f"期望 RESPONSE，实际 type={frame.type}")
        if frame.stream_id != 0:
            raise ProtocolError(f"非流式响应的 stream_id 必须为 0，实际 {frame.stream_id}")
        return parse_response(frame)

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
                raise TimeoutError(f"等待响应超时，未完成 request_id={pending}")

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
                    raise ProtocolError(f"期望 RESPONSE，实际 type={frame.type}")
                if frame.stream_id != 0:
                    raise ProtocolError(f"非流式响应 stream_id 必须为 0，实际 {frame.stream_id}")
                state["responses"].append(parse_response(frame))
                state["done"] = True
                continue

            if frame.type == STREAM_RESPONSE:
                state["expected"] += 1
                if frame.stream_id != state["expected"]:
                    raise ProtocolError(
                        f"STREAM_RESPONSE stream_id 应为 {state['expected']}，实际 {frame.stream_id}")
                state["responses"].append(parse_response(frame))
            elif frame.type == STREAM_END:
                if frame.stream_id != state["expected"] + 1:
                    raise ProtocolError(
                        f"STREAM_END stream_id 应为 {state['expected'] + 1}，实际 {frame.stream_id}")
                state["done"] = True
            else:
                raise ProtocolError(f"未预期的帧类型 {frame.type}")
        return states

    def call_stream(self, requests, service=SERVICE, func=STREAM_FUNC, timeout=None, cuts=None):
        request_id, frames = build_stream_frames(requests, service, func)
        self.send_frames(frames, cuts=cuts)
        state = self.collect({request_id: "stream"}, timeout)[request_id]
        if state["error"] is not None:
            raise RpcError(state["error"])
        return state["responses"]

    def call_streams(self, batches, service=SERVICE, func=STREAM_FUNC, timeout=None):
        """同一连接上多路复用：各批的帧轮转交错发送，响应按 request_id 归一。"""
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


def assert_error(test: unittest.TestCase, frame: Frame, code: int,
                 request_id=None, stream_id=None):
    """校验错误帧的 error_code（以及可选的 request_id / stream_id）。"""
    name = ERROR_NAMES.get(frame.error_code, f"UNKNOWN({frame.error_code})")
    want = ERROR_NAMES.get(code, code)
    test.assertEqual(frame.error_code, code,
                     f"期望错误码 {code}({want})，实际 {frame.error_code}({name})")
    if request_id is not None:
        test.assertEqual(frame.request_id, request_id, "错误帧 request_id 不符")
    if stream_id is not None:
        test.assertEqual(frame.stream_id, stream_id, "错误帧 stream_id 不符")


# ---------------------------------------------------------------- 测试

def setUpModule():
    try:
        socket.create_connection((HOST, PORT), timeout=2.0).close()
    except OSError as e:
        raise unittest.SkipTest(f"无法连接 {HOST}:{PORT}（{e}），请先启动 rpc_server")


class UnaryTest(unittest.TestCase):
    """非流式调用。"""

    def test_echo(self):
        """各种长度/字符集的请求都能原样回显。"""
        msgs = ["hello", "ping", "", "你好", "a" * 200, "x" * 1000, "\x00\x01\x02"]
        with RpcClient() as c:
            for msg in msgs:
                self.assertEqual(c.call_unary(make_request(msg)).res, msg, f"echo {msg!r} 不一致")

    def test_sequential_calls(self):
        """同一条连接上连续调用 100 次。"""
        with RpcClient() as c:
            for i in range(100):
                msg = f"seq-{i}"
                self.assertEqual(c.call_unary(make_request(msg)).res, msg)

    def test_pipelined_calls(self):
        """一次发出 50 个请求，按 request_id 回收（不依赖响应顺序）。"""
        with RpcClient() as c:
            pending = {}
            for i in range(50):
                msg = f"pipe-{i}"
                request_id = next_request_id()
                c.send_frames([build_frame(REQUEST, request_id, 0, SERVICE, FUNC,
                                           make_request(msg).SerializeToString())])
                pending[request_id] = msg
            states = c.collect({rid: "unary" for rid in pending}, timeout=10.0)
            got = {rid: s["responses"][0].res for rid, s in states.items()}
            self.assertEqual(got, pending)

    def test_unknown_function(self):
        """未知函数 -> ERR_UNKNOWN_FUNCTION(100)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(REQUEST, request_id, 0, SERVICE, "no_such_func",
                                       make_request("x").SerializeToString())])
            assert_error(self, c.read_error(), 100, request_id=request_id)

    def test_stream_function_as_unary(self):
        """用 REQUEST 调只注册为流的函数 -> ERR_UNKNOWN_FUNCTION(100)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(REQUEST, request_id, 0, SERVICE, STREAM_FUNC,
                                       make_request("x").SerializeToString())])
            assert_error(self, c.read_error(), 100, request_id=request_id)

    def test_bad_protobuf(self):
        """字段声明长度超过实际数据 -> ERR_UNKNOWN_PARAM(101)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(REQUEST, request_id, 0, SERVICE, FUNC, b"\x0a\xff")])
            assert_error(self, c.read_error(), 101, request_id=request_id)


class StreamTest(unittest.TestCase):
    """流式调用。"""

    def test_echo(self):
        """不同长度的多帧按顺序回显。"""
        msgs = ["x" * 300, "aa", "aaa", "fas", "asdsd"]
        with RpcClient() as c:
            responses = c.call_stream([make_request(m) for m in msgs])
            self.assertEqual([r.res for r in responses], msgs)

    def test_many_frames(self):
        """30 帧：STREAM_RESPONSE 的 stream_id 依次 1..30，STREAM_END 为 31。"""
        msgs = [f"msg-{i}" for i in range(30)]
        with RpcClient() as c:
            responses = c.call_stream([make_request(m) for m in msgs])
            self.assertEqual([r.res for r in responses], msgs)

    def test_thousand_frames(self):
        """1000 帧：数量与顺序都要正确（必然跨多次 recv）。"""
        msgs = [f"m{i}" for i in range(1000)]
        with RpcClient(timeout=30.0) as c:
            responses = c.call_stream([make_request(m) for m in msgs])
            self.assertEqual([r.res for r in responses], msgs)

    def test_three_concurrent_streams(self):
        """同一连接上 3 条流交错发送，各流内部编号与内容互不干扰。"""
        batches = [[make_request(f"a{i}") for i in range(3)],
                   [make_request(f"b{i}") for i in range(2)],
                   [make_request(f"c{i}") for i in range(4)]]
        with RpcClient() as c:
            results = c.call_streams(batches)
        self.assertEqual([[r.res for r in resps] for resps in results],
                         [[r.msg for r in b] for b in batches])

    def test_mixed_with_unary(self):
        """同一连接上 unary 与 stream 混用，响应按 request_id 区分。"""
        unary_msgs = ["u0", "u1"]
        stream_msgs = [f"s{i}" for i in range(3)]
        with RpcClient(timeout=10.0) as c:
            frames, plan, want = [], {}, {}
            for msg in unary_msgs:
                request_id = next_request_id()
                frames.append(build_frame(REQUEST, request_id, 0, SERVICE, FUNC,
                                          make_request(msg).SerializeToString()))
                plan[request_id] = "unary"
                want[request_id] = msg
            stream_id, stream_frames = build_stream_frames([make_request(m) for m in stream_msgs])
            frames += stream_frames
            plan[stream_id] = "stream"

            c.send_frames(frames)
            states = c.collect(plan, timeout=10.0)

            for request_id, state in states.items():
                self.assertIsNone(state["error"], f"request_id={request_id} 收到错误")
                if plan[request_id] == "unary":
                    self.assertEqual(state["responses"][0].res, want[request_id])
            self.assertEqual([r.res for r in states[stream_id]["responses"]], stream_msgs)

    def test_bad_protobuf(self):
        """流式请求里的非法 protobuf -> ERR_UNKNOWN_PARAM(101)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(STREAM_REQUEST, request_id, 1, SERVICE, STREAM_FUNC,
                                       b"\x0a\xff")])
            assert_error(self, c.read_error(), 101, request_id=request_id)


class ErrorCodeTest(unittest.TestCase):
    """错误路径：每类非法输入对应的错误码。"""

    def test_unknown_stream_function(self):
        """未知流式函数 -> 100，错误帧带出错帧的 stream_id。"""
        request_id = next_request_id()
        with RpcClient() as c:
            c.send_frames([build_frame(STREAM_REQUEST, request_id, 1, SERVICE, "no_such_stream",
                                       make_request("x").SerializeToString())])
            assert_error(self, c.read_error(), 100, request_id=request_id, stream_id=1)

    def test_stream_end_as_first_frame(self):
        """第一帧就是 STREAM_END -> ERR_BAD_STREAM_TYPE(3)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(STREAM_END, request_id, 1, SERVICE, STREAM_FUNC)])
            assert_error(self, c.read_error(), 3, request_id=request_id)

    def test_first_stream_id_not_one(self):
        """首帧 stream_id != 1 -> ERR_BAD_STREAM_ID(2)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_stream_request(request_id, 2)])
            assert_error(self, c.read_error(), 2, request_id=request_id, stream_id=2)

    def test_stream_id_gap(self):
        """stream_id 跳号（1 -> 3）-> ERR_BAD_STREAM_ID(2)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_stream_request(request_id, 1, "a"),
                           build_stream_request(request_id, 3, "b")])
            assert_error(self, c.read_error(), 2, request_id=request_id, stream_id=3)

    def test_stream_id_repeat(self):
        """stream_id 重复（1 -> 1）-> ERR_BAD_STREAM_ID(2)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_stream_request(request_id, 1, "a"),
                           build_stream_request(request_id, 1, "b")])
            assert_error(self, c.read_error(), 2, request_id=request_id)

    def test_request_id_zero(self):
        """request_id == 0 -> ERR_BAD_REQUEST(1)，错误帧 request_id 回 0。"""
        with RpcClient() as c:
            c.send_frames([build_frame(STREAM_REQUEST, 0, 1, SERVICE, STREAM_FUNC,
                                       make_request("x").SerializeToString())])
            assert_error(self, c.read_error(), 1, request_id=0)

    def test_unary_request_with_stream_id(self):
        """非流式请求带了 stream_id -> ERR_BAD_REQUEST(1)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(REQUEST, request_id, 5, SERVICE, FUNC,
                                       make_request("x").SerializeToString())])
            assert_error(self, c.read_error(), 1, request_id=request_id)

    def test_bad_type(self):
        """非法 type(99) -> ERR_BAD_REQUEST(1)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(99, request_id, 0, SERVICE, FUNC, b"")])
            assert_error(self, c.read_error(), 1, request_id=request_id)

    def test_bad_tail_magic_eob(self):
        """帧尾 0x0b 位置被改坏 -> ERR_BAD_REQUEST(1)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(STREAM_REQUEST, request_id, 1, SERVICE, STREAM_FUNC,
                                       make_request("x").SerializeToString(),
                                       tail=b"\x00\x0c")])
            assert_error(self, c.read_error(), 1, request_id=request_id)

    def test_bad_tail_magic_eoc(self):
        """帧尾 0x0c 位置被改坏 -> ERR_BAD_REQUEST(1)。"""
        with RpcClient() as c:
            request_id = next_request_id()
            c.send_frames([build_frame(STREAM_REQUEST, request_id, 1, SERVICE, STREAM_FUNC,
                                       make_request("x").SerializeToString(),
                                       tail=b"\x0b\x00")])
            assert_error(self, c.read_error(), 1, request_id=request_id)

    def test_bad_start_magic(self):
        """起始魔数第二个字节错误 -> ERR_BAD_REQUEST(1)。"""
        with RpcClient() as c:
            c.send_frames([b"\x0a\x00"])
            assert_error(self, c.read_error(), 1)


class FragmentationTest(unittest.TestCase):
    """发送侧分段：一帧被 TCP 切开时服务端必须能拼回去。

    服务端曾把整块读缓冲（含尾部补零）当帧数据，一次 recv 拿到半帧就会回
    ERR_BAD_REQUEST；这几条是那次的回归保护。
    """

    def test_byte_by_byte(self):
        """逐字节发送。"""
        msgs = ["one", "two"]
        with RpcClient() as c:
            responses = c.call_stream([make_request(m) for m in msgs], cuts="byte")
            self.assertEqual([r.res for r in responses], msgs)

    def test_frame_per_write(self):
        """每帧一次 sendall。"""
        msgs = [f"msg-{i}" for i in range(30)]
        with RpcClient() as c:
            request_id, frames = build_stream_frames([make_request(m) for m in msgs])
            c.send_frames(frames, cuts="frame")
            state = c.collect({request_id: "stream"})[request_id]
            self.assertIsNone(state["error"])
            self.assertEqual([r.res for r in state["responses"]], msgs)

    def test_every_split_offset(self):
        """在每一个字节位置切一刀，逐点验证。"""
        msgs = ["aa", "bb"]
        request_id, frames = build_stream_frames([make_request(m) for m in msgs])
        total = len(b"".join(frames))

        for cut in range(1, total):
            with RpcClient() as c:
                c.send_frames(frames, cuts=[cut])
                state = c.collect({request_id: "stream"}, timeout=5.0)[request_id]
                self.assertIsNone(state["error"], f"cut={cut}: 收到错误码 {state['error']}")
                self.assertEqual([r.res for r in state["responses"]], msgs, f"cut={cut}")

    def test_random_splits(self):
        """固定种子的随机切分，重复 20 次。"""
        rng = random.Random(20261006)
        msgs = [f"r{i}" for i in range(4)]
        request_id, frames = build_stream_frames([make_request(m) for m in msgs])
        data = b"".join(frames)

        for i in range(20):
            cuts = sorted(rng.sample(range(1, len(data)), rng.randint(1, 5)))
            with RpcClient() as c:
                c.send_frames([data], cuts=cuts)
                state = c.collect({request_id: "stream"}, timeout=5.0)[request_id]
                self.assertIsNone(state["error"], f"iter={i} cuts={cuts}")
                self.assertEqual([r.res for r in state["responses"]], msgs, f"iter={i} cuts={cuts}")


class RobustnessTest(unittest.TestCase):
    """连接级健壮性。"""

    def test_many_short_connections(self):
        """反复建连-调用-断开，服务端始终能接受新连接。"""
        for i in range(60):
            with RpcClient() as c:
                self.assertEqual(c.call_unary(make_request(f"conn-{i}")).res, f"conn-{i}")

    def test_abrupt_close_mid_frame(self):
        """发半帧后 RST，服务端不能挂，后续连接必须正常。"""
        frame = build_frame(STREAM_REQUEST, next_request_id(), 1, SERVICE, STREAM_FUNC,
                            make_request("partial").SerializeToString())
        client = RpcClient()
        try:
            client.sock.sendall(frame[:len(frame) // 2])
            client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                   struct.pack("ii", 1, 0))  # 触发 RST
        finally:
            client.close()
        time.sleep(0.1)
        with RpcClient() as c:
            self.assertEqual(c.call_unary(make_request("ok")).res, "ok")

    def test_junk_prefix_then_valid_frame(self):
        """垃圾前缀应报 ERR_BAD_REQUEST，随后的合法帧仍能被处理。"""
        with RpcClient() as c:
            request_id = next_request_id()
            good = build_frame(REQUEST, request_id, 0, SERVICE, FUNC,
                               make_request("resync").SerializeToString())
            c.send_frames([b"\x0a\x00" + good])

            saw_error, response = False, None
            deadline = time.monotonic() + TIMEOUT
            while time.monotonic() < deadline and response is None:
                frame = c.recv_frame(deadline - time.monotonic())
                if frame.type == REQ_ERROR:
                    saw_error = True
                    self.assertEqual(frame.error_code, 1)
                elif frame.type == RESPONSE and frame.request_id == request_id:
                    response = parse_response(frame)
            self.assertTrue(saw_error, "没有收到针对垃圾数据的错误帧")
            self.assertIsNotNone(response, "垃圾之后没有重新同步并处理合法帧")
            self.assertEqual(response.res, "resync")

    def test_junk_does_not_break_server(self):
        """随机垃圾数据不能把服务端弄挂，之后新连接仍可用。"""
        rng = random.Random(20261006)
        for _ in range(10):
            junk = bytes(rng.randrange(256) for _ in range(64))
            with RpcClient(timeout=0.5) as c:
                try:
                    c.send_frames([junk])
                    c.sock.settimeout(0.2)
                    c.sock.recv(4096)  # 有响应就读掉，超时也算正常
                except (TimeoutError, OSError):
                    pass
        with RpcClient() as c:
            self.assertEqual(c.call_unary(make_request("alive")).res, "alive")


class ConcurrencyTest(unittest.TestCase):
    """多线程并发。"""

    def test_unary_8_threads(self):
        """8 个线程各自建连并发 unary。"""
        errors = []

        def worker(tid):
            try:
                with RpcClient(timeout=15.0) as c:
                    for i in range(10):
                        msg = f"c{tid}-{i}"
                        assert c.call_unary(make_request(msg)).res == msg
            except Exception as e:  # noqa: BLE001
                errors.append(f"thread{tid}: {type(e).__name__}: {e}")

        self._run_threads(worker)
        self.assertEqual(errors, [])

    def test_streams_8_threads(self):
        """8 个线程各自建连并发 stream。"""
        errors = []

        def worker(tid):
            try:
                msgs = [f"t{tid}-{i}" for i in range(5)]
                with RpcClient(timeout=15.0) as c:
                    responses = c.call_stream([make_request(m) for m in msgs])
                    assert [r.res for r in responses] == msgs
            except Exception as e:  # noqa: BLE001
                errors.append(f"thread{tid}: {type(e).__name__}: {e}")

        self._run_threads(worker)
        self.assertEqual(errors, [])

    @staticmethod
    def _run_threads(worker, count=8):
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()


class LargePayloadTest(unittest.TestCase):
    """大负载（必然跨多次 recv）。"""

    def test_unary_60k(self):
        """60KB 单帧 echo。"""
        msg = "A" * 60000
        with RpcClient(timeout=20.0) as c:
            self.assertEqual(c.call_unary(make_request(msg)).res, msg)


if __name__ == "__main__":
    print(f"目标服务端 {HOST}:{PORT}")
    unittest.main(verbosity=2)
