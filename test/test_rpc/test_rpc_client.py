#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RPC 客户端冒烟测试（新协议）

帧布局:
  0x0a 0x0b
  type          (1)  1=REQ 2=RESP 3=STREAM_REQ 4=STREAM_RESP 5=STREAM_END 255=ERROR
  request_id    (8, BE, u64, 非 0)
  stream_id     (8, BE, u64; 非流式=0, 流式非 0)
  error_code    (1)
  service_len   (1)
  func_len      (1)
  param_len     (2, BE)
  service_name
  func_name
  body
  0x0b 0x0c
总长 = 26 + service_len + func_len + param_len
"""

import itertools
import os
import socket
import struct
import sys
from collections import namedtuple

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "protoc"))

import hello_pb2  # noqa: E402

HOST = "127.0.0.1"
PORT = 8891

START = b"\x0a\x0b"
END = b"\x0b\x0c"

REQUEST_TYPE         = 1
RESPONSE_TYPE        = 2
STREAM_REQUEST_TYPE  = 3
STREAM_RESPONSE_TYPE = 4
STREAM_END_TYPE      = 5
ERROR_TYPE           = 255

REQUEST_ID_FMT = ">Q"
STREAM_ID_FMT  = ">Q"
PARAM_LEN_FMT  = ">H"

FIRST_TIMEOUT = 5.0
RECV_SIZE = 65536
DEBUG_RAW = False

DEFAULT_SERVICE = "HelloService"

ERROR_NAMES = {
    0:   "ERR_BAD_INTERNAL",
    1:   "ERR_BAD_REQUEST",
    2:   "ERR_BAD_STREAM_ID",
    100: "ERR_UNKNOWN_FUNCTION",
    101: "ERR_UNKNOWN_PARAM",
    102: "ERR_UNKNOWN_RESPONSE",
}

_id_counter = itertools.count(1)


def next_request_id() -> int:
    while True:
        rid = next(_id_counter) & 0xFFFFFFFFFFFFFFFF
        if rid != 0:
            return rid


# ---------------------- 打包 ----------------------

def build_packet(service_name, func_name, request, request_id=None,
                 stream_id=0, msg_type=REQUEST_TYPE) -> bytes:
    """
    request 为 None 时表示空 body（用于 STREAM_END）。
    """
    if request_id is None:
        request_id = next_request_id()

    assert 0 < request_id <= 0xFFFFFFFFFFFFFFFF
    assert 0 <= stream_id <= 0xFFFFFFFFFFFFFFFF

    svc_b = service_name.encode("utf-8")
    func_b = func_name.encode("utf-8")
    param_b = b"" if request is None else request.SerializeToString()

    assert len(svc_b) <= 0xFF
    assert len(func_b) <= 0xFF
    assert len(param_b) <= 0xFFFF, "param too long for u16"

    pkt = bytearray()
    pkt += START
    pkt.append(msg_type)
    pkt += struct.pack(REQUEST_ID_FMT, request_id)
    pkt += struct.pack(STREAM_ID_FMT, stream_id)
    pkt.append(0)                             # error_code
    pkt.append(len(svc_b))
    pkt.append(len(func_b))
    pkt += struct.pack(PARAM_LEN_FMT, len(param_b))
    pkt += svc_b
    pkt += func_b
    pkt += param_b
    pkt += END
    return bytes(pkt)


Frame = namedtuple("Frame", ["type", "request_id", "stream_id", "body", "extra"])


class RpcError(Exception):
    def __init__(self, code: int):
        self.code = code
        name = ERROR_NAMES.get(code, f"UNKNOWN({code})")
        super().__init__(f"RPC error {code}: {name}")


class RpcClient:
    def __init__(self, host=HOST, port=PORT, first_timeout=FIRST_TIMEOUT):
        self.first_timeout = first_timeout
        self.sock = socket.create_connection((host, port), timeout=first_timeout)
        self._buf = bytearray()
        self._pending = {}

    def _try_parse_frame(self):
        buf = self._buf
        if len(buf) < 2:
            return None

        pos = buf.find(START)
        if pos < 0:
            if buf and buf[-1] == 0x0a:
                del buf[:-1]
            else:
                buf.clear()
            return None
        if pos > 0:
            del buf[:pos]

        if len(buf) < 3:
            return None

        frame_type = buf[2]
        if frame_type not in (RESPONSE_TYPE, STREAM_RESPONSE_TYPE,
                              STREAM_END_TYPE, ERROR_TYPE):
            del buf[0]
            return None

        if len(buf) < 24:
            return None

        request_id  = struct.unpack(">Q", buf[3:11])[0]
        stream_id   = struct.unpack(">Q", buf[11:19])[0]
        error_code  = buf[19]
        service_len = buf[20]
        func_len    = buf[21]
        param_len   = struct.unpack(">H", buf[22:24])[0]

        total = 26 + service_len + func_len + param_len
        if len(buf) < total:
            return None

        body_start = 24 + service_len + func_len
        body = bytes(buf[body_start:body_start + param_len])

        end_start = body_start + param_len
        if bytes(buf[end_start:end_start + 2]) != END:
            del buf[:2]
            return None

        del buf[:total]
        return Frame(frame_type, request_id, stream_id, body, error_code)

    def _recv_frame(self, expect_request_id: int) -> Frame:
        if expect_request_id in self._pending:
            return self._pending.pop(expect_request_id)

        self.sock.settimeout(self.first_timeout)

        while True:
            frame = self._try_parse_frame()
            if frame is not None:
                if frame.request_id == expect_request_id:
                    return frame
                self._pending[frame.request_id] = frame
                continue

            try:
                chunk = self.sock.recv(RECV_SIZE)
            except socket.timeout:
                raise TimeoutError(f"等待响应超时: request_id={expect_request_id}")

            if not chunk:
                raise ConnectionError("server closed connection")

            if DEBUG_RAW:
                print(f"  [raw] recv {chunk.hex(' ')}")
            self._buf.extend(chunk)

    # ---------- 一元 ----------

    def call(self, func_name, request, request_id=None,
             service_name=DEFAULT_SERVICE):
        if request_id is None:
            request_id = next_request_id()

        pkt = build_packet(service_name, func_name, request, request_id,
                           stream_id=0, msg_type=REQUEST_TYPE)
        self.sock.sendall(pkt)

        frame = self._recv_frame(request_id)

        if frame.type == ERROR_TYPE:
            raise RpcError(frame.extra)
        if frame.type != RESPONSE_TYPE:
            raise RpcError(102)
        if frame.extra != 0:
            raise RpcError(frame.extra)

        resp = hello_pb2.HelloWorldResponse()
        resp.ParseFromString(frame.body)
        return resp

    # ---------- 流式 ----------

    def call_stream(self, func_name, request, request_id=None,
                    service_name=DEFAULT_SERVICE):
        """
        发送:
          STREAM_REQUEST (stream_id=1) 带 protobuf body
          STREAM_END     (stream_id=2) 空 body —— 通知服务端输入结束
        接收:
          逐块 yield STREAM_RESPONSE 的 protobuf body (bytes)
          收到 STREAM_END 停止
          收到 ERROR 抛 RpcError
        """
        if request_id is None:
            request_id = next_request_id()

        pkt = build_packet(service_name, func_name, request, request_id,
                           stream_id=1, msg_type=STREAM_REQUEST_TYPE)
        self.sock.sendall(pkt)

        end_pkt = build_packet(service_name, func_name, None, request_id,
                               stream_id=2, msg_type=STREAM_END_TYPE)
        self.sock.sendall(end_pkt)

        while True:
            frame = self._recv_frame(request_id)

            if frame.type == ERROR_TYPE:
                raise RpcError(frame.extra)

            if frame.type == STREAM_END_TYPE:
                return

            if frame.type == STREAM_RESPONSE_TYPE:
                yield frame.body
                continue

            raise RpcError(102)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ---------------------- 测试用例 ----------------------

def check(client, name, req_msg, expect,
          service_name=DEFAULT_SERVICE, func_name="hello"):
    req = hello_pb2.HelloWorldRequest()
    req.msg = req_msg
    resp = client.call(func_name, req, service_name=service_name)
    ok = resp.res == expect
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {req_msg!r} -> {resp.res!r} (expect {expect!r})")
    assert ok


def check_error(client, name, func_name, req_msg, expect_code,
                service_name=DEFAULT_SERVICE):
    req = hello_pb2.HelloWorldRequest()
    req.msg = req_msg
    try:
        client.call(func_name, req, service_name=service_name)
    except RpcError as e:
        ok = e.code == expect_code
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: error_code={e.code} (expect {expect_code})")
        assert ok
    else:
        print(f"[FAIL] {name}: expected RpcError({expect_code}), got success")
        assert False


def check_stream(client, name, func_name, req_msg, expect_resps,
                 service_name=DEFAULT_SERVICE):
    """
    expect_resps: list[str]，每个 STREAM_RESPONSE body 解出来的 HelloWorldResponse.res
    """
    req = hello_pb2.HelloWorldRequest()
    req.msg = req_msg

    chunks = list(client.call_stream(func_name, req, service_name=service_name))

    got = []
    for c in chunks:
        resp = hello_pb2.HelloWorldResponse()
        resp.ParseFromString(c)
        got.append(resp.res)

    ok = got == expect_resps
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: got {got} (expect {expect_resps})")
    assert ok


def main() -> int:
    with RpcClient() as client:
        # 一元
        check(client, "hello", "hello", "hello")
        check(client, "echo ping",      "ping",  "ping")
        check(client, "echo empty",     "",      "")
        check(client, "echo utf8",      "你好",  "你好")
        check(client, "echo long",      "a" * 10, "a" * 10)
        check_error(client, "unknown function", "no_such_function", "x", 100)

        # 流式: helloStream 对每个输入 echo 一个 HelloWorldResponse
        check_stream(client, "stream echo hello", "helloStream", "hello", ["hello"])
        check_stream(client, "stream echo ping",  "helloStream", "ping",  ["ping"])
        check_stream(client, "stream echo empty", "helloStream", "",      [""])
        check_stream(client, "stream echo utf8",  "helloStream", "你好",  ["你好"])

    return 0


if __name__ == "__main__":
    sys.exit(main())