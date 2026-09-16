#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RPC 客户端冒烟测试

请求协议:
  0x0a 0x0b
  type        (1 字节)  1 = REQUEST
  request_id  (8 字节, 大端, 无符号, 不能为 0)
  name_len    (1 字节)
  function name
  param_len   (4 字节, 大端)
  serialized param
  0x0b 0x0c

响应协议:
  成功:
    0x0a 0x0b
    type        (1 字节)  2 = RESPONSE
    request_id  (8 字节, 大端)
    name_len    (1 字节)  服务端固定 0
    param_len   (4 字节, 大端)
    serialized body
    0x0b 0x0c

  错误:
    0x0a 0x0b
    type        (1 字节)  3 = ERROR
    request_id  (8 字节, 大端)
    error_code  (1 字节)
    param_len   (4 字节, 大端) 服务端固定 0
    0x0b 0x0c
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
PORT = 8881

START = b"\x0a\x0b"
END = b"\x0b\x0c"

REQUEST_TYPE = 1
RESPONSE_TYPE = 2
ERROR_TYPE = 3

REQUEST_ID_FMT = ">Q"
PARAM_LEN_FMT = ">I"

# 等第一个响应字节的超时
FIRST_TIMEOUT = 5.0

# 接收缓冲区大小
RECV_SIZE = 65536

DEBUG_RAW = False

ERROR_NAMES = {
    1: "ERR_BAD_REQUEST",
    3: "ERR_BAD_INTERNAL",
    100: "ERR_UNKNOWN_FUNCTION",
    101: "ERR_UNKNOWN_PARAM",
    102: "ERR_UNKNOWN_RESPONSE",
}


_id_counter = itertools.count(1)


def next_request_id() -> int:
    """生成非 0 的 u64 request_id。"""
    while True:
        rid = next(_id_counter) & 0xFFFFFFFFFFFFFFFF
        if rid != 0:
            return rid


# ---------------------- 请求打包 ----------------------

def build_packet(func_name: str, request, request_id: int = None) -> bytes:
    if request_id is None:
        request_id = next_request_id()

    assert 0 < request_id <= 0xFFFFFFFFFFFFFFFF, "request_id must be non-zero u64"

    name_b = func_name.encode("utf-8")
    param_b = request.SerializeToString()

    assert len(name_b) <= 0xFF, "function name too long"

    pkt = bytearray()
    pkt += START
    pkt.append(REQUEST_TYPE)                          # type (1B)
    pkt += struct.pack(REQUEST_ID_FMT, request_id)    # request_id (8B, BE)
    pkt.append(len(name_b))                           # name_len (1B)
    pkt += name_b                                     # function name
    pkt += struct.pack(PARAM_LEN_FMT, len(param_b))   # param_len (4B, BE)
    pkt += param_b                                    # serialized param
    pkt += END

    return bytes(pkt)



# extra:
#   RESPONSE_TYPE -> name_len
#   ERROR_TYPE    -> error_code
Frame = namedtuple("Frame", ["type", "request_id", "body", "extra"])


class RpcError(Exception):
    def __init__(self, code: int):
        self.code = code
        name = ERROR_NAMES.get(code, f"UNKNOWN({code})")
        super().__init__(f"RPC error {code}: {name}")


class RpcClient:
    def __init__(
            self,
            host=HOST,
            port=PORT,
            first_timeout=FIRST_TIMEOUT,
    ):
        self.first_timeout = first_timeout
        self.sock = socket.create_connection((host, port), timeout=first_timeout)

        # 长连接接收缓冲区，可能包含多个帧或半帧
        self._buf = bytearray()

        # 已经收到但暂时不匹配的响应帧，按 request_id 缓存
        self._pending = {}

    def _try_parse_frame(self):
        """
        尝试从 self._buf 中解析一个完整响应帧。
        解析成功则消费缓冲区并返回 Frame；数据不足返回 None。
        """
        buf = self._buf

        if len(buf) < 2:
            return None

        # 同步到 START
        pos = buf.find(START)
        if pos < 0:
            # 保留最后一个字节，可能是 START 的前半部分 0x0a
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

        # 客户端只接受 RESPONSE / ERROR
        if frame_type not in (RESPONSE_TYPE, ERROR_TYPE):
            del buf[0]
            return None

        # start(2) + type(1) + request_id(8) + name_len/error_code(1) + param_len(4)
        if len(buf) < 16:
            return None

        request_id = struct.unpack(">Q", buf[3:11])[0]
        param_len = struct.unpack(">I", buf[12:16])[0]

        # 成功帧和错误帧长度都是 18 + param_len
        total = 18 + param_len
        if len(buf) < total:
            return None

        body = bytes(buf[16:16 + param_len])

        if bytes(buf[16 + param_len:18 + param_len]) != END:
            # 帧尾不合法，丢弃 START 重新同步
            del buf[:2]
            return None

        extra = buf[11]  # RESPONSE: name_len, ERROR: error_code

        del buf[:total]
        return Frame(frame_type, request_id, body, extra)

    def _recv_frame(self, expect_request_id: int) -> Frame:
        """
        读取并返回指定 request_id 的完整响应帧。
        如果先收到其它 request_id 的帧，会缓存起来。
        """
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
                raise TimeoutError(
                    f"等待响应超时: request_id={expect_request_id}"
                )

            if not chunk:
                raise ConnectionError("server closed connection")

            if DEBUG_RAW:
                print(f"  [raw] recv {chunk.hex(' ')}")

            self._buf.extend(chunk)

    def call(self, func_name: str, request, request_id: int = None):
        if request_id is None:
            request_id = next_request_id()

        pkt = build_packet(func_name, request, request_id=request_id)
        self.sock.sendall(pkt)

        frame = self._recv_frame(request_id)

        if frame.type == ERROR_TYPE:
            raise RpcError(frame.extra)

        # frame.type == RESPONSE_TYPE
        resp = hello_pb2.HelloWorldResponse()
        resp.ParseFromString(frame.body)
        return resp

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()



def check(client: RpcClient, name: str, req_msg: str, expect: str):
    req = hello_pb2.HelloWorldRequest()
    req.msg = req_msg

    resp = client.call("hello", req)

    ok = resp.res == expect
    flag = "PASS" if ok else "FAIL"
    print(f"[{flag}] {name}: msg={req_msg!r} -> res={resp.res!r} (expect {expect!r})")
    assert ok, f"{name} failed"


def check_error(client: RpcClient, name: str, func_name: str, req_msg: str, expect_code: int):
    req = hello_pb2.HelloWorldRequest()
    req.msg = req_msg

    try:
        client.call(func_name, req)
    except RpcError as e:
        ok = e.code == expect_code
        flag = "PASS" if ok else "FAIL"
        print(f"[{flag}] {name}: error_code={e.code} (expect {expect_code})")
        assert ok, f"{name} failed"
    else:
        print(f"[FAIL] {name}: expected RpcError({expect_code}), but call succeeded")
        assert False, f"{name} failed"


def main() -> int:
    with RpcClient() as client:
        check(client, "hello -> world", "hello", "world")
        check(client, "echo ping",      "ping",  "ping")
        check(client, "echo empty",     "",      "")
        check(client, "echo utf8",      "你好",  "你好")
        check(client, "echo long",      "a" * 10, "a" * 10)

        check_error(client, "unknown function", "no_such_function", "x", 100)

    return 0


if __name__ == "__main__":
    sys.exit(main())