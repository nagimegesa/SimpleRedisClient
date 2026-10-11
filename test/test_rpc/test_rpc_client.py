#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RPC 客户端冒烟测试（新协议）。

覆盖最小可用路径：一元 echo（长度 / 字符集）、未知函数错误码、流式 echo。
协议一致性、错误码、TCP 分段、健壮性与并发见 `test_client_error.py`；
指标 / 压测见 `rpc_metrics.py`；协议与客户端实现在 `rpc_client.py`。

帧布局::

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

运行::

    python3 test_rpc_client.py                      # 默认 127.0.0.1:8891
    RPC_PORT=8899 python3 test_rpc_client.py        # 换端口
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from rpc_client import (  # noqa: E402
    HOST, PORT, RpcClient, RpcError, make_request, probe,
)

DEFAULT_SERVICE = "HelloService"

# ---------------------------------------------------------------- 测试用例


def check(client, name, req_msg, expect, func_name="hello",
          service_name=DEFAULT_SERVICE):
    """一元调用：期望 resp.res == expect。"""
    resp = client.call_unary(make_request(req_msg), service=service_name, func=func_name)
    ok = resp.res == expect
    print("[%s] %s: %r -> %r (expect %r)"
          % ("PASS" if ok else "FAIL", name, req_msg, resp.res, expect))
    assert ok


def check_error(client, name, func_name, req_msg, expect_code,
                service_name=DEFAULT_SERVICE):
    """一元调用：期望收到指定错误码。"""
    try:
        client.call_unary(make_request(req_msg), service=service_name, func=func_name)
    except RpcError as e:
        ok = e.code == expect_code
        print("[%s] %s: error_code=%s (expect %s)"
              % ("PASS" if ok else "FAIL", name, e.code, expect_code))
        assert ok
    else:
        print("[FAIL] %s: expected RpcError(%s), got success" % (name, expect_code))
        assert False


def check_stream(client, name, func_name, req_msg, expect_resps,
                 service_name=DEFAULT_SERVICE):
    """流式调用：期望每个 STREAM_RESPONSE 解出的 res 与 expect_resps 一致。"""
    responses = client.call_stream([make_request(req_msg)], service=service_name,
                                  func=func_name)
    got = [r.res for r in responses]
    ok = got == expect_resps
    print("[%s] %s: got %s (expect %s)"
          % ("PASS" if ok else "FAIL", name, got, expect_resps))
    assert ok


def setUpModule():
    # 与改版前的行为一致：连不上服务端就是失败（不是 skip），避免 CI 误判绿灯
    if not probe(HOST, PORT):
        raise ConnectionError("无法连接 %s:%s，请先启动 rpc_server" % (HOST, PORT))


class SmokeTest(unittest.TestCase):
    """冒烟：一元 / 流式最小路径。"""

    def test_smoke(self):
        with RpcClient() as client:
            # 一元
            check(client, "hello", "hello", "hello")
            check(client, "echo ping", "ping", "ping")
            check(client, "echo empty", "", "")
            check(client, "echo utf8", "你好", "你好")
            check(client, "echo long", "a" * 10, "a" * 10)
            check_error(client, "unknown function", "no_such_function", "x", 100)

            # 流式: helloStream 对每个输入 echo 一个 HelloWorldResponse
            check_stream(client, "stream echo hello", "helloStream", "hello", ["hello"])
            check_stream(client, "stream echo ping", "helloStream", "ping", ["ping"])
            check_stream(client, "stream echo empty", "helloStream", "", [""])
            check_stream(client, "stream echo utf8", "helloStream", "你好", ["你好"])


if __name__ == "__main__":
    print("目标服务端 %s:%s" % (HOST, PORT))
    unittest.main(verbosity=2)
