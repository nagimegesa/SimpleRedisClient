//
// RPC 层异常定义。
// 载荷编解码（RpcCodec）在解析/序列化失败时抛出，RpcServer 捕获后转成帧错误码。
//

#ifndef DEMO_RPCEXCEPTION_H
#define DEMO_RPCEXCEPTION_H

#include <stdexcept>
#include <string>

namespace rpc {

class RpcException : public std::runtime_error {
public:
    explicit RpcException(const std::string& msg) : std::runtime_error(msg) {}
};

// 请求体（param）反序列化失败
class RpcBadParamException : public RpcException {
public:
    explicit RpcBadParamException(const std::string& msg) : RpcException(msg) {}
};

// 响应体序列化失败
class RpcBadResponseException : public RpcException {
public:
    explicit RpcBadResponseException(const std::string& msg) : RpcException(msg) {}
};

} // namespace rpc

#endif // DEMO_RPCEXCEPTION_H
