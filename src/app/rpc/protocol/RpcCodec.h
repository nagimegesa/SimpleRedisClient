//
// RPC 载荷编解码：protobuf 消息 <-> 帧的 param 字段（std::string）。
// 帧本身的结构与解析见 RpcProtocol.h。
//

#ifndef DEMO_RPCCODEC_H
#define DEMO_RPCCODEC_H

#include <string>
#include <type_traits>

#include <google/protobuf/message.h>

#include "RpcException.h"
#include "corou/Generator.h"

namespace rpc {

struct RpcCodec {
    // param -> protobuf 消息，失败抛 RpcBadParamException
    template <typename T> requires std::is_base_of_v<::google::protobuf::Message, T>
    static T decode(const std::string& param) {
        T message;
        if (!message.ParseFromString(param)) {
            throw RpcBadParamException("bad parameters");
        }
        return message;
    }

    // protobuf 消息 -> param，失败抛 RpcBadResponseException
    template <typename T> requires std::is_base_of_v<::google::protobuf::Message, T>
    static std::string encode(const T& message) {
        std::string param;
        if (!message.SerializeToString(&param)) {
            throw RpcBadResponseException("bad response");
        }
        return param;
    }

    // 一帧一个 param 的流 -> 消息流
    template <typename T> requires std::is_base_of_v<::google::protobuf::Message, T>
    static AsyncGenerator<T> decodeStream(AsyncGenerator<std::string> paramStream) {
        while (auto param = co_await paramStream.next()) {
            co_yield decode<T>(*param);
        }
    }

    // 消息流 -> 一帧一个 param 的流
    template <typename T> requires std::is_base_of_v<::google::protobuf::Message, T>
    static AsyncGenerator<std::string> encodeStream(AsyncGenerator<T> messageStream) {
        while (auto message = co_await messageStream.next()) {
            co_yield encode(*message);
        }
    }
};

} // namespace rpc

#endif // DEMO_RPCCODEC_H
