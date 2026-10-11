//
// RPC 二进制帧协议：帧格式、解析状态机与响应构造。
// 这里只负责字节流与帧结构，protobuf 载荷的编解码见 RpcCodec.h。
//

#ifndef DEMO_RPCPROTOCOL_H
#define DEMO_RPCPROTOCOL_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace rpc {

// 这个是旧的协议
/*
 * 0x0a 0x0b    // 2
 * type 1 字节  1 = REQUEST 2 = RESPONSE 3 = ERROR server端只会接收到 1 // 3
 * request_id   8 字节 大端 无符号，不能为 0 // 11
 * error_code  1 字节，server 端只会接收到 0 // 12
 * service_len 1 字节 // 13
 * service_name for service_len
 * func_len 1 字节 // 14 + service_len
 * function name for func_len // 14 + service_len + func_len
 * parma_len  4 字节 大端  // 18 + service_len + func_len
 * serialize parma for parma_len // 18 + service_len + func_len + parma_len
 * 0x0b 0x0c // 20 + service_len + func_len + parma_len
 */

// 这个是新的协议
/*
 * 0x0a 0x0b    // 2
 * type 1 字节  1 = REQUEST 2 = RESPONSE 3 = STREAM_REQUEST 4 = STREAM_RESPONSE 5 = STREAM_END 255 = ERROR server端只会接收到 1 3 5 // 3
 * request_id 8 字节 大端 无符号，不能为 0 // 11
 * stream_id 8 字节 大端 无符号，若是 STREAM_xxx 或者 STREAM_ERROR 不能为 0， 否则为 0 // 19
 * error_code  1 字节，server 端只会接收到 0 // 20
 * service_len 1 字节 // 21
 * func_len 1 字节 // 22
 * parma_len  2 字节 大端 // 24
 * service_name for service_len
 * function name for func_len // service_len + func_len
 * serialize parma for parma_len // service_len + func_len + parma_len
 * 0x0b 0x0c // 26 + service_len + func_len + parma_len
 */

// 解析状态机的当前状态
enum ParserStatus {
    SOA,          // 找 0x0a
    SOB,          // 校验 0x0b
    HEADER,       // 读固定头 22 字节并统一校验
    SERVICE_NAME, // 读 service_name
    FUNC_NAME,    // 读 func_name
    PARAM_STRING, // 读 params
    EOB,          // 校验 0x0b
    EOC,          // 校验 0x0c，完成
};

// 一次解析的结果
enum ParserResult {
    COMPLETE,
    WAITING,
    ERROR,
};

// 帧头 error_code 字段
enum ErrorCode : char {
    ERR_BAD_INTERNAL = 0,
    ERR_BAD_REQUEST  = 1,
    ERR_BAD_STREAM_ID = 2,
    ERR_BAD_STREAM_TYPE = 3,

    ERR_UNKNOWN_FUNCTION = 100,
    ERR_UNKNOWN_PARAM    = 101,
    ERR_UNKNOWN_RESPONSE = 102,
};

// 帧头 type 字段
enum RequestType : unsigned char {
    REQUEST          = 1,
    RESPONSE         = 2,
    STREAM_REQUEST   = 3,
    STREAM_RESPONSE  = 4,
    STREAM_END       = 5,
    REQ_ERROR        = 255,
};

// 一帧的解析结果，兼作增量解析过程中的状态
struct RpcContext {
    ParserStatus status;
    ParserResult result;

    RequestType type;
    std::uint64_t request_id;
    std::uint64_t stream_id;

    std::string service_name;
    std::string func_name;
    std::string params;
    std::vector<char> buffer;

    RpcContext()
        : status(SOA),
          result(WAITING),
          type(REQUEST),
          request_id(0),
          stream_id(0) {}

    void resetMessage() {
        // 故意不清 type / request_id / stream_id，便于错误处理时回错误响应。
        service_name.clear();
        func_name.clear();
        params.clear();
        buffer.clear();
    }

    bool isStream() const {
        return (type == STREAM_REQUEST || type == STREAM_END) && stream_id != 0;
    }

    bool isRequest() const {
        return type == REQUEST && request_id != 0;
    }

    RpcContext(RpcContext&& context) noexcept
        : status(context.status),
          result(context.result),
          type(context.type),
          request_id(context.request_id),
          stream_id(context.stream_id),
          service_name(std::move(context.service_name)),
          func_name(std::move(context.func_name)),
          params(std::move(context.params)),
          buffer(std::move(context.buffer)) {
        context.status = SOA;
        context.result = WAITING;
        context.type = REQUEST;
        context.request_id = 0;
        context.stream_id = 0;
    }

    RpcContext& operator=(RpcContext&& context) noexcept {
        if (this != &context) {
            status = context.status;
            result = context.result;
            type = context.type;
            request_id = context.request_id;
            stream_id = context.stream_id;
            service_name = std::move(context.service_name);
            func_name = std::move(context.func_name);
            params = std::move(context.params);
            buffer = std::move(context.buffer);

            context.status = SOA;
            context.result = WAITING;
            context.type = REQUEST;
            context.request_id = 0;
            context.stream_id = 0;
        }
        return *this;
    }
};

/*
 * 从 buffer[start, end) 中增量解析一帧，parsed 返回本次消费掉的字节数：
 *   COMPLETE：解析出一帧完整数据，字段放在 context 中；
 *   WAITING ：数据不够，保留 context 状态，等下一次调用继续；
 *   ERROR   ：帧非法，context 只保留 type / request_id / stream_id 用于回错误响应。
 */
void parse(
    const std::string& buffer,
    std::size_t start,
    std::size_t end,
    std::size_t& parsed,
    RpcContext& context
);

// 请求级错误响应：stream_id = 0
std::string buildErrorResponse(std::uint64_t request_id, char error_code);

// 流级错误响应：stream_id != 0
std::string buildErrorResponse(
    std::uint64_t request_id,
    std::uint64_t stream_id,
    char error_code
);

// 非流式响应：stream_id = 0
std::string buildResponse(const std::string& body, std::uint64_t request_id);

// 流式响应数据帧：STREAM_RESPONSE
std::string buildStreamResponse(
    const std::string& body,
    std::uint64_t request_id,
    std::uint64_t stream_id
);

// 流式正常结束：STREAM_END，param_len = 0
std::string buildStreamEnd(std::uint64_t request_id, std::uint64_t stream_id);

} // namespace rpc

#endif // DEMO_RPCPROTOCOL_H
