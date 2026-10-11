//
// RpcProtocol 实现：字节序读写辅助、帧解析状态机、响应构造。
//

#include "RpcProtocol.h"

#include <algorithm>
#include <cstring>

#include "logger/Logger.h"

namespace rpc {

namespace {
    constexpr unsigned char kMagic0 = 0x0a;
    constexpr unsigned char kMagic1 = 0x0b;
    constexpr unsigned char kMagic2 = 0x0c;

    // 固定头长度：type(1)+request_id(8)+stream_id(8)+error_code(1)
    //            +service_len(1)+func_len(1)+param_len(2) = 22
    constexpr std::size_t kFixedHeaderSize = 22;

    // 固定头内部偏移
    constexpr std::size_t kOffsetType       = 0;
    constexpr std::size_t kOffsetRequestId  = 1;
    constexpr std::size_t kOffsetStreamId   = 9;
    constexpr std::size_t kOffsetErrorCode  = 17;
    constexpr std::size_t kOffsetServiceLen = 18;
    constexpr std::size_t kOffsetFuncLen    = 19;
    constexpr std::size_t kOffsetParamLen   = 20;

    inline unsigned char u8(char c) { return static_cast<unsigned char>(c); }

    inline void append_u64_be(std::string& out, std::uint64_t v) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.push_back(static_cast<char>((v >> shift) & 0xff));
        }
    }

    inline void append_u32_be(std::string& out, std::uint32_t v) {
        out.push_back(static_cast<char>((v >> 24) & 0xff));
        out.push_back(static_cast<char>((v >> 16) & 0xff));
        out.push_back(static_cast<char>((v >> 8) & 0xff));
        out.push_back(static_cast<char>(v & 0xff));
    }

    inline void append_u16_be(std::string& out, std::uint16_t v) {
        out.push_back(static_cast<char>((v >> 8) & 0xff));
        out.push_back(static_cast<char>(v & 0xff));
    }

    inline std::uint64_t read_u64_be(const char* p) {
        std::uint64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v = (v << 8) | static_cast<unsigned char>(p[i]);
        }
        return v;
    }

    inline std::uint32_t read_u32_be(const char* p) {
        std::uint32_t v = 0;
        for (int i = 0; i < 4; ++i) {
            v = (v << 8) | static_cast<unsigned char>(p[i]);
        }
        return v;
    }

    inline std::uint16_t read_u16_be(const char* p) {
        return static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(static_cast<unsigned char>(p[0])) << 8) |
            static_cast<std::uint16_t>(static_cast<unsigned char>(p[1]))
        );
    }

    inline void append_magic_begin(std::string& out) {
        out.push_back(static_cast<char>(kMagic0));
        out.push_back(static_cast<char>(kMagic1));
    }

    inline void append_magic_end(std::string& out) {
        out.push_back(static_cast<char>(kMagic1));
        out.push_back(static_cast<char>(kMagic2));
    }
} // namespace

// 请求级错误：stream_id = 0
std::string buildErrorResponse(std::uint64_t request_id, char error_code) {
    std::string res;
    res.reserve(26);

    append_magic_begin(res);
    res.push_back(static_cast<char>(REQ_ERROR)); // 255
    append_u64_be(res, request_id);
    append_u64_be(res, 0); // stream_id = 0，请求级错误
    res.push_back(error_code);
    res.push_back(0);
    res.push_back(0);
    append_u16_be(res, 0);
    append_magic_end(res);

    return res;
}

// 流级错误：stream_id != 0
std::string buildErrorResponse(
    std::uint64_t request_id,
    std::uint64_t stream_id,
    char error_code
) {
    std::string res;
    res.reserve(26);

    append_magic_begin(res);
    res.push_back(static_cast<char>(REQ_ERROR)); // 255
    append_u64_be(res, request_id);
    append_u64_be(res, stream_id);
    res.push_back(error_code);
    res.push_back(0);
    res.push_back(0);
    append_u16_be(res, 0);
    append_magic_end(res);

    return res;
}

// 非流式响应：stream_id = 0
std::string buildResponse(const std::string& body, std::uint64_t request_id) {
    if (body.size() > 0xffffu) {
        return buildErrorResponse(
            request_id,
            static_cast<char>(ErrorCode::ERR_BAD_INTERNAL)
        );
    }

    std::string res;
    res.reserve(26 + body.size());

    append_magic_begin(res);
    res.push_back(static_cast<char>(RequestType::RESPONSE));
    append_u64_be(res, request_id);
    append_u64_be(res, 0); // stream_id = 0，非流式
    res.push_back(0);
    res.push_back(0);
    res.push_back(0);
    append_u16_be(res, static_cast<std::uint16_t>(body.size()));
    res.append(body);
    append_magic_end(res);

    return res;
}

// 流式响应数据帧：STREAM_RESPONSE，第一帧即 begin + data
std::string buildStreamResponse(
    const std::string& body,
    std::uint64_t request_id,
    std::uint64_t stream_id
) {
    if (stream_id == 0) {
        // 没有 stream_id，构造不出合法流帧，只能回请求级错误。
        return buildErrorResponse(
            request_id,
            static_cast<char>(ErrorCode::ERR_BAD_REQUEST)
        );
    }

    if (body.size() > 0xffffu) {
        // 超出 param_len 范围，回该流的流级错误。
        return buildErrorResponse(
            request_id,
            stream_id,
            static_cast<char>(ErrorCode::ERR_BAD_REQUEST)
        );
    }

    std::string res;
    res.reserve(26 + body.size());

    append_magic_begin(res);
    res.push_back(static_cast<char>(RequestType::STREAM_RESPONSE));
    append_u64_be(res, request_id);
    append_u64_be(res, stream_id);
    res.push_back(0); // error_code
    res.push_back(0); // service_len
    res.push_back(0); // func_len
    append_u16_be(res, static_cast<std::uint16_t>(body.size()));
    res.append(body);
    append_magic_end(res);

    return res;
}

// 流式正常结束：STREAM_END，param_len = 0
std::string buildStreamEnd(std::uint64_t request_id, std::uint64_t stream_id) {
    if (stream_id == 0) {
        return buildErrorResponse(
            request_id,
            static_cast<char>(ErrorCode::ERR_BAD_REQUEST)
        );
    }

    std::string res;
    res.reserve(26);

    append_magic_begin(res);
    res.push_back(static_cast<char>(RequestType::STREAM_END));
    append_u64_be(res, request_id);
    append_u64_be(res, stream_id);
    res.push_back(0);
    res.push_back(0);
    res.push_back(0);
    append_u16_be(res, 0);
    append_magic_end(res);

    return res;
}

void parse(
    const std::string& buffer,
    std::size_t start,
    std::size_t end,
    std::size_t& parsed,
    RpcContext& context
) {
    parsed = 0;

    if (start >= end) {
        context.result = WAITING;
        return;
    }

    const char* data = buffer.data();
    std::size_t i = start;

    context.result = WAITING;

    auto set_waiting = [&]() {
        parsed = i - start;
        context.result = WAITING;
    };

    // 出错时保留 type / request_id / stream_id，便于上层回错误响应。
    auto set_error = [&]() {
        LOG(DEBUG) << "parse ERROR at i=" << i << " start=" << start
           << " end=" << end
           << " status=" << static_cast<int>(context.status)
           << " buf.size=" << context.buffer.size();
        context.result = ERROR;
        context.status = SOA;
        context.resetMessage();
        parsed = i - start;
    };

    auto read_fixed = [&](std::size_t need) -> bool {
        while (context.buffer.size() < need && i < end) {
            context.buffer.push_back(data[i++]);
        }

        if (context.buffer.size() < need) {
            set_waiting();
            return false;
        }

        return true;
    };

    auto read_var = [&](std::size_t len, std::string& out) -> bool {
        if (out.size() > len) {
            set_error();
            return false;
        }

        if (out.capacity() < len) {
            out.reserve(len);
        }

        const std::size_t remain = len - out.size();
        if (remain > 0) {
            const std::size_t avail = end - i;
            const std::size_t take = std::min(remain, avail);

            out.append(data + i, take);
            i += take;

            if (out.size() < len) {
                set_waiting();
                return false;
            }
        }

        return true;
    };

    while (i < end) {
        switch (context.status) {
        case SOA: {
            const void* p = std::memchr(data + i, kMagic0, end - i);
            if (p == nullptr) {
                i = end;
                set_waiting();
                return;
            }

            i = static_cast<const char*>(p) - data + 1;
            context.status = SOB;
            break;
        }

        case SOB: {
            if (i >= end) {
                set_waiting();
                return;
            }

            if (u8(data[i]) != kMagic1) {
                set_error();
                return;
            }

            ++i;
            context.resetMessage();
            context.type = RequestType::REQUEST;
            context.request_id = 0;
            context.stream_id = 0;
            context.status = HEADER;
            break;
        }

        case HEADER: {
            if (!read_fixed(kFixedHeaderSize)) {
                return;
            }

            const char* h = context.buffer.data();

            const unsigned char type       = u8(h[kOffsetType]);
            const std::uint64_t request_id = read_u64_be(h + kOffsetRequestId);
            const std::uint64_t stream_id  = read_u64_be(h + kOffsetStreamId);
            const unsigned char error_code = u8(h[kOffsetErrorCode]);

            context.type       = static_cast<RequestType>(type);
            context.request_id = request_id;
            context.stream_id  = stream_id;

            if (type != static_cast<unsigned char>(RequestType::REQUEST) &&
                type != static_cast<unsigned char>(RequestType::STREAM_REQUEST) &&
                type != static_cast<unsigned char>(RequestType::STREAM_END)) {
                set_error();
                return;
            }

            if (request_id == 0) {
                set_error();
                return;
            }

            if (type == static_cast<unsigned char>(RequestType::REQUEST) &&
                stream_id != 0) {
                set_error();
                return;
            }

            // 流式帧（STREAM_REQUEST / STREAM_END）必须带非 0 的 stream_id：
            // 否则上层既当不成 request 也当不成 stream，只能把帧丢掉。
            if ((type == static_cast<unsigned char>(RequestType::STREAM_REQUEST) ||
                 type == static_cast<unsigned char>(RequestType::STREAM_END)) &&
                stream_id == 0) {
                set_error();
                return;
            }

            if (error_code != 0) {
                set_error();
                return;
            }

            context.service_name.clear();
            context.func_name.clear();
            context.params.clear();
            context.status = SERVICE_NAME;
            break;
        }

        case SERVICE_NAME: {
            const std::size_t service_len = u8(context.buffer[kOffsetServiceLen]);
            if (!read_var(service_len, context.service_name)) {
                return;
            }
            context.status = FUNC_NAME;
            break;
        }

        case FUNC_NAME: {
            const std::size_t func_len = u8(context.buffer[kOffsetFuncLen]);
            if (!read_var(func_len, context.func_name)) {
                return;
            }
            context.status = PARAM_STRING;
            break;
        }

        case PARAM_STRING: {
            const std::uint16_t param_len =
                read_u16_be(context.buffer.data() + kOffsetParamLen);
            if (!read_var(static_cast<std::size_t>(param_len), context.params)) {
                return;
            }

            context.buffer.clear();
            context.status = EOB;
            break;
        }

        case EOB: {
            if (i >= end) {
                set_waiting();
                return;
            }

            if (u8(data[i]) != kMagic1) {
                set_error();
                return;
            }

            ++i;
            context.status = EOC;
            break;
        }

        case EOC: {
            if (i >= end) {
                set_waiting();
                return;
            }

            if (u8(data[i]) != kMagic2) {
                set_error();
                return;
            }

            ++i;
            context.status = SOA;
            context.result = COMPLETE;
            parsed = i - start;
            return;
        }
        }
    }

    set_waiting();
}

} // namespace rpc
