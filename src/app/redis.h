//
// Created by computer on 2026/8/20.
//

#ifndef DEMO_REDIS_H
#define DEMO_REDIS_H
#include <sstream>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

enum class RESPType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null,
};

struct RESPValue {
    RESPType                                                     type;
    std::variant<std::string, long long, std::vector<RESPValue>> data;

    std::string&                  as_string() { return std::get<std::string>(data); }
    const std::string&            as_string() const { return std::get<std::string>(data); }
    long long                     as_integer() const { return std::get<long long>(data); }
    std::vector<RESPValue>&       as_array() { return std::get<std::vector<RESPValue>>(data); }
    const std::vector<RESPValue>& as_array() const { return std::get<std::vector<RESPValue>>(data); }
};

class IncompleteRESPException : public std::runtime_error {
public:
    IncompleteRESPException() : std::runtime_error("Incomplete RESP data") {}
};

class RESP_Parser {
public:
    static RESPValue parse(const std::string& data) {
        size_t pos = 0;
        return parse_value(data, pos);
    }

private:
    static RESPValue parse_value(const std::string& data, size_t& pos) {
        if (pos >= data.size()) throw IncompleteRESPException();
        char type = data[pos++];
        switch (type) {
            case '+': return parse_simple_string(data, pos);
            case '-': return parse_error(data, pos);
            case ':': return parse_integer(data, pos);
            case '$': return parse_bulk_string(data, pos);
            case '*': return parse_array(data, pos);
            default: throw std::runtime_error("Unknown RESP type: " + std::string(1, type));
        }
    }

    static std::string read_line(const std::string& data, size_t& pos) {
        size_t start = pos;
        size_t end = data.find("\r\n", pos);
        if (end == std::string::npos) throw IncompleteRESPException();
        pos = end + 2;
        return data.substr(start, end - start);
    }

    static RESPValue parse_simple_string(const std::string& data, size_t& pos) {
        RESPValue val;
        val.type = RESPType::SimpleString;
        val.data = read_line(data, pos);
        return val;
    }

    static RESPValue parse_error(const std::string& data, size_t& pos) {
        RESPValue val;
        val.type = RESPType::Error;
        val.data = read_line(data, pos);
        return val;
    }

    static RESPValue parse_integer(const std::string& data, size_t& pos) {
        std::string line = read_line(data, pos);
        RESPValue val;
        val.type = RESPType::Integer;
        val.data = std::stoll(line);
        return val;
    }

    static RESPValue parse_bulk_string(const std::string& data, size_t& pos) {
        std::string len_str = read_line(data, pos);
        long long len = std::stoll(len_str);
        RESPValue val;
        val.type = RESPType::BulkString;

        if (len == -1) {
            val.data = std::string();
            return val;
        }

        if (pos + len > data.size()) throw IncompleteRESPException();
        std::string content = data.substr(pos, len);
        pos += len;
        if (pos + 2 > data.size() || data[pos] != '\r' || data[pos+1] != '\n')
            throw IncompleteRESPException();
        pos += 2;
        val.data = content;
        return val;
    }

    static RESPValue parse_array(const std::string& data, size_t& pos) {
        std::string len_str = read_line(data, pos);
        long long len = std::stoll(len_str);
        RESPValue val;
        val.type = RESPType::Array;
        std::vector<RESPValue> elements;

        if (len == -1) {
            val.data = elements;
            return val;
        }

        elements.reserve(len);
        for (long long i = 0; i < len; ++i) {
            elements.push_back(parse_value(data, pos));
        }
        val.data = elements;
        return val;
    }
};

// ---------------------- 命令构建与响应格式化 ----------------------
inline std::string buildRESPCommand(const std::vector<std::string>& args) {
    std::string cmd;
    cmd += "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args) {
        cmd += "$" + std::to_string(arg.size()) + "\r\n";
        cmd += arg + "\r\n";
    }
    return cmd;
}

// 格式化 RESP 值为多行字符串（不使用 cout）
inline std::string formatResponse(const RESPValue& resp, int indent = 0) {
    std::ostringstream oss;
    std::string prefix(indent, ' ');
    switch (resp.type) {
        case RESPType::SimpleString:
            oss << prefix << "(string) " << resp.as_string() << "\n";
            break;
        case RESPType::Error:
            oss << prefix << "(error) " << resp.as_string() << "\n";
            break;
        case RESPType::Integer:
            oss << prefix << "(integer) " << resp.as_integer() << "\n";
            break;
        case RESPType::BulkString:
            if (resp.as_string().empty())
                oss << prefix << "(nil)\n";
            else
                oss << prefix << "(bulk) " << resp.as_string() << "\n";
            break;
        case RESPType::Array: {
            const auto& arr = resp.as_array();
            if (arr.empty()) {
                oss << prefix << "(empty array)\n";
            } else {
                oss << prefix << "(array) size=" << arr.size() << "\n";
                for (size_t i = 0; i < arr.size(); ++i) {
                    oss << prefix << "  " << i << ") ";
                    oss << formatResponse(arr[i], indent + 2);
                }
            }
            break;
        }
        case RESPType::Null:
            oss << prefix << "(null)\n";
            break;
        default:
            oss << prefix << "(unknown)\n";
    }
    return oss.str();
}

#endif //DEMO_REDIS_H