#ifndef LOGGER_WRITER_HPP_
#define LOGGER_WRITER_HPP_

#include <sstream>

class LoggerWriter;

enum LogLevel {
    DEBUG = 0, INFO, WARNING, ERR
};

class LogEntry {
public:
    // 构造时：立刻写入时间戳、日志级别、文件名行号
    LogEntry(LoggerWriter* writer, const char* file, int line, LogLevel level);

    // 析构时：把收集好的完整内容 + 换行符 提交给 Writer
    ~LogEntry();

    // 模板流式操作：把内容塞进内部的 ostringstream
    // 低于当前日志级别时直接丢弃，不拼字符串、不分配 —— 否则被过滤掉的日志
    // （热路径上大量 LOG(DEBUG)）依旧会为每条日志做一次完整格式化
    template<typename T>
    LogEntry& operator<<(const T& val) {
        if (enabled_) {
            stream_ << val;
        }
        return *this;
    }

    LogEntry& operator<<(std::ostream& (*manip)(std::ostream&)) {
        if (enabled_) {
            stream_ << manip;
        }
        return *this;
    }

    LogEntry(const LogEntry&) = delete;
    LogEntry& operator=(const LogEntry&) = delete;

private:
    std::ostringstream stream_;
    LoggerWriter* writer_;
    LogLevel level_;
    bool enabled_ = false;   // 本条日志是否达到当前日志级别
};

class Logger {
public:
    LogEntry log(LogLevel level, const char* file, int line);
    ~Logger();

    void set_log_file(const char* file, bool create=true);
    void set_log_level(LogLevel level);
    LogLevel get_log_level() { return level_; }

    static Logger& getInstance();

private:
    LogLevel level_ = INFO;
    Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
};

#define LOG(level) Logger::getInstance().log(level, __FILE__, __LINE__)

#endif