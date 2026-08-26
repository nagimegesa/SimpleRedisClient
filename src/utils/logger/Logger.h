#include <sstream>
#include <string>

// TODO: 这里可以使用无锁队列，但是太复杂，先不搞

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
    template<typename T>
    LogEntry& operator<<(const T& val) {
        stream_ << val;
        return *this;
    }

    LogEntry& operator<<(std::ostream& (*manip)(std::ostream&)) {
        stream_ << manip;
        return *this;
    }

    LogEntry(const LogEntry&) = delete;
    LogEntry& operator=(const LogEntry&) = delete;

private:
    std::ostringstream stream_;
    LoggerWriter* writer_;
    LogLevel level_;
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

