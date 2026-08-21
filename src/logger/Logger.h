#include <sstream>
#include <string>

// TODO: 这里可以使用无锁队列，但是太复杂，先不搞

// 前向声明
class LoggerWriter;



enum LogLevel {
    DEBUG = 0, INFO, WARNING, ERR
};

// 日志条目类（每一条日志的“生命周期”）
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

    // 操纵符重载
    LogEntry& operator<<(std::ostream& (*manip)(std::ostream&)) {
        stream_ << manip;
        return *this;
    }

    // 禁止拷贝（防止生命周期混乱）
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
    LogLevel level_;
    Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
};

#define LOG(level) Logger::getInstance().log(level, __FILE__, __LINE__)

