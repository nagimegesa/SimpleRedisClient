#include "Logger.h"
#include <chrono>
#include <iomanip>
#include <iostream>
#include <thread>

#include "../utils/SimpleCurrentQueue.hpp"

class LoggerWriter {
public:
    void write(std::string str) {
        queue.enqueue(std::move(str));
    }

    void close() {
        closed = true;
        writer_thread.join();
    }

    static LoggerWriter* getInstance() {
        static LoggerWriter* instance = new LoggerWriter;
        return instance;
    }

private:
    LoggerWriter() {
        writer_thread = std::thread(&LoggerWriter::write_log, this);
    };

    void write_log() {
        while (true) {
            if (queue.size() > 0) {
                std::string logs;
                queue.dequeue(&logs);
                std::cout << logs;
            }

            if (closed) {
                break;
            }
        }
    }

    SimpleConcurrentQueue<std::string> queue;
    std::thread writer_thread;
    std::atomic<bool> closed = false;
};

// ----- LogEntry 实现 -----
LogEntry::LogEntry(LoggerWriter* writer, const char* file, int line, LogLevel level)
    : writer_(writer) {
    // 1. 开头先写时间戳
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    stream_ << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") << "] ";
    
    // 2. 写日志级别
    const char* level_str = (level == LogLevel::DEBUG) ? "[DEBUG]" :
                            (level == LogLevel::INFO) ? "[INFO]" :
                            (level == LogLevel::WARNING) ? "[WARNING]" :
                            (level == LogLevel::ERROR) ? "[ERROR]" : "[DEBUG]";
    stream_ << level_str << " ";
    
    // 3. 写文件名和行号（方便定位）
    stream_ << "[" << file << ":" << line << "] ";
}

LogEntry::~LogEntry() {
    // 析构时：自动追加换行符，然后把整条内容交给 Writer
    stream_ << "\n";
    if (writer_) {
        writer_->write(stream_.str());
    }
}

LogEntry Logger::log(LogLevel level, const char* file, int line) {
    return LogEntry(LoggerWriter::getInstance(), file, line, level);
}

Logger::~Logger() {
    LoggerWriter::getInstance()->close();
}

Logger& Logger::getInstance() {
    static Logger instance;
    return instance;
}
