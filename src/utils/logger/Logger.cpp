#include "Logger.h"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <thread>
#include <atomic>

#include "SimpleCurrentQueue.hpp"

class LoggerWriter {
public:
    void write(std::string str) {
        queue.enqueue(std::move(str));
    }

    void set_log_file(const char* file, bool create) {

        if (!std::filesystem::exists(file)) {
            if (!create) {
                throw std::invalid_argument("File does not exist");
            }

            std::filesystem::create_directories(std::filesystem::path(file).parent_path());
        }

        this->stream_ = std::ofstream(file, std::ios::out | std::ios::app);
        if (!this->stream_) {
            throw std::runtime_error("Cannot open file for writing");
        }
    }


    void close() {
        closed = true;
        writer_thread.join();
        this->stream_.flush();
        this->stream_.close();
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
                if (this->stream_) {
                    this->stream_ << logs;
                }
            }
            if (closed && queue.size() == 0) {
                break;
            }
        }
    }

    SimpleConcurrentQueue<std::string> queue;
    std::thread writer_thread;
    std::atomic<bool> closed = false;
    std::ofstream stream_;
};

// ----- LogEntry 实现 -----
LogEntry::LogEntry(LoggerWriter* writer, const char* file, int line, LogLevel level)
    : writer_(writer), level_(level) {

    // 1. 开头先写时间戳
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    stream_ << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") << "] ";
    
    // 2. 写日志级别
    const char* level_str = (level == LogLevel::DEBUG) ? "[DEBUG]" :
                            (level == LogLevel::INFO) ? "[INFO]" :
                            (level == LogLevel::WARNING) ? "[WARNING]" :
                            (level == LogLevel::ERR) ? "[ERROR]" : "[DEBUG]";
    stream_ << level_str << " ";

    // 3. 写文件名和行号
    stream_ << "[" << file << ":" << line << "] ";
}

LogEntry::~LogEntry() {
    // 析构时：自动追加换行符，然后把整条内容交给 Writer
    stream_ << "\n";
    if (writer_ && level_ >= Logger::getInstance().get_log_level()) {
        writer_->write(stream_.str());
    }
}

LogEntry Logger::log(LogLevel level, const char* file, int line) {
    return LogEntry(LoggerWriter::getInstance(), file, line, level);
}

Logger::~Logger() {
    LoggerWriter::getInstance()->close();
}

void Logger::set_log_file(const char* file, bool create) {
    LoggerWriter::getInstance()->set_log_file(file, create);
}

void Logger::set_log_level(LogLevel level) {
    level_ = level;
}

Logger& Logger::getInstance() {
    static Logger instance;
    return instance;
}
