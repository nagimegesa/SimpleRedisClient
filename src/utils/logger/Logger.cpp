#include "Logger.h"
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>
#include <atomic>

#include "thread_pool/queue/LockFreeQueue.h"

class LoggerWriter {
public:
    void write(std::string&& str) {
        // queue.enqueue(std::move(str));
        // 不断写直到成功
        while (!queue.push(std::move(str))) {}

        // 消费者是持锁判断队列为空后才 wait 的，这里先抢一次锁再 notify，
        // 保证 notify 不会发生在 wait 之前。
        { std::lock_guard<std::mutex> lock(mutex_); }
        cv_.notify_one();
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
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed = true;
        }
        cv_.notify_all();

        if (writer_thread.joinable()) {
            writer_thread.join();
        }

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
        std::string logs;
        std::unique_lock<std::mutex> lock(mutex_);
        while (true) {
            if (queue.pop(logs)) {
                lock.unlock();          // 输出/落盘期间不持锁
                std::cout << logs;
                if (this->stream_.is_open()) {
                    this->stream_ << logs;
                }
                lock.lock();
                continue;
            }

            if (closed && queue.empty()) {
                break;
            }

            // 没有日志就阻塞等待，不要空转：
            cv_.wait(lock, [this] { return closed.load() || !queue.empty(); });
        }
    }

    // SimpleConcurrentQueue<std::string> queue;

    MPSCQueue<std::string> queue {};

    std::thread writer_thread;

    std::atomic<bool> closed = false;
    std::ofstream stream_;

    std::mutex              mutex_;   // 保护「队列空判断 + 等待」，兼作丢唤醒屏障
    std::condition_variable cv_;      // 队列非空 / closed 时唤醒写线程
};

// ----- LogEntry 实现 -----
LogEntry::LogEntry(LoggerWriter* writer, const char* file, int line, LogLevel level)
    : writer_(writer), level_(level) {

    enabled_ = (writer_ != nullptr) && (level_ >= Logger::getInstance().get_log_level());

    if (enabled_) {
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
}

LogEntry::~LogEntry() {
    // 析构时：自动追加换行符，然后把整条内容交给 Writer
    if (enabled_) {
        stream_ << "\n";
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
