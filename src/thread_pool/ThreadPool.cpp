#include <sentinelx/thread_pool/ThreadPool.h>

#include <algorithm>
#include <exception>

namespace sentinelx::thread_pool {

thread_local ThreadPool* ThreadPool::active_pool_ = nullptr;

ThreadPool::ThreadPool(std::size_t worker_count)
    : worker_count_(worker_count == 0 ? 1 : worker_count) {
    workers_.reserve(worker_count_);
    try {
        for (std::size_t index = 0; index < worker_count_; ++index) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    } catch (...) {
        {
            std::lock_guard lock(queue_mutex_);
            stopping_ = true;
        }
        condition_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        throw;
    }
}

ThreadPool::~ThreadPool() noexcept {
    try {
        shutdown();
    } catch (...) {
        std::terminate();
    }
}

void ThreadPool::shutdown() {
    if (active_pool_ == this) {
        throw std::logic_error("A ThreadPool worker cannot shut down its own pool");
    }

    std::lock_guard shutdown_lock(shutdown_mutex_);
    {
        std::lock_guard queue_lock(queue_mutex_);
        stopping_ = true;
    }
    condition_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
}

ThreadPoolStats ThreadPool::stats() const {
    std::lock_guard lock(queue_mutex_);
    return {
        worker_count_,
        tasks_.size(),
        active_tasks_.load(std::memory_order_relaxed),
        submitted_tasks_.load(std::memory_order_relaxed),
        completed_tasks_.load(std::memory_order_relaxed),
        failed_tasks_.load(std::memory_order_relaxed)};
}

void ThreadPool::worker_loop() {
    active_pool_ = this;
    for (;;) {
        std::function<void()> task;
        {
            std::unique_lock lock(queue_mutex_);
            condition_.wait(lock, [this] {
                return stopping_ || !tasks_.empty();
            });
            if (stopping_ && tasks_.empty()) {
                break;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
            active_tasks_.fetch_add(1, std::memory_order_relaxed);
        }

        try {
            task();
        } catch (...) {
            failed_tasks_.fetch_add(1, std::memory_order_relaxed);
        }
        active_tasks_.fetch_sub(1, std::memory_order_relaxed);
        completed_tasks_.fetch_add(1, std::memory_order_relaxed);
    }
    active_pool_ = nullptr;
}

} // namespace sentinelx::thread_pool