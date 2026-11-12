#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>

#include <sentinelx/file_reader/FileReader.h>

namespace sentinelx::task_queue {

struct TaskQueueStats {
    std::size_t capacity{0};
    std::size_t queued_batches{0};
    std::uint64_t accepted_batches{0};
    std::uint64_t popped_batches{0};
    std::uint64_t total_wait_nanoseconds{0};
    std::uint64_t max_wait_nanoseconds{0};
    bool closed{false};
    std::size_t max_queued_batches{0};
};

class TaskQueue final {
public:
    explicit TaskQueue(std::size_t capacity);

    TaskQueue(const TaskQueue&) = delete;
    TaskQueue& operator=(const TaskQueue&) = delete;

    bool push(file_reader::ReadBatch batch);
    std::optional<file_reader::ReadBatch> pop();
    void close();

    bool is_closed() const;
    std::size_t size() const;
    std::size_t capacity() const noexcept;
    TaskQueueStats stats() const;

private:
    struct QueuedBatch {
        file_reader::ReadBatch batch;
        std::chrono::steady_clock::time_point enqueued_at;
    };

    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<QueuedBatch> batches_;
    std::uint64_t accepted_batches_{0};
    std::uint64_t popped_batches_{0};
    std::uint64_t total_wait_nanoseconds_{0};
    std::uint64_t max_wait_nanoseconds_{0};
    std::size_t max_queued_batches_{0};
    bool closed_{false};
};

} // namespace sentinelx::task_queue