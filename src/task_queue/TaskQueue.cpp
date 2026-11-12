#include <sentinelx/task_queue/TaskQueue.h>

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace sentinelx::task_queue {

TaskQueue::TaskQueue(std::size_t capacity)
    : capacity_(capacity) {
    if (capacity_ == 0) {
        throw std::invalid_argument("TaskQueue capacity must be greater than zero");
    }
}

bool TaskQueue::push(file_reader::ReadBatch batch) {
    {
        std::unique_lock lock(mutex_);
        not_full_.wait(lock, [this] {
            return closed_ || batches_.size() < capacity_;
        });
        if (closed_) {
            return false;
        }
        batches_.push_back({std::move(batch), std::chrono::steady_clock::now()});
        ++accepted_batches_;
        max_queued_batches_ = std::max(max_queued_batches_, batches_.size());
    }
    not_empty_.notify_one();
    return true;
}

std::optional<file_reader::ReadBatch> TaskQueue::pop() {
    std::optional<file_reader::ReadBatch> batch;
    {
        std::unique_lock lock(mutex_);
        not_empty_.wait(lock, [this] {
            return closed_ || !batches_.empty();
        });
        if (batches_.empty()) {
            return std::nullopt;
        }
        auto queued = std::move(batches_.front());
        batches_.pop_front();
        const auto wait_nanoseconds = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - queued.enqueued_at)
                .count());
        ++popped_batches_;
        total_wait_nanoseconds_ += wait_nanoseconds;
        max_wait_nanoseconds_ = std::max(max_wait_nanoseconds_, wait_nanoseconds);
        batch.emplace(std::move(queued.batch));
    }
    not_full_.notify_one();
    return batch;
}

void TaskQueue::close() {
    {
        std::lock_guard lock(mutex_);
        closed_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
}

bool TaskQueue::is_closed() const {
    std::lock_guard lock(mutex_);
    return closed_;
}

std::size_t TaskQueue::size() const {
    std::lock_guard lock(mutex_);
    return batches_.size();
}

std::size_t TaskQueue::capacity() const noexcept {
    return capacity_;
}

TaskQueueStats TaskQueue::stats() const {
    std::lock_guard lock(mutex_);
    return {
        capacity_,
        batches_.size(),
        accepted_batches_,
        popped_batches_,
        total_wait_nanoseconds_,
        max_wait_nanoseconds_,
        closed_,
        max_queued_batches_};
}

} // namespace sentinelx::task_queue