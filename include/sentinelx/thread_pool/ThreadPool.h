#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace sentinelx::thread_pool {

struct ThreadPoolStats {
    std::size_t worker_count{0};
    std::size_t queued_tasks{0};
    std::size_t active_tasks{0};
    std::size_t submitted_tasks{0};
    std::size_t completed_tasks{0};
    std::size_t failed_tasks{0};
};

class ThreadPool final {
public:
    explicit ThreadPool(
        std::size_t worker_count = std::thread::hardware_concurrency());
    ~ThreadPool() noexcept;

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    template <typename Function, typename... Args>
    auto submit(Function&& function, Args&&... args)
        -> std::future<std::invoke_result_t<
            std::decay_t<Function>, std::decay_t<Args>...>> {
        using Result = std::invoke_result_t<
            std::decay_t<Function>, std::decay_t<Args>...>;

        auto bound_task = [
            this,
            callable = std::forward<Function>(function),
            arguments = std::make_tuple(std::forward<Args>(args)...)]() mutable -> Result {
            try {
                return std::apply(
                    [&callable](auto&&... values) -> Result {
                        return std::invoke(
                            std::move(callable),
                            std::forward<decltype(values)>(values)...);
                    },
                    std::move(arguments));
            } catch (...) {
                failed_tasks_.fetch_add(1, std::memory_order_relaxed);
                throw;
            }
        };

        auto task = std::make_shared<std::packaged_task<Result()>>(
            std::move(bound_task));
        auto result = task->get_future();
        {
            std::lock_guard lock(queue_mutex_);
            if (stopping_) {
                throw std::runtime_error("Cannot submit a task to a stopped ThreadPool");
            }
            tasks_.emplace([task] { (*task)(); });
            submitted_tasks_.fetch_add(1, std::memory_order_relaxed);
        }
        condition_.notify_one();
        return result;
    }

    void shutdown();
    ThreadPoolStats stats() const;

private:
    void worker_loop();
    static thread_local ThreadPool* active_pool_;

    const std::size_t worker_count_;
    mutable std::mutex queue_mutex_;
    std::mutex shutdown_mutex_;
    std::condition_variable condition_;
    std::queue<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    bool stopping_{false};

    std::atomic<std::size_t> active_tasks_{0};
    std::atomic<std::size_t> submitted_tasks_{0};
    std::atomic<std::size_t> completed_tasks_{0};
    std::atomic<std::size_t> failed_tasks_{0};
};

} // namespace sentinelx::thread_pool