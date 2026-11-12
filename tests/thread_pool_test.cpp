#include <sentinelx/thread_pool/ThreadPool.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

using sentinelx::thread_pool::ThreadPool;

TEST(ThreadPool, RunsTasksSimultaneouslyOnDistinctThreads) {
    constexpr std::size_t worker_count = 4;
    ThreadPool pool(worker_count);
    std::mutex mutex;
    std::condition_variable all_arrived;
    std::size_t arrived = 0;
    std::set<std::thread::id> thread_ids;

    // Each task waits until all tasks are running at once; this only succeeds
    // if worker_count tasks are executing on separate threads simultaneously.
    std::vector<std::future<bool>> results;
    for (std::size_t index = 0; index < worker_count; ++index) {
        results.push_back(pool.submit([&] {
            std::unique_lock lock(mutex);
            ++arrived;
            thread_ids.insert(std::this_thread::get_id());
            all_arrived.notify_all();
            return all_arrived.wait_for(lock, std::chrono::seconds(10),
                [&] { return arrived == worker_count; });
        }));
    }

    for (auto& result : results) {
        EXPECT_TRUE(result.get());
    }
    EXPECT_EQ(thread_ids.size(), worker_count);
    EXPECT_EQ(thread_ids.count(std::this_thread::get_id()), 0);
}

TEST(ThreadPool, ExecutesMultipleTasksAndReturnsResults) {
    ThreadPool pool(3);
    std::vector<std::future<int>> results;
    for (int value = 1; value <= 20; ++value) {
        results.push_back(pool.submit([value] { return value * value; }));
    }

    int total = 0;
    for (auto& result : results) {
        total += result.get();
    }
    EXPECT_EQ(total, 2870);
    EXPECT_EQ(pool.stats().completed_tasks, 20);
}

TEST(ThreadPool, SupportsConcurrentTaskSubmission) {
    constexpr std::size_t submitter_count = 8;
    constexpr std::size_t tasks_per_submitter = 50;
    ThreadPool pool(4);
    std::atomic<std::size_t> completed_results{0};
    std::vector<std::thread> submitters;

    for (std::size_t submitter = 0; submitter < submitter_count; ++submitter) {
        submitters.emplace_back([&pool, &completed_results] {
            std::vector<std::future<int>> results;
            results.reserve(tasks_per_submitter);
            for (std::size_t index = 0; index < tasks_per_submitter; ++index) {
                results.push_back(pool.submit([] { return 1; }));
            }
            for (auto& result : results) {
                completed_results.fetch_add(
                    static_cast<std::size_t>(result.get()),
                    std::memory_order_relaxed);
            }
        });
    }
    for (auto& submitter : submitters) {
        submitter.join();
    }

    EXPECT_EQ(completed_results.load(), submitter_count * tasks_per_submitter);
    EXPECT_EQ(pool.stats().submitted_tasks, submitter_count * tasks_per_submitter);
}

TEST(ThreadPool, ShutdownDrainsTasksAndRejectsNewSubmissions) {
    ThreadPool pool(2);
    std::atomic<int> executions{0};
    std::vector<std::future<void>> results;
    for (int index = 0; index < 40; ++index) {
        results.push_back(pool.submit([&executions] {
            executions.fetch_add(1, std::memory_order_relaxed);
        }));
    }

    pool.shutdown();
    for (auto& result : results) {
        result.get();
    }
    EXPECT_EQ(executions.load(), 40);
    EXPECT_THROW(pool.submit([] {}), std::runtime_error);
    EXPECT_NO_THROW(pool.shutdown());
}

TEST(ThreadPool, PropagatesTaskExceptionsThroughFuture) {
    ThreadPool pool(1);
    auto result = pool.submit([]() -> int {
        throw std::runtime_error("task failed");
    });
    EXPECT_THROW(result.get(), std::runtime_error);

    const auto stats = pool.stats();
    EXPECT_EQ(stats.failed_tasks, 1);
    EXPECT_EQ(stats.completed_tasks, 1);
}

TEST(ThreadPool, WorkerCannotDeadlockByShuttingDownItsOwnPool) {
    ThreadPool pool(1);
    auto result = pool.submit([&pool] { pool.shutdown(); });
    EXPECT_THROW(result.get(), std::logic_error);
    EXPECT_EQ(pool.submit([] { return 7; }).get(), 7);
}

TEST(ThreadPool, KeepsStatisticsConsistentUnderConcurrentWork) {
    constexpr std::size_t task_count = 1000;
    ThreadPool pool(4);
    std::atomic<std::size_t> shared_count{0};
    std::vector<std::future<void>> results;
    results.reserve(task_count);

    for (std::size_t index = 0; index < task_count; ++index) {
        results.push_back(pool.submit([&shared_count] {
            shared_count.fetch_add(1, std::memory_order_relaxed);
        }));
    }
    for (auto& result : results) {
        result.get();
    }

    const auto stats = pool.stats();
    EXPECT_EQ(shared_count.load(), task_count);
    EXPECT_EQ(stats.worker_count, 4);
    EXPECT_EQ(stats.submitted_tasks, task_count);
    EXPECT_EQ(stats.completed_tasks, task_count);
    EXPECT_EQ(stats.failed_tasks, 0);
    EXPECT_EQ(stats.active_tasks, 0);
    EXPECT_EQ(stats.queued_tasks, 0);
}

} // namespace