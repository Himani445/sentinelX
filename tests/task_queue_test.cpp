#include <sentinelx/file_reader/FileReader.h>
#include <sentinelx/task_queue/TaskQueue.h>
#include <sentinelx/thread_pool/ThreadPool.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using sentinelx::file_reader::ReadBatch;
using sentinelx::file_reader::ReadStatus;
using sentinelx::task_queue::TaskQueue;
using sentinelx::thread_pool::ThreadPool;

ReadBatch make_batch(std::string value) {
    ReadBatch batch;
    batch.status = ReadStatus::Batch;
    batch.lines.push_back(std::move(value));
    return batch;
}

std::filesystem::path make_temp_path() {
    const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
        ("sentinelx_task_queue_" + std::to_string(ticks) + ".log");
}

TEST(TaskQueue, AppliesBackpressureUntilAConsumerFreesCapacity) {
    TaskQueue queue(1);
    ASSERT_TRUE(queue.push(make_batch("first")));

    std::promise<void> producer_started;
    auto started = producer_started.get_future();
    auto producer = std::async(std::launch::async, [&] {
        producer_started.set_value();
        return queue.push(make_batch("second"));
    });

    started.wait();
    EXPECT_EQ(producer.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    ASSERT_EQ(queue.pop()->lines.front(), "first");
    ASSERT_EQ(producer.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_TRUE(producer.get());
    ASSERT_EQ(queue.pop()->lines.front(), "second");
}

TEST(TaskQueue, CloseDrainsAcceptedBatchesAndWakesWaiters) {
    TaskQueue queue(2);
    ASSERT_TRUE(queue.push(make_batch("accepted")));
    queue.close();

    EXPECT_TRUE(queue.is_closed());
    EXPECT_FALSE(queue.push(make_batch("rejected")));
    const auto batch = queue.pop();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->lines.front(), "accepted");
    EXPECT_FALSE(queue.pop().has_value());
    const auto stats = queue.stats();
    EXPECT_EQ(stats.accepted_batches, 1);
    EXPECT_EQ(stats.popped_batches, 1);
    EXPECT_TRUE(stats.closed);
}

TEST(TaskQueue, ClosingWakesAProducerBlockedByFullCapacity) {
    TaskQueue queue(1);
    ASSERT_TRUE(queue.push(make_batch("occupies capacity")));

    std::promise<void> started_promise;
    auto started = started_promise.get_future();
    auto producer = std::async(std::launch::async, [&] {
        started_promise.set_value();
        return queue.push(make_batch("must not be accepted"));
    });
    ASSERT_EQ(started.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    ASSERT_EQ(producer.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);

    queue.close();
    ASSERT_EQ(producer.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_FALSE(producer.get());
    ASSERT_TRUE(queue.pop().has_value());
    EXPECT_FALSE(queue.pop().has_value());
}

TEST(TaskQueue, ConcurrentProducersAndConsumersDeliverEachBatchOnce) {
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t batches_per_producer = 500;
    constexpr std::size_t total_batches = producer_count * batches_per_producer;
    constexpr std::size_t consumer_count = 4;

    TaskQueue queue(13);
    ThreadPool pool(consumer_count);
    std::mutex counts_mutex;
    std::vector<std::size_t> delivery_counts(total_batches, 0);
    std::vector<std::future<void>> consumers;

    for (std::size_t index = 0; index < consumer_count; ++index) {
        consumers.push_back(pool.submit([&] {
            while (auto batch = queue.pop()) {
                const auto id = static_cast<std::size_t>(
                    std::stoull(batch->lines.front()));
                std::lock_guard lock(counts_mutex);
                ++delivery_counts.at(id);
            }
        }));
    }

    std::vector<std::thread> producers;
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer] {
            for (std::size_t index = 0; index < batches_per_producer; ++index) {
                const auto id = producer * batches_per_producer + index;
                if (!queue.push(make_batch(std::to_string(id)))) {
                    return;
                }
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    queue.close();
    for (auto& consumer : consumers) {
        EXPECT_NO_THROW(consumer.get());
    }

    EXPECT_EQ(delivery_counts.size(), total_batches);
    for (const auto count : delivery_counts) {
        EXPECT_EQ(count, 1);
    }
}

TEST(TaskQueue, NeverExceedsCapacityWhenProducersOutpaceConsumers) {
    constexpr std::size_t capacity = 3;
    constexpr std::size_t producer_count = 4;
    constexpr std::size_t batches_per_producer = 200;
    constexpr std::size_t total_batches = producer_count * batches_per_producer;

    TaskQueue queue(capacity);
    ThreadPool pool(2);
    std::atomic<std::size_t> consumed{0};
    std::vector<std::future<void>> consumers;
    for (int index = 0; index < 2; ++index) {
        consumers.push_back(pool.submit([&] {
            while (queue.pop()) {
                consumed.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        }));
    }

    std::vector<std::thread> producers;
    for (std::size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&] {
            for (std::size_t index = 0; index < batches_per_producer; ++index) {
                ASSERT_TRUE(queue.push(make_batch("x")));
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    queue.close();
    for (auto& consumer : consumers) {
        consumer.get();
    }

    const auto stats = queue.stats();
    EXPECT_EQ(stats.max_queued_batches, capacity);
    EXPECT_EQ(stats.accepted_batches, total_batches);
    EXPECT_EQ(stats.popped_batches, total_batches);
    EXPECT_EQ(consumed.load(), total_batches);
}

TEST(TaskQueue, StreamsFileReaderBatchesThroughThreadPoolConsumers) {
    const auto path = make_temp_path();
    {
        std::ofstream output(path, std::ios::binary);
        ASSERT_TRUE(output.is_open());
        for (int line = 0; line < 120; ++line) {
            output << "log line " << line << '\n';
        }
        ASSERT_TRUE(output.good());
    }

    TaskQueue queue(3);
    ThreadPool pool(3);
    std::atomic<std::size_t> processed_lines{0};
    std::vector<std::future<void>> consumers;
    for (int index = 0; index < 3; ++index) {
        consumers.push_back(pool.submit([&] {
            while (auto batch = queue.pop()) {
                processed_lines.fetch_add(
                    batch->lines.size(), std::memory_order_relaxed);
            }
        }));
    }

    {
        sentinelx::file_reader::FileReader reader(
            path, {.batch_size = 5, .buffer_size = 32, .max_line_bytes = 128});
        for (;;) {
            auto batch = reader.read_next_batch();
            if (batch.status == ReadStatus::EndOfFile) {
                break;
            }
            ASSERT_EQ(batch.status, ReadStatus::Batch) << batch.error;
            ASSERT_TRUE(queue.push(std::move(batch)));
        }
    }
    queue.close();
    for (auto& consumer : consumers) {
        EXPECT_NO_THROW(consumer.get());
    }
    std::filesystem::remove(path);

    EXPECT_EQ(processed_lines.load(), 120);
    EXPECT_TRUE(queue.is_closed());
    EXPECT_EQ(queue.size(), 0);
}

} // namespace