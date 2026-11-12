#include <sentinelx/pipeline/LogPipeline.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <system_error>

namespace {

using sentinelx::pipeline::PipelineOptions;
using sentinelx::pipeline::PipelineResult;
using sentinelx::pipeline::run_log_pipeline;

std::string format_time(std::chrono::sys_seconds time_point) {
    const auto day = std::chrono::floor<std::chrono::days>(time_point);
    const std::chrono::year_month_day date{day};
    const std::chrono::hh_mm_ss time{time_point - day};
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%04d-%02u-%02u %02lld:%02lld:%02lld",
        static_cast<int>(date.year()),
        static_cast<unsigned>(date.month()),
        static_cast<unsigned>(date.day()),
        static_cast<long long>(time.hours().count()),
        static_cast<long long>(time.minutes().count()),
        static_cast<long long>(time.seconds().count()));
    return buffer;
}

class TempLog {
public:
    explicit TempLog(const std::string& contents) {
        static std::atomic<int> counter{0};
        const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
        file_ = std::filesystem::temp_directory_path() /
            ("sentinelx_pipeline_" + std::to_string(ticks) + "_" +
             std::to_string(counter.fetch_add(1)) + ".log");
        std::ofstream output(file_, std::ios::binary);
        output << contents;
    }
    ~TempLog() {
        std::error_code ignored;
        std::filesystem::remove(file_, ignored);
    }
    const std::filesystem::path& file() const { return file_; }

private:
    std::filesystem::path file_;
};

// Ordered log with a Database ERROR spike and periodic malformed lines.
std::string make_log(std::size_t lines) {
    static const char* const components[] = {"Database", "Network", "Memory", "Auth"};
    const std::size_t spike_start = lines * 3 / 5;
    const std::size_t spike_end = spike_start + lines / 20;
    const std::chrono::sys_seconds base{std::chrono::sys_days{
        std::chrono::year{2026} / std::chrono::January / 1}};

    std::string output;
    for (std::size_t index = 0; index < lines; ++index) {
        if (index % 500 == 499) {
            output += "this line is not a valid log entry\n";
            continue;
        }
        const auto timestamp = format_time(base + std::chrono::seconds{index / 4});
        if (index >= spike_start && index < spike_end) {
            output += timestamp + " ERROR Database: Connection pool exhausted\n";
            continue;
        }
        const char* level = index % 10 == 0 ? "ERROR"
            : index % 10 == 1 ? "WARNING"
            : index % 10 == 2 ? "DEBUG" : "INFO";
        output += timestamp + " " + level + " " + components[index % 4] +
            ": event " + std::to_string(index % 7) + "\n";
    }
    return output;
}

PipelineResult run(const TempLog& log, std::size_t workers, std::size_t capacity = 0) {
    PipelineOptions options;
    options.batch_size = 64;
    options.worker_count = workers;
    options.queue_capacity = capacity;
    return run_log_pipeline(log.file(), options);
}

TEST(LogPipeline, ConcurrentResultsMatchSequentialResults) {
    const TempLog log(make_log(6000));
    const auto sequential = run(log, 1);
    const auto concurrent = run(log, 4, 2);

    ASSERT_TRUE(sequential.ok) << sequential.error;
    ASSERT_TRUE(concurrent.ok) << concurrent.error;
    EXPECT_FALSE(sequential.stats.concurrent);
    EXPECT_TRUE(concurrent.stats.concurrent);

    EXPECT_EQ(concurrent.summary.total_records, sequential.summary.total_records);
    EXPECT_EQ(concurrent.summary.malformed_records, sequential.summary.malformed_records);
    EXPECT_GT(sequential.summary.malformed_records, 0u);
    for (std::size_t index = 0; index < 4; ++index) {
        EXPECT_EQ(concurrent.summary.severity_distribution[index].count,
                  sequential.summary.severity_distribution[index].count);
    }
    EXPECT_EQ(concurrent.summary.errors_by_component, sequential.summary.errors_by_component);

    ASSERT_FALSE(sequential.anomalies.empty());
    ASSERT_EQ(concurrent.anomalies.size(), sequential.anomalies.size());
    for (std::size_t index = 0; index < sequential.anomalies.size(); ++index) {
        EXPECT_EQ(concurrent.anomalies[index].component, sequential.anomalies[index].component);
        EXPECT_EQ(concurrent.anomalies[index].window_start, sequential.anomalies[index].window_start);
        EXPECT_EQ(concurrent.anomalies[index].observed_errors,
                  sequential.anomalies[index].observed_errors);
    }

    ASSERT_EQ(concurrent.notable_events.size(), sequential.notable_events.size());
    for (std::size_t index = 0; index < sequential.notable_events.size(); ++index) {
        EXPECT_EQ(concurrent.notable_events[index].timestamp,
                  sequential.notable_events[index].timestamp);
        EXPECT_EQ(concurrent.notable_events[index].message,
                  sequential.notable_events[index].message);
    }
}

TEST(LogPipeline, ProcessesEveryBatchExactlyOnce) {
    const TempLog log(make_log(6000));
    const auto sequential = run(log, 1);
    const auto concurrent = run(log, 4);
    ASSERT_TRUE(concurrent.ok) << concurrent.error;

    const auto& stats = concurrent.stats;
    EXPECT_GT(stats.batches_read, 1u);
    EXPECT_EQ(stats.batches_read, sequential.stats.batches_read);
    EXPECT_EQ(stats.queue_accepted, stats.batches_read);
    EXPECT_EQ(stats.queue_popped, stats.batches_read);
    EXPECT_EQ(stats.batches_aggregated, stats.batches_read);
    ASSERT_EQ(stats.workers.size(), 4u);
    const auto parsed = std::accumulate(stats.workers.begin(), stats.workers.end(), std::size_t{0},
        [](std::size_t total, const auto& worker) { return total + worker.batches_parsed; });
    EXPECT_EQ(parsed, stats.batches_read);
}

TEST(LogPipeline, QueueDepthNeverExceedsCapacity) {
    const TempLog log(make_log(6000));
    const auto result = run(log, 4, 2);
    ASSERT_TRUE(result.ok) << result.error;
    EXPECT_EQ(result.stats.queue_capacity, 2u);
    EXPECT_GE(result.stats.max_queue_depth, 1u);
    EXPECT_LE(result.stats.max_queue_depth, 2u);
}

TEST(LogPipeline, ShutsDownCleanlyWhenAggregationFails) {
    // A timestamp that moves backwards makes AnomalyDetector throw mid-stream.
    std::string contents = make_log(3000);
    contents += "2025-01-01 00:00:00 ERROR Database: out of order\n";
    contents += make_log(200);
    const TempLog log(contents);

    EXPECT_THROW(run(log, 4, 1), std::invalid_argument);
}

TEST(LogPipeline, ReportsMissingFileWithoutHanging) {
    PipelineOptions options;
    options.worker_count = 4;
    const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto result = run_log_pipeline(
        std::filesystem::temp_directory_path() /
            ("sentinelx_missing_pipeline_" + std::to_string(ticks) + ".log"),
        options);
    EXPECT_FALSE(result.ok);
    EXPECT_FALSE(result.error.empty());
    EXPECT_EQ(result.stats.batches_read, 0u);
    EXPECT_EQ(result.stats.workers.size(), 4u);
}

TEST(LogPipeline, ReportsBatchLatencyInBothModes) {
    const TempLog log(make_log(2000));
    for (const std::size_t workers : {std::size_t{1}, std::size_t{4}}) {
        const auto result = run(log, workers);
        ASSERT_TRUE(result.ok) << result.error;
        const auto& stats = result.stats;
        EXPECT_GT(stats.batch_latency_mean_us, 0.0);
        EXPECT_LE(stats.batch_latency_p50_us, stats.batch_latency_p95_us);
        EXPECT_LE(stats.batch_latency_p95_us, stats.batch_latency_max_us);
        EXPECT_GT(stats.batch_latency_max_us, 0.0);
    }
}

TEST(LogPipeline, CountsMalformedTrailingLineAtEndOfFile) {
    std::string contents = "2026-01-01 00:00:00 INFO Core: valid\n";
    contents.append("bad\0tail\n", 9);
    const TempLog log(contents);

    for (const std::size_t workers : {std::size_t{1}, std::size_t{4}}) {
        PipelineOptions options;
        options.batch_size = 1;
        options.worker_count = workers;
        const auto result = run_log_pipeline(log.file(), options);

        ASSERT_TRUE(result.ok) << result.error;
        EXPECT_EQ(result.summary.total_records, 1);
        EXPECT_EQ(result.stats.skipped_lines, 1);
        EXPECT_EQ(result.stats.batches_read, 2);
        EXPECT_EQ(result.stats.batches_aggregated, 2);
    }
}

} // namespace
