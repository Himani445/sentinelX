#include <sentinelx/pipeline/LogPipeline.h>

#include <sentinelx/file_reader/FileReader.h>
#include <sentinelx/log_parser/LogParser.h>
#include <sentinelx/task_queue/TaskQueue.h>
#include <sentinelx/thread_pool/ThreadPool.h>

#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <exception>
#include <functional>
#include <future>
#include <iterator>
#include <map>
#include <mutex>
#include <numeric>
#include <optional>
#include <utility>

namespace sentinelx::pipeline {
namespace {

using Clock = std::chrono::steady_clock;

double nearest_rank(const std::vector<double>& sorted, double percentile) {
    const auto rank = static_cast<std::size_t>(std::ceil(percentile * static_cast<double>(sorted.size())));
    return sorted[std::clamp<std::size_t>(rank, 1, sorted.size()) - 1];
}

struct Aggregator {
    explicit Aggregator(const PipelineOptions& options)
        : analyzer(options.analyzer),
          detector(options.anomaly),
          max_events(options.max_correlation_events) {}

    void consume(const log_parser::BatchParseResult& parsed, std::size_t skipped, Clock::time_point read_time) {
        ++batches;
        skipped_lines += skipped;
        analyzer.consume(parsed);
        auto found = detector.consume(parsed);
        anomalies.insert(anomalies.end(),
            std::make_move_iterator(found.begin()), std::make_move_iterator(found.end()));
        for (const auto& entry : parsed.entries) {
            if (entry.severity != log_parser::Severity::Warning &&
                entry.severity != log_parser::Severity::Error) {
                continue;
            }
            if (notable_events.size() >= max_events) {
                truncated = true;
                continue;
            }
            notable_events.push_back({entry.timestamp, entry.severity, entry.component, entry.message});
        }
        latencies_us.push_back(
            std::chrono::duration<double, std::micro>(Clock::now() - read_time).count());
    }

    void finish(PipelineResult& result) {
        result.summary = analyzer.summary();
        result.anomalies = std::move(anomalies);
        result.notable_events = std::move(notable_events);
        result.correlation_truncated = truncated;
        result.stats.batches_aggregated = batches;
        result.stats.skipped_lines = skipped_lines;
        if (!latencies_us.empty()) {
            std::sort(latencies_us.begin(), latencies_us.end());
            result.stats.batch_latency_mean_us =
                std::accumulate(latencies_us.begin(), latencies_us.end(), 0.0) /
                static_cast<double>(latencies_us.size());
            result.stats.batch_latency_p50_us = nearest_rank(latencies_us, 0.50);
            result.stats.batch_latency_p95_us = nearest_rank(latencies_us, 0.95);
            result.stats.batch_latency_max_us = latencies_us.back();
        }
    }

    log_analyzer::LogAnalyzer analyzer;
    anomaly_detector::AnomalyDetector detector;
    std::size_t max_events;
    std::size_t batches{0};
    std::size_t skipped_lines{0};
    std::vector<anomaly_detector::AnomalyReport> anomalies;
    std::vector<event_correlator::EventRecord> notable_events;
    std::vector<double> latencies_us;
    bool truncated{false};
};

struct ParsedBatch {
    log_parser::BatchParseResult parsed;
    std::size_t skipped_lines{0};
    Clock::time_point read_time{};
};

// Hands parsed batches to the aggregator in file order, bounding how many wait.
class ReorderBuffer {
public:
    explicit ReorderBuffer(std::size_t limit) : limit_(limit) {}

    bool put(std::uint64_t sequence, ParsedBatch batch) {
        std::unique_lock lock(mutex_);
        // The next expected batch is always admitted, so the consumer can always progress.
        has_space_.wait(lock, [&] {
            return aborted_ || ready_.size() < limit_ || sequence == next_;
        });
        if (aborted_) {
            return false;
        }
        ready_.emplace(sequence, std::move(batch));
        lock.unlock();
        has_next_.notify_one();
        return true;
    }

    std::optional<ParsedBatch> take_next() {
        std::unique_lock lock(mutex_);
        has_next_.wait(lock, [&] {
            return aborted_ || ready_.contains(next_) || (finished_ && next_ == total_);
        });
        if (aborted_ || !ready_.contains(next_)) {
            return std::nullopt;
        }
        auto node = ready_.extract(next_);
        ++next_;
        lock.unlock();
        has_space_.notify_all();
        return std::move(node.mapped());
    }

    void finish(std::uint64_t total) {
        {
            std::lock_guard lock(mutex_);
            finished_ = true;
            total_ = total;
        }
        has_next_.notify_all();
    }

    void abort() {
        {
            std::lock_guard lock(mutex_);
            aborted_ = true;
        }
        has_next_.notify_all();
        has_space_.notify_all();
    }

private:
    const std::size_t limit_;
    std::mutex mutex_;
    std::condition_variable has_space_;
    std::condition_variable has_next_;
    std::map<std::uint64_t, ParsedBatch> ready_;
    std::uint64_t next_{0};
    std::uint64_t total_{0};
    bool finished_{false};
    bool aborted_{false};
};

struct ScopeExit {
    std::function<void()> action;
    ~ScopeExit() { action(); }
};

PipelineResult run_sequential(const std::filesystem::path& path, const PipelineOptions& options) {
    PipelineResult result;
    Aggregator aggregator(options);
    file_reader::FileReader reader(path, {.batch_size = options.batch_size});
    for (;;) {
        auto batch = reader.read_next_batch();
        if (batch.status == file_reader::ReadStatus::EndOfFile) {
            break;
        }
        if (batch.status == file_reader::ReadStatus::Error) {
            result.ok = false;
            result.error = batch.error;
            break;
        }
        ++result.stats.batches_read;
        const auto read_time = Clock::now();
        aggregator.consume(
            log_parser::LogParser::parse_batch(batch.lines), batch.malformed_lines, read_time);
    }
    aggregator.finish(result);
    return result;
}

PipelineResult run_concurrent(const std::filesystem::path& path, const PipelineOptions& options) {
    PipelineResult result;
    result.stats.concurrent = true;
    const auto workers = options.worker_count;
    const auto capacity = options.queue_capacity == 0 ? workers * 2 : options.queue_capacity;
    result.stats.queue_capacity = capacity;

    file_reader::FileReader reader(path, {.batch_size = options.batch_size});
    task_queue::TaskQueue queue(capacity);
    ReorderBuffer reorder(capacity + workers);
    Aggregator aggregator(options);
    const auto abort_pipeline = [&] {
        reorder.abort();
        queue.close();
    };
    // Declared after the shared state so it joins its threads before that state is destroyed.
    thread_pool::ThreadPool pool(workers + 1);
    // Unblocks every stage before the pool joins if the producer exits early.
    const ScopeExit stop_on_exit{abort_pipeline};

    std::vector<std::future<WorkerStats>> parsers;
    parsers.reserve(workers);
    for (std::size_t index = 0; index < workers; ++index) {
        parsers.push_back(pool.submit([&] {
            WorkerStats stats{std::this_thread::get_id(), 0};
            try {
                while (auto batch = queue.pop()) {
                    ParsedBatch parsed{
                        log_parser::LogParser::parse_batch(batch->lines),
                        batch->malformed_lines,
                        batch->read_time};
                    ++stats.batches_parsed;
                    if (!reorder.put(batch->sequence, std::move(parsed))) {
                        break;
                    }
                }
            } catch (...) {
                abort_pipeline();
                throw;
            }
            return stats;
        }));
    }
    auto aggregation = pool.submit([&] {
        try {
            while (auto parsed = reorder.take_next()) {
                aggregator.consume(parsed->parsed, parsed->skipped_lines, parsed->read_time);
            }
        } catch (...) {
            abort_pipeline();
            throw;
        }
    });

    std::uint64_t pushed = 0;
    for (;;) {
        auto batch = reader.read_next_batch();
        if (batch.status == file_reader::ReadStatus::EndOfFile) {
            break;
        }
        if (batch.status == file_reader::ReadStatus::Error) {
            result.ok = false;
            result.error = batch.error;
            break;
        }
        batch.sequence = pushed;
        batch.read_time = Clock::now();
        if (!queue.push(std::move(batch))) {
            break;
        }
        ++pushed;
    }
    queue.close();
    reorder.finish(pushed);
    result.stats.batches_read = pushed;

    std::exception_ptr failure;
    for (auto& parser : parsers) {
        try {
            result.stats.workers.push_back(parser.get());
        } catch (...) {
            if (!failure) {
                failure = std::current_exception();
            }
        }
    }
    try {
        aggregation.get();
    } catch (...) {
        if (!failure) {
            failure = std::current_exception();
        }
    }
    pool.shutdown();
    if (failure) {
        std::rethrow_exception(failure);
    }

    const auto queue_stats = queue.stats();
    result.stats.max_queue_depth = queue_stats.max_queued_batches;
    result.stats.queue_accepted = queue_stats.accepted_batches;
    result.stats.queue_popped = queue_stats.popped_batches;
    aggregator.finish(result);
    return result;
}

} // namespace

PipelineResult run_log_pipeline(const std::filesystem::path& path, const PipelineOptions& options) {
    const auto started = std::chrono::steady_clock::now();
    auto result = options.worker_count <= 1
        ? run_sequential(path, options)
        : run_concurrent(path, options);
    result.stats.elapsed = std::chrono::steady_clock::now() - started;
    return result;
}

} // namespace sentinelx::pipeline
