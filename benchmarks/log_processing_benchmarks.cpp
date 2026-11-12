#include <benchmark/benchmark.h>

#include <sentinelx/file_reader/FileReader.h>
#include <sentinelx/log_analyzer/LogAnalyzer.h>
#include <sentinelx/log_parser/LogParser.h>
#include <sentinelx/task_queue/TaskQueue.h>
#include <sentinelx/thread_pool/ThreadPool.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#if defined(_WIN32)
#define NOMINMAX
#include <windows.h>
#include <psapi.h>
#elif defined(__linux__)
#include <unistd.h>
#elif defined(__APPLE__)
#include <sys/resource.h>
#endif

namespace {

using sentinelx::file_reader::ReadStatus;
using sentinelx::log_analyzer::AnalysisSummary;
using sentinelx::log_analyzer::LogAnalyzer;
using sentinelx::log_analyzer::MessageFrequency;
using sentinelx::log_analyzer::SeverityStatistics;

constexpr std::array<std::size_t, 4> kRecordCounts{
    10'000, 100'000, 1'000'000, 5'000'000};
constexpr std::array<std::size_t, 3> kBatchSizes{256, 1024, 4096};
constexpr std::array<std::size_t, 3> kWorkerCounts{2, 4, 8};
constexpr std::size_t kTopErrorLimit = 64;

struct RunMetrics {
    std::uint64_t batch_latency_nanoseconds{0};
    std::uint64_t max_batch_latency_nanoseconds{0};
    std::uint64_t batch_count{0};
    std::uint64_t worker_busy_nanoseconds{0};
    std::uint64_t queue_wait_nanoseconds{0};
    std::uint64_t queue_wait_count{0};
    std::uint64_t peak_resident_memory_bytes{0};
};

struct RunResult {
    AnalysisSummary summary;
    RunMetrics metrics;
};

struct WorkerResult {
    AnalysisSummary summary;
    RunMetrics metrics;
};

struct BenchmarkInputs {
    std::filesystem::path path;
    AnalysisSummary expected_summary;
    std::string error;
};

std::uint64_t resident_memory_bytes() {
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS_EX counters{};
    if (GetProcessMemoryInfo(
            GetCurrentProcess(),
            reinterpret_cast<PROCESS_MEMORY_COUNTERS*>(&counters),
            sizeof(counters))) {
        return static_cast<std::uint64_t>(counters.WorkingSetSize);
    }
#elif defined(__linux__)
    std::ifstream status("/proc/self/statm");
    std::uint64_t total_pages = 0;
    std::uint64_t resident_pages = 0;
    if (status >> total_pages >> resident_pages) {
        const auto page_size = sysconf(_SC_PAGESIZE);
        if (page_size > 0) {
            return resident_pages * static_cast<std::uint64_t>(page_size);
        }
    }
#elif defined(__APPLE__)
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        return static_cast<std::uint64_t>(usage.ru_maxrss);
    }
#endif
    return 0;
}

void sample_peak_memory(std::atomic<std::uint64_t>& peak) {
    auto current = resident_memory_bytes();
    auto observed = peak.load(std::memory_order_relaxed);
    while (current > observed && !peak.compare_exchange_weak(
               observed, current, std::memory_order_relaxed)) {
    }
}

std::filesystem::path benchmark_data_directory() {
    if (const auto* configured = std::getenv("SENTINELX_BENCHMARK_DATA_DIR")) {
        return configured;
    }
    return std::filesystem::temp_directory_path() / "sentinelx_benchmark_data";
}

std::filesystem::path ensure_dataset(std::size_t record_count) {
    static std::mutex datasets_mutex;
    static std::unordered_map<std::size_t, std::filesystem::path> datasets;
    std::lock_guard lock(datasets_mutex);
    if (const auto existing = datasets.find(record_count); existing != datasets.end()) {
        return existing->second;
    }

    const auto directory = benchmark_data_directory();
    std::filesystem::create_directories(directory);
    const auto path = directory / ("sentinelx_" + std::to_string(record_count) + ".log");
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        throw std::runtime_error("Unable to create benchmark dataset: " + path.string());
    }

    constexpr std::array<std::string_view, 4> severities{
        "INFO", "WARNING", "ERROR", "DEBUG"};
    for (std::size_t index = 0; index < record_count; ++index) {
        output << "2026-01-01 00:00:00 " << severities[index % severities.size()]
               << " component-" << (index / severities.size()) % 8
               << ": message-" << index % 32 << '\n';
    }
    output.close();
    if (!output) {
        throw std::runtime_error("Unable to finish benchmark dataset: " + path.string());
    }
    datasets.emplace(record_count, path);
    return path;
}

LogAnalyzer make_analyzer() {
    return LogAnalyzer({.top_error_limit = kTopErrorLimit});
}

RunResult run_sequential(
    const std::filesystem::path& path,
    std::size_t batch_size) {
    sentinelx::file_reader::FileReader reader(
        path, {.batch_size = batch_size});
    auto analyzer = make_analyzer();
    RunMetrics metrics;
    std::atomic<std::uint64_t> peak_memory{resident_memory_bytes()};

    for (;;) {
        const auto batch_start = std::chrono::steady_clock::now();
        auto batch = reader.read_next_batch();
        if (batch.status == ReadStatus::EndOfFile) {
            break;
        }
        if (batch.status == ReadStatus::Error) {
            throw std::runtime_error(batch.error);
        }
        auto parsed = sentinelx::log_parser::LogParser::parse_batch(batch.lines);
        analyzer.consume(parsed);
        const auto elapsed = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - batch_start)
                .count());
        metrics.batch_latency_nanoseconds += elapsed;
        metrics.max_batch_latency_nanoseconds =
            std::max(metrics.max_batch_latency_nanoseconds, elapsed);
        ++metrics.batch_count;
        metrics.worker_busy_nanoseconds += elapsed;
        sample_peak_memory(peak_memory);
    }

    metrics.peak_resident_memory_bytes = peak_memory.load(std::memory_order_relaxed);
    return {analyzer.summary(), metrics};
}

AnalysisSummary merge_worker_summaries(const std::vector<AnalysisSummary>& summaries) {
    AnalysisSummary merged;
    std::array<std::size_t, 4> severity_counts{};
    std::unordered_map<std::string, std::size_t> error_messages;

    for (const auto& summary : summaries) {
        merged.total_records += summary.total_records;
        merged.malformed_records += summary.malformed_records;
        for (const auto& severity : summary.severity_distribution) {
            severity_counts[static_cast<std::size_t>(severity.severity)] += severity.count;
        }
        for (const auto& [component, count] : summary.errors_by_component) {
            merged.errors_by_component[component] += count;
        }
        for (const auto& frequency : summary.most_frequent_errors) {
            error_messages[frequency.message] += frequency.count;
        }
    }

    constexpr std::array<sentinelx::log_parser::Severity, 4> severities{
        sentinelx::log_parser::Severity::Info,
        sentinelx::log_parser::Severity::Warning,
        sentinelx::log_parser::Severity::Error,
        sentinelx::log_parser::Severity::Debug};
    for (std::size_t index = 0; index < severities.size(); ++index) {
        const auto count = severity_counts[index];
        const auto percentage = merged.total_records == 0
            ? 0.0
            : static_cast<double>(count) * 100.0 /
                static_cast<double>(merged.total_records);
        merged.severity_distribution[index] = {severities[index], count, percentage};
    }

    std::vector<MessageFrequency> frequencies;
    frequencies.reserve(error_messages.size());
    for (const auto& [message, count] : error_messages) {
        frequencies.push_back({message, count});
    }
    std::sort(frequencies.begin(), frequencies.end(),
        [](const MessageFrequency& left, const MessageFrequency& right) {
            if (left.count != right.count) {
                return left.count > right.count;
            }
            return left.message < right.message;
        });
    merged.most_frequent_errors = frequencies;
    for (const auto& frequency : frequencies) {
        if (frequency.count > 1) {
            merged.repeated_error_messages.push_back(frequency);
        }
    }
    return merged;
}

bool same_frequencies(
    const std::vector<MessageFrequency>& left,
    const std::vector<MessageFrequency>& right) {
    if (left.size() != right.size()) {
        return false;
    }
    for (std::size_t index = 0; index < left.size(); ++index) {
        if (left[index].message != right[index].message ||
            left[index].count != right[index].count) {
            return false;
        }
    }
    return true;
}

bool same_summary(const AnalysisSummary& left, const AnalysisSummary& right) {
    if (left.total_records != right.total_records ||
        left.malformed_records != right.malformed_records ||
        left.errors_by_component != right.errors_by_component ||
        !same_frequencies(left.most_frequent_errors, right.most_frequent_errors) ||
        !same_frequencies(left.repeated_error_messages, right.repeated_error_messages)) {
        return false;
    }
    for (std::size_t index = 0; index < left.severity_distribution.size(); ++index) {
        const auto& left_severity = left.severity_distribution[index];
        const auto& right_severity = right.severity_distribution[index];
        if (left_severity.severity != right_severity.severity ||
            left_severity.count != right_severity.count ||
            left_severity.percentage != right_severity.percentage) {
            return false;
        }
    }
    return true;
}

RunResult run_concurrent(
    const std::filesystem::path& path,
    std::size_t batch_size,
    std::size_t worker_count) {
    sentinelx::task_queue::TaskQueue queue(std::max<std::size_t>(1, worker_count * 2));
    sentinelx::thread_pool::ThreadPool pool(worker_count);
    std::atomic<std::uint64_t> peak_memory{resident_memory_bytes()};
    std::vector<std::future<WorkerResult>> workers;
    workers.reserve(worker_count);

    for (std::size_t worker = 0; worker < worker_count; ++worker) {
        workers.push_back(pool.submit([&queue, &peak_memory] {
            auto analyzer = make_analyzer();
            WorkerResult result;
            while (auto batch = queue.pop()) {
                const auto batch_start = std::chrono::steady_clock::now();
                const auto parsed = sentinelx::log_parser::LogParser::parse_batch(
                    batch->lines);
                analyzer.consume(parsed);
                const auto elapsed = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - batch_start)
                        .count());
                result.metrics.batch_latency_nanoseconds += elapsed;
                result.metrics.max_batch_latency_nanoseconds = std::max(
                    result.metrics.max_batch_latency_nanoseconds, elapsed);
                ++result.metrics.batch_count;
                result.metrics.worker_busy_nanoseconds += elapsed;
                sample_peak_memory(peak_memory);
            }
            result.summary = analyzer.summary();
            return result;
        }));
    }

    sentinelx::file_reader::FileReader reader(
        path, {.batch_size = batch_size});
    for (;;) {
        auto batch = reader.read_next_batch();
        if (batch.status == ReadStatus::EndOfFile) {
            break;
        }
        if (batch.status == ReadStatus::Error) {
            queue.close();
            throw std::runtime_error(batch.error);
        }
        if (!queue.push(std::move(batch))) {
            throw std::runtime_error("Benchmark task queue closed unexpectedly");
        }
        sample_peak_memory(peak_memory);
    }
    queue.close();

    std::vector<AnalysisSummary> summaries;
    summaries.reserve(worker_count);
    RunMetrics metrics;
    for (auto& worker : workers) {
        auto result = worker.get();
        summaries.push_back(std::move(result.summary));
        metrics.batch_latency_nanoseconds += result.metrics.batch_latency_nanoseconds;
        metrics.max_batch_latency_nanoseconds = std::max(
            metrics.max_batch_latency_nanoseconds,
            result.metrics.max_batch_latency_nanoseconds);
        metrics.batch_count += result.metrics.batch_count;
        metrics.worker_busy_nanoseconds += result.metrics.worker_busy_nanoseconds;
    }
    pool.shutdown();

    const auto queue_stats = queue.stats();
    metrics.queue_wait_nanoseconds = queue_stats.total_wait_nanoseconds;
    metrics.queue_wait_count = queue_stats.popped_batches;
    metrics.peak_resident_memory_bytes = peak_memory.load(std::memory_order_relaxed);
    return {merge_worker_summaries(summaries), metrics};
}

AnalysisSummary expected_summary(
    const std::filesystem::path& path,
    std::size_t record_count) {
    static std::mutex cache_mutex;
    static std::unordered_map<std::size_t, AnalysisSummary> cache;
    std::lock_guard lock(cache_mutex);
    if (const auto existing = cache.find(record_count); existing != cache.end()) {
        return existing->second;
    }
    auto result = run_sequential(path, 4096).summary;
    cache.emplace(record_count, result);
    return result;
}

std::mutex& benchmark_inputs_mutex() {
    static std::mutex mutex;
    return mutex;
}

std::unordered_map<std::size_t, BenchmarkInputs>& benchmark_inputs_by_size() {
    static std::unordered_map<std::size_t, BenchmarkInputs> inputs;
    return inputs;
}

void prepare_benchmark(const benchmark::State& state) {
    const auto record_count = static_cast<std::size_t>(state.range(0));
    BenchmarkInputs inputs;
    try {
        inputs.path = ensure_dataset(record_count);
        inputs.expected_summary = expected_summary(inputs.path, record_count);
    } catch (const std::exception& error) {
        inputs.error = error.what();
    }
    std::lock_guard lock(benchmark_inputs_mutex());
    benchmark_inputs_by_size()[record_count] = std::move(inputs);
}

void run_benchmark(benchmark::State& state, bool concurrent) {
    const auto record_count = static_cast<std::size_t>(state.range(0));
    const auto batch_size = static_cast<std::size_t>(state.range(1));
    const auto worker_count = static_cast<std::size_t>(state.range(2));

    BenchmarkInputs inputs;
    {
        std::lock_guard lock(benchmark_inputs_mutex());
        const auto found = benchmark_inputs_by_size().find(record_count);
        if (found == benchmark_inputs_by_size().end()) {
            state.SkipWithError("Benchmark setup did not prepare this input size");
            return;
        }
        inputs = found->second;
    }
    if (!inputs.error.empty()) {
        state.SkipWithError(inputs.error.c_str());
        return;
    }

    std::uint64_t total_batch_latency = 0;
    std::uint64_t total_batch_samples = 0;
    std::uint64_t total_queue_wait = 0;
    std::uint64_t total_queue_samples = 0;
    std::uint64_t total_worker_busy = 0;
    std::uint64_t total_wall = 0;
    std::uint64_t max_batch_latency = 0;
    std::uint64_t peak_memory = 0;

    for (auto _ : state) {
        const auto start = std::chrono::steady_clock::now();
        try {
            const auto result = concurrent
                ? run_concurrent(inputs.path, batch_size, worker_count)
                : run_sequential(inputs.path, batch_size);
            const auto elapsed = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count());
            if (!same_summary(result.summary, inputs.expected_summary)) {
                state.SkipWithError(
                    "Sequential and concurrent analysis summaries differ");
                break;
            }
            total_wall += elapsed;
            total_batch_latency += result.metrics.batch_latency_nanoseconds;
            total_batch_samples += result.metrics.batch_count;
            total_queue_wait += result.metrics.queue_wait_nanoseconds;
            total_queue_samples += result.metrics.queue_wait_count;
            total_worker_busy += result.metrics.worker_busy_nanoseconds;
            max_batch_latency = std::max(
                max_batch_latency, result.metrics.max_batch_latency_nanoseconds);
            peak_memory = std::max(peak_memory, result.metrics.peak_resident_memory_bytes);
        } catch (const std::exception& error) {
            state.SkipWithError(error.what());
            break;
        }
    }

    const auto iterations = static_cast<std::uint64_t>(state.iterations());
    if (iterations > 0) {
        state.SetItemsProcessed(static_cast<std::int64_t>(iterations * record_count));
        state.counters["throughput_records_per_second"] = benchmark::Counter(
            static_cast<double>(record_count),
            benchmark::Counter::kIsIterationInvariantRate);
        state.counters["average_batch_latency_ns"] = total_batch_samples == 0
            ? 0.0
            : static_cast<double>(total_batch_latency) /
                static_cast<double>(total_batch_samples);
        state.counters["max_batch_latency_ns"] =
            static_cast<double>(max_batch_latency);
        state.counters["average_queue_wait_ns"] = total_queue_samples == 0
            ? 0.0
            : static_cast<double>(total_queue_wait) /
                static_cast<double>(total_queue_samples);
        state.counters["peak_resident_memory_bytes"] =
            static_cast<double>(peak_memory);
        state.counters["worker_utilization_percent"] = total_wall == 0
            ? 0.0
            : 100.0 * static_cast<double>(total_worker_busy) /
                (static_cast<double>(total_wall) * static_cast<double>(worker_count));
    }
    state.counters["input_records"] = static_cast<double>(record_count);
    state.counters["batch_size"] = static_cast<double>(batch_size);
    state.counters["worker_count"] = static_cast<double>(worker_count);
}

void BM_Sequential(benchmark::State& state) {
    run_benchmark(state, false);
}

void BM_Concurrent(benchmark::State& state) {
    run_benchmark(state, true);
}

auto* sequential_benchmark = benchmark::RegisterBenchmark(
    "Sequential", BM_Sequential)->Setup(prepare_benchmark);
auto* concurrent_benchmark = benchmark::RegisterBenchmark(
    "Concurrent", BM_Concurrent)->Setup(prepare_benchmark);

[[maybe_unused]] const bool registered_benchmarks = [] {
    auto* sequential = sequential_benchmark;
    auto* concurrent = concurrent_benchmark;
    for (const auto records : kRecordCounts) {
        for (const auto batch_size : kBatchSizes) {
            sequential->Args({
                static_cast<std::int64_t>(records),
                static_cast<std::int64_t>(batch_size),
                1});
            for (const auto workers : kWorkerCounts) {
                concurrent->Args({
                    static_cast<std::int64_t>(records),
                    static_cast<std::int64_t>(batch_size),
                    static_cast<std::int64_t>(workers)});
            }
        }
    }
    sequential->UseRealTime();
    concurrent->UseRealTime();
    return true;
}();

} // namespace