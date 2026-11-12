#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <sentinelx/anomaly_detector/AnomalyDetector.h>
#include <sentinelx/event_correlator/EventCorrelator.h>
#include <sentinelx/log_analyzer/LogAnalyzer.h>

namespace sentinelx::pipeline {

struct PipelineOptions {
    std::size_t batch_size{1024};
    // 0 or 1 runs the whole pipeline on the calling thread.
    std::size_t worker_count{4};
    // 0 selects 2 x worker_count.
    std::size_t queue_capacity{0};
    std::size_t max_correlation_events{50'000};
    log_analyzer::AnalyzerOptions analyzer{};
    anomaly_detector::AnomalyDetectorOptions anomaly{};
};

struct WorkerStats {
    std::thread::id thread_id;
    std::size_t batches_parsed{0};
};

struct PipelineStats {
    bool concurrent{false};
    std::size_t batches_read{0};
    std::size_t batches_aggregated{0};
    std::size_t skipped_lines{0};
    std::size_t queue_capacity{0};
    std::size_t max_queue_depth{0};
    std::uint64_t queue_accepted{0};
    std::uint64_t queue_popped{0};
    std::vector<WorkerStats> workers;
    std::chrono::duration<double, std::milli> elapsed{};
    // Per batch: from the batch being read until its analysis completes (excludes reading).
    double batch_latency_mean_us{0.0};
    double batch_latency_p50_us{0.0};
    double batch_latency_p95_us{0.0};
    double batch_latency_max_us{0.0};
};

struct PipelineResult {
    bool ok{true};
    std::string error;
    log_analyzer::AnalysisSummary summary;
    std::vector<anomaly_detector::AnomalyReport> anomalies;
    std::vector<event_correlator::EventRecord> notable_events;
    bool correlation_truncated{false};
    PipelineStats stats;
};

// Concurrent runs produce the same results as sequential runs: batches are
// parsed in parallel, then analyzed in file order.
PipelineResult run_log_pipeline(
    const std::filesystem::path& path,
    const PipelineOptions& options = {});

} // namespace sentinelx::pipeline
