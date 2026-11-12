#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <sentinelx/log_parser/LogParser.h>

namespace sentinelx::anomaly_detector {

enum class AnomalySeverity {
    Low,
    Medium,
    High,
    Critical
};

struct AnomalyDetectorOptions {
    std::chrono::seconds window_size{60};
    std::size_t baseline_window_count{5};
    std::size_t minimum_error_count{3};
    double detection_standard_deviations{2.0};
    double medium_severity_standard_deviations{3.0};
    double high_severity_standard_deviations{4.0};
    double critical_severity_standard_deviations{5.0};
};

struct AnomalyReport {
    std::string component;
    std::chrono::sys_seconds window_start;
    std::size_t observed_errors{0};
    double rolling_average{0.0};
    double standard_deviation{0.0};
    double detection_threshold{0.0};
    std::optional<double> standard_deviation_score;
    AnomalySeverity severity{AnomalySeverity::Low};
    std::string explanation;
};

class AnomalyDetector final {
public:
    explicit AnomalyDetector(AnomalyDetectorOptions options = {});

    std::vector<AnomalyReport> consume(
        std::span<const log_parser::LogEntry> entries);
    std::vector<AnomalyReport> consume(
        const log_parser::BatchParseResult& batch);
    void reset();

private:
    struct ComponentWindowState {
        bool initialized{false};
        std::int64_t window_index{0};
        std::size_t current_errors{0};
        std::deque<std::size_t> baseline;
        std::size_t last_reported_error_count{0};
        bool dirty{false};
    };

    std::int64_t window_index(std::chrono::sys_seconds timestamp) const;
    void advance_window(ComponentWindowState& state, std::int64_t target_index);
    std::optional<AnomalyReport> detect(
        const std::string& component,
        const ComponentWindowState& state) const;

    AnomalyDetectorOptions options_;
    std::unordered_map<std::string, ComponentWindowState> components_;
};

} // namespace sentinelx::anomaly_detector