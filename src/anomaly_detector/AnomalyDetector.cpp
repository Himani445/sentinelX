#include <sentinelx/anomaly_detector/AnomalyDetector.h>

#include <cmath>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace sentinelx::anomaly_detector {
namespace {

AnomalySeverity classify_severity(
    double score,
    double medium_threshold,
    double high_threshold,
    double critical_threshold) {
    if (score >= critical_threshold) {
        return AnomalySeverity::Critical;
    }
    if (score >= high_threshold) {
        return AnomalySeverity::High;
    }
    if (score >= medium_threshold) {
        return AnomalySeverity::Medium;
    }
    return AnomalySeverity::Low;
}

const char* severity_name(AnomalySeverity severity) {
    switch (severity) {
    case AnomalySeverity::Low:
        return "LOW";
    case AnomalySeverity::Medium:
        return "MEDIUM";
    case AnomalySeverity::High:
        return "HIGH";
    case AnomalySeverity::Critical:
        return "CRITICAL";
    }
    return "UNKNOWN";
}

} // namespace

AnomalyDetector::AnomalyDetector(AnomalyDetectorOptions options)
    : options_(options) {
    if (options_.window_size <= std::chrono::seconds::zero()) {
        throw std::invalid_argument("Anomaly window size must be positive");
    }
    if (options_.baseline_window_count == 0) {
        throw std::invalid_argument("Baseline window count must be positive");
    }
    if (options_.minimum_error_count == 0) {
        throw std::invalid_argument("Minimum error count must be positive");
    }
    if (!std::isfinite(options_.detection_standard_deviations) ||
        !std::isfinite(options_.medium_severity_standard_deviations) ||
        !std::isfinite(options_.high_severity_standard_deviations) ||
        !std::isfinite(options_.critical_severity_standard_deviations) ||
        options_.detection_standard_deviations < 0.0 ||
        options_.medium_severity_standard_deviations <
            options_.detection_standard_deviations ||
        options_.high_severity_standard_deviations <
            options_.medium_severity_standard_deviations ||
        options_.critical_severity_standard_deviations <
            options_.high_severity_standard_deviations) {
        throw std::invalid_argument("Anomaly deviation thresholds are invalid");
    }
}

std::vector<AnomalyReport> AnomalyDetector::consume(
    std::span<const log_parser::LogEntry> entries) {
    std::vector<AnomalyReport> reports;
    for (const auto& entry : entries) {
        auto& state = components_[entry.component];
        const auto index = window_index(entry.timestamp);
        if (!state.initialized) {
            state.initialized = true;
            state.window_index = index;
        } else if (index < state.window_index) {
            throw std::invalid_argument(
                "Log entries for each component must be timestamp ordered");
        } else if (index > state.window_index) {
            if (state.current_errors > state.last_reported_error_count) {
                if (auto report = detect(entry.component, state)) {
                    reports.push_back(std::move(*report));
                    state.last_reported_error_count = state.current_errors;
                }
            }
            advance_window(state, index);
        }

        if (entry.severity != log_parser::Severity::Error) {
            continue;
        }
        ++state.current_errors;
        state.dirty = true;
    }

    for (auto& [component, state] : components_) {
        if (!state.dirty) {
            continue;
        }
        if (state.current_errors > state.last_reported_error_count) {
            if (auto report = detect(component, state)) {
                reports.push_back(std::move(*report));
                state.last_reported_error_count = state.current_errors;
            }
        }
        state.dirty = false;
    }
    return reports;
}

std::vector<AnomalyReport> AnomalyDetector::consume(
    const log_parser::BatchParseResult& batch) {
    return consume(batch.entries);
}

void AnomalyDetector::reset() {
    components_.clear();
}

std::int64_t AnomalyDetector::window_index(
    std::chrono::sys_seconds timestamp) const {
    const auto seconds = timestamp.time_since_epoch().count();
    const auto width = options_.window_size.count();
    auto index = seconds / width;
    if (seconds < 0 && seconds % width != 0) {
        --index;
    }
    return index;
}

void AnomalyDetector::advance_window(
    ComponentWindowState& state,
    std::int64_t target_index) {
    const auto distance = static_cast<std::uint64_t>(target_index) -
        static_cast<std::uint64_t>(state.window_index);
    if (distance > options_.baseline_window_count) {
        state.baseline.clear();
        state.baseline.resize(options_.baseline_window_count, 0);
    } else {
        state.baseline.push_back(state.current_errors);
        for (std::uint64_t skipped = 1; skipped < distance; ++skipped) {
            state.baseline.push_back(0);
        }
        while (state.baseline.size() > options_.baseline_window_count) {
            state.baseline.pop_front();
        }
    }
    state.window_index = target_index;
    state.current_errors = 0;
    state.last_reported_error_count = 0;
    state.dirty = false;
}

std::optional<AnomalyReport> AnomalyDetector::detect(
    const std::string& component,
    const ComponentWindowState& state) const {
    if (state.baseline.size() < options_.baseline_window_count ||
        state.current_errors < options_.minimum_error_count) {
        return std::nullopt;
    }

    double total = 0.0;
    for (const auto count : state.baseline) {
        total += static_cast<double>(count);
    }
    const auto sample_count = static_cast<double>(state.baseline.size());
    const auto average = total / sample_count;

    double squared_difference_total = 0.0;
    for (const auto count : state.baseline) {
        const auto difference = static_cast<double>(count) - average;
        squared_difference_total += difference * difference;
    }
    const auto deviation = std::sqrt(squared_difference_total / sample_count);
    const auto threshold = average +
        options_.detection_standard_deviations * deviation;
    if (static_cast<double>(state.current_errors) <= threshold) {
        return std::nullopt;
    }

    const auto score = deviation == 0.0
        ? std::optional<double>{}
        : std::optional<double>{
              (static_cast<double>(state.current_errors) - average) / deviation};
    const auto severity = score
        ? classify_severity(
              *score,
              options_.medium_severity_standard_deviations,
              options_.high_severity_standard_deviations,
              options_.critical_severity_standard_deviations)
        : AnomalySeverity::Low;
    const auto width = options_.window_size.count();
    const auto window_start = std::chrono::sys_seconds{
        std::chrono::seconds{state.window_index * width}};

    std::ostringstream explanation;
    explanation << severity_name(severity) << " anomaly for component '" << component
                << "': observed " << state.current_errors << " ERROR entries in the window; "
                << "rolling average " << std::fixed << std::setprecision(2) << average
                << ", standard deviation " << deviation << ", detection threshold "
                << threshold << " (strictly greater than threshold required).";

    return AnomalyReport{
        component,
        window_start,
        state.current_errors,
        average,
        deviation,
        threshold,
        score,
        severity,
        explanation.str()};
}

} // namespace sentinelx::anomaly_detector