#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <sentinelx/log_parser/LogParser.h>

namespace sentinelx::event_correlator {

struct CorrelatorOptions {
    std::chrono::seconds time_window{300};
};

struct EventRecord {
    std::optional<std::chrono::sys_seconds> timestamp;
    log_parser::Severity severity;
    std::string component;
    std::string message;
};

struct ComponentTimeline {
    std::string component;
    std::vector<EventRecord> events;
};

enum class CorrelationType {
    WarningBeforeError,
    RepeatedErrorMessage,
    RepeatedErrorSequence
};

struct CorrelationReport {
    CorrelationType type;
    std::string component;
    std::vector<EventRecord> events;
    double relevance_score{0.0};
    std::size_t rank{0};
    std::string explanation;
};

struct CorrelationResult {
    std::vector<ComponentTimeline> timelines;
    std::vector<CorrelationReport> reports;
    std::size_t missing_timestamp_events{0};
    std::size_t out_of_order_events{0};
};

class EventCorrelator final {
public:
    explicit EventCorrelator(CorrelatorOptions options = {});

    CorrelationResult correlate(
        std::span<const EventRecord> events) const;
    CorrelationResult correlate(
        std::span<const log_parser::LogEntry> entries) const;

private:
    CorrelatorOptions options_;
};

} // namespace sentinelx::event_correlator