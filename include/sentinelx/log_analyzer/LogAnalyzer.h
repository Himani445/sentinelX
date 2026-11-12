#pragma once

#include <array>
#include <cstddef>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include <sentinelx/log_parser/LogParser.h>

namespace sentinelx::log_analyzer {

struct AnalyzerOptions {
    std::size_t top_error_limit{10};
};

struct SeverityStatistics {
    log_parser::Severity severity;
    std::size_t count{0};
    double percentage{0.0};
};

struct MessageFrequency {
    std::string message;
    std::size_t count{0};
};

struct AnalysisSummary {
    std::size_t total_records{0};
    std::size_t malformed_records{0};
    std::array<SeverityStatistics, 4> severity_distribution{};
    std::unordered_map<std::string, std::size_t> errors_by_component;
    std::vector<MessageFrequency> most_frequent_errors;
    std::vector<MessageFrequency> repeated_error_messages;
};

class LogAnalyzer final {
public:
    explicit LogAnalyzer(AnalyzerOptions options = {});

    void consume(const log_parser::BatchParseResult& batch);
    void consume(std::span<const log_parser::LogEntry> entries);
    AnalysisSummary summary() const;
    void reset();

private:
    AnalyzerOptions options_;
    std::size_t total_records_{0};
    std::size_t malformed_records_{0};
    std::array<std::size_t, 4> severity_counts_{};
    std::unordered_map<std::string, std::size_t> errors_by_component_;
    std::unordered_map<std::string, std::size_t> error_message_counts_;
};

} // namespace sentinelx::log_analyzer