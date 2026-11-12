
#include <sentinelx/log_analyzer/LogAnalyzer.h>

#include <algorithm>
#include <utility>

namespace sentinelx::log_analyzer {
namespace {

constexpr std::array<log_parser::Severity, 4> kSeverities{
    log_parser::Severity::Info,
    log_parser::Severity::Warning,
    log_parser::Severity::Error,
    log_parser::Severity::Debug};

bool severity_index(log_parser::Severity severity, std::size_t& index) {
    switch (severity) {
    case log_parser::Severity::Info:
        index = 0;
        return true;
    case log_parser::Severity::Warning:
        index = 1;
        return true;
    case log_parser::Severity::Error:
        index = 2;
        return true;
    case log_parser::Severity::Debug:
        index = 3;
        return true;
    }
    return false;
}

} // namespace

LogAnalyzer::LogAnalyzer(AnalyzerOptions options)
    : options_(options) {}

void LogAnalyzer::consume(const log_parser::BatchParseResult& batch) {
    malformed_records_ += batch.issues.size();
    consume(batch.entries);
}

void LogAnalyzer::consume(std::span<const log_parser::LogEntry> entries) {
    for (const auto& entry : entries) {
        std::size_t index = 0;
        if (!severity_index(entry.severity, index)) {
            ++malformed_records_;
            continue;
        }
        ++total_records_;
        ++severity_counts_[index];

        if (entry.severity == log_parser::Severity::Error) {
            ++errors_by_component_[entry.component];
            ++error_message_counts_[entry.message];
        }
    }
}

AnalysisSummary LogAnalyzer::summary() const {
    AnalysisSummary report;
    report.total_records = total_records_;
    report.malformed_records = malformed_records_;
    report.errors_by_component = errors_by_component_;

    for (std::size_t index = 0; index < kSeverities.size(); ++index) {
        const auto count = severity_counts_[index];
        const auto percentage = total_records_ == 0
            ? 0.0
            : static_cast<double>(count) * 100.0 /
                static_cast<double>(total_records_);
        report.severity_distribution[index] = {
            kSeverities[index], count, percentage};
    }

    std::vector<MessageFrequency> frequencies;
    frequencies.reserve(error_message_counts_.size());
    for (const auto& [message, count] : error_message_counts_) {
        frequencies.push_back({message, count});
    }
    std::sort(frequencies.begin(), frequencies.end(),
        [](const MessageFrequency& left, const MessageFrequency& right) {
            if (left.count != right.count) {
                return left.count > right.count;
            }
            return left.message < right.message;
        });

    const auto top_count = std::min(options_.top_error_limit, frequencies.size());
    report.most_frequent_errors.reserve(top_count);
    for (std::size_t index = 0; index < frequencies.size(); ++index) {
        if (index < top_count) {
            report.most_frequent_errors.push_back(frequencies[index]);
        }
        if (frequencies[index].count > 1) {
            report.repeated_error_messages.push_back(std::move(frequencies[index]));
        }
    }

    return report;
}

void LogAnalyzer::reset() {
    total_records_ = 0;
    malformed_records_ = 0;
    severity_counts_.fill(0);
    errors_by_component_.clear();
    error_message_counts_.clear();
}

} // namespace sentinelx::log_analyzer