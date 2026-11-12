#include <sentinelx/file_reader/FileReader.h>
#include <sentinelx/log_analyzer/LogAnalyzer.h>
#include <sentinelx/log_parser/LogParser.h>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

namespace {

using sentinelx::log_analyzer::AnalyzerOptions;
using sentinelx::log_analyzer::LogAnalyzer;
using sentinelx::log_parser::LogEntry;
using sentinelx::log_parser::Severity;

LogEntry make_entry(Severity severity, std::string component, std::string message) {
    return {
        std::chrono::sys_seconds{},
        severity,
        std::move(component),
        std::move(message)};
}

std::filesystem::path make_temp_path() {
    const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
        ("sentinelx_log_analyzer_" + std::to_string(ticks) + ".log");
}

TEST(LogAnalyzer, CountsSeveritiesAndCalculatesPercentages) {
    const std::vector<LogEntry> entries{
        make_entry(Severity::Info, "Reader", "started"),
        make_entry(Severity::Warning, "Queue", "near capacity"),
        make_entry(Severity::Error, "Disk", "read failed"),
        make_entry(Severity::Error, "Disk", "write failed"),
        make_entry(Severity::Debug, "Worker", "task complete")};

    LogAnalyzer analyzer;
    analyzer.consume(entries);
    const auto report = analyzer.summary();

    EXPECT_EQ(report.total_records, 5);
    EXPECT_EQ(report.malformed_records, 0);
    ASSERT_EQ(report.severity_distribution.size(), 4);
    EXPECT_EQ(report.severity_distribution[0].severity, Severity::Info);
    EXPECT_EQ(report.severity_distribution[0].count, 1);
    EXPECT_NEAR(report.severity_distribution[0].percentage, 20.0, 1e-9);
    EXPECT_EQ(report.severity_distribution[1].severity, Severity::Warning);
    EXPECT_EQ(report.severity_distribution[1].count, 1);
    EXPECT_NEAR(report.severity_distribution[1].percentage, 20.0, 1e-9);
    EXPECT_EQ(report.severity_distribution[2].severity, Severity::Error);
    EXPECT_EQ(report.severity_distribution[2].count, 2);
    EXPECT_NEAR(report.severity_distribution[2].percentage, 40.0, 1e-9);
    EXPECT_EQ(report.severity_distribution[3].severity, Severity::Debug);
    EXPECT_EQ(report.severity_distribution[3].count, 1);
    EXPECT_NEAR(report.severity_distribution[3].percentage, 20.0, 1e-9);
}

TEST(LogAnalyzer, RanksErrorsAndCountsRepeatedMessagesByComponent) {
    LogAnalyzer analyzer(AnalyzerOptions{.top_error_limit = 2});
    const std::vector<LogEntry> entries{
        make_entry(Severity::Error, "Disk", "device unavailable"),
        make_entry(Severity::Error, "Disk", "device unavailable"),
        make_entry(Severity::Error, "Network", "connection timeout"),
        make_entry(Severity::Error, "Network", "connection timeout"),
        make_entry(Severity::Error, "Disk", "device unavailable"),
        make_entry(Severity::Info, "Disk", "recovered")};

    analyzer.consume(entries);
    const auto report = analyzer.summary();

    ASSERT_EQ(report.errors_by_component.size(), 2);
    EXPECT_EQ(report.errors_by_component.at("Disk"), 3);
    EXPECT_EQ(report.errors_by_component.at("Network"), 2);
    ASSERT_EQ(report.most_frequent_errors.size(), 2);
    EXPECT_EQ(report.most_frequent_errors[0].message, "device unavailable");
    EXPECT_EQ(report.most_frequent_errors[0].count, 3);
    EXPECT_EQ(report.most_frequent_errors[1].message, "connection timeout");
    EXPECT_EQ(report.most_frequent_errors[1].count, 2);
    ASSERT_EQ(report.repeated_error_messages.size(), 2);
    EXPECT_EQ(report.repeated_error_messages[0].message, "device unavailable");
    EXPECT_EQ(report.repeated_error_messages[1].message, "connection timeout");
}

TEST(LogAnalyzer, ProcessesFileReaderAndParserBatches) {
    const auto path = make_temp_path();
    {
        std::ofstream output(path, std::ios::binary);
        ASSERT_TRUE(output.is_open());
        output << "2026-11-12 08:15:30 ERROR Disk: device unavailable\n"
                  "not a valid entry\n"
                  "2026-11-12 08:15:31 ERROR Disk: device unavailable\n"
                  "2026-11-12 08:15:32 INFO Reader: complete\n";
        ASSERT_TRUE(output.good());
    }

    LogAnalyzer analyzer;
    {
        sentinelx::file_reader::FileReader reader(
            path, {.batch_size = 2, .buffer_size = 64, .max_line_bytes = 256});
        for (;;) {
            const auto read_batch = reader.read_next_batch();
            if (read_batch.status == sentinelx::file_reader::ReadStatus::EndOfFile) {
                break;
            }
            ASSERT_NE(read_batch.status, sentinelx::file_reader::ReadStatus::Error)
                << read_batch.error;
            analyzer.consume(sentinelx::log_parser::LogParser::parse_batch(read_batch.lines));
        }
    }
    std::filesystem::remove(path);

    const auto report = analyzer.summary();
    EXPECT_EQ(report.total_records, 3);
    EXPECT_EQ(report.malformed_records, 1);
    ASSERT_EQ(report.errors_by_component.size(), 1);
    EXPECT_EQ(report.errors_by_component.at("Disk"), 2);
    ASSERT_EQ(report.repeated_error_messages.size(), 1);
    EXPECT_EQ(report.repeated_error_messages[0].message, "device unavailable");
}

TEST(LogAnalyzer, EmptySummaryHasZeroPercentagesAndCanBeReset) {
    LogAnalyzer analyzer;
    auto report = analyzer.summary();
    EXPECT_EQ(report.total_records, 0);
    for (const auto& severity : report.severity_distribution) {
        EXPECT_DOUBLE_EQ(severity.percentage, 0.0);
    }

    const std::vector<LogEntry> entries{
        make_entry(Severity::Debug, "Worker", "tick")};
    analyzer.consume(entries);
    analyzer.reset();
    report = analyzer.summary();
    EXPECT_EQ(report.total_records, 0);
    EXPECT_TRUE(report.errors_by_component.empty());
}

TEST(LogAnalyzer, RejectsInvalidSeverityValuesWithoutIndexingOutOfBounds) {
    LogAnalyzer analyzer;
    const std::vector<LogEntry> entries{
        make_entry(static_cast<Severity>(255), "Core", "invalid severity")};

    analyzer.consume(entries);
    const auto report = analyzer.summary();
    EXPECT_EQ(report.total_records, 0);
    EXPECT_EQ(report.malformed_records, 1);
    for (const auto& severity : report.severity_distribution) {
        EXPECT_EQ(severity.count, 0);
    }
}

} // namespace