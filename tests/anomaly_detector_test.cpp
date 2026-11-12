#include <sentinelx/anomaly_detector/AnomalyDetector.h>
#include <sentinelx/log_analyzer/LogAnalyzer.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace {

using sentinelx::anomaly_detector::AnomalyDetector;
using sentinelx::anomaly_detector::AnomalyDetectorOptions;
using sentinelx::anomaly_detector::AnomalySeverity;
using sentinelx::log_parser::LogEntry;
using sentinelx::log_parser::Severity;

LogEntry make_entry(
    std::int64_t window,
    Severity severity,
    std::string component = "Storage",
    std::string message = "read failed") {
    return {
        std::chrono::sys_seconds{std::chrono::seconds{window * 60 + 1}},
        severity,
        std::move(component),
        std::move(message)};
}

AnomalyDetectorOptions test_options() {
    return {
        .window_size = std::chrono::seconds{60},
        .baseline_window_count = 3,
        .minimum_error_count = 2,
        .detection_standard_deviations = 2.0,
        .medium_severity_standard_deviations = 3.0,
        .high_severity_standard_deviations = 4.0,
        .critical_severity_standard_deviations = 5.0};
}

TEST(AnomalyDetector, NormalActivityDoesNotTriggerAnomaly) {
    AnomalyDetector detector(test_options());
    std::vector<LogEntry> entries;
    for (std::int64_t window = 0; window < 5; ++window) {
        entries.push_back(make_entry(window, Severity::Info));
        entries.push_back(make_entry(window, Severity::Error));
    }

    EXPECT_TRUE(detector.consume(entries).empty());
}

TEST(AnomalyDetector, DetectsErrorSpikeAndExplainsComponentPattern) {
    AnomalyDetector detector(test_options());
    std::vector<LogEntry> entries;
    for (std::int64_t window = 0; window < 3; ++window) {
        entries.push_back(make_entry(window, Severity::Info, "Disk"));
        entries.push_back(make_entry(window, Severity::Error, "Disk"));
        entries.push_back(make_entry(window, Severity::Info, "Network"));
        entries.push_back(make_entry(window, Severity::Error, "Network"));
    }
    entries.push_back(make_entry(3, Severity::Info, "Disk"));
    entries.push_back(make_entry(3, Severity::Info, "Network"));
    for (int count = 0; count < 5; ++count) {
        entries.push_back(make_entry(3, Severity::Error, "Disk"));
    }
    entries.push_back(make_entry(3, Severity::Error, "Network"));

    const auto reports = detector.consume(entries);
    ASSERT_EQ(reports.size(), 1);
    EXPECT_EQ(reports[0].component, "Disk");
    EXPECT_EQ(reports[0].observed_errors, 5);
    EXPECT_EQ(reports[0].severity, AnomalySeverity::Low);
    EXPECT_NEAR(reports[0].rolling_average, 1.0, 1e-9);
    EXPECT_NEAR(reports[0].standard_deviation, 0.0, 1e-9);
    EXPECT_NE(reports[0].explanation.find("Disk"), std::string::npos);
    EXPECT_NE(reports[0].explanation.find("threshold"), std::string::npos);
}

TEST(AnomalyDetector, UsesStrictThresholdBoundary) {
    auto options = test_options();
    options.minimum_error_count = 1;
    options.detection_standard_deviations = 1.0;
    options.medium_severity_standard_deviations = 1.5;
    options.high_severity_standard_deviations = 2.0;
    options.critical_severity_standard_deviations = 3.0;
    AnomalyDetector detector(options);

    std::vector<LogEntry> history{
        make_entry(0, Severity::Info),
        make_entry(1, Severity::Error),
        make_entry(1, Severity::Error),
        make_entry(2, Severity::Error),
        make_entry(2, Severity::Error),
        make_entry(2, Severity::Error),
        make_entry(2, Severity::Error),
        make_entry(3, Severity::Info),
        make_entry(3, Severity::Error),
        make_entry(3, Severity::Error),
        make_entry(3, Severity::Error)};
    EXPECT_TRUE(detector.consume(history).empty());

    const auto reports = detector.consume(
        std::vector<LogEntry>{make_entry(3, Severity::Error)});
    ASSERT_EQ(reports.size(), 1);
    EXPECT_EQ(reports[0].observed_errors, 4);
    EXPECT_EQ(reports[0].severity, AnomalySeverity::Low);
    EXPECT_NEAR(reports[0].rolling_average, 2.0, 1e-9);
    EXPECT_NEAR(reports[0].standard_deviation, std::sqrt(8.0 / 3.0), 1e-9);
    EXPECT_NEAR(
        reports[0].detection_threshold,
        2.0 + std::sqrt(8.0 / 3.0),
        1e-9);
}

TEST(AnomalyDetector, AssignsSeverityFromDeviationScore) {
    auto options = test_options();
    options.minimum_error_count = 1;
    options.detection_standard_deviations = 1.0;
    options.medium_severity_standard_deviations = 1.5;
    options.high_severity_standard_deviations = 2.0;
    options.critical_severity_standard_deviations = 3.0;
    const std::pair<std::size_t, AnomalySeverity> cases[] = {
        {5, AnomalySeverity::Medium},
        {6, AnomalySeverity::High},
        {7, AnomalySeverity::Critical}};

    for (const auto& [current_errors, expected_severity] : cases) {
        AnomalyDetector detector(options);
        std::vector<LogEntry> entries{
            make_entry(0, Severity::Info),
            make_entry(1, Severity::Error),
            make_entry(1, Severity::Error),
            make_entry(2, Severity::Error),
            make_entry(2, Severity::Error),
            make_entry(2, Severity::Error),
            make_entry(2, Severity::Error),
            make_entry(3, Severity::Info)};
        for (std::size_t count = 0; count < current_errors; ++count) {
            entries.push_back(make_entry(3, Severity::Error));
        }

        const auto reports = detector.consume(entries);
        ASSERT_EQ(reports.size(), 1);
        EXPECT_EQ(reports[0].severity, expected_severity);
    }
}

TEST(AnomalyDetector, WaitsForSufficientHistoricalWindows) {
    AnomalyDetector detector(test_options());
    std::vector<LogEntry> entries{
        make_entry(0, Severity::Info),
        make_entry(0, Severity::Error),
        make_entry(1, Severity::Error),
        make_entry(1, Severity::Error),
        make_entry(1, Severity::Error),
        make_entry(1, Severity::Error),
        make_entry(2, Severity::Error),
        make_entry(2, Severity::Error),
        make_entry(2, Severity::Error),
        make_entry(2, Severity::Error),
        make_entry(2, Severity::Error)};

    EXPECT_TRUE(detector.consume(entries).empty());
}

TEST(AnomalyDetector, ConsumesTheSameParsedBatchAsLogAnalyzer) {
    AnomalyDetector detector(test_options());
    sentinelx::log_analyzer::LogAnalyzer analyzer;
    sentinelx::log_parser::BatchParseResult batch;
    batch.entries = {
        make_entry(0, Severity::Info),
        make_entry(1, Severity::Info),
        make_entry(2, Severity::Info),
        make_entry(3, Severity::Info),
        make_entry(3, Severity::Error),
        make_entry(3, Severity::Error),
        make_entry(3, Severity::Error)};

    analyzer.consume(batch);
    const auto reports = detector.consume(batch);
    EXPECT_EQ(analyzer.summary().total_records, batch.entries.size());
    ASSERT_EQ(reports.size(), 1);
    EXPECT_EQ(reports[0].component, "Storage");
}

} // namespace