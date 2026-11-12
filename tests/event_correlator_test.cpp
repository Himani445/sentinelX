#include <sentinelx/event_correlator/EventCorrelator.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace {

using sentinelx::event_correlator::CorrelationType;
using sentinelx::event_correlator::CorrelatorOptions;
using sentinelx::event_correlator::EventCorrelator;
using sentinelx::event_correlator::EventRecord;
using sentinelx::log_parser::Severity;

EventRecord event(
    std::int64_t seconds,
    Severity severity,
    std::string component,
    std::string message) {
    return {
        std::chrono::sys_seconds{std::chrono::seconds{seconds}},
        severity,
        std::move(component),
        std::move(message)};
}

EventRecord event_without_time(
    Severity severity,
    std::string component,
    std::string message) {
    return {std::nullopt, severity, std::move(component), std::move(message)};
}

TEST(EventCorrelator, GroupsEventsAndBuildsChronologicalTimelines) {
    EventCorrelator correlator;
    const std::vector<EventRecord> events{
        event(20, Severity::Error, "Disk", "read failed"),
        event(10, Severity::Info, "Disk", "starting"),
        event(5, Severity::Warning, "Network", "latency high")};

    const auto result = correlator.correlate(events);
    ASSERT_EQ(result.timelines.size(), 2);
    EXPECT_EQ(result.timelines[0].component, "Disk");
    ASSERT_EQ(result.timelines[0].events.size(), 2);
    EXPECT_EQ(result.timelines[0].events[0].message, "starting");
    EXPECT_EQ(result.timelines[0].events[1].message, "read failed");
    EXPECT_EQ(result.timelines[1].component, "Network");
    EXPECT_EQ(result.out_of_order_events, 1);
}

TEST(EventCorrelator, IncludesExactWindowBoundaryAndExcludesOutsideEvents) {
    EventCorrelator correlator(CorrelatorOptions{
        .time_window = std::chrono::seconds{60}});
    const std::vector<EventRecord> events{
        event(0, Severity::Warning, "Storage", "latency increased"),
        event(60, Severity::Error, "Storage", "request timed out"),
        event(61, Severity::Error, "Storage", "connection reset")};

    const auto result = correlator.correlate(events);
    ASSERT_EQ(result.reports.size(), 1);
    EXPECT_EQ(result.reports[0].type, CorrelationType::WarningBeforeError);
    EXPECT_EQ(result.reports[0].relevance_score, 0.0);
    EXPECT_EQ(result.reports[0].rank, 1);
    EXPECT_NE(result.reports[0].explanation.find("Possible correlation"),
              std::string::npos);
    EXPECT_EQ(result.reports[0].events[1].message, "request timed out");
}

TEST(EventCorrelator, IdentifiesRepeatedErrorMessagesAndSequences) {
    EventCorrelator correlator(CorrelatorOptions{
        .time_window = std::chrono::seconds{60}});
    const std::vector<EventRecord> events{
        event(1, Severity::Error, "Disk", "device unavailable"),
        event(2, Severity::Error, "Disk", "retry failed"),
        event(3, Severity::Error, "Disk", "device unavailable"),
        event(4, Severity::Error, "Disk", "retry failed")};

    const auto result = correlator.correlate(events);
    bool found_repeated_message = false;
    bool found_repeated_sequence = false;
    for (const auto& report : result.reports) {
        found_repeated_message |=
            report.type == CorrelationType::RepeatedErrorMessage;
        if (report.type == CorrelationType::RepeatedErrorSequence) {
            found_repeated_sequence = true;
            ASSERT_EQ(report.events.size(), 4);
            EXPECT_EQ(report.events[0].message, "device unavailable");
            EXPECT_EQ(report.events[1].message, "retry failed");
            EXPECT_EQ(report.events[2].message, "device unavailable");
            EXPECT_EQ(report.events[3].message, "retry failed");
        }
        EXPECT_NE(report.explanation.find("Possible correlation"), std::string::npos);
    }
    EXPECT_TRUE(found_repeated_message);
    EXPECT_TRUE(found_repeated_sequence);
}

TEST(EventCorrelator, KeepsMissingTimestampsAndOrdersOutOfOrderEvents) {
    EventCorrelator correlator(CorrelatorOptions{
        .time_window = std::chrono::seconds{60}});
    const std::vector<EventRecord> events{
        event(30, Severity::Error, "Queue", "overflow"),
        event_without_time(Severity::Warning, "Queue", "capacity rising"),
        event(0, Severity::Warning, "Queue", "backlog growing"),
        event(31, Severity::Error, "Queue", "overflow")};

    const auto result = correlator.correlate(events);
    ASSERT_EQ(result.timelines.size(), 1);
    EXPECT_EQ(result.timelines[0].events.size(), 4);
    EXPECT_EQ(result.missing_timestamp_events, 1);
    EXPECT_EQ(result.out_of_order_events, 1);
    EXPECT_TRUE(result.timelines[0].events.back().timestamp == std::nullopt);
    for (const auto& report : result.reports) {
        for (const auto& related_event : report.events) {
            EXPECT_TRUE(related_event.timestamp.has_value());
        }
    }
}

} // namespace