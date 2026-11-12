#include <sentinelx/event_correlator/EventCorrelator.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace sentinelx::event_correlator {
namespace {

struct ErrorSequenceKey {
    std::string first;
    std::string second;

    bool operator==(const ErrorSequenceKey&) const = default;
};

struct ErrorSequenceHash {
    std::size_t operator()(const ErrorSequenceKey& sequence) const noexcept {
        const auto first_hash = std::hash<std::string>{}(sequence.first);
        const auto second_hash = std::hash<std::string>{}(sequence.second);
        return first_hash ^ (second_hash + 0x9e3779b9U +
                             (first_hash << 6U) + (first_hash >> 2U));
    }
};

struct SequenceOccurrence {
    std::size_t first_event;
    std::size_t second_event;
    std::chrono::sys_seconds end_time;
};

double temporal_score(
    std::chrono::seconds distance,
    std::chrono::seconds window) {
    return 1.0 - static_cast<double>(distance.count()) /
        static_cast<double>(window.count());
}

} // namespace

EventCorrelator::EventCorrelator(CorrelatorOptions options)
    : options_(options) {
    if (options_.time_window <= std::chrono::seconds::zero()) {
        throw std::invalid_argument("Correlation time window must be positive");
    }
}

CorrelationResult EventCorrelator::correlate(
    std::span<const EventRecord> events) const {
    CorrelationResult result;
    std::unordered_map<std::string, std::vector<EventRecord>> grouped;
    std::unordered_map<std::string, std::chrono::sys_seconds> last_timestamps;
    grouped.reserve(events.size());
    last_timestamps.reserve(events.size());

    for (const auto& event : events) {
        auto& component_events = grouped[event.component];
        component_events.push_back(event);
        if (!event.timestamp) {
            ++result.missing_timestamp_events;
            continue;
        }
        const auto last = last_timestamps.find(event.component);
        if (last != last_timestamps.end() && *event.timestamp < last->second) {
            ++result.out_of_order_events;
        }
        if (last == last_timestamps.end() || *event.timestamp > last->second) {
            last_timestamps[event.component] = *event.timestamp;
        }
    }

    result.timelines.reserve(grouped.size());
    for (auto& [component, component_events] : grouped) {
        std::stable_sort(component_events.begin(), component_events.end(),
            [](const EventRecord& left, const EventRecord& right) {
                if (!left.timestamp) {
                    return false;
                }
                if (!right.timestamp) {
                    return true;
                }
                return *left.timestamp < *right.timestamp;
            });

        std::optional<std::size_t> latest_warning;
        std::unordered_map<std::string, std::size_t> latest_error_by_message;
        std::unordered_map<ErrorSequenceKey, SequenceOccurrence, ErrorSequenceHash>
            latest_error_sequences;
        std::optional<std::size_t> previous_error;

        for (std::size_t index = 0; index < component_events.size(); ++index) {
            const auto& event = component_events[index];
            if (!event.timestamp) {
                continue;
            }

            if (event.severity == log_parser::Severity::Warning) {
                latest_warning = index;
                continue;
            }
            if (event.severity != log_parser::Severity::Error) {
                continue;
            }

            if (latest_warning) {
                const auto& warning = component_events[*latest_warning];
                const auto distance = std::chrono::duration_cast<std::chrono::seconds>(
                    *event.timestamp - *warning.timestamp);
                if (distance <= options_.time_window) {
                    const auto score = temporal_score(distance, options_.time_window);
                    result.reports.push_back({
                        CorrelationType::WarningBeforeError,
                        component,
                        {warning, event},
                        score,
                        0,
                        "Possible correlation: a WARNING from component '" + component +
                            "' occurred " + std::to_string(distance.count()) +
                            " seconds before an ERROR, within the configured window. "
                            "This is temporal proximity, not proof of cause."});
                }
            }

            const auto previous_message = latest_error_by_message.find(event.message);
            if (previous_message != latest_error_by_message.end()) {
                const auto& previous = component_events[previous_message->second];
                const auto distance = std::chrono::duration_cast<std::chrono::seconds>(
                    *event.timestamp - *previous.timestamp);
                if (distance <= options_.time_window) {
                    const auto score = temporal_score(distance, options_.time_window);
                    result.reports.push_back({
                        CorrelationType::RepeatedErrorMessage,
                        component,
                        {previous, event},
                        score,
                        0,
                        "Possible correlation: the same ERROR message repeated in component '" +
                            component + "' within " + std::to_string(distance.count()) +
                            " seconds. Repetition alone does not establish a cause."});
                }
            }
            latest_error_by_message[event.message] = index;

            if (previous_error) {
                const auto& previous = component_events[*previous_error];
                const auto distance = std::chrono::duration_cast<std::chrono::seconds>(
                    *event.timestamp - *previous.timestamp);
                if (distance <= options_.time_window) {
                    const ErrorSequenceKey key{previous.message, event.message};
                    const auto previous_sequence = latest_error_sequences.find(key);
                    if (previous_sequence != latest_error_sequences.end()) {
                        const auto sequence_distance =
                            std::chrono::duration_cast<std::chrono::seconds>(
                                *event.timestamp - previous_sequence->second.end_time);
                        if (sequence_distance <= options_.time_window) {
                            const auto score = temporal_score(
                                sequence_distance, options_.time_window);
                            std::vector<EventRecord> sequence_events;
                            sequence_events.reserve(4);
                            std::optional<std::size_t> last_sequence_index;
                            for (const auto sequence_index : {
                                     previous_sequence->second.first_event,
                                     previous_sequence->second.second_event,
                                     *previous_error,
                                     index}) {
                                if (!last_sequence_index ||
                                    sequence_index != *last_sequence_index) {
                                    sequence_events.push_back(
                                        component_events[sequence_index]);
                                    last_sequence_index = sequence_index;
                                }
                            }
                            result.reports.push_back({
                                CorrelationType::RepeatedErrorSequence,
                                component,
                                std::move(sequence_events),
                                score,
                                0,
                                "Possible correlation: the consecutive ERROR message sequence '" +
                                    key.first + "' then '" + key.second +
                                    "' repeated within the configured window. This pattern "
                                    "does not establish a root cause."});
                        }
                    }
                    latest_error_sequences[key] = {
                        *previous_error, index, *event.timestamp};
                }
            }
            previous_error = index;
        }

        result.timelines.push_back({component, std::move(component_events)});
    }

    std::sort(result.timelines.begin(), result.timelines.end(),
        [](const ComponentTimeline& left, const ComponentTimeline& right) {
            return left.component < right.component;
        });
    std::stable_sort(result.reports.begin(), result.reports.end(),
        [](const CorrelationReport& left, const CorrelationReport& right) {
            if (left.relevance_score != right.relevance_score) {
                return left.relevance_score > right.relevance_score;
            }
            if (left.component != right.component) {
                return left.component < right.component;
            }
            return left.type < right.type;
        });
    for (std::size_t index = 0; index < result.reports.size(); ++index) {
        result.reports[index].rank = index + 1;
    }
    return result;
}

CorrelationResult EventCorrelator::correlate(
    std::span<const log_parser::LogEntry> entries) const {
    std::vector<EventRecord> events;
    events.reserve(entries.size());
    for (const auto& entry : entries) {
        events.push_back({entry.timestamp, entry.severity, entry.component, entry.message});
    }
    return correlate(events);
}

} // namespace sentinelx::event_correlator