#include <sentinelx/log_parser/LogParser.h>

#include <charconv>
#include <chrono>
#include <string>
#include <system_error>
#include <utility>

namespace sentinelx::log_parser {
namespace {

bool parse_integer(std::string_view text, int& value) {
    for (const char character : text) {
        if (character < '0' || character > '9') {
            return false;
        }
    }
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

bool parse_timestamp(std::string_view text, std::chrono::sys_seconds& timestamp) {
    if (text.size() < 19 || text[4] != '-' || text[7] != '-' ||
        text[10] != ' ' || text[13] != ':' || text[16] != ':') {
        return false;
    }

    int year_value = 0;
    int month_value = 0;
    int day_value = 0;
    int hour_value = 0;
    int minute_value = 0;
    int second_value = 0;
    if (!parse_integer(text.substr(0, 4), year_value) ||
        !parse_integer(text.substr(5, 2), month_value) ||
        !parse_integer(text.substr(8, 2), day_value) ||
        !parse_integer(text.substr(11, 2), hour_value) ||
        !parse_integer(text.substr(14, 2), minute_value) ||
        !parse_integer(text.substr(17, 2), second_value)) {
        return false;
    }

    const std::chrono::year_month_day date{
        std::chrono::year{year_value},
        std::chrono::month{static_cast<unsigned>(month_value)},
        std::chrono::day{static_cast<unsigned>(day_value)}};
    if (year_value == 0 || !date.ok() || hour_value > 23 ||
        minute_value > 59 || second_value > 59) {
        return false;
    }

    timestamp = std::chrono::sys_days{date} + std::chrono::hours{hour_value} +
        std::chrono::minutes{minute_value} + std::chrono::seconds{second_value};
    return true;
}

bool parse_severity(std::string_view text, Severity& severity) {
    if (text == "INFO") {
        severity = Severity::Info;
    } else if (text == "WARNING") {
        severity = Severity::Warning;
    } else if (text == "ERROR") {
        severity = Severity::Error;
    } else if (text == "DEBUG") {
        severity = Severity::Debug;
    } else {
        return false;
    }
    return true;
}

bool is_valid_component(std::string_view component) {
    if (component.empty()) {
        return false;
    }
    for (const unsigned char character : component) {
        if (character <= 0x20 || character == 0x7f || character == ':') {
            return false;
        }
    }
    return true;
}

} // namespace

ParseResult LogParser::parse_line(std::string_view line) {
    if (line.find('\0') != std::string_view::npos) {
        return {std::nullopt, ParseError::UnexpectedFormat};
    }
    if (line.size() < 19) {
        return {std::nullopt, ParseError::MissingTimestamp};
    }

    std::chrono::sys_seconds timestamp;
    if (!parse_timestamp(line.substr(0, 19), timestamp)) {
        return {std::nullopt, ParseError::InvalidTimestamp};
    }
    if (line.size() == 19) {
        return {std::nullopt, ParseError::MissingSeverity};
    }
    if (line[19] != ' ') {
        return {std::nullopt, ParseError::UnexpectedFormat};
    }

    const auto fields = line.substr(20);
    if (fields.empty()) {
        return {std::nullopt, ParseError::MissingSeverity};
    }
    const auto severity_end = fields.find(' ');
    const auto severity_text = fields.substr(0, severity_end);
    Severity severity{};
    if (!parse_severity(severity_text, severity)) {
        return {std::nullopt, ParseError::UnsupportedSeverity};
    }
    if (severity_end == std::string_view::npos || severity_end + 1 == fields.size()) {
        return {std::nullopt, ParseError::MissingComponent};
    }

    const auto component_and_message = fields.substr(severity_end + 1);
    const auto separator = component_and_message.find(':');
    if (separator == std::string_view::npos) {
        return {std::nullopt, ParseError::UnexpectedFormat};
    }
    const auto component = component_and_message.substr(0, separator);
    if (component.empty()) {
        return {std::nullopt, ParseError::MissingComponent};
    }
    if (!is_valid_component(component)) {
        return {std::nullopt, ParseError::UnexpectedFormat};
    }
    if (separator + 1 == component_and_message.size()) {
        return {std::nullopt, ParseError::MissingMessage};
    }
    if (component_and_message[separator + 1] != ' ') {
        return {std::nullopt, ParseError::UnexpectedFormat};
    }

    const auto message = component_and_message.substr(separator + 2);
    if (message.find_first_not_of(" \t") == std::string_view::npos) {
        return {std::nullopt, ParseError::MissingMessage};
    }

    LogEntry entry{
        timestamp,
        severity,
        std::string{component},
        std::string{message}};
    return {std::move(entry), ParseError::None};
}

BatchParseResult LogParser::parse_batch(std::span<const std::string> lines) {
    BatchParseResult result;
    result.entries.reserve(lines.size());

    for (std::size_t index = 0; index < lines.size(); ++index) {
        auto parsed = parse_line(lines[index]);
        if (parsed.entry) {
            result.entries.push_back(std::move(*parsed.entry));
        } else {
            result.issues.push_back({index, parsed.error});
        }
    }
    return result;
}

} // namespace sentinelx::log_parser