#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sentinelx::log_parser {

enum class Severity {
    Info,
    Warning,
    Error,
    Debug
};

struct LogEntry {
    std::chrono::sys_seconds timestamp;
    Severity severity;
    std::string component;
    std::string message;
};

enum class ParseError {
    None,
    MissingTimestamp,
    InvalidTimestamp,
    MissingSeverity,
    UnsupportedSeverity,
    MissingComponent,
    MissingMessage,
    UnexpectedFormat
};

struct ParseResult {
    std::optional<LogEntry> entry;
    ParseError error{ParseError::None};
};

struct ParseIssue {
    std::size_t line_index;
    ParseError error;
};

struct BatchParseResult {
    std::vector<LogEntry> entries;
    std::vector<ParseIssue> issues;
};

class LogParser final {
public:
    static ParseResult parse_line(std::string_view line);
    static BatchParseResult parse_batch(std::span<const std::string> lines);
};

} // namespace sentinelx::log_parser