#include <sentinelx/file_reader/FileReader.h>
#include <sentinelx/log_parser/LogParser.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>

namespace {

using sentinelx::log_parser::LogParser;
using sentinelx::log_parser::ParseError;
using sentinelx::log_parser::Severity;

void require(bool condition, std::string_view message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

std::filesystem::path make_temp_path() {
    const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
        ("sentinelx_log_parser_" + std::to_string(ticks) + ".log");
}

void test_valid_entries() {
    struct Example {
        std::string_view line;
        Severity severity;
    };
    const Example examples[] = {
        {"2026-11-12 08:15:30 INFO FileReader: opened input", Severity::Info},
        {"2026-11-12 08:15:30 WARNING Queue: backlog is growing", Severity::Warning},
        {"2026-11-12 08:15:30 ERROR Analyzer: record rejected", Severity::Error},
        {"2026-11-12 08:15:30 DEBUG Worker: task started", Severity::Debug}};

    for (const auto& example : examples) {
        const auto result = LogParser::parse_line(example.line);
        require(result.entry.has_value(), "valid entry should parse");
        require(result.entry->severity == example.severity,
                "severity should map to its enum value");
        require(result.entry->component.size() > 0,
                "component should be extracted");
        require(result.entry->message.size() > 0,
                "message should be extracted");
        require(result.entry->timestamp ==
                    std::chrono::sys_days{std::chrono::year{2026} /
                                          std::chrono::November / 12} +
                        std::chrono::hours{8} + std::chrono::minutes{15} +
                        std::chrono::seconds{30},
                "timestamp should be converted to a validated time point");
    }
}

void test_invalid_entries() {
    auto result = LogParser::parse_line(
        "2026-02-30 08:15:30 INFO Core: invalid calendar date");
    require(result.error == ParseError::InvalidTimestamp,
            "impossible calendar date should be rejected");

    result = LogParser::parse_line(
        "2026-11-12 08:15:30 TRACE Core: unsupported severity");
    require(result.error == ParseError::UnsupportedSeverity,
            "unsupported severity should be rejected");

    result = LogParser::parse_line(
        "2026-11-12 -1:15:30 INFO Core: negative hour");
    require(result.error == ParseError::InvalidTimestamp,
            "signed timestamp fields should be rejected");

    std::string nul_entry = "2026-11-12 08:15:30 INFO Core: bad";
    nul_entry.push_back('\0');
    nul_entry += "message";
    result = LogParser::parse_line(nul_entry);
    require(result.error == ParseError::UnexpectedFormat,
            "NUL-containing entry should be rejected");
}

void test_missing_fields() {
    require(LogParser::parse_line("").error == ParseError::MissingTimestamp,
            "empty entry should report missing timestamp");
    require(LogParser::parse_line("2026-11-12 08:15:30").error ==
                ParseError::MissingSeverity,
            "timestamp-only entry should report missing severity");
    require(LogParser::parse_line("2026-11-12 08:15:30 INFO").error ==
                ParseError::MissingComponent,
            "entry without component should be rejected");
    require(LogParser::parse_line("2026-11-12 08:15:30 INFO Core:").error ==
                ParseError::MissingMessage,
            "entry without message should be rejected");
}

void test_unexpected_format() {
    const auto result = LogParser::parse_line(
        "2026-11-12 08:15:30 INFO Core message without delimiter");
    require(result.error == ParseError::UnexpectedFormat,
            "unexpected component/message delimiter should be rejected");
}

void test_file_reader_batch_integration() {
    const auto path = make_temp_path();
    {
        std::ofstream output(path, std::ios::binary);
        require(output.is_open(), "integration fixture should open");
        output << "2026-11-12 08:15:30 INFO Reader: first\n"
                  "not a valid entry\n"
                  "2026-11-12 08:15:31 ERROR Parser: third\n";
        require(output.good(), "integration fixture should be written");
    }

    {
        sentinelx::file_reader::FileReader reader(
            path, {.batch_size = 2, .buffer_size = 32, .max_line_bytes = 256});
        auto read_batch = reader.read_next_batch();
        auto parsed_batch = LogParser::parse_batch(read_batch.lines);
        require(parsed_batch.entries.size() == 1 && parsed_batch.issues.size() == 1,
            "parser should accept valid lines and report invalid lines in a reader batch");
        require(parsed_batch.issues[0].line_index == 1,
            "parse issue should identify its position in the batch");

        read_batch = reader.read_next_batch();
        parsed_batch = LogParser::parse_batch(read_batch.lines);
        require(parsed_batch.entries.size() == 1 && parsed_batch.issues.empty(),
            "parser should handle the next FileReader batch");
    }
    std::filesystem::remove(path);
}

} // namespace

int main() {
    test_valid_entries();
    test_invalid_entries();
    test_missing_fields();
    test_unexpected_format();
    test_file_reader_batch_integration();
    std::cout << "LogParser tests passed\n";
    return 0;
}