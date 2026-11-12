#include <sentinelx/file_reader/FileReader.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

std::filesystem::path make_temp_path(const std::string& suffix) {
    const auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
        ("sentinelx_file_reader_" + std::to_string(ticks) + suffix);
}

void write_file(const std::filesystem::path& path, const std::string& contents) {
    std::ofstream output(path, std::ios::binary);
    require(output.is_open(), "temporary file should open for writing");
    output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    require(output.good(), "temporary file should be written");
}

void test_batches_and_end_of_file() {
    const auto path = make_temp_path("_batches.log");
    write_file(path, "first\r\nsecond\nthird");

    {
        sentinelx::file_reader::FileReader reader(
            path, {2, 8, 64});
        auto batch = reader.read_next_batch();
        require(batch.status == sentinelx::file_reader::ReadStatus::Batch,
            "first read should return a batch");
        require(batch.lines.size() == 2, "first batch should contain two lines");
        require(batch.lines[0] == "first" && batch.lines[1] == "second",
            "CRLF should be normalized and line contents preserved");

        batch = reader.read_next_batch();
        require(batch.status == sentinelx::file_reader::ReadStatus::Batch,
            "second read should return remaining line");
        require(batch.lines.size() == 1 && batch.lines[0] == "third",
            "unterminated final line should be returned");

        batch = reader.read_next_batch();
        require(batch.status == sentinelx::file_reader::ReadStatus::EndOfFile,
            "read after final batch should report EOF");
    }
    std::filesystem::remove(path);
}

void test_empty_and_missing_files() {
    const auto empty_path = make_temp_path("_empty.log");
    write_file(empty_path, "");
    {
        sentinelx::file_reader::FileReader empty_reader(empty_path);
        require(empty_reader.read_next_batch().status ==
                    sentinelx::file_reader::ReadStatus::EndOfFile,
                "empty file should report EOF");
    }
    std::filesystem::remove(empty_path);

    const auto missing_path = make_temp_path("_missing.log");
    sentinelx::file_reader::FileReader missing_reader(missing_path);
    const auto missing_batch = missing_reader.read_next_batch();
    require(missing_batch.status == sentinelx::file_reader::ReadStatus::Error,
            "missing file should report an error");
    require(!missing_batch.error.empty(), "open error should include a message");
}

void test_malformed_lines_are_skipped() {
    const auto path = make_temp_path("_malformed.log");
    const std::string contents = std::string("good\nlonger\n", 12) +
        std::string("bad\0line\n", 9) + "last";
    write_file(path, contents);

    {
        sentinelx::file_reader::FileReader reader(
            path, {2, 8, 4});
        const auto batch = reader.read_next_batch();
        require(batch.status == sentinelx::file_reader::ReadStatus::Batch,
                "valid line should still be returned when malformed lines exist");
        require(batch.lines.size() == 2 && batch.lines[0] == "good" &&
                    batch.lines[1] == "last",
                "oversized and NUL-containing lines should be skipped");
        require(batch.malformed_lines == 2,
                "batch should count both malformed lines");
    }
    std::filesystem::remove(path);
}

void test_malformed_only_and_trailing_lines_are_counted() {
    const auto path = make_temp_path("_malformed_tail.log");
    write_file(path, "good\nlonger");

    {
        sentinelx::file_reader::FileReader reader(path, {1, 8, 4});
        const auto valid_batch = reader.read_next_batch();
        require(valid_batch.status == sentinelx::file_reader::ReadStatus::Batch &&
                    valid_batch.lines.size() == 1,
                "valid record should be returned before malformed tail");

        const auto malformed_batch = reader.read_next_batch();
        require(malformed_batch.status == sentinelx::file_reader::ReadStatus::Batch,
                "malformed-only batch should be observable before EOF");
        require(malformed_batch.lines.empty() && malformed_batch.malformed_lines == 1,
                "malformed-only batch should preserve its rejected-line count");
        require(reader.read_next_batch().status ==
                    sentinelx::file_reader::ReadStatus::EndOfFile,
                "EOF should follow the malformed-only batch");
    }
    std::filesystem::remove(path);
}

} // namespace

int main() {
    test_batches_and_end_of_file();
    test_empty_and_missing_files();
    test_malformed_lines_are_skipped();
    test_malformed_only_and_trailing_lines_are_counted();
    std::cout << "FileReader tests passed\n";
    return 0;
}