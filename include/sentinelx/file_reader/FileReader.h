#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace sentinelx::file_reader {

enum class ReadStatus {
    Batch,
    EndOfFile,
    Error
};

struct ReadBatch {
    ReadStatus status{ReadStatus::EndOfFile};
    std::vector<std::string> lines;
    std::size_t malformed_lines{0};
    std::string error;
    // Assigned by producers that need to restore file order after parallel processing.
    std::uint64_t sequence{0};
    // Set by pipelines that measure how long a batch takes after it has been read.
    std::chrono::steady_clock::time_point read_time{};
};

struct FileReaderOptions {
    std::size_t batch_size{256};
    std::size_t buffer_size{64 * 1024};
    std::size_t max_line_bytes{1024 * 1024};
};

class FileReader {
public:
    explicit FileReader(
        std::filesystem::path path,
        FileReaderOptions options = {});

    ReadBatch read_next_batch();

private:
    enum class LineStatus {
        Line,
        Malformed,
        EndOfFile,
        Error
    };

    LineStatus read_line(std::string& line);

    std::filesystem::path path_;
    FileReaderOptions options_;
    std::vector<char> io_buffer_;
    std::ifstream input_;
    bool eof_{false};
    std::string error_;
};

} // namespace sentinelx::file_reader