#include <sentinelx/file_reader/FileReader.h>

#include <utility>

namespace sentinelx::file_reader {

FileReader::FileReader(std::filesystem::path path, FileReaderOptions options)
    : path_(std::move(path)), options_(options) {
    if (options_.batch_size == 0) {
        error_ = "Batch size must be greater than zero";
        return;
    }
    if (options_.buffer_size == 0) {
        error_ = "Buffer size must be greater than zero";
        return;
    }
    if (options_.max_line_bytes == 0) {
        error_ = "Maximum line size must be greater than zero";
        return;
    }

    io_buffer_.resize(options_.buffer_size);
    input_.rdbuf()->pubsetbuf(
        io_buffer_.data(), static_cast<std::streamsize>(io_buffer_.size()));
    input_.open(path_, std::ios::in | std::ios::binary);
    if (!input_.is_open()) {
        error_ = "Unable to open file: " + path_.string();
    }
}

ReadBatch FileReader::read_next_batch() {
    ReadBatch batch;
    if (!error_.empty()) {
        batch.status = ReadStatus::Error;
        batch.error = error_;
        return batch;
    }
    if (eof_) {
        batch.status = ReadStatus::EndOfFile;
        return batch;
    }

    batch.lines.reserve(options_.batch_size);
    std::string line;
    while (batch.lines.size() < options_.batch_size) {
        const auto line_status = read_line(line);
        if (line_status == LineStatus::Line) {
            batch.lines.push_back(std::move(line));
            line.clear();
        } else if (line_status == LineStatus::Malformed) {
            ++batch.malformed_lines;
        } else if (line_status == LineStatus::EndOfFile) {
            eof_ = true;
            break;
        } else {
            error_ = "Error while reading file: " + path_.string();
            batch.status = ReadStatus::Error;
            batch.error = error_;
            return batch;
        }
    }

    batch.status = batch.lines.empty() && batch.malformed_lines == 0
        ? ReadStatus::EndOfFile
        : ReadStatus::Batch;
    return batch;
}

FileReader::LineStatus FileReader::read_line(std::string& line) {
    bool malformed = false;
    char character = '\0';

    while (input_.get(character)) {
        if (character == '\n') {
            if (malformed) {
                return LineStatus::Malformed;
            }
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            return LineStatus::Line;
        }

        if (!malformed) {
            if (character == '\0' || line.size() >= options_.max_line_bytes) {
                malformed = true;
                line.clear();
            } else {
                line.push_back(character);
            }
        }
    }

    if (input_.bad() || (!input_.eof() && input_.fail())) {
        return LineStatus::Error;
    }
    if (input_.eof()) {
        if (malformed) {
            return LineStatus::Malformed;
        }
        if (!line.empty()) {
            if (line.back() == '\r') {
                line.pop_back();
            }
            return LineStatus::Line;
        }
    }
    return LineStatus::EndOfFile;
}

} // namespace sentinelx::file_reader