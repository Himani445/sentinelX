// sentinelx_bench: compares sequential and concurrent runs of the real SentinelX pipeline.
//
// The parent process generates datasets and launches one child process per
// measured run, so each run's peak memory is measured in isolation.

#include <sentinelx/pipeline/LogPipeline.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <numeric>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#if defined(_WIN32)
#define NOMINMAX
#include <windows.h>
#include <psapi.h>
#define popen _popen
#define pclose _pclose
#else
#include <sys/resource.h>
#endif

namespace {

namespace fs = std::filesystem;
using sentinelx::pipeline::PipelineOptions;
using sentinelx::pipeline::PipelineResult;
using sentinelx::pipeline::run_log_pipeline;

constexpr double kBytesPerMb = 1024.0 * 1024.0;
constexpr std::string_view kResultPrefix = "RESULT";

// ---------------------------------------------------------------------------
// Platform helpers

struct MemoryUsage {
    double working_set_mb{0.0};
    double peak_working_set_mb{0.0};
    double peak_commit_mb{0.0};
};

MemoryUsage memory_usage() {
#if defined(_WIN32)
    PROCESS_MEMORY_COUNTERS_EX counters{};
    if (GetProcessMemoryInfo(GetCurrentProcess(),
            reinterpret_cast<PROCESS_MEMORY_COUNTERS*>(&counters), sizeof(counters))) {
        return {counters.WorkingSetSize / kBytesPerMb,
                counters.PeakWorkingSetSize / kBytesPerMb,
                counters.PeakPagefileUsage / kBytesPerMb};
    }
#else
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
#if defined(__APPLE__)
        const double peak = static_cast<double>(usage.ru_maxrss) / kBytesPerMb;
#else
        const double peak = static_cast<double>(usage.ru_maxrss) / 1024.0;
#endif
        return {0.0, peak, 0.0};
    }
#endif
    return {};
}

fs::path executable_path(const char* argv0) {
#if defined(_WIN32)
    std::wstring buffer(MAX_PATH, L'\0');
    for (;;) {
        const DWORD length = GetModuleFileNameW(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
        if (length == 0) {
            break;
        }
        if (length < buffer.size()) {
            buffer.resize(length);
            return fs::path(buffer);
        }
        buffer.resize(buffer.size() * 2);
    }
#elif defined(__linux__)
    std::error_code error;
    if (auto path = fs::read_symlink("/proc/self/exe", error); !error) {
        return path;
    }
#endif
    return fs::absolute(argv0);
}

struct CommandOutput {
    int status{-1};
    std::string text;
};

CommandOutput run_command(const std::string& command) {
#if defined(_WIN32)
    // cmd.exe strips one pair of outer quotes from the command line.
    const std::string shell_command = "\"" + command + "\"";
#else
    const std::string& shell_command = command;
#endif
    CommandOutput output;
    FILE* pipe = popen(shell_command.c_str(), "r");
    if (pipe == nullptr) {
        return output;
    }
    std::array<char, 4096> buffer{};
    while (std::fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        output.text += buffer.data();
    }
    output.status = pclose(pipe);
    return output;
}

std::string quote(const std::string& value) {
    return "\"" + value + "\"";
}

// ---------------------------------------------------------------------------
// Formatting helpers

std::string format_time(std::chrono::sys_seconds time_point) {
    const auto day = std::chrono::floor<std::chrono::days>(time_point);
    const std::chrono::year_month_day date{day};
    const std::chrono::hh_mm_ss time{time_point - day};
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%04d-%02u-%02u %02lld:%02lld:%02lld",
        static_cast<int>(date.year()),
        static_cast<unsigned>(date.month()),
        static_cast<unsigned>(date.day()),
        static_cast<long long>(time.hours().count()),
        static_cast<long long>(time.minutes().count()),
        static_cast<long long>(time.seconds().count()));
    return buffer;
}

std::string size_label(std::size_t records) {
    if (records % 1'000'000 == 0) {
        return std::to_string(records / 1'000'000) + "M";
    }
    if (records % 1'000 == 0) {
        return std::to_string(records / 1'000) + "K";
    }
    return std::to_string(records);
}

std::string fixed(double value, int precision) {
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(precision) << value;
    return stream.str();
}

std::vector<std::size_t> parse_list(std::string_view text) {
    std::vector<std::size_t> values;
    std::size_t start = 0;
    while (start <= text.size()) {
        const auto end = std::min(text.find(',', start), text.size());
        std::string item(text.substr(start, end - start));
        std::size_t multiplier = 1;
        if (!item.empty()) {
            const auto suffix = static_cast<char>(std::tolower(static_cast<unsigned char>(item.back())));
            if (suffix == 'k') {
                multiplier = 1'000;
                item.pop_back();
            } else if (suffix == 'm') {
                multiplier = 1'000'000;
                item.pop_back();
            }
        }
        std::size_t parsed = 0;
        const auto value = item.empty() ? 0 : std::stoull(item, &parsed);
        if (item.empty() || parsed != item.size() || value == 0) {
            throw std::invalid_argument("invalid list value '" + std::string(text) + "'");
        }
        values.push_back(static_cast<std::size_t>(value) * multiplier);
        start = end + 1;
    }
    return values;
}

// ---------------------------------------------------------------------------
// Dataset generation (same log format as scripts/generate_logs.py)

struct ComponentMessages {
    std::string_view name;
    int pid;
    std::array<std::string_view, 3> info;
    std::array<std::string_view, 2> debug;
    std::array<std::string_view, 2> warning;
    std::array<std::string_view, 3> error;
};

constexpr std::array<ComponentMessages, 4> kComponents{{
    {"Database", 2134,
     {"Query executed in", "Rows returned", "Checkpoint completed after"},
     {"Prepared statement cache hit", "Query plan reused"},
     {"Slow query detected", "Connection pool utilization above 80 percent"},
     {"Connection pool exhausted", "Deadlock detected during transaction", "Query timeout exceeded"}},
    {"Network", 3071,
     {"Request served in", "Bytes transferred", "Health check passed in"},
     {"Keep-alive probe sent", "Socket buffer resized"},
     {"Upstream latency above threshold", "Retrying request to upstream"},
     {"Connection refused by upstream", "DNS resolution failed", "Socket read timeout"}},
    {"Memory", 4410,
     {"Garbage collection reclaimed", "Heap usage percent", "Cache entries"},
     {"Page cache lookup completed", "Allocator arena created"},
     {"Heap usage above 85 percent", "Allocation latency increased"},
     {"Out of memory while allocating buffer", "Memory allocation failed", "Cache eviction failed"}},
    {"Authentication", 5268,
     {"User login succeeded for session", "Token refreshed for session", "Session closed after"},
     {"Token signature validated", "Password hash verified"},
     {"Multiple failed login attempts detected", "Identity provider response slow"},
     {"Invalid credentials rejected", "Token validation failed", "Account locked after failed attempts"}},
}};

void generate_dataset(const fs::path& file, std::size_t records, std::uint32_t seed) {
    fs::create_directories(file.parent_path());
    auto temporary = file;
    temporary += ".tmp";
    {
        std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
        if (!output) {
            throw std::runtime_error("cannot create " + temporary.string());
        }
        std::mt19937 rng(seed);
        std::uniform_real_distribution<double> unit(0.0, 1.0);
        std::uniform_int_distribution<int> value(1, 5000);
        auto pick = [&rng](std::size_t count) {
            return std::uniform_int_distribution<std::size_t>(0, count - 1)(rng);
        };

        const std::size_t spike_length = std::max<std::size_t>(200, records / 500);
        const std::array<std::size_t, 3> spike_starts{
            records * 3 / 10, records * 55 / 100, records * 8 / 10};

        std::chrono::sys_seconds timestamp{std::chrono::sys_days{
            std::chrono::year{2026} / std::chrono::January / 1}};
        std::chrono::sys_seconds formatted_second{};
        std::string timestamp_text;
        std::string buffer;
        buffer.reserve(1 << 20);

        for (std::size_t index = 0; index < records; ++index) {
            const double step = unit(rng);
            timestamp += std::chrono::seconds{step < 0.3 ? 0 : step < 0.8 ? 1 : 2};
            if (timestamp_text.empty() || timestamp != formatted_second) {
                timestamp_text = format_time(timestamp);
                formatted_second = timestamp;
            }

            std::size_t spike = spike_starts.size();
            for (std::size_t candidate = 0; candidate < spike_starts.size(); ++candidate) {
                if (index >= spike_starts[candidate] && index < spike_starts[candidate] + spike_length) {
                    spike = candidate;
                }
            }

            const ComponentMessages* component = nullptr;
            std::string_view level;
            std::string_view message;
            bool with_value = false;
            if (spike < spike_starts.size() && unit(rng) < 0.7) {
                component = &kComponents[spike % kComponents.size()];
                level = "ERROR";
                message = component->error[0];
            } else {
                component = &kComponents[pick(kComponents.size())];
                const double roll = unit(rng);
                if (roll < 0.70) {
                    level = "INFO";
                    message = component->info[pick(component->info.size())];
                    with_value = true;
                } else if (roll < 0.85) {
                    level = "DEBUG";
                    message = component->debug[pick(component->debug.size())];
                } else if (roll < 0.95) {
                    level = "WARNING";
                    message = component->warning[pick(component->warning.size())];
                } else {
                    level = "ERROR";
                    message = component->error[pick(component->error.size())];
                }
            }

            buffer += timestamp_text;
            buffer += ' ';
            buffer += level;
            buffer += ' ';
            buffer += component->name;
            buffer += ": pid=";
            buffer += std::to_string(component->pid);
            buffer += ' ';
            buffer += message;
            if (with_value) {
                buffer += ' ';
                buffer += std::to_string(value(rng));
            }
            buffer += '\n';
            if (buffer.size() >= (1 << 20)) {
                output.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
                buffer.clear();
            }
        }
        output.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        if (!output) {
            throw std::runtime_error("failed writing " + temporary.string());
        }
    }
    fs::rename(temporary, file);
}

// ---------------------------------------------------------------------------
// Result fingerprint: proves both modes produced identical analysis output

class Fingerprint {
public:
    void add(std::uint64_t value) {
        for (int shift = 0; shift < 64; shift += 8) {
            mix(static_cast<unsigned char>(value >> shift));
        }
    }
    void add(std::string_view text) {
        add(static_cast<std::uint64_t>(text.size()));
        for (const char character : text) {
            mix(static_cast<unsigned char>(character));
        }
    }
    std::uint64_t value() const { return hash_; }

private:
    void mix(unsigned char byte) {
        hash_ ^= byte;
        hash_ *= 1099511628211ULL;
    }
    std::uint64_t hash_{1469598103934665603ULL};
};

std::uint64_t fingerprint(const PipelineResult& result) {
    Fingerprint print;
    const auto& summary = result.summary;
    print.add(summary.total_records);
    print.add(summary.malformed_records);
    print.add(result.stats.skipped_lines);
    for (const auto& severity : summary.severity_distribution) {
        print.add(severity.count);
    }
    const std::map<std::string, std::size_t> components(
        summary.errors_by_component.begin(), summary.errors_by_component.end());
    for (const auto& [component, count] : components) {
        print.add(component);
        print.add(count);
    }
    for (const auto* list : {&summary.most_frequent_errors, &summary.repeated_error_messages}) {
        for (const auto& error : *list) {
            print.add(error.message);
            print.add(error.count);
        }
    }
    for (const auto& anomaly : result.anomalies) {
        print.add(anomaly.component);
        print.add(static_cast<std::uint64_t>(anomaly.window_start.time_since_epoch().count()));
        print.add(anomaly.observed_errors);
        print.add(static_cast<std::uint64_t>(anomaly.severity));
    }
    for (const auto& event : result.notable_events) {
        print.add(static_cast<std::uint64_t>(event.timestamp ? event.timestamp->time_since_epoch().count() : 0));
        print.add(static_cast<std::uint64_t>(event.severity));
        print.add(event.component);
        print.add(event.message);
    }
    print.add(static_cast<std::uint64_t>(result.correlation_truncated));
    return print.value();
}

// ---------------------------------------------------------------------------
// Child mode: one measured pipeline run

int run_child(const fs::path& file, std::size_t workers, std::size_t batch_size,
              std::size_t expected_records) {
    const auto baseline = memory_usage();
    PipelineOptions options;
    options.batch_size = batch_size;
    options.worker_count = workers;
    const auto result = run_log_pipeline(file, options);
    const auto memory = memory_usage();
    if (!result.ok) {
        std::cerr << "pipeline error: " << result.error << '\n';
        return 1;
    }
    const auto& stats = result.stats;
    if (stats.batches_aggregated != stats.batches_read) {
        std::cerr << "batch accounting mismatch\n";
        return 1;
    }
    if (result.summary.total_records != expected_records ||
        result.summary.malformed_records != 0 || stats.skipped_lines != 0) {
        std::cerr << "dataset validation failed: expected " << expected_records
                  << " valid records, parsed " << result.summary.total_records
                  << ", parser malformed " << result.summary.malformed_records
                  << ", reader skipped " << stats.skipped_lines << '\n';
        return 1;
    }
    std::ostringstream hash;
    hash << std::hex << std::setw(16) << std::setfill('0') << fingerprint(result);
    std::cout << std::fixed << std::setprecision(3) << kResultPrefix
              << " elapsed_ms=" << stats.elapsed.count()
              << " parsed=" << result.summary.total_records
              << " malformed=" << result.summary.malformed_records + stats.skipped_lines
              << " batches=" << stats.batches_read
              << " latency_mean_us=" << stats.batch_latency_mean_us
              << " latency_p50_us=" << stats.batch_latency_p50_us
              << " latency_p95_us=" << stats.batch_latency_p95_us
              << " latency_max_us=" << stats.batch_latency_max_us
              << " baseline_ws_mb=" << baseline.working_set_mb
              << " peak_ws_mb=" << memory.peak_working_set_mb
              << " peak_commit_mb=" << memory.peak_commit_mb
              << " fingerprint=" << hash.str() << '\n';
    return 0;
}

// ---------------------------------------------------------------------------
// Parent mode: orchestration, statistics and reporting

struct Config {
    std::size_t records{0};
    std::size_t workers{0};
    std::size_t batch_size{0};
    auto operator<=>(const Config&) const = default;
};

struct Measurement {
    Config config;
    std::size_t repetition{0};
    double elapsed_ms{0.0};
    std::size_t parsed{0};
    std::size_t malformed{0};
    std::size_t batches{0};
    double latency_mean_us{0.0};
    double latency_p50_us{0.0};
    double latency_p95_us{0.0};
    double latency_max_us{0.0};
    double baseline_ws_mb{0.0};
    double peak_ws_mb{0.0};
    double peak_commit_mb{0.0};
    std::string fingerprint;
};

Measurement launch(const fs::path& exe, const fs::path& file, const Config& config) {
    const auto command = quote(exe.string()) + " --child --file " + quote(file.string()) +
        " --workers " + std::to_string(config.workers) +
        " --batch-size " + std::to_string(config.batch_size) +
        " --records " + std::to_string(config.records);
    const auto output = run_command(command);
    const auto line_start = output.text.find(kResultPrefix);
    if (output.status != 0 || line_start == std::string::npos) {
        throw std::runtime_error("benchmark run failed (" + command + "): " + output.text);
    }
    std::istringstream fields(output.text.substr(line_start + kResultPrefix.size()));
    std::map<std::string, std::string> values;
    for (std::string field; fields >> field;) {
        const auto equals = field.find('=');
        if (equals != std::string::npos) {
            values[field.substr(0, equals)] = field.substr(equals + 1);
        }
    }
    auto number = [&values](const std::string& key) { return std::stod(values.at(key)); };
    Measurement measurement;
    measurement.config = config;
    measurement.elapsed_ms = number("elapsed_ms");
    measurement.parsed = static_cast<std::size_t>(number("parsed"));
    measurement.malformed = static_cast<std::size_t>(number("malformed"));
    measurement.batches = static_cast<std::size_t>(number("batches"));
    measurement.latency_mean_us = number("latency_mean_us");
    measurement.latency_p50_us = number("latency_p50_us");
    measurement.latency_p95_us = number("latency_p95_us");
    measurement.latency_max_us = number("latency_max_us");
    measurement.baseline_ws_mb = number("baseline_ws_mb");
    measurement.peak_ws_mb = number("peak_ws_mb");
    measurement.peak_commit_mb = number("peak_commit_mb");
    measurement.fingerprint = values.at("fingerprint");
    return measurement;
}

double median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    const auto middle = values.size() / 2;
    return values.size() % 2 == 1 ? values[middle] : (values[middle - 1] + values[middle]) / 2.0;
}

double standard_deviation(const std::vector<double>& values) {
    if (values.size() < 2) {
        return 0.0;
    }
    const double mean = std::accumulate(values.begin(), values.end(), 0.0) / static_cast<double>(values.size());
    double squares = 0.0;
    for (const double value : values) {
        squares += (value - mean) * (value - mean);
    }
    return std::sqrt(squares / static_cast<double>(values.size() - 1));
}

struct Summary {
    Config config;
    std::size_t runs{0};
    double median_ms{0.0};
    double mean_ms{0.0};
    double min_ms{0.0};
    double max_ms{0.0};
    double stddev_ms{0.0};
    double median_throughput{0.0};
    double speedup{0.0};
    std::string verdict;
    double median_latency_p50_us{0.0};
    double median_latency_p95_us{0.0};
    double median_peak_ws_mb{0.0};
    double median_peak_commit_mb{0.0};
    double median_baseline_ws_mb{0.0};
    bool results_match{true};
};

std::string mode_name(std::size_t workers) {
    return workers <= 1 ? "sequential" : "concurrent";
}

// AnomalyDetector re-evaluates at batch boundaries, so results are compared per batch size.
using ReferenceKey = std::pair<std::size_t, std::size_t>;
using References = std::map<ReferenceKey, std::string>;

ReferenceKey reference_key(const Config& config) {
    return {config.records, config.batch_size};
}

// "faster"/"slower" only when the two time ranges do not overlap at all.
std::string compare_ranges(const Summary& candidate, const Summary& baseline) {
    if (candidate.max_ms < baseline.min_ms) {
        return "faster";
    }
    if (candidate.min_ms > baseline.max_ms) {
        return "slower";
    }
    return "no clear difference";
}

std::vector<Summary> summarize(const std::vector<Measurement>& measurements,
                               const References& reference_fingerprints) {
    std::map<Config, std::vector<const Measurement*>> grouped;
    for (const auto& measurement : measurements) {
        grouped[measurement.config].push_back(&measurement);
    }
    std::vector<Summary> summaries;
    for (const auto& [config, runs] : grouped) {
        Summary summary;
        summary.config = config;
        summary.runs = runs.size();
        std::vector<double> times;
        std::vector<double> p50;
        std::vector<double> p95;
        std::vector<double> peak_ws;
        std::vector<double> peak_commit;
        std::vector<double> baseline;
        for (const auto* run : runs) {
            times.push_back(run->elapsed_ms);
            p50.push_back(run->latency_p50_us);
            p95.push_back(run->latency_p95_us);
            peak_ws.push_back(run->peak_ws_mb);
            peak_commit.push_back(run->peak_commit_mb);
            baseline.push_back(run->baseline_ws_mb);
            summary.results_match = summary.results_match &&
                run->fingerprint == reference_fingerprints.at(reference_key(config));
        }
        summary.median_ms = median(times);
        summary.mean_ms = std::accumulate(times.begin(), times.end(), 0.0) / static_cast<double>(times.size());
        summary.min_ms = *std::min_element(times.begin(), times.end());
        summary.max_ms = *std::max_element(times.begin(), times.end());
        summary.stddev_ms = standard_deviation(times);
        summary.median_throughput = static_cast<double>(config.records) / (summary.median_ms / 1000.0);
        summary.median_latency_p50_us = median(p50);
        summary.median_latency_p95_us = median(p95);
        summary.median_peak_ws_mb = median(peak_ws);
        summary.median_peak_commit_mb = median(peak_commit);
        summary.median_baseline_ws_mb = median(baseline);
        summaries.push_back(summary);
    }
    for (auto& summary : summaries) {
        if (summary.config.workers <= 1) {
            summary.speedup = 1.0;
            summary.verdict = "baseline";
            continue;
        }
        const auto baseline = std::find_if(summaries.begin(), summaries.end(), [&](const Summary& other) {
            return other.config.records == summary.config.records &&
                other.config.batch_size == summary.config.batch_size && other.config.workers <= 1;
        });
        if (baseline == summaries.end()) {
            summary.verdict = "no sequential baseline";
            continue;
        }
        summary.speedup = baseline->median_ms / summary.median_ms;
        summary.verdict = compare_ranges(summary, *baseline);
    }
    return summaries;
}

void write_runs_csv(const fs::path& file, const std::vector<Measurement>& measurements,
                    const References& reference_fingerprints) {
    std::ofstream csv(file);
    csv << "records,mode,workers,batch_size,repetition,elapsed_ms,throughput_records_per_s,"
           "parsed_records,malformed_records,batches,batch_latency_mean_us,batch_latency_p50_us,"
           "batch_latency_p95_us,batch_latency_max_us,baseline_working_set_mb,peak_working_set_mb,"
           "peak_commit_mb,result_fingerprint,results_match\n";
    for (const auto& run : measurements) {
        const auto& config = run.config;
        csv << config.records << ',' << mode_name(config.workers) << ',' << config.workers << ','
            << config.batch_size << ',' << run.repetition << ',' << fixed(run.elapsed_ms, 3) << ','
            << fixed(static_cast<double>(config.records) / (run.elapsed_ms / 1000.0), 0) << ','
            << run.parsed << ',' << run.malformed << ',' << run.batches << ','
            << fixed(run.latency_mean_us, 1) << ',' << fixed(run.latency_p50_us, 1) << ','
            << fixed(run.latency_p95_us, 1) << ',' << fixed(run.latency_max_us, 1) << ','
            << fixed(run.baseline_ws_mb, 2) << ',' << fixed(run.peak_ws_mb, 2) << ','
            << fixed(run.peak_commit_mb, 2) << ',' << run.fingerprint << ','
            << (run.fingerprint == reference_fingerprints.at(reference_key(config)) ? "yes" : "no") << '\n';
    }
}

void write_summary_csv(const fs::path& file, const std::vector<Summary>& summaries) {
    std::ofstream csv(file);
    csv << "records,mode,workers,batch_size,runs,median_ms,mean_ms,min_ms,max_ms,stddev_ms,"
           "median_throughput_records_per_s,speedup_vs_sequential,verdict,median_batch_latency_p50_us,"
           "median_batch_latency_p95_us,median_baseline_working_set_mb,median_peak_working_set_mb,"
           "median_peak_commit_mb,results_match\n";
    for (const auto& summary : summaries) {
        const auto& config = summary.config;
        csv << config.records << ',' << mode_name(config.workers) << ',' << config.workers << ','
            << config.batch_size << ',' << summary.runs << ',' << fixed(summary.median_ms, 3) << ','
            << fixed(summary.mean_ms, 3) << ',' << fixed(summary.min_ms, 3) << ','
            << fixed(summary.max_ms, 3) << ',' << fixed(summary.stddev_ms, 3) << ','
            << fixed(summary.median_throughput, 0) << ',' << fixed(summary.speedup, 3) << ','
            << summary.verdict << ',' << fixed(summary.median_latency_p50_us, 1) << ','
            << fixed(summary.median_latency_p95_us, 1) << ','
            << fixed(summary.median_baseline_ws_mb, 2) << ',' << fixed(summary.median_peak_ws_mb, 2)
            << ',' << fixed(summary.median_peak_commit_mb, 2) << ','
            << (summary.results_match ? "yes" : "no") << '\n';
    }
}

std::string compiler_name() {
#if defined(_MSC_VER)
    return "MSVC " + std::to_string(_MSC_FULL_VER);
#elif defined(__clang__)
    return std::string("Clang ") + __clang_version__;
#elif defined(__GNUC__)
    return std::string("GCC ") + __VERSION__;
#else
    return "unknown";
#endif
}

bool optimized_build() {
#if defined(NDEBUG)
    return true;
#else
    return false;
#endif
}

struct RunSettings {
    std::vector<std::size_t> sizes;
    std::vector<std::size_t> workers;
    std::vector<std::size_t> batch_sizes;
    std::size_t repetitions{5};
    std::size_t warmup{1};
    std::uint32_t seed{42};
    fs::path output_dir;
    fs::path data_dir;
};

void write_report(const fs::path& file, const RunSettings& settings, const std::vector<Summary>& summaries,
                  const std::map<std::size_t, double>& dataset_mb, const References& references,
                  double total_seconds) {
    std::ofstream report(file);
    const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    report << "# SentinelX Pipeline Benchmark Report\n\n"
           << "Generated by `sentinelx_bench` on " << format_time(now) << " UTC. "
           << "All numbers below were measured in this run; nothing is estimated.\n\n"
           << "## Environment\n\n"
           << "| Item | Value |\n| --- | --- |\n"
           << "| Logical CPUs | " << std::thread::hardware_concurrency() << " |\n"
           << "| Compiler | " << compiler_name() << " |\n"
           << "| Build | " << (optimized_build() ? "optimized (NDEBUG)" : "**unoptimized debug build - results are not representative**") << " |\n"
           << "| Warm-up runs per configuration | " << settings.warmup << " |\n"
           << "| Measured repetitions per configuration | " << settings.repetitions << " |\n"
           << "| Dataset seed | " << settings.seed << " |\n"
           << "| Total benchmark time | " << fixed(total_seconds / 60.0, 1) << " min |\n\n"
           << "## Method\n\n"
           << "- Both modes call the same `run_log_pipeline()` on the same file. `workers = 1` runs the\n"
           << "  sequential path; `workers = N > 1` runs the concurrent path (main thread reads,\n"
           << "  N threads parse, 1 thread aggregates in file order).\n"
           << "- Each run is a separate process. Time is measured inside the process around the\n"
           << "  pipeline call only (dataset generation and process start-up are excluded).\n"
           << "- Warm-up runs are discarded and prime the OS file cache. The same file and read\n"
           << "  path are used for every mode; OS cache eviction under memory pressure is possible.\n"
           << "- Repetitions rotate the configuration order to reduce ordering bias.\n"
           << "- Peak memory: Windows `PeakWorkingSetSize` and `PeakPagefileUsage` (peak commit)\n"
           << "  of the run's process. Baseline working set is measured before the pipeline starts.\n"
           << "- Batch latency: time from a batch being read until its analysis completes.\n"
           << "- Speedup = sequential median / configuration median at the same batch size.\n"
           << "  The verdict is **faster** or **slower** only if the slowest and fastest runs of the two\n"
           << "  configurations do not overlap; otherwise it is **no clear difference**.\n"
           << "- Every run's analysis output is hashed; *Results match* confirms it equals the\n"
           << "  sequential result for the same dataset and batch size.\n\n";

    for (const auto records : settings.sizes) {
        report << "## " << size_label(records) << " records (" << fixed(dataset_mb.at(records), 1) << " MB)\n\n"
               << "| Mode | Workers | Batch | Median ms | Min-Max ms | Throughput (rec/s) | Speedup | Verdict "
                  "| Batch p95 us | Peak WS MB | Peak commit MB | Results match |\n"
               << "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |\n";
        const Summary* best = nullptr;
        const Summary* best_concurrent = nullptr;
        bool all_match = true;
        std::size_t faster = 0;
        std::size_t slower = 0;
        std::size_t unclear = 0;
        for (const auto& summary : summaries) {
            if (summary.config.records != records) {
                continue;
            }
            report << "| " << mode_name(summary.config.workers) << " | " << summary.config.workers << " | "
                   << summary.config.batch_size << " | " << fixed(summary.median_ms, 1) << " | "
                   << fixed(summary.min_ms, 1) << "-" << fixed(summary.max_ms, 1) << " | "
                   << fixed(summary.median_throughput, 0) << " | " << fixed(summary.speedup, 2) << "x | "
                   << summary.verdict << " | " << fixed(summary.median_latency_p95_us, 0) << " | "
                   << fixed(summary.median_peak_ws_mb, 1) << " | " << fixed(summary.median_peak_commit_mb, 1)
                   << " | " << (summary.results_match ? "yes" : "**NO**") << " |\n";
            all_match = all_match && summary.results_match;
            if (best == nullptr || summary.median_ms < best->median_ms) {
                best = &summary;
            }
            if (summary.config.workers > 1 &&
                       (best_concurrent == nullptr || summary.median_ms < best_concurrent->median_ms)) {
                best_concurrent = &summary;
            }
            if (summary.verdict == "faster") {
                ++faster;
            } else if (summary.verdict == "slower") {
                ++slower;
            } else if (summary.verdict == "no clear difference") {
                ++unclear;
            }
        }
        report << "\n**Findings for " << size_label(records) << ":**\n\n"
               << "- Analysis results " << (all_match ? "were identical between sequential and concurrent runs at every batch size."
                                                     : "**differed between runs - see results_match.**") << '\n';
        std::set<std::string> per_batch_results;
        for (const auto& [key, value] : references) {
            if (key.first == records) {
                per_batch_results.insert(value);
            }
        }
        if (per_batch_results.size() > 1) {
            report << "- The analysis output differs between batch sizes (in every mode alike): AnomalyDetector\n"
                   << "  evaluates windows at batch boundaries and re-reports a window whose error count grows.\n";
        }
        report << "- Concurrent configurations vs. sequential at the same batch size: " << faster
               << " faster, " << slower << " slower, " << unclear << " no clear difference.\n";
        if (best != nullptr) {
            report << "- Fastest median: " << mode_name(best->config.workers) << ", " << best->config.workers
                   << " worker(s), batch " << best->config.batch_size << " at "
                   << fixed(best->median_throughput, 0) << " records/s.\n";
        }
        if (best_concurrent != nullptr) {
            const auto matching_sequential = std::find_if(
                summaries.begin(), summaries.end(), [&](const Summary& candidate) {
                    return candidate.config.records == records &&
                        candidate.config.workers == 1 &&
                        candidate.config.batch_size == best_concurrent->config.batch_size;
                });
            if (matching_sequential != summaries.end()) {
                report << "- Best concurrent median at batch " << best_concurrent->config.batch_size
                       << " (" << best_concurrent->config.workers << " workers): "
                       << fixed(best_concurrent->median_ms, 1) << " ms versus "
                       << fixed(matching_sequential->median_ms, 1)
                       << " ms sequential; " << fixed(best_concurrent->speedup, 2)
                       << "x, " << best_concurrent->verdict << ".\n";
            }
        }
        if (best != nullptr) {
            report << "- Fastest median regardless of mode or batch size: "
                   << mode_name(best->config.workers) << ", " << best->config.workers
                   << " worker(s), batch " << best->config.batch_size << " at "
                         << fixed(best->median_throughput, 0)
                         << " records/s. This is not a sequential/concurrent comparison because "
                         << "the batch sizes may differ.\n";
        }
        report << '\n';
    }
}

void print_usage() {
    std::cout <<
        "Usage: sentinelx_bench [options]\n"
        "  --sizes LIST        Record counts (default 10k,100k,1m,5m)\n"
        "  --workers LIST      Worker counts; 1 = sequential (default 1,2,4,8)\n"
        "  --batch-sizes LIST  Batch sizes (default 256,1024,4096)\n"
        "  --repetitions N     Measured runs per configuration (default 5)\n"
        "  --warmup N          Discarded warm-up runs per configuration (default 1)\n"
        "  --seed N            Dataset seed (default 42)\n"
        "  --output-dir DIR    Results directory (default benchmark_results)\n"
        "  --data-dir DIR      Dataset directory (default <output-dir>/data)\n";
}

int run_parent(const fs::path& exe, const RunSettings& settings) {
    if (std::find(settings.workers.begin(), settings.workers.end(), 1) == settings.workers.end()) {
        throw std::invalid_argument("--workers must include 1 to provide the sequential reference");
    }
    if (!optimized_build()) {
        std::cout << "WARNING: this is an unoptimized debug build; results will not be representative.\n";
    }
    fs::create_directories(settings.output_dir);
    std::vector<Measurement> measurements;
    References reference_fingerprints;
    std::map<std::size_t, double> dataset_mb;
    const auto started = std::chrono::steady_clock::now();

    for (const auto records : settings.sizes) {
        const auto dataset = settings.data_dir /
            ("bench_" + size_label(records) + "_seed" + std::to_string(settings.seed) + ".log");
        if (!fs::exists(dataset)) {
            std::cout << "Generating " << size_label(records) << " records -> " << dataset.string() << std::endl;
            generate_dataset(dataset, records, settings.seed);
        }
        dataset_mb[records] = static_cast<double>(fs::file_size(dataset)) / kBytesPerMb;

        std::vector<Config> dataset_configs;
        auto worker_order = settings.workers;
        std::sort(worker_order.begin(), worker_order.end());
        worker_order.erase(std::unique(worker_order.begin(), worker_order.end()), worker_order.end());
        for (const auto batch_size : settings.batch_sizes) {
            for (const auto workers : worker_order) {
                dataset_configs.push_back({records, workers, batch_size});
            }
        }

        std::cout << "\n=== " << size_label(records) << " records (" << fixed(dataset_mb[records], 1)
                  << " MB), " << dataset_configs.size() << " configurations ===" << std::endl;
        for (const auto& config : dataset_configs) {
            for (std::size_t run = 0; run < settings.warmup; ++run) {
                const auto warm = launch(exe, dataset, config);
                if (!reference_fingerprints.contains(reference_key(config)) && config.workers <= 1) {
                    reference_fingerprints[reference_key(config)] = warm.fingerprint;
                }
            }
        }

        for (std::size_t repetition = 1; repetition <= settings.repetitions; ++repetition) {
            for (std::size_t offset = 0; offset < dataset_configs.size(); ++offset) {
                const auto& config = dataset_configs[(offset + repetition - 1) % dataset_configs.size()];
                auto measurement = launch(exe, dataset, config);
                measurement.repetition = repetition;
                const auto key = reference_key(config);
                if (!reference_fingerprints.contains(key)) {
                    reference_fingerprints[key] = measurement.fingerprint;
                }
                std::cout << "  rep " << repetition << "/" << settings.repetitions << "  "
                          << std::setw(10) << mode_name(config.workers) << "  workers=" << config.workers
                          << "  batch=" << std::setw(4) << config.batch_size << "  "
                          << std::setw(10) << fixed(measurement.elapsed_ms, 1) << " ms  peak WS "
                          << fixed(measurement.peak_ws_mb, 1) << " MB"
                          << (measurement.fingerprint == reference_fingerprints[key] ? "" : "  RESULT MISMATCH")
                          << std::endl;
                measurements.push_back(std::move(measurement));
            }
        }
    }

    const double total_seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    const auto summaries = summarize(measurements, reference_fingerprints);
    const auto runs_csv = settings.output_dir / "benchmark_runs.csv";
    const auto summary_csv = settings.output_dir / "benchmark_summary.csv";
    const auto report_md = settings.output_dir / "benchmark_report.md";
    write_runs_csv(runs_csv, measurements, reference_fingerprints);
    write_summary_csv(summary_csv, summaries);
    write_report(report_md, settings, summaries, dataset_mb, reference_fingerprints, total_seconds);

    std::cout << "\nWrote:\n  " << runs_csv.string() << "\n  " << summary_csv.string()
              << "\n  " << report_md.string() << '\n';
    const bool all_match = std::all_of(summaries.begin(), summaries.end(),
        [](const Summary& summary) { return summary.results_match; });
    if (!all_match) {
        std::cout << "ERROR: analysis results differed between modes.\n";
        return 3;
    }
    return 0;
}

} // namespace

int main(int argc, char* argv[]) {
    try {
        const std::vector<std::string> args(argv + 1, argv + argc);
        auto value_of = [&args](std::string_view name) -> const std::string* {
            const auto option = std::find(args.begin(), args.end(), name);
            if (option == args.end()) {
                return nullptr;
            }
            if (std::next(option) == args.end()) {
                throw std::invalid_argument(std::string(name) + " needs a value");
            }
            return &*std::next(option);
        };
        if (std::find(args.begin(), args.end(), "--help") != args.end()) {
            print_usage();
            return 0;
        }

        if (std::find(args.begin(), args.end(), "--child") != args.end()) {
            const auto* file = value_of("--file");
            const auto* workers = value_of("--workers");
            const auto* batch = value_of("--batch-size");
            const auto* records = value_of("--records");
            if (file == nullptr || workers == nullptr || batch == nullptr || records == nullptr) {
                throw std::invalid_argument("--child needs --file, --workers, --batch-size and --records");
            }
            return run_child(*file, std::stoul(*workers), std::stoul(*batch), std::stoul(*records));
        }

        RunSettings settings;
        settings.sizes = parse_list(value_of("--sizes") ? *value_of("--sizes") : "10k,100k,1m,5m");
        settings.workers = parse_list(value_of("--workers") ? *value_of("--workers") : "1,2,4,8");
        settings.batch_sizes = parse_list(value_of("--batch-sizes") ? *value_of("--batch-sizes") : "256,1024,4096");
        settings.repetitions = value_of("--repetitions") ? std::stoul(*value_of("--repetitions")) : 5;
        settings.warmup = value_of("--warmup") ? std::stoul(*value_of("--warmup")) : 1;
        settings.seed = value_of("--seed") ? static_cast<std::uint32_t>(std::stoul(*value_of("--seed"))) : 42;
        settings.output_dir = fs::absolute(value_of("--output-dir") ? *value_of("--output-dir") : "benchmark_results");
        settings.data_dir = value_of("--data-dir") ? fs::absolute(*value_of("--data-dir")) : settings.output_dir / "data";
        if (settings.repetitions < 3) {
            throw std::invalid_argument("--repetitions must be at least 3");
        }
        return run_parent(executable_path(argv[0]), settings);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}
