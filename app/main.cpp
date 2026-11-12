#include <sentinelx/anomaly_detector/AnomalyDetector.h>
#include <sentinelx/event_correlator/EventCorrelator.h>
#include <sentinelx/file_reader/FileReader.h>
#include <sentinelx/log_analyzer/LogAnalyzer.h>
#include <sentinelx/log_parser/LogParser.h>
#include <sentinelx/pipeline/LogPipeline.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace sentinelx;

// Bounds memory for correlation on very large inputs.
constexpr std::size_t kMaxCorrelationEvents = 50'000;
constexpr std::size_t kDemoRecords = 20'000;
constexpr std::size_t kDefaultThreads = 4;

struct ComponentMessages {
    std::string_view name;
    int pid;
    std::vector<std::string_view> info;
    std::vector<std::string_view> debug;
    std::vector<std::string_view> warning;
    std::vector<std::string_view> error;
};

const std::array<ComponentMessages, 4>& demo_components() {
    static const std::array<ComponentMessages, 4> components{{
        {"Database", 2134,
         {"Query executed", "Transaction committed", "Checkpoint completed"},
         {"Prepared statement cache hit"},
         {"Slow query detected", "Connection pool utilization above 80 percent"},
         {"Connection pool exhausted", "Deadlock detected during transaction", "Query timeout exceeded"}},
        {"Network", 3071,
         {"Request served", "Health check passed"},
         {"Keep-alive probe sent"},
         {"Upstream latency above threshold", "Retrying request to upstream"},
         {"Connection refused by upstream", "DNS resolution failed", "Socket read timeout"}},
        {"Memory", 4410,
         {"Garbage collection completed", "Heap usage within limits"},
         {"Page cache lookup completed"},
         {"Heap usage above 85 percent"},
         {"Out of memory while allocating buffer", "Memory allocation failed"}},
        {"Authentication", 5268,
         {"User login succeeded", "Session closed"},
         {"Token signature validated"},
         {"Multiple failed login attempts detected"},
         {"Invalid credentials rejected", "Token validation failed"}},
    }};
    return components;
}

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

const char* severity_name(log_parser::Severity severity) {
    switch (severity) {
    case log_parser::Severity::Info: return "INFO";
    case log_parser::Severity::Warning: return "WARNING";
    case log_parser::Severity::Error: return "ERROR";
    case log_parser::Severity::Debug: return "DEBUG";
    }
    return "UNKNOWN";
}

const char* anomaly_name(anomaly_detector::AnomalySeverity severity) {
    switch (severity) {
    case anomaly_detector::AnomalySeverity::Low: return "LOW";
    case anomaly_detector::AnomalySeverity::Medium: return "MEDIUM";
    case anomaly_detector::AnomalySeverity::High: return "HIGH";
    case anomaly_detector::AnomalySeverity::Critical: return "CRITICAL";
    }
    return "UNKNOWN";
}

const char* correlation_name(event_correlator::CorrelationType type) {
    switch (type) {
    case event_correlator::CorrelationType::WarningBeforeError: return "Warning before error";
    case event_correlator::CorrelationType::RepeatedErrorMessage: return "Repeated error message";
    case event_correlator::CorrelationType::RepeatedErrorSequence: return "Repeated error sequence";
    }
    return "Unknown";
}

void write_demo_log(const std::filesystem::path& path) {
    if (path.has_parent_path()) {
        std::filesystem::create_directories(path.parent_path());
    }
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output) {
        throw std::runtime_error("Unable to create demo log: " + path.string());
    }

    const auto& components = demo_components();
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    auto pick = [&rng](const std::vector<std::string_view>& values) {
        return values[std::uniform_int_distribution<std::size_t>(0, values.size() - 1)(rng)];
    };

    const std::size_t spike_start = kDemoRecords * 6 / 10;
    const std::size_t spike_end = spike_start + kDemoRecords / 25;
    const std::size_t lead_start = spike_start - (spike_end - spike_start) / 4;
    const auto& spike_component = components[0];

    std::chrono::sys_seconds timestamp{std::chrono::sys_days{
        std::chrono::year{2026} / std::chrono::January / 1}};
    for (std::size_t index = 0; index < kDemoRecords; ++index) {
        const double step = unit(rng);
        timestamp += std::chrono::seconds{step < 0.3 ? 0 : step < 0.8 ? 1 : 2};

        const ComponentMessages* component = nullptr;
        std::string_view level;
        std::string_view message;
        if (index >= spike_start && index < spike_end && unit(rng) < 0.7) {
            component = &spike_component;
            level = "ERROR";
            message = spike_component.error.front();
        } else if (index >= lead_start && index < spike_start && unit(rng) < 0.2) {
            component = &spike_component;
            level = "WARNING";
            message = pick(component->warning);
        } else {
            component = &components[std::uniform_int_distribution<std::size_t>(0, 3)(rng)];
            const double roll = unit(rng);
            if (roll < 0.70) {
                level = "INFO";
                message = pick(component->info);
            } else if (roll < 0.85) {
                level = "DEBUG";
                message = pick(component->debug);
            } else if (roll < 0.95) {
                level = "WARNING";
                message = pick(component->warning);
            } else {
                level = "ERROR";
                message = pick(component->error);
            }
        }
        output << format_time(timestamp) << ' ' << level << ' ' << component->name
               << ": pid=" << component->pid << ' ' << message << '\n';
    }
    if (!output) {
        throw std::runtime_error("Unable to write demo log: " + path.string());
    }
}

int analyze(const std::filesystem::path& path, std::size_t batch_size, std::size_t threads) {
    const auto started = std::chrono::steady_clock::now();
    auto result = pipeline::run_log_pipeline(path, {
        .batch_size = batch_size,
        .worker_count = threads,
        .max_correlation_events = kMaxCorrelationEvents,
        .analyzer = {.top_error_limit = 5}});
    if (!result.ok) {
        std::cerr << "error: " << result.error << '\n';
        return 1;
    }
    const auto& summary = result.summary;
    const auto& stats = result.stats;
    const auto correlations = event_correlator::EventCorrelator{}.correlate(result.notable_events);
    const auto elapsed = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - started);

    // A window can be re-reported as its count grows; keep only its latest report.
    std::map<std::pair<std::string, std::chrono::sys_seconds>, anomaly_detector::AnomalyReport> latest;
    for (auto& anomaly : result.anomalies) {
        latest[{anomaly.component, anomaly.window_start}] = std::move(anomaly);
    }
    std::vector<anomaly_detector::AnomalyReport> unique_anomalies;
    for (auto& [key, anomaly] : latest) {
        unique_anomalies.push_back(std::move(anomaly));
    }
    std::sort(unique_anomalies.begin(), unique_anomalies.end(),
        [](const auto& left, const auto& right) {
            if (left.severity != right.severity) {
                return left.severity > right.severity;
            }
            return left.observed_errors > right.observed_errors;
        });

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\n=== SentinelX report: " << path.string() << " ===\n\n";
    std::cout << "Records parsed:     " << summary.total_records << '\n'
              << "Malformed records:  " << summary.malformed_records + stats.skipped_lines << '\n'
              << "Batches processed:  " << stats.batches_aggregated << " (batch size " << batch_size << ")\n"
              << "Processing time:    " << elapsed.count() << " ms\n";

    std::cout << "\n-- Execution --\n";
    if (!stats.concurrent) {
        std::cout << "  Mode: sequential (single thread)\n";
    } else {
        std::cout << "  Mode: concurrent (" << stats.workers.size()
                  << " parser threads + 1 ordered aggregation thread + reader on main thread)\n"
                  << "  Task queue: capacity " << stats.queue_capacity << ", peak depth "
                  << stats.max_queue_depth << ", " << stats.queue_accepted << " batches in, "
                  << stats.queue_popped << " out\n";
        std::set<std::thread::id> active_threads;
        for (std::size_t index = 0; index < stats.workers.size(); ++index) {
            const auto& worker = stats.workers[index];
            std::cout << "  Parser " << index + 1 << " (thread " << worker.thread_id << "): "
                      << worker.batches_parsed << " batches\n";
            if (worker.batches_parsed > 0) {
                active_threads.insert(worker.thread_id);
            }
        }
        std::cout << "  Distinct threads that parsed batches: " << active_threads.size() << '\n';
    }

    std::cout << "\n-- Severity distribution --\n";
    for (const auto& severity : summary.severity_distribution) {
        std::cout << "  " << std::left << std::setw(8) << severity_name(severity.severity)
                  << std::right << std::setw(10) << severity.count
                  << std::setw(9) << severity.percentage << " %\n";
    }

    std::cout << "\n-- Errors by component --\n";
    std::vector<std::pair<std::string, std::size_t>> components(
        summary.errors_by_component.begin(), summary.errors_by_component.end());
    std::sort(components.begin(), components.end(),
        [](const auto& left, const auto& right) { return left.second > right.second; });
    for (const auto& [component, count] : components) {
        std::cout << "  " << std::left << std::setw(16) << component << std::right
                  << std::setw(8) << count << '\n';
    }

    std::cout << "\n-- Most frequent errors --\n";
    for (const auto& error : summary.most_frequent_errors) {
        std::cout << "  " << std::setw(6) << error.count << "  " << error.message << '\n';
    }

    std::cout << "\n-- Anomalies (" << unique_anomalies.size() << " windows) --\n";
    if (unique_anomalies.empty()) {
        std::cout << "  None detected.\n";
    }
    for (std::size_t index = 0; index < std::min<std::size_t>(10, unique_anomalies.size()); ++index) {
        const auto& anomaly = unique_anomalies[index];
        std::cout << "  [" << anomaly_name(anomaly.severity) << "] "
                  << format_time(anomaly.window_start) << "  " << anomaly.explanation << '\n';
    }

    std::cout << "\n-- Possible correlations (" << correlations.reports.size() << " found) --\n";
    if (result.correlation_truncated) {
        std::cout << "  Note: only the first " << kMaxCorrelationEvents
                  << " WARNING/ERROR events were correlated.\n";
    }
    for (std::size_t index = 0; index < std::min<std::size_t>(5, correlations.reports.size()); ++index) {
        const auto& report = correlations.reports[index];
        std::cout << "  #" << report.rank << " " << correlation_name(report.type)
                  << " (" << report.component << ", relevance " << report.relevance_score << ")\n"
                  << "     " << report.explanation << '\n';
    }
    std::cout << '\n';
    return 0;
}

void print_usage() {
    std::cout << "Usage:\n"
                 "  sentinelx <log-file> [options]          Analyze a log file\n"
                 "  sentinelx --demo [output-file] [options] Generate a sample log, then analyze it\n"
                 "\nOptions:\n"
                 "  --batch-size N   Lines per batch (default 1024)\n"
                 "  --threads N      Parser threads; 1 runs sequentially (default 4)\n";
}

} // namespace

int main(int argc, char* argv[]) {
    try {
        const std::vector<std::string> args(argv + 1, argv + argc);
        if (args.empty() || args[0] == "--help" || args[0] == "-h") {
            print_usage();
            return args.empty() ? 2 : 0;
        }

        auto option_value = [&args](std::string_view name, std::size_t fallback) {
            const auto option = std::find(args.begin(), args.end(), name);
            if (option == args.end()) {
                return fallback;
            }
            if (std::next(option) == args.end()) {
                throw std::invalid_argument(std::string(name) + " needs a value");
            }
            const auto& text = *std::next(option);
            std::size_t value = 0;
            const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
            if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
                throw std::invalid_argument(std::string(name) + " must be an unsigned integer");
            }
            return value;
        };
        const auto batch_size = option_value("--batch-size", 1024);
        const auto threads = option_value("--threads", kDefaultThreads);
        if (batch_size == 0 || threads == 0) {
            std::cerr << "error: --batch-size and --threads must be greater than zero\n";
            return 2;
        }

        std::filesystem::path path;
        if (args[0] == "--demo") {
            path = args.size() > 1 && !args[1].starts_with("--")
                ? std::filesystem::path(args[1])
                : std::filesystem::path("sample_data") / "demo_logs.log";
            write_demo_log(path);
            std::cout << "Generated " << kDemoRecords << " demo records in " << path.string() << '\n';
        } else {
            path = args[0];
        }
        return analyze(path, batch_size, threads);
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return 1;
    }
}
