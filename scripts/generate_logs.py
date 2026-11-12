#!/usr/bin/env python3
"""Generate SentinelX sample logs in the format accepted by LogParser.

Each line has the form:
    YYYY-MM-DD HH:MM:SS LEVEL COMPONENT: pid=<pid> event_id=<id> <message>
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import pathlib
import random

DATASET_SIZES = {
    "10k": 10_000,
    "100k": 100_000,
    "1m": 1_000_000,
    "5m": 5_000_000,
}
COMPONENTS = ("Database", "Network", "Memory", "Authentication")
LEVEL_THRESHOLDS = (("INFO", 0.70), ("DEBUG", 0.85), ("WARNING", 0.95), ("ERROR", 1.0))
LEVEL_CODES = {"INFO": 100, "DEBUG": 200, "WARNING": 300, "ERROR": 400}
SPIKE_ERROR_RATE = 0.7
SPIKE_LEAD_WARNING_RATE = 0.2
WRITE_CHUNK_LINES = 20_000
DEFAULT_OUTPUT_DIR = pathlib.Path(__file__).resolve().parent.parent / "sample_data"

_MESSAGE_TEXT = {
    "Database": {
        "INFO": (
            "Query executed in {value} ms",
            "Connection acquired from pool",
            "Transaction committed",
            "Checkpoint completed",
        ),
        "DEBUG": (
            "Prepared statement cache hit",
            "Query plan reused for {value} rows",
        ),
        "WARNING": (
            "Slow query detected after {value} ms",
            "Connection pool utilization above 80 percent",
            "Replication lag increasing",
        ),
        "ERROR": (
            "Connection pool exhausted",
            "Deadlock detected during transaction",
            "Query timeout exceeded",
            "Replica connection lost",
        ),
    },
    "Network": {
        "INFO": (
            "Request served in {value} ms",
            "Connection established to upstream",
            "Health check passed",
            "TLS session resumed",
        ),
        "DEBUG": (
            "Socket buffer resized to {value} bytes",
            "Keep-alive probe sent",
        ),
        "WARNING": (
            "Packet loss detected at {value} per mille",
            "Upstream latency above threshold",
            "Retrying request to upstream",
        ),
        "ERROR": (
            "Connection refused by upstream",
            "DNS resolution failed",
            "Socket read timeout",
            "TLS handshake failed",
        ),
    },
    "Memory": {
        "INFO": (
            "Garbage collection reclaimed {value} KB",
            "Heap usage within limits",
            "Cache warmed",
        ),
        "DEBUG": (
            "Allocator arena count {value}",
            "Page cache lookup completed",
        ),
        "WARNING": (
            "Heap usage above 85 percent",
            "Swap usage increased by {value} MB",
            "Allocation latency increased",
        ),
        "ERROR": (
            "Out of memory while allocating buffer",
            "Memory allocation failed",
            "Cache eviction failed",
        ),
    },
    "Authentication": {
        "INFO": (
            "User login succeeded",
            "Token refreshed for session {value}",
            "Session closed",
        ),
        "DEBUG": (
            "Password hash verified in {value} ms",
            "Token signature validated",
        ),
        "WARNING": (
            "Multiple failed login attempts detected",
            "Token nearing expiry",
            "Identity provider response slow",
        ),
        "ERROR": (
            "Invalid credentials rejected",
            "Token validation failed",
            "Identity provider unreachable",
            "Account locked after failed attempts",
        ),
    },
}


@dataclasses.dataclass(frozen=True)
class MessageTemplate:
    event_id: int
    text: str
    has_value: bool


@dataclasses.dataclass(frozen=True)
class Spike:
    start: int
    end: int
    lead_start: int
    component: str
    error: MessageTemplate


# Event IDs identify the message type, so identical errors share an event ID.
MESSAGES = {
    component: {
        level: tuple(
            MessageTemplate(
                1000 * (component_index + 1) + LEVEL_CODES[level] + index,
                text,
                "{value}" in text,
            )
            for index, text in enumerate(texts)
        )
        for level, texts in levels.items()
    }
    for component_index, (component, levels) in enumerate(_MESSAGE_TEXT.items())
}


def choose_level(rng: random.Random) -> str:
    roll = rng.random()
    for level, threshold in LEVEL_THRESHOLDS:
        if roll < threshold:
            return level
    return "ERROR"


def build_spikes(record_count: int, spike_count: int, rng: random.Random) -> list[Spike]:
    if spike_count <= 0:
        return []
    length = max(200, record_count // 500)
    usable_start = record_count // 10
    usable_span = record_count - usable_start
    spike_count = min(spike_count, usable_span // (length * 2))
    if spike_count <= 0:
        return []

    spacing = usable_span // spike_count
    spikes = []
    for index in range(spike_count):
        start = usable_start + index * spacing + rng.randrange(spacing - length)
        component = rng.choice(COMPONENTS)
        spikes.append(Spike(
            start=start,
            end=start + length,
            lead_start=max(0, start - length // 4),
            component=component,
            error=rng.choice(MESSAGES[component]["ERROR"]),
        ))
    return spikes


def generate_dataset(
    path: pathlib.Path,
    record_count: int,
    start_time: dt.datetime,
    spike_count: int,
    seed: int,
) -> tuple[int, list[Spike]]:
    rng = random.Random(seed)
    spikes = build_spikes(record_count, spike_count, rng)
    pids = {component: 1000 + index * 1000 + rng.randrange(1, 900)
            for index, component in enumerate(COMPONENTS)}

    current_second = int(start_time.timestamp())
    formatted_second = -1
    timestamp_text = ""
    spike_index = 0
    error_count = 0

    temporary_path = path.with_name(path.name + ".tmp")
    try:
        with temporary_path.open("w", encoding="ascii", newline="\n") as output:
            buffer: list[str] = []
            for index in range(record_count):
                while spike_index < len(spikes) and index >= spikes[spike_index].end:
                    restarted = spikes[spike_index].component
                    pids[restarted] += rng.randrange(1, 50)
                    spike_index += 1
                spike = spikes[spike_index] if spike_index < len(spikes) else None

                if spike and spike.start <= index and rng.random() < SPIKE_ERROR_RATE:
                    component, level, template = spike.component, "ERROR", spike.error
                elif (spike and spike.lead_start <= index < spike.start
                      and rng.random() < SPIKE_LEAD_WARNING_RATE):
                    component, level = spike.component, "WARNING"
                    template = rng.choice(MESSAGES[component][level])
                else:
                    component = rng.choice(COMPONENTS)
                    level = choose_level(rng)
                    template = rng.choice(MESSAGES[component][level])

                if level == "ERROR":
                    error_count += 1
                text = (template.text.format(value=rng.randint(1, 5000))
                        if template.has_value else template.text)

                roll = rng.random()
                current_second += 0 if roll < 0.3 else 1 if roll < 0.8 else 2
                if current_second != formatted_second:
                    timestamp_text = dt.datetime.fromtimestamp(
                        current_second, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    formatted_second = current_second

                buffer.append(
                    f"{timestamp_text} {level} {component}: "
                    f"pid={pids[component]} event_id={template.event_id} {text}\n")
                if len(buffer) >= WRITE_CHUNK_LINES:
                    output.writelines(buffer)
                    buffer.clear()
            output.writelines(buffer)
        temporary_path.replace(path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
    return error_count, spikes


def parse_start_time(text: str) -> dt.datetime:
    try:
        parsed = dt.datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "start time must use YYYY-MM-DD HH:MM:SS") from error
    return parsed.replace(tzinfo=dt.timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sizes", nargs="*", default=["10k"],
                        choices=[*DATASET_SIZES, "all"],
                        help="dataset sizes to generate (default: 10k)")
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--spikes", type=int, default=3,
                        help="number of artificial ERROR spikes per dataset")
    parser.add_argument("--start", type=parse_start_time,
                        default=parse_start_time("2026-01-01 00:00:00"),
                        help="first timestamp in UTC, formatted YYYY-MM-DD HH:MM:SS")
    args = parser.parse_args()
    if args.spikes < 0:
        parser.error("--spikes must be zero or greater")

    names = list(DATASET_SIZES) if "all" in args.sizes else list(dict.fromkeys(args.sizes))
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    for name in names:
        record_count = DATASET_SIZES[name]
        path = output_dir / f"system_logs_{name}.log"
        error_count, spikes = generate_dataset(
            path, record_count, args.start, args.spikes, args.seed)
        print(f"Wrote {record_count:,} records to {path} ({error_count:,} ERROR entries)")
        for spike in spikes:
            print(f"  spike: {spike.component} '{spike.error.text}' "
                  f"on lines {spike.start + 1}-{spike.end}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
