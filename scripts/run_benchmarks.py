#!/usr/bin/env python3
"""Run SentinelX Google Benchmark cases and save their measured JSON report."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import platform
import subprocess
import tempfile


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=pathlib.Path, help="SentinelXBenchmarks executable")
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("benchmark-report.json"))
    parser.add_argument("--data-dir", type=pathlib.Path, help="Keep generated benchmark input files here")
    parser.add_argument("--min-time", default="0.2", help="Google Benchmark minimum seconds per case")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--filter", help="Optional Google Benchmark regular expression")
    args = parser.parse_args()

    executable = args.executable.expanduser().resolve()
    if not executable.is_file():
        parser.error(f"benchmark executable does not exist: {executable}")
    if args.repetitions < 1:
        parser.error("--repetitions must be at least 1")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    temporary_directory = None
    if args.data_dir is not None:
        data_directory = args.data_dir.expanduser().resolve()
        data_directory.mkdir(parents=True, exist_ok=True)
    else:
        temporary_directory = tempfile.TemporaryDirectory(prefix="sentinelx-bench-")
        data_directory = pathlib.Path(temporary_directory.name) / "data"
        data_directory.mkdir()
    environment["SENTINELX_BENCHMARK_DATA_DIR"] = str(data_directory)

    with tempfile.TemporaryDirectory(prefix="sentinelx-benchmark-result-") as temporary:
        raw_path = pathlib.Path(temporary) / "google-benchmark.json"
        command = [
            str(executable),
            f"--benchmark_out={raw_path}",
            "--benchmark_out_format=json",
            f"--benchmark_min_time={args.min_time}",
            f"--benchmark_repetitions={args.repetitions}",
        ]
        if args.filter:
            command.append(f"--benchmark_filter={args.filter}")
        subprocess.run(command, check=True, env=environment)
        with raw_path.open("r", encoding="utf-8") as report_file:
            measured = json.load(report_file)

    report = {
        "schema_version": 1,
        "measured_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "environment": {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "benchmark_executable": str(executable),
        },
        "configuration": {
            "minimum_seconds_per_case": args.min_time,
            "repetitions": args.repetitions,
            "filter": args.filter,
            "data_directory": str(data_directory) if args.data_dir else None,
            "temporary_data_removed": args.data_dir is None,
        },
        "google_benchmark_context": measured.get("context", {}),
        "benchmarks": measured.get("benchmarks", []),
    }
    with args.output.expanduser().resolve().open("w", encoding="utf-8") as output_file:
        json.dump(report, output_file, indent=2)
        output_file.write("\n")
    if temporary_directory is not None:
        temporary_directory.cleanup()
    print(f"Wrote measured benchmark report: {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())