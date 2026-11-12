#!/usr/bin/env python3
"""Prepare 10k / 100k / 300k patient CSV inputs from the M1 patients.csv file.

If the M1 source has fewer records than a requested size, this utility cycles
through its rows and assigns unique benchmark-only patient IDs. The manifest
marks those files as expanded sample data; do not present their timings as a
benchmark on the final 300k M1 dataset. Use --no-expansion once M1's complete
patient dataset is available.

Run from the project folder (needs Python 3.8+ on your laptop):
  python jobs/m5/prepare_benchmark_data.py --no-expansion
Reads data/m1/patients.csv and writes data/benchmark/patients_{10k,100k,300k}.csv.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from collections import Counter
from pathlib import Path

SIZES_DEFAULT = (10_000, 100_000, 300_000)
REQUIRED_COLUMNS = {"patient_id", "branch_id"}


def read_patients(source: Path) -> tuple[list[str], list[dict[str, str]]]:
    with source.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"No CSV header found in {source}")
        missing = REQUIRED_COLUMNS.difference(reader.fieldnames)
        if missing:
            raise ValueError(f"Missing required column(s): {', '.join(sorted(missing))}")
        rows = [row for row in reader if (row.get("patient_id") or "").strip() and (row.get("branch_id") or "").strip()]
    if not rows:
        raise ValueError(f"No usable patient rows found in {source}")
    return list(reader.fieldnames), rows


def create_rows(
    source_rows: list[dict[str, str]],
    target_size: int,
    allow_expansion: bool,
    seed: int,
) -> tuple[list[dict[str, str]], str]:
    if len(source_rows) >= target_size:
        if len(source_rows) == target_size:
            selected = source_rows[:]
            method = "full_source_exact_size"
        else:
            selected = random.Random(seed + target_size).sample(source_rows, target_size)
            method = "deterministic_random_sample_from_full_m1_source"
        return selected, method

    if not allow_expansion:
        raise ValueError(
            f"Source has {len(source_rows):,} valid patients, fewer than the requested "
            f"{target_size:,}. Provide M1's full data or omit --no-expansion for a clearly "
            "labelled sample-expansion validation dataset."
        )

    expanded: list[dict[str, str]] = []
    for i in range(target_size):
        row = dict(source_rows[i % len(source_rows)])
        row["patient_id"] = f"M5BENCH_{i + 1:09d}"
        expanded.append(row)
    return expanded, "cycled_m1_sample_with_unique_benchmark_patient_ids"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    # This file lives in jobs/m5/, so the project root is two folders up.
    repo_root = Path(__file__).resolve().parents[2]
    parser.add_argument("--source", type=Path, default=repo_root / "data/m1/patients.csv")
    parser.add_argument("--output-dir", type=Path, default=repo_root / "data/benchmark")
    parser.add_argument("--sizes", nargs="+", type=int, default=list(SIZES_DEFAULT))
    parser.add_argument("--seed", type=int, default=312)
    parser.add_argument("--no-expansion", action="store_true", help="Fail if the M1 source is smaller than a requested size")
    args = parser.parse_args()

    if any(size <= 0 for size in args.sizes):
        parser.error("All --sizes values must be positive integers")
    if not args.source.is_file():
        parser.error(f"M1 source file not found: {args.source}")

    columns, source_rows = read_patients(args.source)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "source_file": str(args.source),
        "source_rows": len(source_rows),
        "required_columns": sorted(REQUIRED_COLUMNS),
        "seed": args.seed,
        "datasets": [],
        "warning": (
            "Any dataset expanded from a smaller M1 sample is for pipeline validation only; "
            "replace it with M1's complete dataset before claiming final 10k/100k/300k "
            "MapReduce-vs-Spark empirical results."
        ),
    }

    for size in sorted(set(args.sizes)):
        rows, method = create_rows(source_rows, size, not args.no_expansion, args.seed)
        output = args.output_dir / f"patients_{size // 1000}k.csv"
        with output.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        branch_counts = Counter(row["branch_id"].strip() for row in rows)
        entry = {
            "file": output.name,
            "rows": len(rows),
            "method": method,
            "expanded_sample": method.startswith("cycled_"),
            "branch_counts": dict(sorted(branch_counts.items())),
        }
        manifest["datasets"].append(entry)
        print(f"{output}: {len(rows):,} rows; method={method}; branches={dict(sorted(branch_counts.items()))}")

    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Manifest written to: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
