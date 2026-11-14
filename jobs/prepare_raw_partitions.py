#!/usr/bin/env python3
"""
M1 · Stage local CSVs into data/raw for HDFS upload.

Reads patients/visits/billing from data/incoming/ (or --input files) and writes:

  data/raw/patients/patients.csv                         (ONE file — required by MapReduce)
  data/raw/visits/branch_id=<id>/date=<yyyy-MM-dd>/visits.csv
  data/raw/billing/branch_id=<id>/date=<yyyy-MM-dd>/billing.csv

billing.csv has no branch_id column, so each bill is filed under its visit's branch.
Unpaid bills (no payment_date) go in date=unknown.

Usage (from isit312-datalake-project root):
  python3 jobs/prepare_raw_partitions.py --incoming data/m1     # the team's usual folder
  python3 jobs/prepare_raw_partitions.py                        # reads data/incoming/
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


def detect_kind(df: pd.DataFrame, path: Path) -> str:
    cols = {c.lower() for c in df.columns}
    name = path.name.lower()
    # bill_id BEFORE visit_id — billing also has visit_id
    if "bill_id" in cols or name.startswith("billing"):
        return "billing"
    if "visit_id" in cols or name.startswith("visits"):
        return "visits"
    if "patient_id" in cols and ("first_name" in cols or "dob" in cols or name.startswith("patients")):
        return "patients"
    if "patient_id" in cols and "visit_id" not in cols and "bill_id" not in cols:
        return "patients"
    raise SystemExit(f"Cannot detect table type for {path} (cols={list(df.columns)})")


def date_series(df: pd.DataFrame, kind: str) -> pd.Series:
    if kind == "visits":
        if "visit_datetime" in df.columns:
            return pd.to_datetime(df["visit_datetime"], errors="coerce").dt.strftime("%Y-%m-%d")
        if "visit_date" in df.columns:
            return pd.to_datetime(df["visit_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if kind == "billing" and "payment_date" in df.columns:
        # unpaid rows may have empty payment_date — put those in date=unknown
        raw = pd.to_datetime(df["payment_date"], errors="coerce")
        out = raw.dt.strftime("%Y-%m-%d")
        return out.fillna("unknown")
    return pd.Series(["all"] * len(df))


def write_patients(df: pd.DataFrame, out_root: Path) -> int:
    """Single flat patients file (not partitioned) for Shahir's MapReduce job."""
    folder = out_root / "patients"
    folder.mkdir(parents=True, exist_ok=True)
    out = folder / "patients.csv"
    part = df.copy()
    if out.exists():
        prev = pd.read_csv(out, dtype=str)
        part = pd.concat([prev, part.astype(str)], ignore_index=True)
    if "patient_id" in part.columns:
        part = part.drop_duplicates(subset=["patient_id"], keep="last")
    part.to_csv(out, index=False)
    print(f"  wrote data/raw/patients/patients.csv  ({len(part):,} rows)")
    return 1


def write_partitions(df: pd.DataFrame, kind: str, out_root: Path, drop_cols: tuple = ()) -> int:
    if kind == "patients":
        return write_patients(df, out_root)

    if "branch_id" not in df.columns:
        raise SystemExit(f"{kind}: missing branch_id column")

    work = df.copy()
    work["branch_id"] = work["branch_id"].fillna("unknown")
    work["_date"] = date_series(work, kind)
    if work["_date"].isna().any():
        bad = int(work["_date"].isna().sum())
        raise SystemExit(f"{kind}: {bad} rows have invalid dates")

    written = 0
    key = {"visits": "visit_id", "billing": "bill_id"}[kind]
    for (branch, day), part in work.groupby(["branch_id", "_date"], dropna=False):
        folder = out_root / kind / f"branch_id={branch}" / f"date={day}"
        folder.mkdir(parents=True, exist_ok=True)
        out = folder / f"{kind}.csv"
        part = part.drop(columns=["_date", *drop_cols])
        if out.exists():
            prev = pd.read_csv(out, dtype=str)
            part = pd.concat([prev, part.astype(str)], ignore_index=True)
            if key in part.columns:
                part = part.drop_duplicates(subset=[key], keep="last")
        part.to_csv(out, index=False)
        written += 1
        print(f"  wrote data/raw/{kind}/branch_id={branch}/date={day}/{kind}.csv  ({len(part):,} rows)")
    return written


def collect_inputs(incoming: Path, extra: list[Path]) -> list[Path]:
    files: list[Path] = []
    if incoming.exists():
        files.extend(sorted(incoming.rglob("*.csv")))
    files.extend(extra)
    seen = set()
    out = []
    for f in files:
        r = f.resolve()
        if r not in seen and f.exists():
            seen.add(r)
            out.append(f)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Stage CSVs into data/raw for HDFS upload")
    ap.add_argument("--incoming", type=Path, default=ROOT / "data" / "incoming")
    ap.add_argument("--out", type=Path, default=ROOT / "data" / "raw")
    ap.add_argument("--input", action="append", type=Path, default=[], help="Extra CSV paths")
    args = ap.parse_args()

    files = collect_inputs(args.incoming, args.input)
    if not files:
        raise SystemExit(f"No CSVs found under {args.incoming} (or via --input)")

    print(f"Staging {len(files)} file(s) -> {args.out}")
    if args.out.exists():
        shutil.rmtree(args.out)
    args.out.mkdir(parents=True, exist_ok=True)

    # Read every file first: billing.csv has no branch_id (the branch belongs to the
    # visit), so each bill takes its branch from its visit before partitioning.
    tables: dict[str, list[pd.DataFrame]] = {}
    for path in files:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
        kind = detect_kind(df, path)
        print(f"{path.name} -> {kind} ({len(df):,} rows)")
        tables.setdefault(kind, []).append(df)
    frames = {kind: pd.concat(parts, ignore_index=True) for kind, parts in tables.items()}

    drop_cols: dict[str, tuple] = {}
    billing = frames.get("billing")
    if billing is not None and "branch_id" not in billing.columns:
        visits = frames.get("visits")
        if visits is None or "branch_id" not in visits.columns:
            raise SystemExit("billing has no branch_id and no visits file to take it from")
        branch_of_visit = visits[["visit_id", "branch_id"]].drop_duplicates("visit_id")
        frames["billing"] = billing.merge(branch_of_visit, on="visit_id", how="left")
        drop_cols["billing"] = ("branch_id",)   # keep billing.csv in the agreed column layout
        missing = int(frames["billing"]["branch_id"].isna().sum())
        print(f"billing: branch taken from each bill's visit ({missing:,} bills without a visit -> branch_id=unknown)")

    total_parts = 0
    for kind in ("patients", "visits", "billing"):
        if kind in frames:
            print(f"\n{kind} ({len(frames[kind]):,} rows)")
            total_parts += write_partitions(frames[kind], kind, args.out, drop_cols.get(kind, ()))

    print(f"\nDone. {total_parts} file(s) under {args.out}")
    print("  patients -> data/raw/patients/patients.csv  (single file)")
    print("  visits/billing -> partitioned by branch_id + date")
    print("Next: bash jobs/load_raw_to_hdfs.sh")


if __name__ == "__main__":
    main()
