#!/usr/bin/env python3
"""
M1 · Data check script (ISIT312 Alpha Group)

Week 8 task: count rows, find empty fields, find duplicate patient IDs.

Checks local staged data under data/raw/ (same files you load to HDFS):
  patients / visits / billing

Usage:
  python3 jobs/data_check.py
  python3 jobs/data_check.py --raw data/raw
  python3 jobs/data_check.py --input data/incoming/visits/*.csv
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
# Keep reports under data/ so they are not committed to GitHub (data/ is ignored).
REPORT_DIR = ROOT / "data"

EXPECTED = {
    "patients": [
        "patient_id",
        "first_name",
        "last_name",
        "dob",
        "gender",
        "branch_id",
        "contact_number",
        "email",
        "address",
    ],
    "visits": [
        "visit_id",
        "patient_id",
        "branch_id",
        "visit_datetime",
        "doctor_id",
        "diagnosis",
    ],
    "billing": [
        "bill_id",
        "visit_id",
        "patient_id",
        "amount",
        "payment_status",
        "payment_date",
    ],
}

PRIMARY_KEY = {"patients": "patient_id", "visits": "visit_id", "billing": "bill_id"}


def is_empty(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    return series.isna() | s.eq("") | s.str.lower().isin(["nan", "none", "null"])


def detect_kind(df: pd.DataFrame, path: Path) -> str:
    cols = {c.lower() for c in df.columns}
    name = path.name.lower()
    parent = path.parent.as_posix().lower()
    # bill_id BEFORE visit_id — billing rows also contain visit_id
    if "billing" in parent or name.startswith("billing") or "bill_id" in cols:
        return "billing"
    if "visits" in parent or name.startswith("visits") or "visit_id" in cols:
        return "visits"
    if "patients" in parent or name.startswith("patients") or (
        "patient_id" in cols and "first_name" in cols
    ):
        return "patients"
    if "patient_id" in cols and "visit_id" not in cols and "bill_id" not in cols:
        return "patients"
    return "unknown"


def load_frames(paths: list[Path]) -> dict[str, pd.DataFrame]:
    buckets: dict[str, list[pd.DataFrame]] = {
        "patients": [],
        "visits": [],
        "billing": [],
        "unknown": [],
    }
    for path in paths:
        df = pd.read_csv(path, dtype=str)
        kind = detect_kind(df, path)
        df["__source_file"] = str(path)
        buckets[kind].append(df)
        print(f"  loaded {path} -> {kind} ({len(df):,} rows)")

    out: dict[str, pd.DataFrame] = {}
    for kind, parts in buckets.items():
        if not parts:
            continue
        out[kind] = pd.concat(parts, ignore_index=True)
    return out


def empty_report(df: pd.DataFrame) -> dict:
    result = {}
    for col in [c for c in df.columns if not c.startswith("__")]:
        n = int(is_empty(df[col]).sum())
        if n:
            result[col] = {
                "empty_count": n,
                "empty_pct": round(100.0 * n / max(len(df), 1), 2),
            }
    return result


def unexpected_empty_payment_dates(df: pd.DataFrame) -> int:
    """Empty payment_date is OK when payment_status is Unpaid; otherwise count as bad."""
    if "payment_date" not in df.columns or "payment_status" not in df.columns:
        return 0
    empty_date = is_empty(df["payment_date"])
    status = df["payment_status"].astype(str).str.strip().str.lower()
    unpaid = status.eq("unpaid")
    return int((empty_date & ~unpaid).sum())


def check_table(kind: str, df: pd.DataFrame) -> dict:
    print(f"\n{'=' * 64}")
    print(f"DATA CHECK · {kind.upper()}  ({len(df):,} rows)")
    print(f"{'=' * 64}")

    expected = EXPECTED.get(kind, [])
    missing_cols = [c for c in expected if c not in df.columns]
    extra_cols = [c for c in df.columns if c not in expected and not c.startswith("__")]

    print("\n1) ROW COUNT")
    print(f"   Total rows: {len(df):,}")
    if "branch_id" in df.columns:
        print("   By branch_id:")
        for b, n in df["branch_id"].value_counts().sort_index().items():
            print(f"      branch_id={b}: {n:,}")
    if kind == "visits":
        date_col = "visit_datetime" if "visit_datetime" in df.columns else "visit_date"
        if date_col in df.columns:
            days = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
            vc = days.value_counts()
            print(f"   Distinct days: {days.nunique()}")
            print(
                f"   Visits/day min/mean/max: "
                f"{vc.min()}/{vc.mean():.1f}/{vc.max()}"
            )

    empties = empty_report(df)
    print("\n2) EMPTY FIELDS")
    if not empties:
        print("   None — every cell has a value.")
    else:
        print(f"   {'column':<22} {'empty':>8} {'pct':>8}")
        for col, info in sorted(empties.items(), key=lambda x: -x[1]["empty_count"]):
            note = ""
            if kind == "billing" and col == "payment_date":
                note = "  (OK when payment_status=Unpaid)"
            print(
                f"   {col:<22} {info['empty_count']:>8,} "
                f"{info['empty_pct']:>7.2f}%{note}"
            )

    bad_payment_dates = unexpected_empty_payment_dates(df) if kind == "billing" else 0
    if kind == "billing" and "payment_date" in empties:
        unpaid_empty = empties["payment_date"]["empty_count"] - bad_payment_dates
        print(
            f"   payment_date detail: {unpaid_empty:,} empty on Unpaid (OK), "
            f"{bad_payment_dates:,} empty on non-Unpaid"
        )

    print("\n3) DUPLICATE PATIENT IDs")
    pid_info: dict = {}
    if "patient_id" not in df.columns:
        print("   No patient_id column.")
        pid_info = {"error": "no patient_id"}
    else:
        empty_pids = int(is_empty(df["patient_id"]).sum())
        unique_pids = int(df["patient_id"].nunique())
        dup_mask = df["patient_id"].duplicated(keep=False) & ~is_empty(df["patient_id"])
        dup_ids = df.loc[dup_mask, "patient_id"].value_counts()

        conflict_cols = [
            c
            for c in (
                "first_name",
                "last_name",
                "dob",
                "gender",
                "contact_number",
                "email",
                "address",
            )
            if c in df.columns
        ]
        conflicts = []
        if conflict_cols:
            for pid, g in df.loc[~is_empty(df["patient_id"])].groupby("patient_id"):
                detail = {}
                for col in conflict_cols:
                    vals = sorted(
                        {v for v in g[col].tolist() if str(v).strip() not in ("", "nan")}
                    )
                    if len(vals) > 1:
                        detail[col] = vals[:5]
                if detail:
                    conflicts.append(
                        {"patient_id": pid, "rows": int(len(g)), "conflicts": detail}
                    )
                if len(conflicts) >= 50:
                    break

        print(f"   Unique patient_ids     : {unique_pids:,}")
        print(f"   Empty patient_ids      : {empty_pids:,}")
        print(
            f"   Duplicate patient_ids  : {int(dup_ids.shape[0]):,} values "
            f"({int(dup_mask.sum()):,} rows)"
        )
        print(f"   Attribute conflicts    : {len(conflicts):,}")
        if kind == "visits":
            print("   Note: repeat patient_id on visits = expected (return visits).")
        elif kind == "patients":
            print("   Note: on patients table, duplicate patient_id is an ERROR.")
        if not dup_ids.empty and kind == "patients":
            print("   Top duplicate patient_ids:")
            for pid, n in dup_ids.head(10).items():
                print(f"      {pid}: {n}")
        if conflicts:
            print("   Sample conflicts:")
            for item in conflicts[:5]:
                print(f"      {item}")

        pid_info = {
            "unique_patient_ids": unique_pids,
            "empty_patient_ids": empty_pids,
            "duplicate_patient_id_values": int(dup_ids.shape[0]),
            "rows_with_duplicate_patient_ids": int(dup_mask.sum()),
            "attribute_conflicts": conflicts,
            "attribute_conflict_count": len(conflicts),
            "top_duplicate_patient_ids": dup_ids.head(20).to_dict(),
        }

    pk = PRIMARY_KEY.get(kind)
    pk_dups = 0
    if pk and pk in df.columns:
        pk_dups = int(df[pk].duplicated().sum())
        print(f"\n4) PRIMARY KEY ({pk})")
        print(f"   Duplicate {pk} values: {pk_dups:,}")

    if missing_cols:
        print("\n5) SCHEMA")
        print(f"   Missing expected columns: {missing_cols}")
    if extra_cols:
        print(f"   Extra columns: {extra_cols}")

    # Empties that should fail — ignore payment_date empties on Unpaid bills
    failing_empty_cols = {
        col: info
        for col, info in empties.items()
        if not (kind == "billing" and col == "payment_date")
    }
    fail = []
    if failing_empty_cols:
        fail.append(f"{len(failing_empty_cols)} column(s) with empties")
    if bad_payment_dates:
        fail.append(f"{bad_payment_dates} paid/non-Unpaid bill(s) with empty payment_date")
    if pid_info.get("empty_patient_ids"):
        fail.append("empty patient_id values")
    if kind == "patients" and pid_info.get("duplicate_patient_id_values"):
        fail.append("duplicate patient_id on patients table")
    if pid_info.get("attribute_conflict_count"):
        fail.append("conflicting attributes for same patient_id")
    if pk_dups:
        fail.append(f"duplicate {pk}")
    if missing_cols:
        fail.append(f"missing columns {missing_cols}")

    status = "FAIL" if fail else "PASS"
    print(f"\nRESULT: {status}")
    for r in fail:
        print(f"   - {r}")
    if not fail:
        print("   All checks passed.")

    return {
        "table": kind,
        "rows": int(len(df)),
        "missing_columns": missing_cols,
        "extra_columns": extra_cols,
        "empty_fields": empties,
        "unexpected_empty_payment_dates": bad_payment_dates,
        "patient_ids": pid_info,
        "primary_key_duplicates": pk_dups,
        "status": status,
        "fail_reasons": fail,
        "by_branch": df["branch_id"].value_counts().sort_index().to_dict()
        if "branch_id" in df.columns
        else {},
    }


def gather_paths(raw: Path | None, inputs: list[Path]) -> list[Path]:
    paths: list[Path] = []
    if inputs:
        for item in inputs:
            if item.is_dir():
                paths.extend(sorted(item.rglob("*.csv")))
            else:
                paths.append(item)
    elif raw and raw.exists():
        paths.extend(sorted(raw.rglob("*.csv")))
    else:
        for cand in (
            ROOT / "data" / "raw",
            ROOT / "data" / "incoming",
            ROOT / "data" / "incoming" / "visits",
        ):
            if cand.exists():
                paths.extend(sorted(cand.rglob("*.csv")))
                break
    seen = set()
    out = []
    for p in paths:
        r = p.resolve()
        if p.exists() and r not in seen:
            seen.add(r)
            out.append(p)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="M1 data check for Alpha Group lake")
    ap.add_argument("--raw", type=Path, default=ROOT / "data" / "raw")
    ap.add_argument("--input", action="append", type=Path, default=[])
    ap.add_argument(
        "--report-dir",
        type=Path,
        default=REPORT_DIR,
        help="Default: data/ (so reports stay out of GitHub)",
    )
    args = ap.parse_args()

    paths = gather_paths(args.raw, args.input)
    if not paths:
        raise SystemExit(
            "No CSV files found. Stage data first:\n"
            "  python3 jobs/prepare_raw_partitions.py\n"
            "or pass --input path/to/visits.csv"
        )

    print("M1 DATA CHECK — Alpha Group")
    print(f"Files: {len(paths)}")
    tables = load_frames(paths)
    if not tables:
        raise SystemExit("No tables loaded.")

    summaries = []
    for kind in ("patients", "visits", "billing", "unknown"):
        if kind in tables:
            summaries.append(check_table(kind, tables[kind]))

    args.report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    payload = {
        "checked_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "files": [str(p) for p in paths],
        "tables": summaries,
    }
    json_path = args.report_dir / f"data_check_{stamp}.json"
    txt_path = args.report_dir / f"data_check_{stamp}.txt"
    latest_json = args.report_dir / "data_check_latest.json"
    latest_txt = args.report_dir / "data_check_latest.txt"

    lines = [
        "ISIT312 · M1 Batch Ingestion · Data Check Report",
        f"Generated (UTC): {payload['checked_at_utc']}",
        "",
    ]
    for s in summaries:
        lines.append(f"{s['table']}: {s['status']}  rows={s['rows']:,}")
        if s["fail_reasons"]:
            lines.append(f"  failures: {', '.join(s['fail_reasons'])}")
        lines.append("")

    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    latest_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"\nSaved -> {txt_path}")
    print(f"Saved -> {json_path}")


if __name__ == "__main__":
    main()
