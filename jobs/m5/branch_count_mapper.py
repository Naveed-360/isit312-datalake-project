#!/usr/bin/env python3
"""Hadoop Streaming mapper: emit (branch_id, 1) for each patient CSV row.

Checks every input row for a CSV header because Hadoop may split one file
across multiple mappers, and later splits may start without the header.
When a chunk has no header, the mapper uses the agreed README column order:
patient_id at index 0 and branch_id at index 5.
"""
import csv
import sys


def main():
    reader = csv.reader(sys.stdin)

    # Default positions from the agreed M1 README schema. Hadoop input splits
    # other than the first one may not contain a header row.
    pid_i, bid_i = 0, 5

    for row in reader:
        if not row:
            continue

        # Headers may occur at the start of any input split or between
        # stacked CSV files. Detect and skip every header row encountered.
        names = [c.strip().lstrip("\ufeff").lower() for c in row]
        if "patient_id" in names and "branch_id" in names:
            pid_i = names.index("patient_id")
            bid_i = names.index("branch_id")
            continue

        # Ignore short or malformed records rather than failing the task.
        if len(row) <= max(pid_i, bid_i):
            continue

        patient_id = row[pid_i].strip()
        branch_id = row[bid_i].strip()
        if patient_id and branch_id:
            sys.stdout.write("%s\t1\n" % branch_id)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
