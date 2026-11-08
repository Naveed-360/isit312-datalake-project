#!/usr/bin/env python3
"""Hadoop Streaming reducer: sum patient counts for each sorted branch key."""
import sys


def main():
    current_branch = None
    total = 0

    for raw_line in sys.stdin:
        line = raw_line.rstrip("\n")
        if not line:
            continue
        try:
            branch_id, value = line.split("\t", 1)
            count = int(value)
        except (ValueError, TypeError):
            # Ignore malformed records rather than failing the whole task.
            continue

        if current_branch is None:
            current_branch = branch_id
            total = count
        elif branch_id == current_branch:
            total += count
        else:
            sys.stdout.write("%s\t%d\n" % (current_branch, total))
            current_branch = branch_id
            total = count

    if current_branch is not None:
        sys.stdout.write("%s\t%d\n" % (current_branch, total))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
