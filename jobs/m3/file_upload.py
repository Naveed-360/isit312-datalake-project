#!/usr/bin/env python3
"""Upload M3's images, lab reports and PDFs to HDFS.

Layout:  /raw/medical_files/branch=<branch_id>/date=<date>/<file>
         /raw/files_metadata/metadata.csv

Written without f-strings so it runs on the older Python inside the namenode container.
"""
import csv
import os
import subprocess
import sys
from collections import defaultdict

ROOT = os.environ.get("M3_ROOT", "/data/m3")
META = os.path.join(ROOT, "metadata.csv")
HDFS_FILES = "/raw/medical_files"
HDFS_META = "/raw/files_metadata"
DRY = "--dry-run" in sys.argv


def hdfs(args):
    cmd = ["hdfs", "dfs"] + list(args)
    shown = " ".join(cmd[:5])
    if len(cmd) > 5:
        shown += " ... (+%d more)" % (len(cmd) - 5)
    print(shown)
    if not DRY:
        subprocess.check_call(cmd)


groups = defaultdict(list)
missing = []
with open(META, newline="") as f:
    for row in csv.DictReader(f):
        local = os.path.join(ROOT, row["file_path"])
        if os.path.exists(local):
            groups[(row["branch_id"], row["date"])].append(local)
        else:
            missing.append(row["file_path"])

if missing:
    print("WARNING: %d files listed in metadata.csv were not found, e.g. %s" % (len(missing), missing[0]))

total = 0
for (branch, date), files in sorted(groups.items()):
    target = "%s/branch=%s/date=%s" % (HDFS_FILES, branch, date)
    hdfs(["-mkdir", "-p", target])
    hdfs(["-put", "-f"] + files + [target])
    total += len(files)

hdfs(["-mkdir", "-p", HDFS_META])
hdfs(["-put", "-f", META, HDFS_META + "/"])
print("Done: %d files in %d branch/date folders%s" % (
    total, len(groups), " (dry run, nothing uploaded)" if DRY else ""))
