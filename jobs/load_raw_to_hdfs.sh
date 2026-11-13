#!/usr/bin/env bash
# M1 · Load script — put staged raw CSVs into HDFS on the Alpha Group Docker setup.
#
# Prerequisites:
#   1. Docker Desktop running
#   2. From the isit312-datalake-project root:  docker compose up -d
#   3. Local data ready under data/raw/...  (run: python3 jobs/prepare_raw_partitions.py)
#
# HDFS layout:
#   /raw/patients/patients.csv                          (ONE file — MapReduce needs this)
#   /raw/visits/branch_id=<id>/date=<yyyy-MM-dd>/visits.csv
#   /raw/billing/branch_id=<id>/date=<yyyy-MM-dd>/billing.csv
#
# Usage:
#   bash jobs/load_raw_to_hdfs.sh
#   bash jobs/load_raw_to_hdfs.sh --fresh     # delete /raw then reload
#   bash jobs/load_raw_to_hdfs.sh --dry-run   # print commands only

set -euo pipefail

# Git Bash on Windows rewrites /raw -> C:/Program Files/Git/raw without this.
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW_LOCAL="${ROOT}/data/raw"
NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"
HDFS_RAW_ROOT="${HDFS_RAW_ROOT:-/raw}"

FRESH=0
DRY=0
for arg in "$@"; do
  case "$arg" in
    --fresh) FRESH=1 ;;
    --dry-run) DRY=1 ;;
    -h|--help)
      sed -n '2,22p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 1
      ;;
  esac
done

run() {
  if [[ "$DRY" -eq 1 ]]; then
    echo "DRY: $*"
  else
    eval "$@"
  fi
}

echo "=== M1 HDFS load ==="
echo "Local staged data : ${RAW_LOCAL}"
echo "Namenode container: ${NAMENODE_CONTAINER}"
echo "HDFS target root  : ${HDFS_RAW_ROOT}"

if [[ ! -d "${RAW_LOCAL}" ]]; then
  echo "ERROR: ${RAW_LOCAL} not found. Run: python3 jobs/prepare_raw_partitions.py" >&2
  exit 1
fi

if [[ ! -f "${RAW_LOCAL}/patients/patients.csv" ]]; then
  echo "WARNING: ${RAW_LOCAL}/patients/patients.csv missing (Shahir needs a single patients file)." >&2
fi

if [[ "$DRY" -eq 0 ]]; then
  if ! docker ps --format '{{.Names}}' | grep -qx "${NAMENODE_CONTAINER}"; then
    echo "ERROR: container '${NAMENODE_CONTAINER}' is not running." >&2
    echo "Start the Alpha setup:  docker compose up -d" >&2
    echo "Or dry-run without Docker:  bash jobs/load_raw_to_hdfs.sh --dry-run" >&2
    exit 1
  fi
fi

if [[ "$FRESH" -eq 1 ]]; then
  echo "Removing existing ${HDFS_RAW_ROOT} ..."
  run "docker exec ${NAMENODE_CONTAINER} hdfs dfs -rm -r -f ${HDFS_RAW_ROOT}"
fi

run "docker exec ${NAMENODE_CONTAINER} hdfs dfs -mkdir -p ${HDFS_RAW_ROOT}"

# One put for the whole tree (much faster than per-file docker exec).
# Inside the container, ./data is mounted at /data. The * must be expanded by a shell
# INSIDE the container (hdfs itself doesn't expand it), hence bash -c '...'.
echo "Bulk loading /data/raw/* -> ${HDFS_RAW_ROOT}/ ..."
run "docker exec ${NAMENODE_CONTAINER} bash -c 'hdfs dfs -put -f /data/raw/* ${HDFS_RAW_ROOT}/'"

echo
echo "Loaded data/raw into HDFS ${HDFS_RAW_ROOT}/"
echo
echo "Verify:"
echo "  docker exec ${NAMENODE_CONTAINER} hdfs dfs -ls -R ${HDFS_RAW_ROOT} | head"
echo "  open http://localhost:50070  (HDFS UI)"
echo
echo "Row counts / empties / duplicate patient IDs:"
echo "  python3 jobs/data_check.py --raw data/raw"
