#!/usr/bin/env bash
# M1 · Load script — put staged raw CSVs into HDFS on the Alpha Group Docker setup.
#
# Prerequisites (from TEAM README):
#   1. Docker Desktop running
#   2. From the isit312-datalake-project root:  docker compose up -d
#   3. Local partitions ready under data/raw/...  (run jobs/prepare_raw_partitions.py)
#
# HDFS layout (team convention):
#   /raw/patients/branch_id=<id>/date=<yyyy-MM-dd>/patients.csv
#   /raw/visits/branch_id=<id>/date=<yyyy-MM-dd>/visits.csv
#   /raw/billing/branch_id=<id>/date=<yyyy-MM-dd>/billing.csv
#
# Usage:
#   bash jobs/load_raw_to_hdfs.sh
#   bash jobs/load_raw_to_hdfs.sh --fresh     # delete /raw then reload
#   bash jobs/load_raw_to_hdfs.sh --dry-run   # print commands only

set -euo pipefail

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
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 1
      ;;
  esac
done

hdfs() {
  if [[ "$DRY" -eq 1 ]]; then
    echo "DRY: docker exec ${NAMENODE_CONTAINER} hdfs dfs $*"
  else
    docker exec "${NAMENODE_CONTAINER}" hdfs dfs "$@"
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

# Quick Docker sanity check (skip in dry-run if docker missing)
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
  hdfs -rm -r -f "${HDFS_RAW_ROOT}" || true
fi

hdfs -mkdir -p "${HDFS_RAW_ROOT}/patients" "${HDFS_RAW_ROOT}/visits" "${HDFS_RAW_ROOT}/billing"

# Copy each local partition file into the matching HDFS path.
# Local path:  data/raw/visits/branch_id=1/date=2025-08-08/visits.csv
# Inside container the same tree is mounted at /data/raw/...
count=0
while IFS= read -r -d '' file; do
  rel="${file#${RAW_LOCAL}/}"          # visits/branch_id=1/date=.../visits.csv
  hdfs_dir="${HDFS_RAW_ROOT}/$(dirname "${rel}")"
  hdfs_path="${HDFS_RAW_ROOT}/${rel}"
  local_in_container="/data/raw/${rel}"

  hdfs -mkdir -p "${hdfs_dir}"
  # -f overwrites if re-run
  hdfs -put -f "${local_in_container}" "${hdfs_path}"
  echo "  PUT ${rel}"
  count=$((count + 1))
done < <(find "${RAW_LOCAL}" -type f -name '*.csv' -print0 | sort -z)

echo
echo "Loaded ${count} file(s) into HDFS ${HDFS_RAW_ROOT}/"
echo
echo "Verify:"
echo "  docker exec ${NAMENODE_CONTAINER} hdfs dfs -ls -R ${HDFS_RAW_ROOT} | head"
echo "  open http://localhost:9870  (HDFS UI; mapped from container 50070)"
echo
echo "Row counts / empties / duplicate patient IDs:"
echo "  python3 jobs/data_check.py --raw data/raw"
