-- M3: Hive tables linking scans, lab reports and documents to patients.
-- They only point at the Parquet folders written by build_scan_tables.py,
-- so dropping a table never deletes data (EXTERNAL).
--
-- Run (after build_scan_tables.py):
--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/create_m3_tables.sql
-- Safe to run again at any time.

CREATE DATABASE IF NOT EXISTS alpha;
USE alpha;

DROP TABLE IF EXISTS scans;
DROP TABLE IF EXISTS reports;

-- X-ray and MRI images. branch_id = the branch that holds the file.
CREATE EXTERNAL TABLE scans (
  file_id     STRING,
  patient_id  STRING,
  file_type   STRING,
  file_name   STRING,
  hdfs_path   STRING,
  file_date   DATE
)
PARTITIONED BY (branch_id INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/curated/scans';

-- Lab reports and medical documents (PDFs).
CREATE EXTERNAL TABLE reports (
  file_id     STRING,
  patient_id  STRING,
  file_type   STRING,
  file_name   STRING,
  hdfs_path   STRING,
  file_date   DATE
)
PARTITIONED BY (branch_id INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/curated/reports';

MSCK REPAIR TABLE scans;
MSCK REPAIR TABLE reports;

-- Quick check
SELECT 'scans' AS tbl, COUNT(*) AS row_count FROM scans
UNION ALL
SELECT 'reports', COUNT(*) FROM reports;