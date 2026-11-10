-- M3: Hive tables linking scans, lab reports and documents to patients.
-- They only point at the Parquet folders written by scan_table.py,
-- so dropping a table never deletes data (EXTERNAL).
--
--   alpha.medical_files = ALL files, one row per file (the team convention)
--   alpha.scans         = X-ray and MRI only
--   alpha.reports       = lab reports and documents only
--
-- Run (after scan_table.py):
--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/m3_table_creation.sql
-- Safe to run again at any time.

CREATE DATABASE IF NOT EXISTS alpha;
USE alpha;

DROP TABLE IF EXISTS scans;
DROP TABLE IF EXISTS reports;
DROP TABLE IF EXISTS medical_files;

-- All files. file_type is one of: xray, mri, lab_report, document.
-- branch_id = the branch that holds the file.
CREATE EXTERNAL TABLE medical_files (
  file_id     STRING,
  patient_id  STRING,
  file_date   DATE,
  file_type   STRING,
  hdfs_path   STRING
)
PARTITIONED BY (branch_id INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/curated/medical_files';

-- X-ray and MRI images.
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

MSCK REPAIR TABLE medical_files;
MSCK REPAIR TABLE scans;
MSCK REPAIR TABLE reports;

-- Quick check
SELECT 'medical_files' AS tbl, COUNT(*) AS row_count FROM medical_files
UNION ALL
SELECT 'scans', COUNT(*) FROM scans
UNION ALL
SELECT 'reports', COUNT(*) FROM reports;
