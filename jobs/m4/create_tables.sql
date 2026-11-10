-- M4: Hive tables on the curated zone.
-- The tables only point at the Parquet folders written by clean_batch.py,
-- so dropping a table never deletes data (EXTERNAL).
--
-- Run (after clean_batch.py):
--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m4/create_tables.sql
-- Safe to run again at any time, e.g. after clean_batch.py has been rerun on new data.

CREATE DATABASE IF NOT EXISTS alpha;
USE alpha;

-- Drop and recreate so the definition always matches this file.
-- EXTERNAL tables: dropping removes only the table definition, never the data.
DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS visits_billing;

-- One row per patient. branch_id = the patient's home branch.
CREATE EXTERNAL TABLE patients (
  patient_id      STRING,
  first_name      STRING,
  last_name       STRING,
  dob             DATE,
  gender          STRING,
  contact_number  STRING,
  email           STRING,
  address         STRING
)
PARTITIONED BY (branch_id INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/curated/patients';

-- One row per visit, with the patient's details and the visit's bill.
-- branch_id = the branch where the visit happened; home_branch_id = the patient's home branch.
CREATE EXTERNAL TABLE visits_billing (
  visit_id        STRING,
  patient_id      STRING,
  doctor_id       STRING,
  diagnosis       STRING,
  visit_ts        TIMESTAMP,
  visit_date      DATE,
  visit_hour      INT,
  first_name      STRING,
  last_name       STRING,
  dob             DATE,
  gender          STRING,
  home_branch_id  INT,
  bill_id         STRING,
  amount          DOUBLE,
  payment_status  STRING,
  payment_date    DATE
)
PARTITIONED BY (branch_id INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/curated/visits_billing';

-- Register the branch_id=1/2/3 folders (also picks up new branches after a rerun)
MSCK REPAIR TABLE patients;
MSCK REPAIR TABLE visits_billing;

-- Quick check
SELECT 'patients' AS tbl, COUNT(*) AS row_count FROM patients
UNION ALL
SELECT 'visits_billing', COUNT(*) FROM visits_billing;
