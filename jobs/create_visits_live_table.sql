-- M2: Hive table for live streaming visits (visits_live).
-- External table pointing at /curated/visits_live written by streaming.py.
--
-- Run:
--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/create_visits_live_table.sql

CREATE DATABASE IF NOT EXISTS alpha;
USE alpha;

DROP TABLE IF EXISTS visits_live;

CREATE EXTERNAL TABLE visits_live (
  visit_id        STRING,
  patient_id      STRING,
  branch_id       INT,
  visit_datetime  STRING,
  doctor_id       STRING,
  diagnosis       STRING,
  sent_timestamp  DOUBLE,
  saved_timestamp DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/curated/visits_live';

-- Quick check
SELECT * FROM visits_live LIMIT 5;
