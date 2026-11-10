-- M3 demo: every scan, lab report and document for ONE patient, across all branches.
-- Change the patient id on both lines to try another patient.
-- Run:
--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/patient_files_demo.sql
USE alpha;

SELECT 'scan' AS kind, file_id, file_type, branch_id, file_date, hdfs_path
FROM scans
WHERE patient_id = 'P00001'
UNION ALL
SELECT 'report' AS kind, file_id, file_type, branch_id, file_date, hdfs_path
FROM reports
WHERE patient_id = 'P00001';