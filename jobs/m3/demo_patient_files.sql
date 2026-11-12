-- M3 demo: every scan, lab report and document for ONE patient, across all branches.
-- Change the patient id to try another patient.
-- Run:
--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/demo_patient_files.sql
USE alpha;

SELECT patient_id, file_type, branch_id, file_date, file_id, hdfs_path
FROM medical_files
WHERE patient_id = 'P000005'
ORDER BY file_type;
