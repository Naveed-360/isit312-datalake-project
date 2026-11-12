--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/demo_patient_files.sql
USE alpha;

-- 1. All of the patient's files (scans, lab reports, documents), with the patient's name
SELECT p.patient_id,
       p.first_name,
       p.last_name,
       p.branch_id    AS home_branch,
       f.file_type,
       f.branch_id    AS file_branch,
       f.file_date,
       f.hdfs_path
FROM patients p
JOIN medical_files f ON p.patient_id = f.patient_id
WHERE p.patient_id = 'P000005'
ORDER BY f.file_type;

-- 2. The same patient's visits: which branch each one happened at
SELECT v.patient_id,
       v.visit_id,
       v.branch_id    AS visit_branch,
       v.visit_date,
       v.diagnosis,
       v.amount,
       v.payment_status
FROM visits_billing v
WHERE v.patient_id = 'P000005'
ORDER BY v.visit_date;