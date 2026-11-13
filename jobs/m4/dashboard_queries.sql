-- M4 (Naveed): the 4 dashboard queries for the clinic managers' dashboard.
-- They run on M4's Hive tables (alpha.patients, alpha.visits_billing), so run
-- clean_batch.py and create_tables.sql first.
--
-- Run all 4:
--   docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m4/dashboard_queries.sql
-- In Power BI, paste each query on its own (Get Data → ODBC → Advanced options → SQL statement).

USE alpha;

-- 1. Patients per branch (patients counted at their home branch)
SELECT branch_id,
       COUNT(*) AS patients
FROM alpha.patients
GROUP BY branch_id
ORDER BY branch_id;

-- 2. Daily revenue per branch (billed amount, by the day and branch of the visit)
SELECT visit_date,
       branch_id,
       COUNT(*)              AS visits,
       ROUND(SUM(amount), 2) AS revenue
FROM alpha.visits_billing
GROUP BY visit_date, branch_id
ORDER BY visit_date, branch_id;

-- 3. Repeat visits: how many patients made 1 visit, 2 visits, and so on
SELECT visits,
       COUNT(*) AS patients
FROM (
    SELECT patient_id,
           COUNT(*) AS visits
    FROM alpha.visits_billing
    GROUP BY patient_id
) per_patient
GROUP BY visits
ORDER BY visits;

-- 4. Busiest hours: visits per hour of the day, per branch
SELECT visit_hour,
       branch_id,
       COUNT(*) AS visits
FROM alpha.visits_billing
GROUP BY visit_hour, branch_id
ORDER BY visit_hour, branch_id;
