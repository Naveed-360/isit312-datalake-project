# Fawaz: every file and every visit for patient P000005, across all branches
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true -f /jobs/m3/demo_patient_files.sql 2>$null | Select-String -NotMatch '^null|^\s*$'
