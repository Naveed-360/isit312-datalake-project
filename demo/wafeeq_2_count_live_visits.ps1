# Wafeeq, live step 2: count the live visits that reached Hive
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true -e "SELECT COUNT(*) AS live_visits FROM alpha.visits_live;" 2>$null | Select-String -NotMatch '^null|^\s*$'
