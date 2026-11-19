# Naveed: the SQL layer, all tables in the alpha database
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true -e "SHOW TABLES IN alpha;" 2>$null | Select-String -NotMatch '^null|^\s*$'
