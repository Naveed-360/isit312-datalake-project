# ISIT312 demo prep. Run this about 15 minutes before presenting:
#   powershell -ExecutionPolicy Bypass -File demo\prepare_demo.ps1
#
# It starts everything, runs the Airflow batch pipeline once, starts the live
# streaming job in its own window, creates the live Hive table and opens every
# tab in demo order. It stops with a red FAIL line if anything is wrong.

$ErrorActionPreference = 'Continue'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

function Step($m) { Write-Host "`n== $m" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "   OK   $m" -ForegroundColor Green }
function Fail($m) { Write-Host "   FAIL $m" -ForegroundColor Red; exit 1 }

# 1. Docker -------------------------------------------------------------------
Step "1/7 Docker"
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    Write-Host "   starting Docker Desktop..."
    for ($i = 0; $i -lt 60; $i++) { Start-Sleep 5; docker info *> $null; if ($LASTEXITCODE -eq 0) { break } }
    if ($LASTEXITCODE -ne 0) { Fail "Docker Desktop did not start. Open it by hand and run this again." }
}
Ok "Docker is running"

# 2. Containers ---------------------------------------------------------------
Step "2/7 Containers"
$supabase = docker ps -q --filter "name=supabase_"
if ($supabase) { docker stop $supabase *> $null; Ok "stopped Supabase to free memory" }
docker start namenode datanode hive-metastore-postgresql hive-metastore hive-server spark-master spark-worker kafka airflow *> $null
$running = (docker ps --format "{{.Names}}" | Where-Object { $_ -notlike "supabase*" }).Count
if ($running -lt 9) { Fail "only $running of 9 containers are running. Check Docker Desktop." }
Ok "9 containers running"

for ($i = 0; $i -lt 60; $i++) {
    $sm = docker exec namenode hdfs dfsadmin -safemode get 2>$null
    if ("$sm" -match "OFF") { break }; Start-Sleep 3
}
if ("$sm" -notmatch "OFF") { Fail "HDFS did not finish starting." }
Ok "HDFS ready"

# 3. Stop an old streaming job so the batch pipeline can use Spark ------------
Step "3/7 Spark"
docker exec spark-master pkill -f streaming.py *> $null
Start-Sleep 5
Ok "Spark free for the batch run"

# 4. Airflow: run the batch pipeline once -------------------------------------
Step "4/7 Airflow batch pipeline (about 5 minutes)"
$web = $false
for ($i = 0; $i -lt 24; $i++) {
    try { $h = Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 http://localhost:8090/health; if ($h.StatusCode -eq 200) { $web = $true; break } } catch {}
    Start-Sleep 5
}
if (-not $web) { Fail "the Airflow web page did not start. Run: docker restart airflow   then run this script again." }
Ok "Airflow web page is up"
docker exec airflow airflow dags unpause alpha_batch_pipeline *> $null
$runId = "demo_" + (Get-Date -Format "yyyyMMdd_HHmmss")
docker exec airflow airflow dags trigger alpha_batch_pipeline -r $runId *> $null
if ($LASTEXITCODE -ne 0) { Fail "could not start the Airflow pipeline. Open http://localhost:8090 and press play." }
Ok "pipeline started ($runId)"

$state = ""
for ($i = 0; $i -lt 120; $i++) {
    Start-Sleep 10
    $runs = docker exec airflow airflow dags list-runs -d alpha_batch_pipeline -o json 2>$null | Out-String
    try { $state = (($runs | ConvertFrom-Json) | Where-Object { $_.run_id -eq $runId }).state } catch { $state = "" }
    Write-Host "   ... $state ($(($i + 1) * 10) s)"
    if ($state -eq "success" -or $state -eq "failed") { break }
}
if ($state -ne "success") { Fail "the Airflow run did not finish green (state: $state). Open http://localhost:8090 to see which step is red." }
Ok "all 8 steps green"

# 5. Streaming job in its own window ------------------------------------------
Step "5/7 Live streaming job"
$streamCmd = "`$host.UI.RawUI.WindowTitle = 'STREAMING - leave open'; docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /jobs/streaming.py"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $streamCmd -WindowStyle Minimized
$up = $false
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep 5
    try { $j = Invoke-RestMethod -TimeoutSec 5 http://localhost:8080/json/; if ($j.activeapps.Count -gt 0) { $up = $true; break } } catch {}
}
if (-not $up) { Fail "the streaming job did not start. Look at the minimised STREAMING window." }
Start-Sleep 15
Ok "streaming job running (minimised window STREAMING, do not close it)"

# 6. Hive tables --------------------------------------------------------------
Step "6/7 Hive"
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true -f /jobs/create_visits_live_table.sql *> $null
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true -f /jobs/m3/m3_table_creation.sql *> $null
$tables = docker exec hive-server beeline -u jdbc:hive2://localhost:10000 --silent=true -e "SHOW TABLES IN alpha;" 2>$null | Out-String
foreach ($t in "patients", "visits_billing", "visits_live", "medical_files", "scans", "reports") {
    if ($tables -notmatch "\b$t\b") { Fail "Hive table alpha.$t is missing." }
}
Ok "all 6 Hive tables exist"

# 7. Open the tabs in demo order ----------------------------------------------
Step "7/7 Opening tabs"
Start-Process "http://localhost:8090/dags/alpha_batch_pipeline/graph"
Start-Process "http://localhost:50070/explorer.html#/raw/visits/branch_id=1"
Start-Process "http://localhost:50070/explorer.html#/curated/visits_billing"
Start-Process "http://localhost:50070/explorer.html#/raw/medical_files"
Start-Process "http://localhost:8080"
$pbix = Join-Path $root "results\dashboard\isit312_project_dashboard.pbix"
if (Test-Path $pbix) { Start-Process $pbix }
Ok "Airflow, HDFS raw, HDFS curated, HDFS medical files, Spark UI and Power BI opened"

Write-Host "`nREADY. Airflow login is admin / admin." -ForegroundColor Green
