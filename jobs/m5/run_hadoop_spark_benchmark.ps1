param(
    [int]$Runs = 3,                                    # timed runs per size; the median is reported
    [string]$SparkMaster = 'spark://spark-master:7077'  # or 'local[*]' to run Spark in one process
)

# M5: time the same "patients per branch" job in Hadoop Streaming MapReduce and in Spark
# on 10k, 100k and 300k patients, and check that both give the same answer.
#
# Run from the project folder in PowerShell (Docker must be running):
#   python jobs/m5/prepare_benchmark_data.py --no-expansion
#   powershell -ExecutionPolicy Bypass -File jobs/m5/run_hadoop_spark_benchmark.ps1
#
# Each command runs inside its own container with docker exec. The jobs/ and data/ folders
# are shared with the containers as /jobs and /data, so nothing needs copying in.
#
# Output:
#   results/mapreduce_vs_spark.csv   median time per size, plus every individual run
#   data/benchmark/logs/             full Hadoop and Spark logs for each run

$ErrorActionPreference = 'Stop'

$RepoRoot   = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)   # jobs/m5 -> project folder
$DataDir    = Join-Path $RepoRoot 'data/benchmark'
$LogDir     = Join-Path $DataDir 'logs'
$ResultsDir = Join-Path $RepoRoot 'results'
$ResultFile = Join-Path $ResultsDir 'mapreduce_vs_spark.csv'

$NameNode  = 'namenode'
$SparkNode = 'spark-master'
$StreamingJar = '/opt/hadoop-2.7.4/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar'
$Mapper   = '/jobs/m5/branch_count_mapper.py'
$Reducer  = '/jobs/m5/branch_count_reducer.py'
$SparkJob = '/jobs/m5/branch_count_spark.py'
$HdfsBase = '/results/benchmark'

$Sizes = @(
    @{ Label = '10k';  Rows = 10000 },
    @{ Label = '100k'; Rows = 100000 },
    @{ Label = '300k'; Rows = 300000 }
)

New-Item -ItemType Directory -Force -Path $ResultsDir, $LogDir | Out-Null

function Invoke-Docker {
    # Runs a docker command that must succeed; output is discarded.
    param([string[]]$DockerArgs)
    $ErrorActionPreference = 'Continue'   # Hadoop/HDFS print normal INFO lines to stderr
    & docker @DockerArgs *> $null
    if ($LASTEXITCODE -ne 0) { throw "Failed (exit $LASTEXITCODE): docker $($DockerArgs -join ' ')" }
}

function Measure-Job {
    # Runs one timed job, writes its full output to a log, returns milliseconds.
    param([string[]]$DockerArgs, [string]$Log, [string]$Name)
    $ErrorActionPreference = 'Continue'   # Hadoop/Spark print normal INFO lines to stderr
    $Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    & docker @DockerArgs *> $Log
    $ExitCode = $LASTEXITCODE
    $Stopwatch.Stop()
    if ($ExitCode -ne 0) {
        Get-Content -LiteralPath $Log -Tail 30
        throw "$Name failed (exit $ExitCode). Full log: $Log"
    }
    return [math]::Round($Stopwatch.Elapsed.TotalMilliseconds, 0)
}

function Get-HdfsLines {
    param([string]$Path)
    $ErrorActionPreference = 'Continue'
    $Lines = & docker exec $NameNode sh -c "hdfs dfs -cat $Path/part-* 2>/dev/null"
    return @($Lines | Where-Object { $_ -ne '' } | Sort-Object)
}

function Get-Median {
    param([double[]]$Values)
    $Sorted = $Values | Sort-Object
    $Mid = [math]::Floor($Sorted.Count / 2)
    if ($Sorted.Count % 2 -eq 1) { return $Sorted[$Mid] }
    return [math]::Round(($Sorted[$Mid - 1] + $Sorted[$Mid]) / 2, 0)
}

# ---------- checks before any timing ----------
foreach ($Size in $Sizes) {
    $File = Join-Path $DataDir "patients_$($Size.Label).csv"
    if (-not (Test-Path -LiteralPath $File)) {
        throw "Missing $File. Run first: python jobs/m5/prepare_benchmark_data.py --no-expansion"
    }
}
foreach ($Job in 'branch_count_mapper.py', 'branch_count_reducer.py', 'branch_count_spark.py') {
    if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot $Job))) { throw "Missing jobs/m5/$Job" }
}
Invoke-Docker @('exec', $NameNode, 'hdfs', 'dfs', '-ls', '/')
Invoke-Docker @('exec', $SparkNode, 'test', '-x', '/spark/bin/spark-submit')

# ---------- timed runs ----------
$Summary = @()
foreach ($Size in $Sizes) {
    $Label = $Size.Label
    $Rows  = $Size.Rows
    $HdfsInput = "$HdfsBase/input/$Rows"
    $MrOut = "$HdfsBase/mapreduce/$Rows"
    $SpOut = "$HdfsBase/spark/$Rows"

    Write-Host "Loading $Label patients into HDFS ($HdfsInput)..."
    Invoke-Docker @('exec', $NameNode, 'hdfs', 'dfs', '-rm', '-r', '-f', '-skipTrash', $HdfsInput)
    Invoke-Docker @('exec', $NameNode, 'hdfs', 'dfs', '-mkdir', '-p', $HdfsInput)
    Invoke-Docker @('exec', $NameNode, 'hdfs', 'dfs', '-put', "/data/benchmark/patients_$Label.csv", "$HdfsInput/patients.csv")

    $MrTimes = @(); $SpTimes = @(); $Check = 'PASS'
    for ($Run = 1; $Run -le $Runs; $Run++) {
        Invoke-Docker @('exec', $NameNode, 'hdfs', 'dfs', '-rm', '-r', '-f', '-skipTrash', $MrOut, $SpOut)

        $MrMs = Measure-Job -Name "MapReduce $Label run $Run" -Log (Join-Path $LogDir "mapreduce_${Label}_run$Run.log") -DockerArgs @(
            'exec', $NameNode, 'hadoop', 'jar', $StreamingJar,
            '-D', "mapreduce.job.name=M5-patient-branch-count-$Label",
            '-D', 'mapreduce.job.reduces=1',
            '-files', "$Mapper,$Reducer",
            '-mapper', 'python3 branch_count_mapper.py',
            '-reducer', 'python3 branch_count_reducer.py',
            '-input', $HdfsInput,
            '-output', $MrOut)

        $SpMs = Measure-Job -Name "Spark $Label run $Run" -Log (Join-Path $LogDir "spark_${Label}_run$Run.log") -DockerArgs @(
            'exec', $SparkNode, '/spark/bin/spark-submit', '--master', $SparkMaster,
            $SparkJob, '--input', $HdfsInput, '--output', $SpOut)

        # Both must give the same counts per branch
        $MrLines = Get-HdfsLines $MrOut
        $SpLines = Get-HdfsLines $SpOut
        if ($MrLines.Count -eq 0 -or (($MrLines -join "`n") -cne ($SpLines -join "`n"))) {
            $Check = 'FAIL'
            Write-Host "MapReduce: $($MrLines -join ' | ')"
            Write-Host "Spark:     $($SpLines -join ' | ')"
            throw "MapReduce and Spark outputs differ (or are empty) for $Label run $Run"
        }

        $MrTimes += $MrMs; $SpTimes += $SpMs
        Write-Host ("  {0} run {1}/{2}: MapReduce {3} ms, Spark {4} ms, outputs match" -f $Label, $Run, $Runs, $MrMs, $SpMs)
    }

    $Summary += [pscustomobject]@{
        rows                = $Rows
        mapreduce_median_ms = Get-Median $MrTimes
        spark_median_ms     = Get-Median $SpTimes
        mapreduce_runs_ms   = $MrTimes -join ' '
        spark_runs_ms       = $SpTimes -join ' '
        spark_master        = $SparkMaster
        branch_counts       = ($MrLines -replace "`t", '=') -join ' '
        output_check        = $Check
    }
    Write-Host ("{0}: median MapReduce {1} ms, Spark {2} ms" -f $Label, (Get-Median $MrTimes), (Get-Median $SpTimes))
}

$Summary | Export-Csv -LiteralPath $ResultFile -NoTypeInformation -Encoding UTF8
Write-Host ""
$Summary | Format-Table rows, mapreduce_median_ms, spark_median_ms, output_check -AutoSize | Out-String | Write-Host
Write-Host "Results: $ResultFile"
Write-Host "Logs:    $LogDir"
Write-Host "HDFS:    $HdfsBase/{input,mapreduce,spark}/{10000,100000,300000}"
