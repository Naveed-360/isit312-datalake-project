"""
Alpha Group batch pipeline, run by Airflow every night at 2am.

  prepare_raw  ->  data_check (+ gate)  ->  load_raw_to_hdfs  ->  spark_clean  ->  hive_tables  ->  check_counts
  (data/m1 CSVs    (stops the run if      (/raw zone)          (/curated,       (alpha.* tables)  (visits and
   split by         a table FAILS)                              Parquet)                           revenue per
   branch + day)                                                                                   branch)

Tested on the full data (18 Nov 2026): 4 to 5 minutes end to end, nothing lost.

The first two steps run inside the Airflow container (it has pandas).
The others run inside our lake containers with `docker exec`, through the
Docker socket mounted into the Airflow container (see docker-compose.yml).

Open http://localhost:8090 , then press the play button on alpha_batch_pipeline.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.timetables.trigger import CronTriggerTimetable

PROJECT = "/project"          # ./jobs and ./data are mounted here
INCOMING = f"{PROJECT}/data/m1"
RAW_LOCAL = f"{PROJECT}/data/raw"
BEELINE = "beeline -u jdbc:hive2://localhost:10000 --silent=true"

LOAD_RAW = r"""
set -e
hdfs dfs -rm -r -f -skipTrash /raw/patients /raw/visits /raw/billing
hdfs dfs -mkdir -p /raw/patients /raw/visits /raw/billing
pids=""
hdfs dfs -put /data/raw/patients/* /raw/patients/ & pids="$pids $!"
for table in visits billing; do
  for part in /data/raw/$table/*; do
    hdfs dfs -put "$part" /raw/$table/ & pids="$pids $!"
  done
done
for p in $pids; do wait $p; done
hdfs dfs -count /raw/patients /raw/visits /raw/billing
"""


def docker_exec(container, command):
    """Run a shell command in one of our containers, print its output, fail on a non zero exit."""
    import docker

    client = docker.from_env()
    exec_id = client.api.exec_create(client.containers.get(container).id, ["bash", "-c", command])
    print(f"[{container}] $ {command}")
    for chunk in client.api.exec_start(exec_id, stream=True):
        print(chunk.decode(errors="replace"), end="")
    code = client.api.exec_inspect(exec_id)["ExitCode"]
    if code != 0:
        raise AirflowException(f"{container} exited with code {code}")


def on_time_or_manual(dag_run, logical_date, **_):
    """Airflow starts a catch-up run for last night as soon as the DAG is switched on.
    Skip a scheduled run that starts more than 2 hours late (laptop was off at 2am),
    so switching on and pressing play runs the pipeline once, not twice.
    Manual runs (the play button) always go ahead."""
    if dag_run.run_type != "scheduled":
        return True
    late = datetime.now(timezone.utc) - logical_date
    print(f"Scheduled for {logical_date}, starting {late} late")
    return late < timedelta(hours=2)


def check_data_report():
    """data_check.py writes a report but always exits 0, so read the report and stop on FAIL."""
    report = json.loads(Path(f"{PROJECT}/data/data_check_latest.json").read_text())
    failed = []
    for table in report["tables"]:
        print(f"{table['table']}: {table['status']} rows={table['rows']:,}")
        if table["status"] == "FAIL":
            failed.append(f"{table['table']} ({', '.join(table['fail_reasons'])})")
    if failed:
        raise AirflowException("Data check failed, raw zone not loaded: " + "; ".join(failed))


with DAG(
    dag_id="alpha_batch_pipeline",
    description="Nightly batch: CSVs -> HDFS raw -> Spark clean -> curated Parquet -> Hive",
    schedule=CronTriggerTimetable("0 2 * * *", timezone="UTC"),   # every night at 2am UTC
    start_date=datetime(2026, 11, 1),
    catchup=False,                        # never replay missed nights
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["isit312", "batch"],
) as dag:

    skip_late_runs = ShortCircuitOperator(
        task_id="skip_late_runs",
        python_callable=on_time_or_manual,
    )

    prepare_raw = BashOperator(
        task_id="prepare_raw",
        bash_command=f"python {PROJECT}/jobs/prepare_raw_partitions.py --incoming {INCOMING} --out {RAW_LOCAL}",
    )

    data_check = BashOperator(
        task_id="data_check",
        bash_command=f"python {PROJECT}/jobs/data_check.py --raw {RAW_LOCAL}",
    )

    data_check_gate = PythonOperator(
        task_id="data_check_gate",
        python_callable=check_data_report,
    )

    # Like jobs/load_raw_to_hdfs.sh, run inside the namenode (./data is /data there).
    # Full reload of the three batch tables only (medical files in /raw are left alone).
    # One put per table and branch, run in parallel: thousands of small files load
    # about 5 times faster than with a single put.
    load_raw_to_hdfs = PythonOperator(
        task_id="load_raw_to_hdfs",
        python_callable=docker_exec,
        op_args=["namenode", LOAD_RAW],
    )

    spark_clean = PythonOperator(
        task_id="spark_clean",
        python_callable=docker_exec,
        op_args=["spark-master", "/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m4/clean_batch.py"],
        execution_timeout=timedelta(minutes=30),
    )

    hive_tables = PythonOperator(
        task_id="hive_tables",
        python_callable=docker_exec,
        op_args=["hive-server", f"{BEELINE} -f /jobs/m4/create_tables.sql"],
    )

    check_counts = PythonOperator(
        task_id="check_counts",
        python_callable=docker_exec,
        op_args=[
            "hive-server",
            f'{BEELINE} -e "SELECT branch_id, COUNT(*) AS visits, ROUND(SUM(amount),2) AS revenue '
            f'FROM alpha.visits_billing GROUP BY branch_id ORDER BY branch_id;"',
        ],
    )

    skip_late_runs >> prepare_raw >> data_check >> data_check_gate >> load_raw_to_hdfs >> spark_clean >> hive_tables >> check_counts
