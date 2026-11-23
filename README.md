# ISIT312 Alpha Group Data Lake

A big data prototype for Alpha Group, a healthcare clinic in Saudi Arabia with three branches.

Each branch keeps its own patient records, visits, bills and medical files, so the clinic cannot
easily see how busy each branch is, how much it earned, or all of one patient's files. This project
brings that data into one data lake that runs on a laptop with Docker.

- HDFS stores the raw and cleaned data
- Spark cleans and joins the data and processes live visits
- Kafka carries new visits as they happen
- Hive lets you query the data with SQL
- Power BI shows the dashboard for managers
- Airflow runs the batch pipeline every night

To set up Docker on your laptop, start with [SETUP.md](SETUP.md).

## Architecture

![Architecture diagram](results/Updated%20Architecture%20Diagram%20Simplified.png)

Full version with every script, folder and table:
[Updated Architecture Diagram Detailed.png](results/Updated%20Architecture%20Diagram%20Detailed.png)

How data moves through the lake:

```text
Batch:     CSV files -> /raw -> Spark clean job -> /curated -> Hive tables (alpha.*) -> Power BI
           (run every night at 2am by Airflow)
Streaming: producer -> Kafka topic new-visits -> Spark Streaming -> /curated/visits_live
Files:     scans, lab reports, documents -> /raw/medical_files -> Spark job -> /curated/medical_files
Test:      the same patients per branch job in MapReduce and Spark on 10k, 100k and 300k rows
```

## Team

| Member | Module | Code |
|---|---|---|
| Afrith | Batch ingestion | `jobs/prepare_raw_partitions.py`, `jobs/load_raw_to_hdfs.sh`, `jobs/data_check.py` |
| Wafeeq | Streaming ingestion | `jobs/producer.py`, `jobs/streaming.py`, `jobs/measure_delay.py`, `jobs/create_visits_live_table.sql` |
| Fawaz | Medical files | `jobs/m3/` |
| Naveed | Docker setup, processing, Hive tables, dashboard, scheduler | `docker-compose.yml`, `jobs/m4/`, `airflow/` |
| Shahir | Architecture diagram, MapReduce vs Spark test | `jobs/m5/`, `results/` |
| Sherwin | Documentation and presentation | Report and slides |

## Repository layout

```text
isit312-datalake-project/
├── README.md
├── SETUP.md                      install Docker and start the setup
├── docker-compose.yml            the 8 containers, plus optional Airflow
├── demo/                         short scripts for the class demo
├── airflow/dags/
│   └── alpha_batch_pipeline.py   nightly batch pipeline
├── hadoop.env                    shared Hadoop settings
├── hadoop/Dockerfile             adds Python to the Hadoop container
├── conf/
│   ├── core-site.xml             where HDFS is
│   └── hive-site.xml             where the Hive tables are
├── jobs/                         all code, mounted as /jobs in the containers
│   ├── example_job.py
│   ├── prepare_raw_partitions.py split data by branch and day
│   ├── load_raw_to_hdfs.sh       load data/raw into HDFS /raw
│   ├── data_check.py             row counts, empty fields, duplicate IDs
│   ├── producer.py               send live visits to Kafka
│   ├── streaming.py              Kafka to /curated/visits_live
│   ├── create_visits_live_table.sql
│   ├── measure_delay.py          average send to save delay
│   ├── m3/                       medical files
│   ├── m4/                       clean job, Hive tables, dashboard queries
│   └── m5/                       MapReduce vs Spark test
├── results/                      diagrams, timing results, Power BI dashboard
└── data/                         local data files, mounted as /data (not on GitHub)
```

## Running the pipeline

Run these in order with the setup from [SETUP.md](SETUP.md) running. Steps 1 and 2 can also run
every night with Airflow, see step 7.

### 1. Load the data into HDFS

Download `patients.csv`, `visits.csv` and `billing.csv` from the
[shared Google Drive](https://drive.google.com/drive/folders/1svlCUGZgF8khU83YYHuyOXLwNPH6pgVi)
into `data/m1/`, then run:

```bash
docker exec namenode bash -c 'for n in patients visits billing; do hdfs dfs -mkdir -p /raw/$n && hdfs dfs -put -f /data/m1/$n.csv /raw/$n/; done'
```

Check: `docker exec namenode hdfs dfs -ls /raw` lists `billing`, `patients` and `visits`.

Check the data (needs `pip install pandas` once):

```bash
python jobs/data_check.py --input data/m1
```

All 3 tables should end with `RESULT: PASS`. Empty `payment_date` on unpaid bills is expected.

Optional split layout, with visits and billing in one folder per branch and day:

```bash
python jobs/prepare_raw_partitions.py --incoming data/m1
```

```bash
bash jobs/load_raw_to_hdfs.sh --fresh
```

`--fresh` deletes `/raw` first. Loading about 2,700 small files takes around 15 minutes.
All jobs read either layout.

### 2. Clean the data and create the Hive tables

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m4/clean_batch.py
```

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m4/create_tables.sql
```

The clean job prints rows in and rows out, and the table script prints the row count of each table.
Try a query:

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SELECT branch_id, COUNT(*) AS visits, ROUND(SUM(amount),2) AS revenue FROM alpha.visits_billing GROUP BY branch_id;"
```

When the raw data changes, run both commands again in this order. If you skip the table script,
Hive leaves out any new branch without an error.

### 3. Medical files

Needs `patients.csv` in `data/m1/` and `pip install pillow reportlab` once.

```bash
python jobs/m3/generate_medical_files.py
```

This writes 350 files and `metadata.csv` to `data/m3/`. Then upload them and create the tables:

```bash
docker exec namenode python3 /jobs/m3/file_upload.py
```

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m3/scan_table.py
```

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/m3_table_creation.sql
```

Show every file and visit for one patient across all branches:

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/demo_patient_files.sql
```

### 4. Stream live visits

Needs `pip install pandas kafka-python` once. Use two terminals.

Terminal 1, start the streaming job:

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /jobs/streaming.py
```

Terminal 2, send 100 new visits (needs `visits.csv` in `data/m1/`):

```bash
python jobs/producer.py
```

Visit IDs continue from `V324807` and never repeat between runs. Then stop the streaming job with
Ctrl+C, create the Hive table and measure the delay:

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/create_visits_live_table.sql
```

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/measure_delay.py
```

To start again from scratch:
`docker exec namenode hdfs dfs -rm -r -skipTrash /curated/visits_live /checkpoints/visits_live`

### 5. MapReduce vs Spark

```bash
docker exec namenode hadoop jar /opt/hadoop-2.7.4/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar -files /jobs/m5/branch_count_mapper.py,/jobs/m5/branch_count_reducer.py -mapper "python3 branch_count_mapper.py" -reducer "python3 branch_count_reducer.py" -input /raw/patients -output /results/branch_count
```

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m5/branch_count_spark.py --input /raw/patients --output /results/spark/branch_count
```

Both give the number of patients per branch and the numbers match. Hadoop will not overwrite an
output folder, so delete it first with `docker exec namenode hdfs dfs -rm -r /results/branch_count`.

Timing test on 10k, 100k and 300k patients (PowerShell, from the project folder):

```bash
python jobs/m5/prepare_benchmark_data.py --no-expansion
```

```bash
powershell -ExecutionPolicy Bypass -File jobs/m5/run_hadoop_spark_benchmark.ps1
```

It runs both jobs 3 times on each size, checks they give the same counts and writes the median
times to `results/mapreduce_vs_spark.csv`.

### 6. Dashboard

The dashboard is in `results/dashboard/`. Open the `.pbix` in Power BI Desktop or the `.pdf`
anywhere. It uses the 4 queries in [`jobs/m4/dashboard_queries.sql`](jobs/m4/dashboard_queries.sql):

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m4/dashboard_queries.sql
```

To refresh the dashboard from Hive (only needed if the data changes):

1. Install the Cloudera ODBC Driver for Apache Hive, 64-bit
   (https://www.cloudera.com/downloads/connectors/hive/odbc.html).
2. Open ODBC Data Sources (64-bit), add a System DSN with the Cloudera driver:

   | Setting | Value |
   |---|---|
   | Data Source Name | `AlphaHive` |
   | Hive Server Type | Hive Server 2 |
   | Host / Port / Database | `localhost` / `10000` / `alpha` |
   | Mechanism | User Name, user `hive`, no password |
   | Thrift Transport | SASL |
   | SSL | off |

3. In Power BI, go to File > Options > Data Load and turn off parallel loading of tables and
   background data preview. Hive fails when it gets several queries at once.
4. Click Refresh.

### 7. Schedule the batch pipeline with Airflow

Airflow runs steps 1 and 2 every night at 2am in order and stops if a step fails. The pipeline is
[`airflow/dags/alpha_batch_pipeline.py`](airflow/dags/alpha_batch_pipeline.py):

```text
skip_late_runs -> prepare_raw -> data_check -> data_check_gate -> load_raw_to_hdfs -> spark_clean -> hive_tables -> check_counts
```

Airflow does not start with `docker compose up -d` because it needs about 1.4 GB of memory.
Start it on its own:

```bash
docker compose --profile airflow up -d airflow
```

1. After about a minute open http://localhost:8090 and log in with `admin` / `admin`.
2. Click `alpha_batch_pipeline`, then Trigger DAG at the top right.
3. Open the Graph tab. Each box turns green when its step finishes. Click a box and then Logs to
   see its output.

All 8 steps finish in 4 to 5 minutes on the full data. `check_counts` shows
108,481 / 108,279 / 108,046 visits for branches 1 / 2 / 3.

Notes:

- The scheduled run is at 2am UTC once the DAG is switched on. If the laptop was off at 2am,
  `skip_late_runs` skips the late run so you never get two runs in a row.
- Each step runs with `docker exec` in `namenode`, `spark-master` or `hive-server`, the same as the
  commands in steps 1 and 2.
- The streaming job uses all Spark cores. Run the pipeline before starting the stream.
- The load step reloads `/raw/patients`, `/raw/visits` and `/raw/billing`. Medical files are not touched.
- Stop Airflow with `docker compose stop airflow`.

## Team conventions

Everyone writes to these names so the modules join up.

### HDFS folders

| Folder | Owner | Contents | Format |
|---|---|---|---|
| `/raw/patients/` | Afrith | Patient data | CSV |
| `/raw/visits/` | Afrith | Visit data | CSV |
| `/raw/billing/` | Afrith | Billing data | CSV |
| `/raw/medical_files/` | Fawaz | Scans, lab reports and documents in `branch=<id>/date=<yyyy-MM-dd>/` folders | Image / PDF |
| `/raw/files_metadata/` | Fawaz | `metadata.csv`, which patient, branch and date each file belongs to | CSV |
| `/curated/patients/` | Naveed | Cleaned patients, one folder per branch | Parquet |
| `/curated/visits_billing/` | Naveed | Cleaned visits joined with patients and billing | Parquet |
| `/curated/visits_live/` | Wafeeq | Live visits from Kafka | Parquet |
| `/curated/medical_files/` | Fawaz | One row per file, linked to its patient, plus `/curated/scans/` and `/curated/reports/` | Parquet |
| `/checkpoints/` | Wafeeq | Spark Streaming progress | |
| `/results/` | Shahir | MapReduce and Spark output | Text |

Raw data can be one file per folder or split into subfolders such as
`/raw/visits/branch_id=1/date=2026-11-01/visits.csv`. All jobs read the whole folder.

### Hive tables (database `alpha`)

| Table | Owner | Built from | One row per |
|---|---|---|---|
| `alpha.patients` | Naveed | `/curated/patients/` | patient |
| `alpha.visits_billing` | Naveed | `/curated/visits_billing/` | visit, with its patient and bill |
| `alpha.visits_live` | Wafeeq | `/curated/visits_live/` | live visit |
| `alpha.medical_files` | Fawaz | `/curated/medical_files/` | file, linked to its patient |
| `alpha.scans` | Fawaz | `/curated/scans/` | X-ray or MRI image |
| `alpha.reports` | Fawaz | `/curated/reports/` | lab report or document |

- In `alpha.visits_billing`, `branch_id` is where the visit happened and `home_branch_id` is the
  patient's home branch. It also has `visit_ts`, `visit_date` and `visit_hour`.
- `alpha.medical_files` has `file_id, patient_id, file_date, file_type, hdfs_path, branch_id`.
  `file_type` is `xray`, `mri`, `lab_report` or `document`.
- The tables only point at HDFS folders, so dropping a table never deletes data.

### Kafka

- Topic `new-visits`, created the first time it is used.
- One JSON message per visit with the visit columns plus `sent_timestamp`, for example
  `{"visit_id":"V000001","patient_id":"P018087","branch_id":2,"visit_datetime":"2025-08-08 09:09:25","doctor_id":"D016","diagnosis":"GERD","sent_timestamp":1794139629.55}`
- Address `kafka:9092` from Spark inside Docker and `localhost:29092` from your laptop.
- Messages are wiped when Docker restarts.

### Column names

| File | Columns |
|---|---|
| `patients.csv` | `patient_id, first_name, last_name, dob, gender, branch_id, contact_number, email, address` |
| `visits.csv` | `visit_id, patient_id, branch_id, visit_datetime, doctor_id, diagnosis` |
| `billing.csv` | `bill_id, visit_id, patient_id, amount, payment_status, payment_date` |

- IDs have 6 digits, such as `P000001`, `V000001` and `B000001`. Branches are `1`, `2` and `3`.
- In `patients.csv`, `branch_id` is the home branch. In `visits.csv` it is the branch visited.
- Dates are `yyyy-MM-dd` and date times are `yyyy-MM-dd HH:mm:ss`.
- `payment_status` is `Paid`, `Partial` or `Unpaid`. Unpaid bills have no `payment_date`.
