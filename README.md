# ISIT312 – Alpha Group Data Lake

A big data prototype for **Alpha Group**, a healthcare clinic in Saudi Arabia with 3 branches.

**The problem:** each branch keeps its own patient records, visits, bills and medical files
in different places and formats, so the clinic can't easily answer simple questions like
"how busy is each branch?", "how much did we earn this month?" or "show me all of this
patient's files".

**Our solution:** one central **data lake** that collects all of that data, cleans it,
and makes it queryable. We built a working version that runs on any laptop with Docker:

- **HDFS** stores everything: raw data in, cleaned data out
- **Spark** cleans and joins the data, and processes live visits
- **Kafka** carries new visits as they happen
- **Hive** lets anyone query the data with SQL
- **Power BI** shows the dashboard the clinic's managers would use
- **Airflow** runs the batch pipeline every night on a schedule

> **New to the project?** Start with **[SETUP.md](SETUP.md)** to get Docker running on your laptop.

ISIT312 Big Data Management · Presentation Wed 25 Nov 2026

---

## Architecture

![Architecture diagram](results/Updated%20Architecture%20Diagram%20Simplified.png)

Both diagrams were updated on 19 Nov to show Airflow, the nightly batch scheduler.

The full version, with every script, folder and table:
[Updated Architecture Diagram Detailed.png](results/Updated%20Architecture%20Diagram%20Detailed.png)

How data moves through the lake:

```text
 Afrith's CSV files ──► /raw ──(Naveed's clean job)──► /curated ──► Hive tables (alpha.*) ──► Power BI dashboard
   └── run every night at 2am by Airflow (airflow/dags/alpha_batch_pipeline.py)

 Afrith's visits ──(Wafeeq's producer)──► Kafka "new-visits" ──(Wafeeq's Spark Streaming)──► /curated/visits_live

 Fawaz's scans, lab reports, documents ──► /raw/medical_files ──(Fawaz's Spark job)──► /curated/medical_files ──► Hive table alpha.medical_files

 Shahir: the same "patients per branch" job in MapReduce and Spark, timed on 10k / 100k / 300k rows
```

---

## Team and modules

| Member | Module | Code | Status (19 Nov) |
|---|---|---|---|
| **Afrith** | Batch ingestion: synthetic patients, visits, billing | `jobs/prepare_raw_partitions.py`, `jobs/load_raw_to_hdfs.sh`, `jobs/data_check.py` (CSV files in the shared Google Drive) | ✅ Full dataset done: 300k patients, 325k visits, 325k bills (Aug 2025 – Oct 2026). ✅ Done: data check passes on all 3 files; split layout prepared and loaded by script |
| **Wafeeq** | Streaming ingestion: live visits through Kafka | `jobs/producer.py`, `jobs/streaming.py`, `jobs/measure_delay.py`, `jobs/create_visits_live_table.sql` | ✅ Done: new visits in Saudi time, unique visit IDs across runs, Hive table, delay 0.50 s over 100 visits |
| **Fawaz** | Medical files: dummy scans, lab reports, documents | `jobs/m3/` | ✅ Done: 350 files for 200 patients, linked to the final 6-digit patient IDs; upload, Spark job, Hive tables, demo |
| **Naveed** | Docker setup, processing, Hive tables, dashboard, scheduler | `docker-compose.yml`, `jobs/m4/`, `airflow/` | ✅ Done: setup, clean job, Hive tables, 4 dashboard queries, Power BI dashboard (`results/dashboard/`), Airflow pipeline (18 Nov) |
| **Shahir** | Architecture diagram, MapReduce vs Spark test | `jobs/m5/`, `results/` | ✅ Done: final diagrams (detailed + simplified, updated 19 Nov with Airflow), both jobs, timing test on 10k / 100k / 300k (all PASS, `results/mapreduce_vs_spark.csv`) |
| **Sherwin** | Documentation and presentation | Report sections, slides, demo talk | ✅ Done: scheduling and limitations slides, lessons learned, demo narration |

---

## Repository layout

```text
isit312-datalake-project/
├── README.md                 ← you are here: what the project is and how to run it
├── SETUP.md                  ← install Docker and start the setup (do this first)
├── docker-compose.yml        ← the 8 containers: HDFS, Hive, Spark, Kafka (+ optional Airflow)
├── demo/                     ← short scripts for the class demo (prepare_demo.ps1 starts everything)
├── airflow/
│   └── dags/
│       └── alpha_batch_pipeline.py ← Naveed: the nightly batch pipeline (Airflow DAG)
├── hadoop.env                ← shared Hadoop settings for the containers
├── hadoop/
│   └── Dockerfile            ← adds Python to the Hadoop container (for MapReduce)
├── conf/
│   ├── core-site.xml         ← tells Spark where HDFS is
│   └── hive-site.xml         ← tells Spark where the Hive tables are
├── jobs/                     ← all code; appears as /jobs inside the containers
│   ├── example_job.py        ← starter Spark job: visits per branch
│   ├── prepare_raw_partitions.py ← Afrith: splits data/m1/ into data/raw/ (visits + billing by branch and day)
│   ├── load_raw_to_hdfs.sh   ← Afrith: loads data/raw/ into HDFS /raw
│   ├── data_check.py         ← Afrith: row counts, empty fields, duplicate IDs
│   ├── producer.py           ← Wafeeq: sends 100 new live visits to Kafka
│   ├── streaming.py          ← Wafeeq: Kafka → /curated/visits_live (Spark Streaming)
│   ├── create_visits_live_table.sql ← Wafeeq: Hive table alpha.visits_live
│   ├── measure_delay.py      ← Wafeeq: average send → save delay
│   ├── m3/
│   │   ├── generate_medical_files.py ← Fawaz: makes the dummy scans, reports, documents + metadata.csv
│   │   ├── file_upload.py           ← Fawaz: uploads scans, reports and metadata.csv to HDFS
│   │   ├── scan_table.py            ← Fawaz: metadata → /curated/medical_files, scans, reports
│   │   ├── m3_table_creation.sql    ← Fawaz: Hive tables alpha.medical_files, alpha.scans, alpha.reports
│   │   └── demo_patient_files.sql   ← Fawaz: every file for one patient, across all branches
│   ├── m4/
│   │   ├── clean_batch.py    ← Naveed: /raw → /curated (clean, de-duplicate, join)
│   │   ├── create_tables.sql ← Naveed: Hive tables alpha.patients, alpha.visits_billing
│   │   └── dashboard_queries.sql ← Naveed: the 4 dashboard queries (Hive SQL)
│   └── m5/
│       ├── branch_count_mapper.py   ← Shahir: MapReduce mapper (Hadoop Streaming)
│       ├── branch_count_reducer.py  ← Shahir: MapReduce reducer
│       ├── branch_count_spark.py    ← Shahir: the same count in Spark
│       ├── prepare_benchmark_data.py      ← Shahir: makes 10k / 100k / 300k patient files
│       └── run_hadoop_spark_benchmark.ps1 ← Shahir: times MapReduce vs Spark on each size
├── results/
│   ├── Updated Architecture Diagram Simplified.png     ← Shahir: diagram for slides (shown above)
│   ├── Updated Architecture Diagram Simplified.drawio  ← Shahir: editable version (draw.io)
│   ├── Updated Architecture Diagram Detailed.png       ← Shahir: full diagram for the report
│   ├── Updated Architecture Diagram Detailed.drawio    ← Shahir: editable version (draw.io)
│   ├── mapreduce_vs_spark.csv                          ← Shahir: timing results (median of 3 runs per size)
│   └── dashboard/
│       ├── isit312_project_dashboard.pbix  ← Naveed: Power BI dashboard (open in Power BI Desktop)
│       └── isit312_project_dashboard.pdf   ← Naveed: the same dashboard as a PDF, for anyone without Power BI
└── data/                     ← your local data files; appears as /data (not on GitHub)
```

---

## Running the pipeline

Do these in order. Your setup must be running ([SETUP.md](SETUP.md)).

> Steps 1 and 2 (the batch part) can also run on their own every night with **Airflow**, see [step 7](#7-schedule-the-batch-pipeline-with-airflow-naveed).

### 1. Load Afrith's data into HDFS

Download `patients.csv`, `visits.csv` and `billing.csv` from the
[shared Google Drive](https://drive.google.com/drive/folders/1svlCUGZgF8khU83YYHuyOXLwNPH6pgVi)
into `data/m1/` in this project, then run:

```bash
docker exec namenode bash -c 'for n in patients visits billing; do hdfs dfs -mkdir -p /raw/$n && hdfs dfs -put -f /data/m1/$n.csv /raw/$n/; done'
```

✅ Check: `docker exec namenode hdfs dfs -ls /raw` lists `billing`, `patients` and `visits`.

**Check the data (Afrith's data check).** Needs `pip install pandas` once. From the project folder:

```bash
python jobs/data_check.py --input data/m1
```

✅ All 3 tables end with `RESULT: PASS` (row counts, empty fields, duplicate IDs). Empty
`payment_date` on Unpaid bills is expected. The report is saved in `data/`.

**Alternative: Afrith's split layout** (visits and billing in one folder per branch and day, as a
real clinic would deliver them). First split the 3 files into `data/raw/` (about 10 seconds):

```bash
python jobs/prepare_raw_partitions.py --incoming data/m1
```

✅ It ends with `Done. 2696 file(s)`: one `patients.csv`, visits by visit date, and billing by
payment date (Unpaid bills go in `date=unknown`). Then load them, from **Git Bash**:

```bash
bash jobs/load_raw_to_hdfs.sh --fresh
```

- `--fresh` deletes `/raw` first. Use it whenever you switch layouts, otherwise old files stay next
  to the new folders and visits appear twice.
- It loads about 2,700 small files, which takes **about 15 minutes**. HDFS is slow with many small
  files (the "small files problem"). All jobs read either layout.

### 2. Clean the data and create the Hive tables (Naveed)

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m4/clean_batch.py
```

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m4/create_tables.sql
```

✅ The clean job prints a summary (rows in → rows out). The table script ends with the
row count of each table. Then try a query:

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SELECT branch_id, COUNT(*) AS visits, ROUND(SUM(amount),2) AS revenue FROM alpha.visits_billing GROUP BY branch_id;"
```

**Whenever the raw data changes, run both again, in this order.** If you skip the table
script, Hive quietly leaves out any new branch and gives no error.

### 3. Medical files (Fawaz)

First make the dummy files. This needs Afrith's `patients.csv` in `data/m1/` and, once,
`pip install pillow reportlab`. From the project folder:

```bash
python jobs/m3/generate_medical_files.py
```

✅ It writes 350 files (200 scans, 100 lab reports, 50 documents) and `metadata.csv` to `data/m3/`,
and ends with `Missing files: 0`. Then upload them, build the table data and create the Hive tables:

```bash
docker exec namenode python3 /jobs/m3/file_upload.py
```

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m3/scan_table.py
```

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/m3_table_creation.sql
```

✅ The upload ends with `Done: … files` and no `WARNING` line, and the table script ends with the
row count of each table. Then run the demo: every file for one patient, across all branches:

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m3/demo_patient_files.sql
```

### 4. Stream live visits (Wafeeq)

Needs `pip install pandas kafka-python` once. Use two terminals.

Terminal 1, start the streaming job (it keeps running):

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /jobs/streaming.py
```

Terminal 2, from the project folder, send live visits. It needs Afrith's `visits.csv` in `data/m1/`:

```bash
python jobs/producer.py
```

✅ It sends 100 **new** visits in about 20 seconds, stamped with Saudi time. Visit IDs carry on
from `V324807` and never repeat between runs (the last ID used is kept in `data/.producer_state.json`).

Then stop the streaming job (Ctrl+C in terminal 1), create the Hive table and measure the delay:

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/create_visits_live_table.sql
```

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/measure_delay.py
```

To start again from scratch, clear the old output first:
`docker exec namenode hdfs dfs -rm -r -skipTrash /curated/visits_live /checkpoints/visits_live`

### 5. MapReduce vs Spark (Shahir)

```bash
docker exec namenode hadoop jar /opt/hadoop-2.7.4/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar -files /jobs/m5/branch_count_mapper.py,/jobs/m5/branch_count_reducer.py -mapper "python3 branch_count_mapper.py" -reducer "python3 branch_count_reducer.py" -input /raw/patients -output /results/branch_count
```

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m5/branch_count_spark.py --input /raw/patients --output /results/spark/branch_count
```

✅ Both print or write the number of patients per branch, and the numbers match.
See the results with `docker exec namenode hdfs dfs -cat /results/branch_count/part-00000`.
Hadoop won't overwrite an output folder: delete it first with
`docker exec namenode hdfs dfs -rm -r /results/branch_count`.

**Timing test on 10k / 100k / 300k patients.** Needs Afrith's `patients.csv` in `data/m1/` and
Python on your laptop. From the project folder in PowerShell:

```bash
python jobs/m5/prepare_benchmark_data.py --no-expansion
```

```bash
powershell -ExecutionPolicy Bypass -File jobs/m5/run_hadoop_spark_benchmark.ps1
```

✅ It runs both jobs 3 times on each size (about 3 minutes), checks they give the same counts, and
writes the median times to `results/mapreduce_vs_spark.csv`. Add `-SparkMaster 'local[*]'` to run
Spark in one process instead of on the Spark cluster.

### 6. Dashboard (Naveed)

The clinic managers' dashboard is in `results/dashboard/`. Open the `.pbix` in Power BI Desktop
(Windows), or the `.pdf` anywhere. It shows patients per branch, monthly revenue per branch,
repeat visits and busiest hours, from the 4 queries in [`jobs/m4/dashboard_queries.sql`](jobs/m4/dashboard_queries.sql).

Run the 4 queries in Hive directly (after step 2):

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/m4/dashboard_queries.sql
```

**To refresh the dashboard from Hive** (only needed if the data changes; the `.pbix` keeps a copy):

1. Install the free **Cloudera ODBC Driver for Apache Hive**, 64-bit
   (https://www.cloudera.com/downloads/connectors/hive/odbc.html, free account needed).
2. Start menu → **ODBC Data Sources (64-bit)** → System DSN → Add → Cloudera ODBC Driver for Apache Hive:

   | Setting | Value |
   |---|---|
   | Data Source Name | `AlphaHive` |
   | Hive Server Type | Hive Server 2 |
   | Host / Port / Database | `localhost` / `10000` / `alpha` |
   | Mechanism | User Name, user `hive`, no password |
   | Thrift Transport | **SASL** |
   | SSL Options | **Enable SSL unticked** |

   **Test** should say `SUCCESS`. Docker must be running.
3. In Power BI: **File → Options → Data Load**, untick **"Enable parallel loading of tables"** (current file)
   and **"Allow data preview to download in the background"** (global). Hive runs each query in its own
   process with little memory, so parallel queries fail with `return code 2` (`OutOfMemoryError` in the Hive log).
4. **Home → Refresh**.

### 7. Schedule the batch pipeline with Airflow (Naveed)

**Airflow** is the job scheduler. It runs the whole batch part (steps 1 and 2) every night at 2am,
in order, and stops if a step fails. The pipeline is one Python file,
[`airflow/dags/alpha_batch_pipeline.py`](airflow/dags/alpha_batch_pipeline.py) (a "DAG"):

```text
skip_late_runs ─► prepare_raw ─► data_check ─► data_check_gate ─► load_raw_to_hdfs ─► spark_clean ─► hive_tables ─► check_counts
                  data/m1 split   Afrith's      stops the run if    full reload of      Naveed's        create_tables  visits and
                  by branch+day   data check    a table FAILS       /raw (in parallel)  clean job       .sql           revenue per branch
```

Airflow is **not started** by `docker compose up -d` (it needs about 1.4 GB of memory). Start it on its own:

```bash
docker compose --profile airflow up -d airflow
```

1. Wait about 1 minute, then open **http://localhost:8090** and log in with `admin` / `admin` (local only).
2. Click **alpha_batch_pipeline**, then the **▶ (Trigger DAG)** button at the top right. This also switches the DAG on.
3. Open the **Graph** tab: each box turns green as its step finishes. Click a box → **Logs** to see its output,
   e.g. the PASS lines from the data check or the row counts from the clean job.

✅ Check: all 8 boxes green after **4 to 5 minutes** on the full data (tested 3 times on 18 Nov 2026):

| Step | Time |
|---|---|
| prepare_raw | 45 s |
| data_check + gate | 1 min 25 s |
| load_raw_to_hdfs (about 2,700 files) | 40 s to 1 min 40 s |
| spark_clean | 54 s |
| hive_tables + check_counts | 15 s |

`check_counts` shows 108,481 / 108,279 / 108,046 visits for branches 1 / 2 / 3.

Good to know:

- **The scheduled run** is at 2am UTC every night once the DAG is switched on (the toggle next to its name).
  If the laptop was off at 2am, Airflow tries to catch up as soon as it starts; `skip_late_runs` skips that
  late run (it shows pink, "skipped"), so you never get two runs in a row. The ▶ button always runs.
- **How it reaches our containers:** the Airflow container has the Docker socket mounted and runs each step
  with `docker exec` in `namenode`, `spark-master` or `hive-server`, exactly like the commands in steps 1 and 2.
  The first two steps run inside the Airflow container itself (it has Python and pandas).
- **Streaming uses all Spark cores.** If Wafeeq's streaming job is running, `spark_clean` waits for it.
  Run the pipeline before starting the stream, or stop the stream first.
- **Load is a full reload:** `/raw/patients`, `/raw/visits` and `/raw/billing` are deleted and loaded again.
  The medical files in `/raw/medical_files` are not touched.
- **To stop Airflow:** `docker compose stop airflow`. Its run history is kept until the container is removed.

---

## Team conventions

Everyone writes to these exact names, so the modules join up.

### HDFS folders

| Folder | Owner | What goes there | Format |
|---|---|---|---|
| `/raw/patients/` | Afrith | Patient data | CSV |
| `/raw/visits/` | Afrith | Visit data | CSV |
| `/raw/billing/` | Afrith | Billing data | CSV |
| `/raw/medical_files/` | Fawaz | Dummy X-rays, MRIs, lab reports and documents, in `branch=<id>/date=<yyyy-MM-dd>/` folders | Image / PDF |
| `/raw/files_metadata/` | Fawaz | `metadata.csv`: which patient, branch and date each file belongs to | CSV |
| `/curated/patients/` | Naveed | Cleaned patients, one folder per branch | Parquet |
| `/curated/visits_billing/` | Naveed | Cleaned visits joined with patients and billing, one folder per branch | Parquet |
| `/curated/visits_live/` | Wafeeq | Visits arriving live from Kafka | Parquet |
| `/curated/medical_files/` | Fawaz | One row per file, linked to its patient, one folder per branch. Also `/curated/scans/` (X-ray, MRI) and `/curated/reports/` (lab reports, documents) | Parquet |
| `/checkpoints/` | Wafeeq | Spark Streaming progress (don't touch) | – |
| `/results/` | Shahir | MapReduce and Spark output | Text |

Raw data can be one file per folder or split into subfolders (for example
`/raw/visits/branch_id=1/date=2026-11-01/visits.csv`). All jobs read the whole folder either way.

### Hive tables (database `alpha`)

| Table | Owner | Built from | One row per |
|---|---|---|---|
| `alpha.patients` | Naveed | `/curated/patients/` | patient |
| `alpha.visits_billing` | Naveed | `/curated/visits_billing/` | visit, with its patient and bill |
| `alpha.visits_live` | Wafeeq | `/curated/visits_live/` | live visit |
| `alpha.medical_files` | Fawaz | `/curated/medical_files/` | file, linked to its patient |
| `alpha.scans` | Fawaz | `/curated/scans/` | X-ray or MRI image |
| `alpha.reports` | Fawaz | `/curated/reports/` | lab report or document |

- `alpha.visits_billing`: `branch_id` is the branch where the visit happened and `home_branch_id`
  is the patient's home branch. It also has `visit_ts`, `visit_date` and `visit_hour`.
- `alpha.medical_files` columns: `file_id, patient_id, file_date, file_type, hdfs_path, branch_id`.
  `file_type` is one of `xray`, `mri`, `lab_report`, `document`. Scans, lab reports and documents all
  go in this one table, so "all files for one patient" is a single query. `branch_id` is the branch
  that holds the file. Join to `alpha.patients` on `patient_id` for the patient's name and home branch.
- The tables only point at the HDFS folders. Dropping a table never deletes data.

### Kafka

- **Topic:** `new-visits` (created automatically the first time anyone uses it)
- **Message:** one JSON object per visit, with the visit columns below plus `sent_timestamp`
  (added by the producer for the delay measurement). Example:
  `{"visit_id":"V000001","patient_id":"P018087","branch_id":2,"visit_datetime":"2025-08-08 09:09:25","doctor_id":"D016","diagnosis":"GERD","sent_timestamp":1794139629.55}`
- **Address:** `kafka:9092` from Spark jobs inside Docker; `localhost:29092` from a Python script on your laptop.
- Messages are temporary. They're wiped when Docker restarts, so don't store anything only in Kafka.

### Column names (Afrith's files)

| File | Columns |
|---|---|
| `patients.csv` | `patient_id, first_name, last_name, dob, gender, branch_id, contact_number, email, address` |
| `visits.csv` | `visit_id, patient_id, branch_id, visit_datetime, doctor_id, diagnosis` |
| `billing.csv` | `bill_id, visit_id, patient_id, amount, payment_status, payment_date` |

- IDs have 6 digits: `P000001`, `V000001`, `B000001`. Branches: `1`, `2`, `3`.
- In `patients.csv`, `branch_id` is the home branch. In `visits.csv`, it's the branch visited.
- Dates: `yyyy-MM-dd`. Date and time: `yyyy-MM-dd HH:mm:ss`.
- `payment_status` is `Paid`, `Partial` or `Unpaid`. Unpaid bills have no `payment_date`.
