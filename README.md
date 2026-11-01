# ISIT312 – Alpha Group Data Lake

This repo gives every team member the **same big data setup on their own laptop**:
HDFS (storage), Hive (SQL tables), Spark (processing) and Kafka (live data).
Because we all run the same setup, each person's module will plug into the others
at the integration call.

Follow the steps in order. It takes about **30 minutes**, most of it waiting for downloads.
If you get stuck, see [Problems](#problems) at the bottom or message M4 (Naveed).

---

## Step 1 – Install the tools (one time only)

You need two programs:

| Program | Download | Check it works |
|---|---|---|
| **Docker Desktop** | https://www.docker.com/products/docker-desktop/ | Open it. The whale icon appears in your taskbar (Windows) or menu bar (Mac). |
| **Git** | https://git-scm.com/downloads | Run `git --version` in a terminal. It should print a version number. |

**Which terminal to use:**
- **Windows:** use **PowerShell** (Start menu → type "PowerShell").
  Don't use Git Bash. It changes paths and the `docker exec` commands below fail.
- **Mac:** use **Terminal** (Applications → Utilities → Terminal).

## Step 2 – Give Docker enough memory (one time only)

The setup runs 8 programs at once, so Docker needs more memory than its default.

1. Open Docker Desktop.
2. Click the ⚙️ **Settings** icon (top right).
3. Go to **Resources**.
4. Set **Memory** to **8 GB** (or more), then click **Apply & restart**.

> **Windows:** if you don't see a Memory slider, Docker is using WSL 2 and already
> shares your laptop's memory. You can skip this step.
>
> **Mac with an M1/M2/M3/M4 chip:** also go to **Settings → General** and turn on
> **"Use Rosetta for x86/amd64 emulation"**. Some of our images are made for Intel chips.

## Step 3 – Get the project onto your laptop (one time only)

1. Accept the GitHub invite (check your email or https://github.com/notifications).
2. Open your terminal and run these two commands, one at a time:

```bash
git clone https://github.com/Naveed-360/isit312-datalake-project.git
```

```bash
cd isit312-datalake-project
```

You now have a folder called `isit312-datalake-project`. **Always run the commands below
from inside this folder.**

## Step 4 – Start the setup

Make sure Docker Desktop is open, then run:

```bash
docker compose up -d
```

- **First time:** this downloads about **3 GB**, so it can take 10–20 minutes. Use good Wi-Fi.
- **After that:** it starts in under a minute.

Wait **2 minutes** after it finishes so everything can warm up.

## Step 5 – Check it works

Run these checks one at a time. Each one tests a different part.

**a) Are all 8 parts running?**

```bash
docker compose ps
```

✅ You should see 8 rows, and each one says **Up** or **running**:
`namenode, datanode, hive-metastore, hive-metastore-postgresql, hive-server, spark-master, spark-worker, kafka`

**b) HDFS (storage)** – open http://localhost:50070 in your browser.
✅ You see the Hadoop overview page.

**c) Spark (processing)** – open http://localhost:8080 in your browser.
✅ You see the Spark Master page with **1 worker** listed.

**d) Hive (SQL tables)**

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "show databases;"
```

✅ You see a small table that includes `default`. Ignore any lines starting with `SLF4J`.

**e) Kafka (live data)**

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

✅ It finishes without an error. (It may print nothing yet, which is fine.)

**All 5 passed? You're done.** Post "Docker works ✅" in the group chat.

---

## Everyday use

| I want to… | Run this |
|---|---|
| Start the setup | `docker compose up -d` |
| Stop it (frees up memory, keeps your data) | `docker compose down` |
| See what's running | `docker compose ps` |
| Get the latest changes from the team | `git pull` |
| Wipe everything and start fresh (**deletes all HDFS data**) | `docker compose down -v` |

**Where to put your files:** the `data/` and `jobs/` folders in this project are shared
with the setup. Put data files in `data/` and your code in `jobs/`. Inside the setup they
appear as `/data` and `/jobs`.

**Useful addresses:**

| What | Address |
|---|---|
| HDFS file browser | http://localhost:50070 |
| Spark jobs | http://localhost:8080 |
| Hive (from inside Docker) | `jdbc:hive2://localhost:10000` |
| Kafka from your laptop (e.g. a Python script) | `localhost:29092` |
| Kafka from a Spark job inside Docker | `kafka:9092` |

---

## Team conventions

> **Proposed by M4 on Sun 1 Nov. We haven't had a call yet. If you disagree with anything,
> say so in the group chat by Tue 3 Nov. Otherwise this is what we use.**
> Everyone writes to these exact names, so the modules join up at the integration call.

### HDFS folders

| Folder | Owner | What goes there | Format |
|---|---|---|---|
| `/raw/patients/` | M1 | Patient data | CSV |
| `/raw/visits/` | M1 | Visit data | CSV |
| `/raw/billing/` | M1 | Billing data | CSV |
| `/raw/medical_files/` | M3 | Dummy X-rays, MRIs and lab report PDFs | Image / PDF |
| `/curated/patients/` | M4 | Cleaned patients | Parquet |
| `/curated/visits_billing/` | M4 | Cleaned visits joined with patients and billing | Parquet |
| `/curated/visits_live/` | M2 | Visits arriving live from Kafka | Parquet |
| `/checkpoints/` | M2 | Spark Streaming progress (don't touch) | – |
| `/results/` | M5 | MapReduce vs Spark timing output | Any |

**Full dataset (from Wed 4 Nov):** M1 splits raw data into one folder per branch and day,
for example `/raw/visits/branch_id=1/date=2026-11-01/visits.csv`.
Spark reads the whole `/raw/visits/` folder either way, and turns `branch_id` and `date`
into columns automatically.

### Hive tables (database `alpha`)

| Table | Owner | Built from |
|---|---|---|
| `alpha.patients` | M4 | `/curated/patients/` |
| `alpha.visits_billing` | M4 | `/curated/visits_billing/` |
| `alpha.visits_live` | M2 | `/curated/visits_live/` |
| `alpha.medical_files` | M3 | `/raw/medical_files/`: one row per file, linking it to its patient |

`alpha.medical_files` columns: `file_id, patient_id, branch_id, file_date, file_type, hdfs_path`.
`file_type` is one of `xray`, `mri`, `lab_report`.

### Kafka

- **Topic:** `new-visits` (created automatically the first time anyone uses it)
- **Message:** one JSON object per visit, using the visit column names below.
  Example: `{"visit_id":"V01001","patient_id":"P00042","branch_id":2,"visit_datetime":"2026-11-01 10:42:00","doctor_id":"D003","diagnosis":"Asthma"}`
- **Address:** `kafka:9092` from Spark jobs inside Docker; `localhost:29092` from a Python script on your laptop.
- Messages are temporary. They're wiped when Docker restarts, so don't store anything only in Kafka.

### Column names (from M1's files)

| File | Columns |
|---|---|
| patients | `patient_id, first_name, last_name, dob, gender, branch_id, contact_number, email, address` |
| visits | `visit_id, patient_id, branch_id, visit_datetime, doctor_id, diagnosis` |
| billing | `bill_id, visit_id, patient_id, amount, payment_status, payment_date` |

- IDs: `P00001`, `V00001`, `B00001`. Branches: `1`, `2`, `3`.
- Dates: `yyyy-MM-dd`. Date and time: `yyyy-MM-dd HH:mm:ss`.
- `visit_datetime` replaces `visit_date` in the full dataset (M1 is adding the time of each visit).

---

## Run a Spark job

1. Put your Python file in the `jobs` folder. To start, copy [`jobs/example_job.py`](jobs/example_job.py).
2. Run it from VS Code's terminal (Docker must be running):

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/example_job.py
```

Replace `example_job.py` with your file's name. The output appears in the terminal, and the job
shows up at http://localhost:8080 while it runs.

The example counts visits per branch. If M1's data is loaded, you'll see something like:

```text
Visits loaded: 1000
+---------+-----+
|branch_id|count|
+---------+-----+
|        1|  367|
|        2|  326|
|        3|  307|
+---------+-----+
```

**Tips**

- Paths like `/raw/visits` point to HDFS. For files in your laptop's `data` folder, use `file:///data/...`.
- To read or write Hive tables, build your session with `.enableHiveSupport()` (the example does this).
- Spark prints a lot of `INFO` and `WARN` lines. That's normal. Your own `print` output is mixed in with them.

**Jobs that read Kafka (M2):** add the Kafka package. The first run downloads about 60 MB, so it needs internet.

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /jobs/your_stream.py
```

**Python scripts on your laptop that send to Kafka** (e.g. M2's producer) need the library first:
`pip install kafka-python`, then connect to `localhost:29092`.

**Run a Hive SQL file:**

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/your_file.sql
```

---

## Problems

| What you see | What to do |
|---|---|
| `Cannot connect to the Docker daemon` or `failed to connect to the docker API` | Docker Desktop isn't open. Open it, wait for the whale icon to stop moving, then try again. |
| A part keeps restarting or says **Exited** | Usually not enough memory. Redo Step 2, then run `docker compose down` and `docker compose up -d`. |
| `Connection refused` when checking Hive | Hive is still starting. Wait 2 more minutes and try again. |
| `port is already allocated` | Another program is using that port (often 8080). Close it, or ask M4 to change the port. |
| `no such file or directory` with a `C:/Program Files/Git/...` path | You're in Git Bash. Use PowerShell instead (see Step 1). |
| `docker compose` is not recognised | Your Docker is old. Update Docker Desktop. |
| Very slow on a Mac | Normal for M-chip Macs (see Step 2). Close other apps while it runs. |
| Anything else | Send M4 (Naveed) a screenshot plus the output of `docker compose ps`. |
