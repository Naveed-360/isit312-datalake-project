# Setup guide

How to get the shared big data setup (HDFS, Hive, Spark, Kafka) running on your own laptop.
It takes about **30 minutes** the first time, most of it waiting for downloads.
If you get stuck, see [Problems](#problems) at the bottom or message Naveed.

← Back to the [project README](README.md)

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
docker compose up -d --build
```

- **First time:** this downloads about **3 GB**, so it can take 10–20 minutes. Use good Wi-Fi.
  When Docker Desktop first opens it can show "Starting the Docker Engine..." for up to 10 minutes.
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

**All 5 passed? You're done.** Next, load Afrith's data: see
[Running the pipeline](README.md#running-the-pipeline) in the README.

---

## Everyday use

| I want to… | Run this |
|---|---|
| Start the setup | `docker compose up -d` |
| Stop it (frees up memory, keeps your data) | `docker compose down` |
| See what's running | `docker compose ps` |
| Get the latest changes from the team | `git pull` |
| After a `git pull` that changed `docker-compose.yml` or `hadoop/` | `docker compose up -d --build` |
| Wipe everything and start fresh (**deletes all HDFS data**) | `docker compose down -v` |

**Where to put your files:** the `data/` and `jobs/` folders in this project are shared
with the setup. Put data files in `data/` and your code in `jobs/`. Inside the setup they
appear as `/data` and `/jobs`. The `data/` folder is never uploaded to GitHub.

**Useful addresses:**

| What | Address |
|---|---|
| HDFS file browser | http://localhost:50070 |
| Spark jobs | http://localhost:8080 |
| Hive (from inside Docker, or Power BI / DBeaver on your laptop) | `jdbc:hive2://localhost:10000` |
| Kafka from your laptop (e.g. a Python script) | `localhost:29092` |
| Kafka from a Spark job inside Docker | `kafka:9092` |

---

## Writing your own job

**Spark job:** put your Python file in `jobs/` (start by copying [`jobs/example_job.py`](jobs/example_job.py)), then:

```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/example_job.py
```

- Paths like `/raw/visits` point to HDFS. For files in your laptop's `data` folder, use `file:///data/...`.
- To read or write Hive tables, build your session with `.enableHiveSupport()` (the example does this).
- Spark prints a lot of `INFO` and `WARN` lines. That's normal. Your own `print` output is mixed in with them.
- Jobs that read Kafka need `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0`
  after `spark-master:7077`. The first run downloads about 60 MB.
- Only one long-running job fits at a time: a streaming job takes all of Spark's cores, and any
  other job waits until it stops.

**Hive SQL file:**

```bash
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /jobs/your_file.sql
```

**MapReduce job (Python, Hadoop Streaming):** runs in the `namenode` container, which has **Python 3.4**,
so scripts can't use f-strings (write `"%s\t%d\n" % (key, total)` instead). Hadoop won't overwrite an
output folder, so delete it before re-running: `docker exec namenode hdfs dfs -rm -r /results/your_output`.
MapReduce runs in local mode (one process, no YARN cluster).

**Python script on your laptop that talks to Kafka** (e.g. Wafeeq's producer): install the libraries first
with `pip install pandas kafka-python`, then connect to `localhost:29092`.

---

## Problems

| What you see | What to do |
|---|---|
| `Cannot connect to the Docker daemon` or `failed to connect to the docker API` | Docker Desktop isn't open. Open it, wait for the whale icon to stop moving, then try again. |
| Docker Desktop stuck on "Starting the Docker Engine..." for over 10 minutes | Quit and reopen Docker Desktop. If that fails, run `wsl --update` in PowerShell as Administrator and restart the laptop. |
| A part keeps restarting or says **Exited** | Usually not enough memory. Redo Step 2, then run `docker compose down` and `docker compose up -d`. |
| `Connection refused` when checking Hive | Hive is still starting. Wait 2 more minutes and try again. |
| `Name node is in safe mode` | HDFS is still starting. Wait a minute and try again. |
| `port is already allocated` | Another program is using that port (often 8080). Close it, or ask Naveed to change the port. |
| `no such file or directory` with a `C:/Program Files/Git/...` path | You're in Git Bash. Use PowerShell instead (see Step 1). |
| `docker compose` is not recognised | Your Docker is old. Update Docker Desktop. |
| Very slow on a Mac | Normal for M-chip Macs (see Step 2). Close other apps while it runs. |
| MapReduce job fails with `subprocess failed with code 1` | Usually a Python error in your mapper or reducer, often an f-string (Python 3.4 doesn't support them). Test it with `docker exec namenode sh -c "head -5 /data/m1/patients.csv \| python3 /jobs/m5/branch_count_mapper.py"`. |
| `python3: not found` when running a MapReduce job | Your setup is from before 8 Nov. Run `git pull`, then `docker compose up -d --build`. |
| Hive query returns 0 rows, or `MSCK finds a file rather than a directory` | Run Naveed's clean job and then `/jobs/m4/create_tables.sql` again (see the README). |
| A Spark job sits at "waiting" and never starts | Another Spark job (often the streaming job) is using all the cores. Stop it first. |
| Anything else | Send Naveed a screenshot plus the output of `docker compose ps`. |
