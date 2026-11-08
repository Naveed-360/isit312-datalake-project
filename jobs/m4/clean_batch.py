"""M4 batch job: raw zone -> curated zone.

Reads M1's patients, visits and billing CSVs from HDFS, cleans them,
joins visits with their patient and bill, and writes Parquet to the
curated zone:
  /curated/patients        one row per patient
  /curated/visits_billing  one row per visit, with patient and billing details

Run:
  docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m4/clean_batch.py

Optional arguments (for testing): <raw folder> <curated folder>
"""
import sys

from pyspark.sql import SparkSession, Window, functions as F

RAW = sys.argv[1] if len(sys.argv) > 1 else "/raw"
CURATED = sys.argv[2] if len(sys.argv) > 2 else "/curated"

# Accepted date formats. M1's files use yyyy-MM-dd; the full dataset adds a time.
DATE_FORMATS = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd", "dd/MM/yyyy"]

spark = SparkSession.builder.appName("m4-clean-batch").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")


def read_raw(name):
    # recursiveFileLookup reads every CSV under the folder, so this works for
    # both /raw/visits/visits.csv and /raw/visits/branch_id=1/date=.../visits.csv.
    # Every column is read as text and typed below, so bad values become null
    # instead of failing the job.
    df = (spark.read
          .option("header", True)
          .option("recursiveFileLookup", True)
          .option("multiLine", True)        # addresses are quoted and contain commas
          .csv(f"{RAW}/{name}"))
    return df.select([F.trim(F.col(c)).alias(c) for c in df.columns])


def to_timestamp(col):
    return F.coalesce(*[F.to_timestamp(col, fmt) for fmt in DATE_FORMATS])


def keep_one_per_id(df, id_col):
    # Exact duplicate rows and repeated IDs: keep one row per ID
    w = Window.partitionBy(id_col).orderBy(F.col(id_col))
    return df.withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n")


def not_blank(col):
    return F.col(col).isNotNull() & (F.col(col) != "")


counts = {}

# ---------- patients ----------
patients_raw = read_raw("patients")
counts["patients_raw"] = patients_raw.count()
patients = (patients_raw
            .filter(not_blank("patient_id"))
            .withColumn("dob", F.to_date(to_timestamp("dob")))
            .withColumn("gender", F.initcap("gender"))
            .withColumn("branch_id", F.col("branch_id").cast("int"))
            .withColumn("email", F.lower("email")))
patients = keep_one_per_id(patients, "patient_id")
counts["patients_clean"] = patients.count()

# ---------- visits ----------
visits_raw = read_raw("visits")
counts["visits_raw"] = visits_raw.count()
# The full dataset renames visit_date to visit_datetime (date + time)
time_col = "visit_datetime" if "visit_datetime" in visits_raw.columns else "visit_date"
visits = (visits_raw
          .filter(not_blank("visit_id") & not_blank("patient_id"))
          .withColumn("visit_ts", to_timestamp(time_col))
          .filter(F.col("visit_ts").isNotNull())
          .withColumn("visit_date", F.to_date("visit_ts"))
          .withColumn("visit_hour",
                      F.hour("visit_ts") if time_col == "visit_datetime" else F.lit(None).cast("int"))
          .withColumn("branch_id", F.col("branch_id").cast("int"))
          .drop(*[c for c in ("visit_datetime",) if c in visits_raw.columns]))
visits = keep_one_per_id(visits, "visit_id")
counts["visits_clean"] = visits.count()

# ---------- billing ----------
billing_raw = read_raw("billing")
counts["bills_raw"] = billing_raw.count()
billing = (billing_raw
           .filter(not_blank("bill_id") & not_blank("visit_id"))
           # "SAR 120.50" or "1,200" -> 120.5 / 1200; keep the minus sign so negatives are dropped
           .withColumn("amount", F.regexp_replace("amount", "[^0-9.-]", "").cast("double"))
           .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
           .withColumn("payment_status", F.initcap("payment_status"))
           .withColumn("payment_date", F.to_date(to_timestamp("payment_date"))))
billing = keep_one_per_id(billing, "bill_id")
counts["bills_clean"] = billing.count()

# ---------- join: one row per visit ----------
visits_billing = (visits
                  .join(patients.select("patient_id", "first_name", "last_name", "dob", "gender",
                                        F.col("branch_id").alias("home_branch_id")),
                        "patient_id", "left")
                  .join(billing.select("visit_id", "bill_id", "amount", "payment_status", "payment_date"),
                        "visit_id", "left"))

counts["visits_no_patient"] = visits_billing.filter(F.col("gender").isNull()).count()
counts["visits_no_bill"] = visits_billing.filter(F.col("bill_id").isNull()).count()
counts["bills_no_visit"] = billing.join(visits, "visit_id", "left_anti").count()

# ---------- write curated zone ----------
patients.write.mode("overwrite").partitionBy("branch_id").parquet(f"{CURATED}/patients")
visits_billing.write.mode("overwrite").partitionBy("branch_id").parquet(f"{CURATED}/visits_billing")

# Summary for the report's "sample running jobs" screenshot
print("\n========== M4 clean_batch summary ==========")
print(f"Visit time column used : {time_col}")
for table in ("patients", "visits", "bills"):
    raw, clean = counts[f"{table}_raw"], counts[f"{table}_clean"]
    print(f"{table:<9} raw {raw:>8,}  ->  clean {clean:>8,}   (removed {raw - clean:,})")
print(f"visits with no matching patient : {counts['visits_no_patient']:,}")
print(f"visits with no bill             : {counts['visits_no_bill']:,}")
print(f"bills with no matching visit    : {counts['bills_no_visit']:,} (not in visits_billing)")
print(f"Written: {CURATED}/patients, {CURATED}/visits_billing")
print("============================================\n")

spark.stop()
