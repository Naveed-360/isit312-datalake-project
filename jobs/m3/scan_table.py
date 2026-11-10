"""M3: build the scans, reports and medical_files data from metadata.csv.

Reads  : /raw/files_metadata/metadata.csv   (uploaded by file_upload.py)
Writes : /curated/scans          X-ray and MRI images
         /curated/reports        lab reports and medical documents
         /curated/medical_files  ALL files in one table (the team convention)
All are Parquet, partitioned by branch_id, the same style as M4's tables.
Every row links one file (hdfs_path) to its patient (patient_id).

Run (then run m3_table_creation.sql to register the Hive tables):
  docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/m3/scan_table.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = os.environ.get("M3_HDFS", "hdfs://namenode:8020")
META = HDFS + "/raw/files_metadata/metadata.csv"
RAW_FILES = HDFS + "/raw/medical_files"
OUT_SCANS = HDFS + "/curated/scans"
OUT_REPORTS = HDFS + "/curated/reports"
OUT_ALL = HDFS + "/curated/medical_files"

SCAN_TYPES = ["X-ray", "MRI"]

spark = SparkSession.builder.appName("m3_scans_reports").getOrCreate()

meta = spark.read.option("header", True).csv(META)
print("metadata rows read:", meta.count())

files = (
    meta
    .withColumn("patient_id", F.trim("patient_id"))
    .withColumn("file_type", F.trim("file_type"))
    .withColumn("branch_id", F.col("branch_id").cast("int"))
    .withColumn("file_date", F.to_date("date"))
    .withColumn("file_name", F.regexp_extract("file_path", "[^/]+$", 0))
    # where the file really lives in HDFS (matches the upload layout)
    .withColumn(
        "hdfs_path",
        F.concat(
            F.lit(RAW_FILES + "/branch="),
            F.col("branch_id").cast("string"),
            F.lit("/date="),
            F.col("date"),
            F.lit("/"),
            F.col("file_name"),
        ),
    )
    .filter(F.col("patient_id").isNotNull() & (F.col("patient_id") != ""))
    .dropDuplicates(["file_id"])
    .select("file_id", "patient_id", "file_type", "file_name",
            "hdfs_path", "file_date", "branch_id")
)

scans = files.filter(F.col("file_type").isin(SCAN_TYPES))
reports = files.filter(~F.col("file_type").isin(SCAN_TYPES))

# One table for everything, with the file_type values the team agreed on
# (xray, mri, lab_report). Medical PDFs that are not lab reports become "document".
medical_files = (
    files
    .withColumn(
        "file_type",
        F.when(F.col("file_type") == "X-ray", "xray")
         .when(F.col("file_type") == "MRI", "mri")
         .when(F.col("file_type") == "Lab Report", "lab_report")
         .otherwise("document"),
    )
    .select("file_id", "patient_id", "file_date", "file_type", "hdfs_path", "branch_id")
)

scans.write.mode("overwrite").partitionBy("branch_id").parquet(OUT_SCANS)
reports.write.mode("overwrite").partitionBy("branch_id").parquet(OUT_REPORTS)
medical_files.write.mode("overwrite").partitionBy("branch_id").parquet(OUT_ALL)

print("scans rows        :", scans.count())
print("reports rows      :", reports.count())
print("medical_files rows:", medical_files.count())
spark.stop()
