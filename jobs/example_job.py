"""Example Spark job: counts visits per branch from M1's raw data.

Copy this file as a starting point for your own module.

Run:
  docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /jobs/example_job.py
"""
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("example-visits-per-branch")
         .enableHiveSupport()      # lets you read and write Hive tables
         .getOrCreate())

# Paths without hdfs:// point to HDFS
visits = spark.read.option("header", True).csv("/raw/visits")

print("Visits loaded:", visits.count())
visits.groupBy("branch_id").count().orderBy("branch_id").show()

spark.stop()
