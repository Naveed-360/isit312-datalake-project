#!/usr/bin/env python3
"""Count M1 patients per branch using PySpark.

Example (run inside the shared Docker setup):
  docker exec spark-master /spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      /jobs/m5/branch_count_spark.py \
      --input hdfs:///raw/patients \
      --output hdfs:///results/spark/10000

The output directory is overwritten when it already exists.
"""
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description="Count M1 patients per branch with Spark"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="HDFS path containing patients CSV file(s), e.g. hdfs:///raw/patients",
    )
    parser.add_argument(
        "--output",
        default="hdfs:///results/spark/10000",
        help=("HDFS output directory; defaults to "
              "hdfs:///results/spark/10000 and is overwritten if it exists"),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("ISIT312-M5-Patient-Count-By-Branch")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        patients = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .option("recursiveFileLookup", "true")
            .csv(args.input)
        )

        required = {"patient_id", "branch_id"}
        missing = required.difference(set(patients.columns))
        if missing:
            raise ValueError(
                "M1 patients input is missing required column(s): "
                + ", ".join(sorted(missing))
            )

        valid_patients = patients.filter(
            F.col("patient_id").isNotNull()
            & (F.trim(F.col("patient_id")) != "")
            & (F.trim(F.col("patient_id")) != "patient_id")
            & F.col("branch_id").isNotNull()
            & (F.trim(F.col("branch_id")) != "")
        )

        counts = (
            valid_patients
            .groupBy(F.trim(F.col("branch_id")).alias("branch_id"))
            .count()
            .orderBy("branch_id")
            .cache()
        )

        # Match Hadoop Streaming's reducer output:
        # one tab-separated "branch_id<TAB>count" value per output row.
        output_rows = counts.select(
            F.concat_ws(
                "\t",
                F.col("branch_id"),
                F.col("count").cast("string"),
            ).alias("value")
        )

        # The write is the single action that materializes the cached counts.
        # No counts.show(): it would trigger a second Spark action.
        output_rows.write.mode("overwrite").text(args.output)
        counts.unpersist()

        print("Spark result written to: " + args.output)
        return 0

    except Exception as exc:
        print("ERROR: Spark branch-count job failed: " + str(exc), file=sys.stderr)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
