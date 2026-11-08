from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("MeasureDelay").getOrCreate()

# Read the saved Parquet files
df = spark.read.parquet("hdfs://namenode:8020/curated/visits_live")

# Calculate the difference between sent and saved
delay_df = df.withColumn(
    "delay_seconds", 
    col("saved_timestamp").cast("double") - col("sent_timestamp").cast("double")
)

# Calculate the average delay
avg_delay = delay_df.agg({"delay_seconds": "avg"}).collect()[0][0]

print(f"\n======================================")
print(f"Total live visits processed: {delay_df.count()}")
print(f"Average Pipeline Delay: {avg_delay:.4f} seconds")
print(f"======================================\n")