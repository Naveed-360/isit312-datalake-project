from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, unix_timestamp
from pyspark.sql.types import DoubleType, StructType, StringType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("LiveVisitsIngestion_FinalTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Define the schema to match your visits.csv columns + the sent_timestamp you added
schema = StructType() \
    .add("visit_id", StringType()) \
    .add("patient_id", StringType()) \
    .add("branch_id", StringType()) \
    .add("visit_date", StringType()) \
    .add("doctor_id", StringType()) \
    .add("diagnosis", StringType()) \
    .add("sent_timestamp", DoubleType())

# Read the live stream from Kafka (using the internal Docker port 9092)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "new-visits") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data and add a saved_timestamp
clean_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").cast("double").alias("sent_timestamp")  # Docker-internal Kafka arrival time
) \
.select("data.visit_id", "data.patient_id", "data.branch_id", "data.visit_date", "data.doctor_id", "data.diagnosis", "sent_timestamp") \
.withColumn("saved_timestamp", current_timestamp().cast("double"))

# Write the live data to HDFS /curated/visits_live as Parquet files
query = clean_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/curated/visits_live") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/visits_live") \
    .start()

print("Spark Streaming Job Started. Waiting for live visits...")
query.awaitTermination()