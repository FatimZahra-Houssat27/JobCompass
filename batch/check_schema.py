from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-schema").getOrCreate()

df = spark.read.parquet(
    "hdfs://namenode:8020/jobcompass/indicators/top_jobs"
)

print("=== SCHEMA ===")
df.printSchema()

print("=== DONNEES ===")
df.show(truncate=False)

spark.stop()
