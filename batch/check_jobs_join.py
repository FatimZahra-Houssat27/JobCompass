
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

HDFS = "hdfs://namenode:8020/jobcompass"

spark = SparkSession.builder \
    .appName("jobcompass-check-jobs-join") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

inter = spark.read.parquet(
    f"{HDFS}/raw/candidate_interactions"
).where(col("event_time") < "2024-01-01")

jobs = spark.read.parquet(
    f"{HDFS}/reference/jobs"
)

print("=== VERIFICATION DE LA JOINTURE ===")

print("Nombre d'interactions :", inter.count())
print("Nombre d'offres :", jobs.count())

joined = inter.join(
    jobs,
    inter.job_id == jobs.id,
    "inner"
)

print("Interactions associées à une offre :", joined.count())

print("=== EXEMPLES DE JOINTURE ===")

joined.select(
    inter.job_id,
    jobs.Position,
    inter.event_type
).show(10, truncate=False)

spark.stop()
