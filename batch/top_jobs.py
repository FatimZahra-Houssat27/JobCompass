from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020/jobcompass"

spark = SparkSession.builder \
    .appName("jobcompass-top-jobs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lecture des interactions historiques
inter = (
    spark.read.parquet(f"{HDFS}/raw/candidate_interactions")
    .where("event_time < '2024-01-01'")
)

# Calcul des indicateurs par offre
top = (
    inter.groupBy("job_id")
    .agg(
        F.count("*").alias("nb_interactions"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("nb_views"),
        F.sum(F.when(F.col("event_type") == "save", 1).otherwise(0)).alias("nb_saves"),
        F.sum(F.when(F.col("event_type") == "apply", 1).otherwise(0)).alias("nb_applications")
    )
    .orderBy(F.desc("nb_interactions"))
    .limit(10)
)

print("=== TOP 10 DES OFFRES LES PLUS POPULAIRES ===")
top.show(10, truncate=False)

# Sauvegarde dans HDFS
top.coalesce(1).write \
    .mode("overwrite") \
    .parquet(f"{HDFS}/indicators/top_jobs")

print("Top 10 sauvegardé dans HDFS.")

spark.stop()
