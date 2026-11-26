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
# Ajout des noms des offres (jointure avec les données de référence)
jobs = (
    spark.read.parquet(f"{HDFS}/reference/jobs/jobs.parquet")
    .select(F.col("id").alias("job_id"),
            F.col("Position").alias("position"),
            F.col("Company Name").alias("company"))
    .dropDuplicates(["job_id"])
)

top = (
    top.join(jobs, on="job_id", how="left")
    .withColumn("label", F.concat_ws(" – ", "position", "company"))
    .select("label", "position", "company", "nb_interactions",
            "nb_views", "nb_saves", "nb_applications", "job_id")
    .orderBy(F.desc("nb_interactions"))
)
print("=== TOP 10 DES OFFRES LES PLUS POPULAIRES ===")
top.drop("job_id").show(10, truncate=45)

# Sauvegarde dans HDFS
top.coalesce(1).write \
    .mode("overwrite") \
    .parquet(f"{HDFS}/indicators/top_jobs")

print("Top 10 sauvegardé dans HDFS.")

spark.stop()
