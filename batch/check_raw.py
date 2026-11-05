"""
JobCompass - Vérification de l'historique archivé dans HDFS.

  docker exec spark-master /opt/spark/bin/spark-submit /opt/jobs/batch/check_raw.py
"""
from pyspark.sql import SparkSession, functions as F

HDFS = "hdfs://namenode:8020/jobcompass/raw"

spark = SparkSession.builder.appName("jobcompass-check-raw").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

inter = spark.read.parquet(f"{HDFS}/candidate_interactions")
print("\n>>> INTERACTIONS archivées dans HDFS")
print(">>> Nombre d'événements :", inter.count())
print(">>> Candidats distincts :", inter.select("candidate_id").distinct().count(),
      "| Offres distinctes :", inter.select("job_id").distinct().count())
inter.agg(F.min("event_time").alias("premier"), F.max("event_time").alias("dernier"),
          F.countDistinct("event_month").alias("nb_mois")).show(truncate=False)
inter.groupBy("event_type").count().orderBy(F.desc("count")).show()

offers = spark.read.parquet(f"{HDFS}/job_offers")
print(">>> OFFRES archivées dans HDFS :", offers.count())
offers.groupBy("primary_keyword").count().orderBy(F.desc("count")).show(5)

spark.stop()
