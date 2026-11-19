
from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:8020/jobcompass"

spark = SparkSession.builder \
    .appName("jobcompass-interaction-types") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lecture des interactions historiques
inter = (
    spark.read.parquet(f"{HDFS}/raw/candidate_interactions")
    .where("event_time < '2024-01-01'")
)

inter.createOrReplaceTempView("interactions")

# Répartition des interactions par type
df = spark.sql("""
    SELECT
        event_type,
        COUNT(*) AS nb_interactions,
        ROUND(
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
            2
        ) AS pourcentage
    FROM interactions
    GROUP BY event_type
    ORDER BY nb_interactions DESC
""")

print("=== REPARTITION DES INTERACTIONS ===")
df.show(truncate=False)

# Sauvegarde dans HDFS
df.coalesce(1).write \
    .mode("overwrite") \
    .parquet(f"{HDFS}/indicators/interaction_types")

print("Indicateurs sauvegardés dans HDFS.")

spark.stop()
