
from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:8020/jobcompass"

# Création de la session Spark
spark = SparkSession.builder \
    .appName("jobcompass-indicators") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Lecture de l'historique des interactions
inter = (
    spark.read.parquet(f"{HDFS}/raw/candidate_interactions")
    .where("event_time < '2024-01-01'")
)

inter.createOrReplaceTempView("interactions")

# 2. Lecture des références
jobs = spark.read.parquet(f"{HDFS}/reference/jobs")
jobs.createOrReplaceTempView("jobs")

candidates = spark.read.parquet(f"{HDFS}/reference/candidates")
candidates.createOrReplaceTempView("candidates")
# Diagnostic de la structure des données

print("=== SCHEMA INTERACTIONS ===")
inter.printSchema()

print("=== SCHEMA JOBS ===")
jobs = spark.read.parquet(f"{HDFS}/reference/jobs")
jobs.printSchema()

print("=== SCHEMA CANDIDATES ===")
candidates = spark.read.parquet(f"{HDFS}/reference/candidates")
candidates.printSchema()

print("=== EXEMPLES INTERACTIONS ===")
inter.show(5, truncate=False)

print("=== EXEMPLES JOBS ===")
jobs.show(5, truncate=False)

print("=== EXEMPLES CANDIDATES ===")
candidates.show(5, truncate=False)
# 3. Analyse SQL : activité mensuelle
df = spark.sql("""
    SELECT
        event_month,
        COUNT(*) AS nb_events
    FROM interactions
    GROUP BY event_month
    ORDER BY event_month
""")

# 4. Affichage des résultats
print("=== ACTIVITE MENSUELLE ===")
df.show(50, truncate=False)

# 5. Sauvegarde des indicateurs dans HDFS
df.coalesce(1).write \
    .mode("overwrite") \
    .parquet(f"{HDFS}/indicators/monthly_activity")

print("Indicateurs sauvegardés dans HDFS.")

spark.stop()
