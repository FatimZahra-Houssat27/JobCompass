
from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:8020/jobcompass"

spark = SparkSession.builder \
    .appName("JobCompass-Job-Position-Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Chargement des donnees
interactions = spark.read.parquet(
    f"{HDFS}/raw/candidate_interactions"
)

jobs = spark.read.parquet(
    f"{HDFS}/reference/jobs/jobs.parquet"
)

# Enregistrement des vues SQL
interactions.createOrReplaceTempView("interactions")
jobs.createOrReplaceTempView("jobs")

# Analyse par poste
result = spark.sql("""
    SELECT
        j.Position AS position,
        COUNT(i.event_id) AS nb_interactions,

        SUM(CASE WHEN i.event_type = 'view'
            THEN 1 ELSE 0 END) AS nb_views,

        SUM(CASE WHEN i.event_type = 'save'
            THEN 1 ELSE 0 END) AS nb_saves,

        SUM(CASE WHEN i.event_type = 'apply'
            THEN 1 ELSE 0 END) AS nb_applications,

        ROUND(
            100.0 * SUM(CASE WHEN i.event_type = 'save'
                THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN i.event_type = 'view'
                THEN 1 ELSE 0 END), 0), 2
        ) AS save_rate,

        ROUND(
            100.0 * SUM(CASE WHEN i.event_type = 'apply'
                THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN i.event_type = 'view'
                THEN 1 ELSE 0 END), 0), 2
        ) AS application_rate

    FROM interactions i
    INNER JOIN jobs j
        ON i.job_id = j.id

    GROUP BY j.Position
    ORDER BY nb_interactions DESC
""")

# Affichage des resultats
result.show(30, truncate=False)

# Sauvegarde dans HDFS
output_path = f"{HDFS}/indicators/job_position_performance"

result.write.mode("overwrite").parquet(output_path)

print("Analyse par poste terminee.")
print("Resultats sauvegardes dans :", output_path)

spark.stop()
