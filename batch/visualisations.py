
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import os

HDFS = "hdfs://namenode:8020/jobcompass"
OUTPUT = "/tmp/jobcompass_visualizations"

os.makedirs(OUTPUT, exist_ok=True)

spark = SparkSession.builder \
    .appName("JobCompass-All-Visualizations") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Chargement des indicateurs depuis HDFS
monthly = spark.read.parquet(
    f"{HDFS}/indicators/monthly_activity"
).orderBy("event_month").toPandas()

types = spark.read.parquet(
    f"{HDFS}/indicators/interaction_types"
).toPandas()

top_jobs = spark.read.parquet(
    f"{HDFS}/indicators/top_jobs"
).toPandas()

# 1. Evolution mensuelle
plt.figure(figsize=(14, 6))
plt.plot(monthly["event_month"], monthly["nb_events"], marker="o")
plt.title("Evolution mensuelle des interactions")
plt.xlabel("Mois")
plt.ylabel("Nombre d'interactions")
plt.xticks(rotation=60)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUTPUT}/01_monthly_activity.png", dpi=300)
plt.close()

# 2. Répartition des types d'interactions
plt.figure(figsize=(8, 8))
plt.pie(
    types["nb_interactions"],
    labels=types["event_type"],
    autopct="%1.1f%%",
    startangle=90
)
plt.title("Repartition des types d'interactions")
plt.tight_layout()
plt.savefig(f"{OUTPUT}/02_interaction_distribution.png", dpi=300)
plt.close()

# 3. Top 10 des offres les plus consultées
views = top_jobs.sort_values("nb_views", ascending=False).head(10)
plt.figure(figsize=(12, 6))
plt.barh(views["job_id"].astype(str), views["nb_views"])
plt.title("Top 10 des offres les plus consultees")
plt.xlabel("Nombre de vues")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(f"{OUTPUT}/03_top_viewed_jobs.png", dpi=300)
plt.close()

# 4. Top 10 des offres sauvegardées
saved = top_jobs.sort_values("nb_saves", ascending=False).head(10)
plt.figure(figsize=(12, 6))
plt.barh(saved["job_id"].astype(str), saved["nb_saves"])
plt.title("Top 10 des offres les plus sauvegardees")
plt.xlabel("Nombre de sauvegardes")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(f"{OUTPUT}/04_top_saved_jobs.png", dpi=300)
plt.close()

# 5. Top 10 des offres avec le plus de candidatures
applied = top_jobs.sort_values("nb_applications", ascending=False).head(10)
plt.figure(figsize=(12, 6))
plt.barh(applied["job_id"].astype(str), applied["nb_applications"])
plt.title("Top 10 des offres avec le plus de candidatures")
plt.xlabel("Nombre de candidatures")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(f"{OUTPUT}/05_top_applied_jobs.png", dpi=300)
plt.close()

# 6. Funnel global des interactions
counts = types.set_index("event_type")["nb_interactions"].to_dict()

funnel_labels = ["Vues", "Sauvegardes", "Candidatures"]
funnel_values = [
    counts.get("view", 0),
    counts.get("save", 0),
    counts.get("apply", 0)
]

plt.figure(figsize=(9, 5))
plt.bar(funnel_labels, funnel_values)
plt.title("Entonnoir des interactions")
plt.ylabel("Nombre d'interactions")
plt.tight_layout()
plt.savefig(f"{OUTPUT}/06_conversion_funnel.png", dpi=300)
plt.close()

print("6 visualisations generees dans", OUTPUT)

spark.stop()
