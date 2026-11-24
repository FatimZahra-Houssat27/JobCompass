
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

HDFS = "hdfs://namenode:8020/jobcompass"

spark = SparkSession.builder \
    .appName("jobcompass-monthly-visualization") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lecture des indicateurs calculés par Spark
df = spark.read.parquet(
    f"{HDFS}/indicators/monthly_activity"
)

# Conversion en Pandas pour la visualisation
pdf = df.orderBy("event_month").toPandas()

# Création du graphique
plt.figure(figsize=(14, 6))
plt.plot(
    pdf["event_month"],
    pdf["nb_events"],
    marker="o",
    linewidth=2
)

plt.title("Évolution mensuelle des interactions - JobCompass")
plt.xlabel("Mois")
plt.ylabel("Nombre d'interactions")
plt.xticks(rotation=60)
plt.grid(True, alpha=0.3)
plt.tight_layout()

# Sauvegarde
plt.savefig(
    "/opt/jobs/visualizations/monthly_activity.png",
    dpi=300
)

print("Graphique généré : monthly_activity.png")

spark.stop()
