"""
JobCompass - Archivage Kafka -> HDFS (voie froide).

Job Spark Structured Streaming qui lit les deux topics Kafka et écrit les
événements dans HDFS au format Parquet. Cet historique servira ensuite aux
indicateurs batch (Spark SQL) et à l'entraînement du modèle (Spark ML).

  - Source       : Kafka (topics candidate-interactions et job-offers)
  - Destination  : hdfs://namenode:8020/jobcompass/raw/<topic>/
  - Partitionnement par mois (event_month / publish_month)
  - Checkpoints dans HDFS : après un arrêt, le job reprend là où il s'était
    arrêté, sans perte ni doublon (garantie exactly-once du sink fichier).

Lancement :
  docker exec spark-master /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/jobs/streaming/archive_to_hdfs.py
"""
import os
from pyspark.sql import SparkSession, functions as F, types as T

HDFS = os.getenv("HDFS_BASE", "hdfs://namenode:8020/jobcompass")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TRIGGER = os.getenv("ARCHIVE_TRIGGER", "30 seconds")

INTERACTION_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("candidate_id", T.StringType()),
    T.StructField("job_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("weight", T.IntegerType()),
    T.StructField("event_time", T.TimestampType()),
])

OFFER_SCHEMA = T.StructType([
    T.StructField("job_id", T.StringType()),
    T.StructField("position", T.StringType()),
    T.StructField("company", T.StringType()),
    T.StructField("primary_keyword", T.StringType()),
    T.StructField("exp_years", T.StringType()),
    T.StructField("english_level", T.StringType()),
    T.StructField("published", T.TimestampType()),
    T.StructField("description", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
])

# topic -> (schéma JSON, colonne de date servant au partitionnement, nom de la partition)
TOPICS = {
    "candidate-interactions": (INTERACTION_SCHEMA, "event_time", "event_month"),
    "job-offers": (OFFER_SCHEMA, "published", "publish_month"),
}


def start_archive(spark, topic):
    schema, date_col, part_col = TOPICS[topic]
    name = topic.replace("-", "_")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")   # 1er lancement : tout l'historique du topic
           .option("maxOffsetsPerTrigger", 50000)   # limite la taille des micro-batchs (RAM)
           .option("failOnDataLoss", "false")
           .load())

    events = (raw
              .select(F.from_json(F.col("value").cast("string"), schema).alias("e"),
                      F.col("partition").alias("kafka_partition"),
                      F.col("offset").alias("kafka_offset"),
                      F.col("timestamp").alias("kafka_timestamp"))
              .select("e.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
              .where(F.col(date_col).isNotNull())          # écarte les messages illisibles
              .withColumn("ingestion_time", F.current_timestamp())
              .withColumn(part_col, F.date_format(F.col(date_col), "yyyy-MM"))
              .repartition(part_col))                      # 1 fichier par mois et par batch

    return (events.writeStream
            .format("parquet")
            .option("path", f"{HDFS}/raw/{name}")
            .option("checkpointLocation", f"{HDFS}/checkpoints/{name}")
            .partitionBy(part_col)
            .outputMode("append")
            .trigger(processingTime=TRIGGER)
            .queryName(f"archive_{name}")
            .start())


def main():
    spark = SparkSession.builder.appName("jobcompass-archive-kafka-hdfs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    queries = [start_archive(spark, t) for t in TOPICS]
    for q in queries:
        print(f">>> Requête démarrée : {q.name} (id {q.id})", flush=True)
    print(f">>> Archivage vers {HDFS}/raw/ toutes les {TRIGGER}. Ctrl+C pour arrêter.", flush=True)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
