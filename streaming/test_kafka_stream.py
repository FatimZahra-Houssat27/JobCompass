"""Test : Spark Structured Streaming lit le topic Kafka et affiche les événements."""
from pyspark.sql import SparkSession, functions as F, types as T

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("candidate_id", T.StringType()),
    T.StructField("job_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("weight", T.IntegerType()),
    T.StructField("event_time", T.TimestampType()),
])

spark = SparkSession.builder.appName("jobcompass-test-kafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "candidate-interactions")
       .option("startingOffsets", "latest")
       .load())

events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
             .select("e.*"))

query = events.writeStream.format("console").option("truncate", False).start()
query.awaitTermination()
