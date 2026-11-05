"""Test : Spark (cluster) écrit puis relit un fichier Parquet dans HDFS."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("jobcompass-test-hdfs").getOrCreate()
path = "hdfs://namenode:8020/jobcompass/test_spark"

df = spark.range(0, 100000).withColumnRenamed("id", "n")
df.write.mode("overwrite").parquet(path)
print(">>> Lignes relues depuis HDFS :", spark.read.parquet(path).count())
spark.stop()
