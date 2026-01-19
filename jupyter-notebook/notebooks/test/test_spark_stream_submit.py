from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .master('spark://spark-master:7077')
    .appName("Simple-Streaming-Demo")
    .getOrCreate()
)

# Source: sinh dữ liệu stream (timestamp, value)
df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
)

# Xử lý đơn giản (stateless)
result = df.select(
    col("timestamp"),
    col("value")
)

query = (
    result.writeStream
    .format("console")          # in ra console
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
