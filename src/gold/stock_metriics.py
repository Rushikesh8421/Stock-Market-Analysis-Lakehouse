# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Gold Stock Metrics")
    .getOrCreate()
)

# Read Silver
df = spark.read.parquet(
    "data/silver/stock_prices"
)

df.printSchema()
df.show(5)

# Import Window Functions
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    lag,
    col
)

# Define Window
window_spec = (
    Window
    .partitionBy("symbol")
    .orderBy("date")
)

# Create previous_close
from pyspark.sql.functions import lag
df = df.withColumn(
    "previous_close",
    lag("close", 1).over(window_spec)
)

df.select(
    "symbol",
    "date",
    "close",
    "previous_close"
).show(10, truncate=False)

# Daily Return %
df = df.withColumn(
    "daily_return_pct",
    (
        (col("close") - col("previous_close"))
        / col("previous_close")
    ) * 100
)

df.select(
    "symbol",
    "date",
    "close",
    "previous_close",
    "daily_return_pct"
).show(10, truncate=False)


# Write Gold Data
df.write \
    .mode("overwrite") \
    .parquet("data/gold/stock_metrics")

df = spark.read.parquet(
    "data/gold/stock_metrics"
)

df.show(5)

spark.stop()