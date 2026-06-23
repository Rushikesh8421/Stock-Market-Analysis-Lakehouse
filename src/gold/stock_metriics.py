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
    col,
    avg
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


# Moving avg of 7 days
moving_avg_window = (
    Window
    .partitionBy("symbol")
    .orderBy("date")
    .rowsBetween(-6, 0)
)

df = df.withColumn(
    "moving_avg_7d",
    avg("close").over(moving_avg_window)
)

df.select(
    "symbol",
    "date",
    "close",
    "previous_close",
    "daily_return_pct",
    "moving_avg_7d"
).show(20, truncate=False)

# moving_avg_30d
moving_avg_30d_window = (
    Window
    .partitionBy("symbol")
    .orderBy("date")
    .rowsBetween(-29, 0)
)

from pyspark.sql.functions import avg

df = df.withColumn(
    "moving_avg_30d",
    avg("close").over(moving_avg_30d_window)
)

df.select(
    "symbol",
    "date",
    "close",
    "previous_close",
    "daily_return_pct",
    "moving_avg_7d",
    "moving_avg_30d"
).show(20, truncate=False)

# rolling_avg_volume_7d 
# What is the average trading activity over the last 7 trading days?
volume_window = (
    Window
    .partitionBy("symbol")
    .orderBy("date")
    .rowsBetween(-6, 0)
)

df = df.withColumn(
    "rolling_avg_volume_7d",
    avg("volume").over(volume_window)
)

df.select(
    "symbol",
    "date",
    "close",
    "previous_close",
    "daily_return_pct",
    "moving_avg_7d",
    "moving_avg_30d",
    "rolling_avg_volume_7d"
).show(20, truncate=False)


# Write Gold Data
df.write \
    .mode("overwrite") \
    .parquet("data/gold/stock_metrics")

df = spark.read.parquet(
    "data/gold/stock_metrics"
)

df.show(10)

spark.stop()