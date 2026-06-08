from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Read Silver")
    .getOrCreate()
)

df = spark.read.parquet(
    "data/silver/stock_prices"
)

df.printSchema()
df.show(5, truncate=False)

print(df.count())

spark.stop()