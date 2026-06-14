from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Silver Stock Transformation")
    .getOrCreate()
)

# Read the Bronze File
import json, glob

bronze_files = glob.glob(
    "data/bronze/stock_prices/*.json"
)

print(f"Found {len(bronze_files)} bronze files")

all_records = []

for bronze_file in bronze_files:

    with open(bronze_file, "r") as f:
        data = json.load(f)

    symbol = data["Meta Data"]["2. Symbol"]

    daily_data = data["Time Series (Daily)"]

    for date, values in daily_data.items():

        record = {
            "symbol": symbol,
            "date": date,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"])
        }

        all_records.append(record)

print(f'Total no of records: {len(all_records)}')   

# Create Spark DataFrame
df = spark.createDataFrame(all_records)
df.show(5)

from pyspark.sql.functions import year
from pyspark.sql.functions import month
from pyspark.sql.functions import to_date, current_timestamp

# Convert date from string datatype to date datatype format
df = df.withColumn(
    "date",
    to_date("date", "yyyy-MM-dd")
)

df = df.withColumn(
    "year",
    year("date")
)

df = df.withColumn(
    "month",
    month("date")
)

# Add Created Timestamp
df = df.withColumn(
    "created_ts",
    current_timestamp()
)

# Reorder Columns
df = df.select(
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "created_ts",
    "year",
    "month"
)

df.printSchema()


df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("data/silver/stock_prices")


before_dedup = df.count()

# Deduplication
df = df.dropDuplicates(
    ["symbol", "date"]
)

after_dedup = df.count()

print(f'Total no of records before deduplication: {before_dedup}')
print(f'Total no of records after deduplication: {after_dedup}')
print(f'Duplicates count: {before_dedup - after_dedup}')

# Data Quality
from pyspark.sql.functions import col

valid_df = df.filter(
    (col("symbol").isNotNull()) &
    (col("date").isNotNull()) &
    (col("open") > 0) &
    (col("high") > 0) &
    (col("low") > 0) &
    (col("close") > 0) &
    (col("volume") >= 0)
)

#invalid_df = df.subtract(valid_df) this may create issues use the below one
invalid_df = df.filter(
    (col("symbol").isNull()) |
    (col("date").isNull()) |
    (col("open") <= 0) |
    (col("high") <= 0) |
    (col("low") <= 0) |
    (col("close") <= 0) |
    (col("volume") < 0)
)

print(f"Total Records: {df.count()}")
print(f"Valid Records: {valid_df.count()}")
print(f"Invalid Records: {invalid_df.count()}")

# Stop Spark
spark.stop()