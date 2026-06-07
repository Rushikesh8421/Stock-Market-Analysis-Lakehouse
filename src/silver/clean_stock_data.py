from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Silver Stock Transformation")
    .getOrCreate()
)

# Read the Bronze File
import json

bronze_file = "data/bronze/stock_prices/AAPL_20260604T192718.json"

with open(bronze_file, "r") as f:
    data = json.load(f)

# Extract the Symbol
symbol = data["Meta Data"]["2. Symbol"]

print(symbol)

# Extract Daily Records
daily_data = data["Time Series (Daily)"]

# Build Records
records = []

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

    records.append(record)

print(len(records))   

# Create Spark DataFrame
df = spark.createDataFrame(records)
df.show(5)
df.printSchema()

# Stop Spark
spark.stop()