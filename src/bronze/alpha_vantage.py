import os
import json
from datetime import datetime

import time
import requests
from dotenv import load_dotenv


# Load the API key
load_dotenv()

print("Current working directory:", os.getcwd())

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

if not API_KEY:
    raise ValueError("Alpha Vantage API key not found")


# Build the URL
symbols = [
    "AAPL",
    "MSFT",
    "GOOG",
    "NVDA"
]


for symbol in symbols:

    url = (
        "https://www.alphavantage.co/query"
        f"?function=TIME_SERIES_DAILY"
        f"&symbol={symbol}"
        f"&apikey={API_KEY}"
    )

    # Call the API
    response = requests.get(url)
    data = response.json()

    time.sleep(15)

    # Validate response
    if "Error Message" in data:
        raise Exception(data["Error Message"])

    if "Note" in data:
        print(f"Rate limit hit for {symbol}")
        # Save it to a rejected folder.
        bronze_error_path = "data/rejected/api_errors"
        os.makedirs(bronze_error_path, exist_ok=True)
        ingestion_time = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        file_name = f"{symbol}_{ingestion_time}.json"
        file_path = os.path.join(bronze_error_path, file_name)
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
        continue


    if "Meta Data" not in data or "Time Series (Daily)" not in data:
        print(f"Skipping invalid API response for {symbol}: missing Meta Data or Time Series (Daily)")
        # Save it to a rejected folder.
        bronze_error_path = "data/rejected/api_errors"
        os.makedirs(bronze_error_path, exist_ok=True)
        ingestion_time = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        file_name = f"{symbol}_{ingestion_time}.json"
        file_path = os.path.join(bronze_error_path, file_name)
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
        continue

    # Create Bronze directory
    bronze_path = "data/bronze/stock_prices"

    os.makedirs(bronze_path, exist_ok=True)

    # Create filename
    ingestion_time = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    file_name = f"{symbol}_{ingestion_time}.json"

    # Save raw JSON
    file_path = os.path.join(bronze_path, file_name)

    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)

    # Print Success message
    print(f"Successfully saved Bronze file: {file_path} for {symbol}")

print("All bronze data stored!")


