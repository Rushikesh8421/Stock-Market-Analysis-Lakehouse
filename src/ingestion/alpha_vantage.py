import os
import json
from datetime import datetime

import requests
from dotenv import load_dotenv


# Load the API key
load_dotenv()

print("Current working directory:", os.getcwd())

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

if not API_KEY:
    raise ValueError("Alpha Vantage API key not found")


# Build the URL
symbol = "AAPL"

url = (
    "https://www.alphavantage.co/query"
    f"?function=TIME_SERIES_DAILY"
    f"&symbol={symbol}"
    f"&apikey={API_KEY}"
)


# Call the API
response = requests.get(url)
data = response.json()


# Validate response
if "Error Message" in data:
    raise Exception(data["Error Message"])

if "Note" in data:
    raise Exception(data["Note"])

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
print(f"Successfully saved Bronze file: {file_path}")


