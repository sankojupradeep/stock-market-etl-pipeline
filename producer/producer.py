import time
import json
import requests
from kafka import KafkaProducer

API_KEY = "d380m7pr01qlbdj3d9f0d380m7pr01qlbdj3d9fg"
API_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_stock_data(symbol):
    """Fetch stock data for a given symbol from Finnhub API."""
    url = f"{API_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except requests.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

if __name__ == "__main__":
    while True:
        for symbol in SYMBOLS:
            stock_data = fetch_stock_data(symbol)
            if stock_data:
                producer.send("stock_data", value=stock_data)
                print(f"✅ Sent data for {symbol}: {stock_data}")
            time.sleep(5)  # Sleep between requests (API rate-limiting)
        time.sleep(60)  # Wait before the next full cycle
