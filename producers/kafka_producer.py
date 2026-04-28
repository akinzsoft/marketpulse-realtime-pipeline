import json
import time
from kafka import KafkaProducer
from producers.api_client import fetch_stock_data
from producers.config import STOCK_SYMBOLS

# Kafka topic name
TOPIC = "stock-data"

def create_producer():
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def run_kafka_producer():
    """
    Fetch stock data and stream it to Kafka topic.
    """
    print("🚀 Starting MarketPulse Kafka Producer...")
    print(f"📡 Streaming to Kafka topic: {TOPIC}")
    print(f"📈 Tracking: {', '.join(STOCK_SYMBOLS)}")
    print("-" * 50)

    producer = create_producer()

    while True:
        for symbol in STOCK_SYMBOLS:
            data = fetch_stock_data(symbol)

            if data:
                producer.send(TOPIC, value=data)
                producer.flush()

                print(f"✅ Sent to Kafka → [{data['timestamp']}] "
                      f"{data['symbol']} | "
                      f"Close: ${data['close']} | "
                      f"Volume: {data['volume']:,}")
            else:
                print(f"❌ No data for {symbol}")

            print(f"⏳ Waiting 15 seconds...")
            time.sleep(15)

        print("-" * 50)
        print("⏳ Waiting 90 seconds before next round...")
        time.sleep(90)

if __name__ == "__main__":
    run_kafka_producer()
