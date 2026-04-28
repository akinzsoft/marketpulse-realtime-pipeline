import json
from kafka import KafkaConsumer

TOPIC = "stock-data"

def run_kafka_consumer():
    """
    Listen to Kafka topic and print incoming stock data.
    """
    print("👂 Starting Kafka Consumer...")
    print(f"📡 Listening to topic: {TOPIC}")
    print("-" * 50)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    for message in consumer:
        data = message.value
        print(f"📩 Received → [{data['timestamp']}] "
              f"{data['symbol']} | "
              f"Close: ${data['close']} | "
              f"Volume: {data['volume']:,}")

if __name__ == "__main__":
    run_kafka_consumer()
