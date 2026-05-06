import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType
)

# ── 1. Create Spark Session ──────────────────────────────
def create_spark_session():
    """
    Create and return a Spark session configured
    to connect to Kafka.
    """
    return (
        SparkSession.builder
        .appName("MarketPulse-Stream-Consumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )

# ── 2. Define Stock Data Schema ──────────────────────────
def get_stock_schema():
    """
    Define the structure of incoming stock data.
    Spark needs to know the shape of the data.
    """
    return StructType([
        StructField("symbol",    StringType(),  True),
        StructField("timestamp", StringType(),  True),
        StructField("open",      FloatType(),   True),
        StructField("high",      FloatType(),   True),
        StructField("low",       FloatType(),   True),
        StructField("close",     FloatType(),   True),
        StructField("volume",    IntegerType(), True)
    ])

# ── 3. Read Stream from Kafka ────────────────────────────
def read_kafka_stream(spark):
    """
    Connect to Kafka and read the stock-data topic
    as a real-time stream.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "stock-data")
        .option("startingOffsets", "earliest")
        .load()
    )

# ── 4. Parse Raw Kafka Messages ──────────────────────────
def parse_stream(df, schema):
    """
    Kafka sends raw bytes. This function:
    1. Converts bytes → string
    2. Parses JSON string → structured columns
    3. Adds a processed_at timestamp
    """
    return (
        df.select(
            from_json(
                col("value").cast("string"), schema
            ).alias("data")
        )
        .select("data.*")
        .withColumn("processed_at", current_timestamp())
    )

# ── 5. Main Runner ───────────────────────────────────────
def run_spark_consumer():
    print("🚀 Starting MarketPulse Spark Consumer...")

    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Get schema
    schema = get_stock_schema()

    # Read from Kafka
    raw_stream = read_kafka_stream(spark)

    # Parse the stream
    parsed_stream = parse_stream(raw_stream, schema)
    # Process the stream
    from spark.spark_processor import process_stock_data
    processed_stream = process_stock_data(parsed_stream)
    # Print to console (for testing)
    query = (
        parsed_stream.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
    )

    print("✅ Spark is consuming from Kafka...")
    print("📊 Waiting for stock data...")
    print("-" * 50)

    # Keep running until stopped
    query.awaitTermination()

if __name__ == "__main__":
    run_spark_consumer()
