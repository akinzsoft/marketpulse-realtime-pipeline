from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, round, when, lit,
    avg, max, min, count
)

# ── 1. Clean Data ────────────────────────────────────────
def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean the raw stock data:
    - Remove rows with null values
    - Round prices to 2 decimal places
    """
    return (
        df
        .dropna()
        .withColumn("open",  round(col("open"),  2))
        .withColumn("high",  round(col("high"),  2))
        .withColumn("low",   round(col("low"),   2))
        .withColumn("close", round(col("close"), 2))
    )

# ── 2. Add Price Movement ────────────────────────────────
def add_price_movement(df: DataFrame) -> DataFrame:
    """
    Add a price_change column showing how much
    the stock moved from open to close.

    positive = price went UP   📈
    negative = price went DOWN 📉
    """
    return (
        df.withColumn(
            "price_change",
            round(col("close") - col("open"), 2)
        )
        .withColumn(
            "movement",
            when(col("price_change") > 0, "UP")
            .when(col("price_change") < 0, "DOWN")
            .otherwise("FLAT")
        )
    )

# ── 3. Add Price Range ───────────────────────────────────
def add_price_range(df: DataFrame) -> DataFrame:
    """
    Add a price_range column showing the
    difference between high and low price.
    High range = volatile stock
    Low range  = stable stock
    """
    return df.withColumn(
        "price_range",
        round(col("high") - col("low"), 2)
    )

# ── 4. Classify Volume ───────────────────────────────────
def classify_volume(df: DataFrame) -> DataFrame:
    """
    Classify trading volume as:
    HIGH   = lots of trading activity
    MEDIUM = normal trading activity
    LOW    = quiet trading day
    """
    return df.withColumn(
        "volume_category",
        when(col("volume") > 1000000, "HIGH")
        .when(col("volume") > 100000,  "MEDIUM")
        .otherwise("LOW")
    )

# ── 5. Run All Transformations ───────────────────────────
def process_stock_data(df: DataFrame) -> DataFrame:
    """
    Run all transformations in sequence.
    This is the main function called by Spark.
    """
    print("⚙️  Processing stock data...")

    df = clean_data(df)
    df = add_price_movement(df)
    df = add_price_range(df)
    df = classify_volume(df)

    return df
