from db.db_connection import get_connection

# ── Write Single Record ──────────────────────────────────
def write_stock_data(data: dict) -> bool:
    """
    Write a single stock record to PostgreSQL.
    Uses INSERT ... ON CONFLICT to prevent duplicates.
    """
    conn = get_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO stock_data (
                symbol, timestamp, open, high, low, close,
                volume, price_change, movement,
                price_range, volume_category
            )
            VALUES (
                %(symbol)s, %(timestamp)s, %(open)s, %(high)s,
                %(low)s, %(close)s, %(volume)s, %(price_change)s,
                %(movement)s, %(price_range)s, %(volume_category)s
            )
            ON CONFLICT (symbol, timestamp)
            DO UPDATE SET
                close           = EXCLUDED.close,
                volume          = EXCLUDED.volume,
                price_change    = EXCLUDED.price_change,
                movement        = EXCLUDED.movement,
                price_range     = EXCLUDED.price_range,
                volume_category = EXCLUDED.volume_category,
                processed_at    = CURRENT_TIMESTAMP;
        """, data)

        conn.commit()
        return True

    except Exception as e:
        print(f"❌ Failed to write data: {e}")
        conn.rollback()
        return False

    finally:
        conn.close()

# ── Write Batch of Records ───────────────────────────────
def write_batch(records: list) -> int:
    """
    Write a batch of stock records to PostgreSQL.
    Returns number of successfully written records.
    """
    success_count = 0
    for record in records:
        if write_stock_data(record):
            success_count += 1
    return success_count

# ── Read Latest Stock Data ───────────────────────────────
def get_latest_stocks() -> list:
    """
    Fetch the latest price for each stock symbol.
    """
    conn = get_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ON (symbol)
                symbol, timestamp, open, high, low,
                close, volume, price_change,
                movement, price_range, volume_category
            FROM stock_data
            ORDER BY symbol, timestamp DESC;
        """)

        rows = cursor.fetchall()
        columns = [
            "symbol", "timestamp", "open", "high",
            "low", "close", "volume", "price_change",
            "movement", "price_range", "volume_category"
        ]
        return [dict(zip(columns, row)) for row in rows]

    except Exception as e:
        print(f"❌ Failed to read data: {e}")
        return []

    finally:
        conn.close()

# ── Count Records ────────────────────────────────────────
def get_record_count() -> int:
    """
    Return total number of records in stock_data table.
    """
    conn = get_connection()
    if not conn:
        return 0

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM stock_data;")
        return cursor.fetchone()[0]

    except Exception as e:
        print(f"❌ Failed to count records: {e}")
        return 0

    finally:
        conn.close()

if __name__ == "__main__":
    # Quick test
    test_data = {
        "symbol":           "AAPL",
        "timestamp":        "2026-05-06 14:30:00",
        "open":             282.40,
        "high":             282.61,
        "low":              282.26,
        "close":            282.40,
        "volume":           937,
        "price_change":     0.00,
        "movement":         "FLAT",
        "price_range":      0.35,
        "volume_category":  "LOW"
    }

    print("🧪 Testing db_writer...")
    result = write_stock_data(test_data)
    if result:
        print("✅ Test record written!")
        count = get_record_count()
        print(f"📊 Total records in database: {count}")
        latest = get_latest_stocks()
        print(f"📈 Latest stocks: {latest}")
