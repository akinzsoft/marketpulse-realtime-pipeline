import time
from producers.api_client import fetch_all_stock_data
from producers.config import STOCK_SYMBOLS
from db.db_writer import write_batch
from db.neon_connection import get_neon_connection


def process_records(records: list) -> list:
    """
    Apply transformations to raw records.
    """
    processed = []
    for r in records:
        price_change    = round(r["close"] - r["open"], 2)
        movement        = "UP" if price_change > 0 else "DOWN" if price_change < 0 else "FLAT"
        price_range     = round(r["high"] - r["low"], 2)
        volume_category = "HIGH" if r["volume"] > 1000000 else "MEDIUM" if r["volume"] > 100000 else "LOW"

        processed.append({
            **r,
            "price_change":    price_change,
            "movement":        movement,
            "price_range":     price_range,
            "volume_category": volume_category
        })
    return processed


def write_to_neon(records: list) -> int:
    """
    Write batch of records to Neon Cloud PostgreSQL.
    """
    conn = get_neon_connection()
    if not conn:
        return 0

    success = 0
    try:
        cursor = conn.cursor()
        for r in records:
            try:
                cursor.execute("""
                    INSERT INTO stock_data (
                        symbol, timestamp, open, high, low,
                        close, volume, price_change, movement,
                        price_range, volume_category
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (symbol, timestamp)
                    DO UPDATE SET
                        close           = EXCLUDED.close,
                        volume          = EXCLUDED.volume,
                        price_change    = EXCLUDED.price_change,
                        movement        = EXCLUDED.movement,
                        price_range     = EXCLUDED.price_range,
                        volume_category = EXCLUDED.volume_category,
                        processed_at    = CURRENT_TIMESTAMP;
                """, (
                    r["symbol"],    r["timestamp"],
                    r["open"],      r["high"],
                    r["low"],       r["close"],
                    r["volume"],    r["price_change"],
                    r["movement"],  r["price_range"],
                    r["volume_category"]
                ))
                success += 1
            except Exception as e:
                print(f"❌ Row error: {e}")

        conn.commit()
        return success

    except Exception as e:
        print(f"❌ Neon batch error: {e}")
        conn.rollback()
        return 0

    finally:
        conn.close()


def run_bulk_load():
    """
    Fetch all historical data for all symbols
    and save to both local and Neon PostgreSQL.
    """
    print("🚀 Starting Bulk Load...")
    print(f"📈 Symbols: {', '.join(STOCK_SYMBOLS)}")
    print("-" * 50)

    total_local = 0
    total_neon  = 0

    for symbol in STOCK_SYMBOLS:
        print(f"\n📡 Fetching all data for {symbol}...")

        # Fetch all 100 records from API
        raw_records = fetch_all_stock_data(symbol)

        if not raw_records:
            print(f"❌ No data for {symbol}")
            continue

        # Process/transform records
        processed = process_records(raw_records)

        # Save to local PostgreSQL
        local_saved = write_batch(processed)
        print(f"💾 Local PostgreSQL → Saved {local_saved} records")
        total_local += local_saved

        # Save to Neon Cloud
        neon_saved = write_to_neon(processed)
        print(f"☁️  Neon Cloud → Saved {neon_saved} records")
        total_neon += neon_saved

        print(f"⏳ Waiting 15 seconds before next symbol...")
        time.sleep(15)

    print("-" * 50)
    print(f"✅ Bulk load complete!")
    print(f"💾 Local total: {total_local} records")
    print(f"☁️  Neon total:  {total_neon} records")


if __name__ == "__main__":
    run_bulk_load()
