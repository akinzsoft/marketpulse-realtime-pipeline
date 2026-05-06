from db.db_connection import get_connection
from db.neon_connection import get_neon_connection

def sync_all_to_neon():
    """
    Copy ALL records from local PostgreSQL to Neon Cloud.
    """
    print("🚀 Starting full sync to Neon Cloud...")

    local_conn = get_connection()
    neon_conn  = get_neon_connection()

    if not local_conn or not neon_conn:
        return False

    try:
        local_cursor = local_conn.cursor()
        neon_cursor  = neon_conn.cursor()

        # Fetch ALL records from local
        local_cursor.execute("""
            SELECT
                symbol, timestamp, open, high, low,
                close, volume, price_change, movement,
                price_range, volume_category
            FROM stock_data
            ORDER BY timestamp ASC;
        """)
        rows = local_cursor.fetchall()
        print(f"📦 Found {len(rows)} records in local database")

        # Insert all into Neon
        success = 0
        for row in rows:
            try:
                neon_cursor.execute("""
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
                """, row)
                success += 1
            except Exception as e:
                print(f"❌ Row error: {e}")

        neon_conn.commit()
        print(f"✅ Synced {success} records to Neon Cloud!")

        # Verify
        neon_cursor.execute("SELECT COUNT(*) FROM stock_data;")
        count = neon_cursor.fetchone()[0]
        print(f"📊 Total records now in Neon: {count}")
        return True

    except Exception as e:
        print(f"❌ Sync failed: {e}")
        neon_conn.rollback()
        return False

    finally:
        local_conn.close()
        neon_conn.close()

if __name__ == "__main__":
    sync_all_to_neon()
