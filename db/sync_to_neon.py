from db.db_connection import get_connection
from db.neon_connection import get_neon_connection

def sync_local_to_neon():
    """
    Copy all data from local PostgreSQL to Neon Cloud.
    """
    local_conn = get_connection()
    neon_conn  = get_neon_connection()

    if not local_conn or not neon_conn:
        return False

    try:
        local_cursor = local_conn.cursor()
        neon_cursor  = neon_conn.cursor()

        # Fetch all local data
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

        # Insert into Neon
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
                print(f"❌ Failed to insert row: {e}")

        neon_conn.commit()
        print(f"✅ Synced {success} records to Neon Cloud!")
        return True

    except Exception as e:
        print(f"❌ Sync failed: {e}")
        neon_conn.rollback()
        return False

    finally:
        local_conn.close()
        neon_conn.close()

if __name__ == "__main__":
    sync_local_to_neon()
