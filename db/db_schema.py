from db.db_connection import get_connection

# ── Create Tables ────────────────────────────────────────
def create_tables():
    """
    Create the stock_data table in PostgreSQL.
    This is where all processed stock data will be stored.
    """
    conn = get_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # Create stock_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                id              SERIAL PRIMARY KEY,
                symbol          VARCHAR(10)    NOT NULL,
                timestamp       VARCHAR(30)    NOT NULL,
                open            DECIMAL(10, 4) NOT NULL,
                high            DECIMAL(10, 4) NOT NULL,
                low             DECIMAL(10, 4) NOT NULL,
                close           DECIMAL(10, 4) NOT NULL,
                volume          INTEGER        NOT NULL,
                price_change    DECIMAL(10, 4),
                movement        VARCHAR(10),
                price_range     DECIMAL(10, 4),
                volume_category VARCHAR(10),
                processed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, timestamp)
            );
        """)

        # Create index for faster queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_symbol
            ON stock_data(symbol);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_timestamp
            ON stock_data(timestamp);
        """)

        conn.commit()
        print("✅ Tables created successfully!")
        print("📋 Table: stock_data")
        print("📋 Index: idx_stock_symbol")
        print("📋 Index: idx_stock_timestamp")
        return True

    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        conn.rollback()
        return False

    finally:
        conn.close()

# ── Drop Tables (for reset) ──────────────────────────────
def drop_tables():
    """
    Drop all tables — use carefully!
    """
    conn = get_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS stock_data CASCADE;")
        conn.commit()
        print("🗑️  Tables dropped successfully!")
        return True

    except Exception as e:
        print(f"❌ Failed to drop tables: {e}")
        conn.rollback()
        return False

    finally:
        conn.close()

if __name__ == "__main__":
    create_tables()
