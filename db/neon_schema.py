from db.neon_connection import get_neon_connection

def create_neon_tables():
    conn = get_neon_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()
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
        conn.commit()
        print("✅ Neon tables created successfully!")
        print("📋 Table: stock_data")
        return True

    except Exception as e:
        print(f"❌ Failed to create Neon tables: {e}")
        conn.rollback()
        return False

    finally:
        conn.close()

if __name__ == "__main__":
    create_neon_tables()
