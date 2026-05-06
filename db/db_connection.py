import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from root folder
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

# ── Database Configuration ───────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", "5433"),
    "database": os.getenv("DB_NAME", "marketpulse"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres")
}

# ── Create Connection ────────────────────────────────────
def get_connection():
    """
    Create and return a PostgreSQL connection.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Connected to PostgreSQL!")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

# ── Test Connection ──────────────────────────────────────
def test_connection():
    """
    Test the database connection.
    """
    conn = get_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"📦 PostgreSQL version: {version[0]}")
        conn.close()
        return True
    return False

if __name__ == "__main__":
    test_connection()
