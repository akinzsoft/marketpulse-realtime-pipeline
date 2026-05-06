import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from root folder
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

# Neon Config
NEON_CONFIG = {
    "host":     os.getenv("NEON_HOST"),
    "port":     os.getenv("NEON_PORT", "5432"),
    "database": os.getenv("NEON_DB"),
    "user":     os.getenv("NEON_USER"),
    "password": os.getenv("NEON_PASSWORD"),
    "sslmode":  "require"
}

def get_neon_connection():
    """
    Create and return a Neon PostgreSQL connection.
    """
    try:
        conn = psycopg2.connect(**NEON_CONFIG)
        print("✅ Connected to Neon Cloud PostgreSQL!")
        return conn
    except Exception as e:
        print(f"❌ Neon connection failed: {e}")
        return None

def test_neon_connection():
    conn = get_neon_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"📦 Neon version: {version[0]}")
        conn.close()
        return True
    return False

if __name__ == "__main__":
    test_neon_connection()
