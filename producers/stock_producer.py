import time
from producers.api_client import fetch_stock_data
from producers.config import STOCK_SYMBOLS

def run_producer():
    """
    Continuously fetch stock data for all symbols.
    """
    print("🚀 Starting MarketPulse Stock Producer...")
    print(f"📈 Tracking: {', '.join(STOCK_SYMBOLS)}")
    print("-" * 50)

    while True:
        for symbol in STOCK_SYMBOLS:
            data = fetch_stock_data(symbol)
            if data:
                print(f"[{data['timestamp']}] {data['symbol']} | "
                      f"Close: ${data['close']} | "
                      f"Volume: {data['volume']:,}")
            else:
                print(f"No data for {symbol}")

            print(f"⏳ Waiting 15 seconds before next symbol...")
            time.sleep(15)  # ← delay between each symbol

        print("-" * 50)
        print("⏳ Waiting 90 seconds before next round...")
        time.sleep(90)  # ← delay between rounds

if __name__ == "__main__":
    run_producer()