import requests
from producers.config import RAPIDAPI_KEY, RAPIDAPI_HOST, INTERVAL

def fetch_stock_data(symbol: str) -> dict:
    """
    Fetch latest single record - used by Kafka producer.
    """
    url = "https://alpha-vantage.p.rapidapi.com/query"

    querystring = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": INTERVAL,
        "datatype": "json"
    }

    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()

        time_series_key = f"Time Series ({INTERVAL})"
        if time_series_key not in data:
            print(f"[WARNING] No time series data for {symbol}")
            return {}

        # Get the latest entry only
        latest_timestamp = sorted(data[time_series_key].keys())[-1]
        latest_data = data[time_series_key][latest_timestamp]

        return {
            "symbol":    symbol,
            "timestamp": latest_timestamp,
            "open":      float(latest_data["1. open"]),
            "high":      float(latest_data["2. high"]),
            "low":       float(latest_data["3. low"]),
            "close":     float(latest_data["4. close"]),
            "volume":    int(latest_data["5. volume"])
        }

    except Exception as e:
        print(f"[ERROR] Failed to fetch data for {symbol}: {e}")
        return {}


def fetch_all_stock_data(symbol: str) -> list:
    """
    Fetch ALL 100 historical minutes for a symbol.
    Returns a list of records.
    """
    url = "https://alpha-vantage.p.rapidapi.com/query"

    querystring = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": INTERVAL,
        "datatype": "json"
    }

    headers = {
        "X-RapidAPI-Key": RAPIDAPI_KEY,
        "X-RapidAPI-Host": RAPIDAPI_HOST
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()

        time_series_key = f"Time Series ({INTERVAL})"
        if time_series_key not in data:
            print(f"[WARNING] No time series data for {symbol}")
            return []

        records = []
        for timestamp, values in data[time_series_key].items():
            records.append({
                "symbol":    symbol,
                "timestamp": timestamp,
                "open":      float(values["1. open"]),
                "high":      float(values["2. high"]),
                "low":       float(values["3. low"]),
                "close":     float(values["4. close"]),
                "volume":    int(values["5. volume"])
            })

        # Sort by timestamp oldest to newest
        records.sort(key=lambda x: x["timestamp"])
        print(f"✅ Fetched {len(records)} records for {symbol}")
        return records

    except Exception as e:
        print(f"[ERROR] Failed to fetch all data for {symbol}: {e}")
        return []
