import requests
import os
from dotenv import load_dotenv

load_dotenv()

url = "https://alpha-vantage.p.rapidapi.com/query"

querystring = {
    "function": "TIME_SERIES_INTRADAY",
    "symbol": "AAPL",
    "interval": "1min",
    "datatype": "json"
}

headers = {
    "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
    "X-RapidAPI-Host": os.getenv("RAPIDAPI_HOST")
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()
print(data)
