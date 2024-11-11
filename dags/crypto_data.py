import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CryptoData:
    BASE_URL = "https://api.binance.com/api/v3/klines"

    def __init__(self, symbol: str, interval: str, cache_duration=60):
        self.symbol = symbol
        self.interval = interval
        self.cache_duration = timedelta(seconds=cache_duration)
        self.cache = {}
        self.cache_timestamp = None

    async def get_live_data(self):
        if self.cache_timestamp and (datetime.now() - self.cache_timestamp) < self.cache_duration:
            return self.cache
        
        params = {
            "symbol": self.symbol,
            "interval": self.interval,
            "limit": 500  # Number of data points to fetch
        }
        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract the desired data and convert to a DataFrame
            df = pd.DataFrame(data, columns=[
                "timestamp", "open", "high", "low", "close", "volume", "close_time",
                "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume", "ignore"
            ])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
            df.set_index("timestamp", inplace=True)
            df = df.astype(float)
            
            # Cache the data
            self.cache = df
            self.cache_timestamp = datetime.now()
            
            df = pd.DataFrame(df)
            return df
        except requests.RequestException as e:
            logger.error(f"Error fetching live data: {e}")
            raise
