import pandas as pd
from datetime import datetime

from crypto_data import CryptoData


class Indicators:
    def __init__(self, symbol, timeinterval):
        self.symbol = symbol
        self.timeinterval = timeinterval

    # RSI
    async def rsi(self, data, period=14):
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Se esperaba un DataFrame como entrada")

        if "close" not in data.columns:
            raise ValueError("El DataFrame debe contener una columna 'close'")

        data["close"] = data["close"].astype(float)
        delta = data["close"].diff()

        up, down = delta.clip(lower=0), delta.clip(upper=0).abs()
        _gain = up.ewm(com=(period - 1), min_periods=period).mean()
        _loss = down.ewm(com=(period - 1), min_periods=period).mean()

        RS = _gain / _loss
        rsi = 100 - (100 / (1 + RS))

        df = pd.DataFrame({"rsi": rsi})

        return df  # return rsi series

    # RSI stochastic
    async def stochastic_rsi(self, rsi, period=14, smooth_k=3, smooth_d=3):
        # RSI standard
        # Convert data to numeric
        # data[['open', 'high', 'low', 'close']] = data[['open', 'high', 'low', 'close']].apply(pd.to_numeric)

        """delta = data['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        avg_gain = gain.rolling(window=period, min_periods=1).mean()
        avg_loss = loss.rolling(window=period, min_periods=1).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))"""

        # RSI to method RSI

        # Calculate %K of stochastic RSI
        rsi_min = rsi.rolling(window=period, center=False).min()
        rsi_max = rsi.rolling(window=period, center=False).max()
        stoch = ((rsi - rsi_min) / (rsi_max - rsi_min)) * 100

        # Smoothing %K
        k = stoch.rolling(window=smooth_k, center=False).mean()

        # Calculate %D of stochastic RSI
        d = k.rolling(window=smooth_d, center=False).mean()

        df = pd.DataFrame({"k": k, "d": d})

        return df

    # MACD: receive a DataFrame with the 'close' column
    async def macd(self, data, short_period=12, long_period=26, signal_period=9):
        # Calculate Short EMA
        short_ema = (
            data["close"].ewm(span=short_period, min_periods=1, adjust=False).mean()
        )

        # Calculate Long EMA
        long_ema = (
            data["close"].ewm(span=long_period, min_periods=1, adjust=False).mean()
        )

        # Calculate MACD line
        macd_line = short_ema - long_ema

        # Calculate Signal line
        signal_line = macd_line.ewm(
            span=signal_period, min_periods=1, adjust=False
        ).mean()

        # Calculate MACD histogram
        histogram = macd_line - signal_line

        df = pd.DataFrame(
            {
                "macd_line": macd_line,
                "signal_line": signal_line,
                "histogram": histogram,
            }
        )

        return df

    # Bands of Bollinger
    async def bollinger_bands(self, data):
        # SMA 21
        sma_21 = data["close"].rolling(window=21).mean()

        # Calculate lower and upper bands
        lower = sma_21 - 2 * data["close"].rolling(window=20).std()
        upper = sma_21 + 2 * data["close"].rolling(window=20).std()
        close = data['close']

        df = pd.DataFrame(
            {
                "close" : close,
                "sma_21": sma_21,
                "lower": lower,
                "upper": upper,
            }
        )

        return df

    # Calculate EMA of 200 periods
    async def ema_200(self, data):
        ema = data["close"].ewm(span=200, adjust=False).mean()
        close = data['close']

        df = pd.DataFrame({ "close": close, "ema_200": ema })

        return df

    # 3 Moving Averages
    async def moving_averages(self, data):
        # Calculate MMS 5 periods
        MM_5 = pd.DataFrame()
        MM_5["close"] = data["close"].rolling(window=5).mean()
        # Calculate MM_5S 10 periods
        MM_10 = pd.DataFrame()
        MM_10["close"] = (
            data["close"].rolling(window=10).mean()
        )  # Calculate mean of 10 periods
        # Calculate MM_5S 10 periods
        MM_20 = pd.DataFrame()
        MM_20["close"] = (
            data["close"].rolling(window=20).mean()
        )  # Calculate mean of 20 periods

        # Price of crossover of MM_5  and MM_10 and MM_20
        df = pd.DataFrame()
        data["MM_5"] = MM_5["close"]
        data["MM_10"] = MM_10["close"]
        data["MM_20"] = MM_20["close"]

        return df
