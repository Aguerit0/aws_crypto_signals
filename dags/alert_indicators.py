import pandas as pd
from datetime import datetime
from indicators import Indicators
from crypto_data import CryptoData


class AlertLive:
    def __init__(self):
        self.symbols = [
            "BTCUSDT",
            "ETHUSDT",
            "BNBUSDT",
            "ADAUSDT",
            "XRPUSDT",
            "SOLUSDT",
            "DOTUSDT",
            "DOGEUSDT",
            "LINKUSDT",
            "LTCUSDT",
        ]
        self.timeinterval = 1
        self.temporalidad = f"{self.timeinterval}m"
        self.now = datetime.now().strftime("%H:%M:%S")

    async def fetch_data(self, symbol: str):
        """Obtiene los datos en vivo solo una vez por símbolo"""
        df_live_data = CryptoData(symbol, self.temporalidad)
        return await df_live_data.get_live_data()

    async def check_rsi(self, data: pd.DataFrame):
        results = []
        for symbol in self.symbols:
            indicators = Indicators(symbol, self.temporalidad)
            rsi = await indicators.rsi(data)
            last_rsi = rsi.iloc[-1].round(2)
            if last_rsi <= 30:
                signal = 1  # Compra
            elif last_rsi >= 70:
                signal = 2  # Vende
            else:
                signal = 0  # Neutral
            results.append((symbol, self.now, signal))
        return results

    async def check_stochastic_rsi(self, data: pd.DataFrame):
        results = []
        for symbol in self.symbols:
            indicators = Indicators(symbol, self.temporalidad)
            rsi_values = await indicators.rsi(data)
            k, d = await indicators.stochastic_rsi(rsi_values)
            if k.iloc[-1] <= 15 and k.iloc[-1] > d.iloc[-1] and k.iloc[-2] < d.iloc[-2]:
                signal = 1  # Compra
            elif (k.iloc[-1] >= 85 and k.iloc[-1] < d.iloc[-1] and k.iloc[-2] > d.iloc[-2]):
                signal = 2  # Vende
            else:
                signal = 0  # Neutral
            results.append((symbol, self.now, signal))
        return results

    async def check_macd(self, data: pd.DataFrame):
        results = []
        for symbol in self.symbols:
            indicators = Indicators(symbol, self.temporalidad)
            macd_line, signal_line, histogram = await indicators.macd(data)
            data_macd = pd.DataFrame()
            data_macd["MACD"] = macd_line
            data_macd["Signal"] = signal_line
            data_macd["Histogram"] = histogram
            if (data_macd["MACD"].iloc[-2] > data_macd["Signal"].iloc[-2] and data_macd["Histogram"].iloc[-2] > 0) and (data_macd["MACD"].iloc[-1] < data_macd["Signal"].iloc[-1] and data_macd["Histogram"].iloc[-1] < 0):
                signal = 2  # Vende
            elif (
                data_macd["MACD"].iloc[-2] < data_macd["Signal"].iloc[-2]
                and data_macd["Histogram"].iloc[-2] < 0
            ) and (
                data_macd["MACD"].iloc[-1] > data_macd["Signal"].iloc[-1]
                and data_macd["Histogram"].iloc[-1] > 0
            ):
                signal = 1  # Compra
            else:
                signal = 0  # Neutral
            results.append((symbol, self.now, signal))
        return results

    async def check_bollinger_bands(self, data: pd.DataFrame):
        results = []
        for symbol in self.symbols:
            indicators = Indicators(symbol, self.temporalidad)
            sma_21, lower, upper = await indicators.bollinger_bands(data)
            data["close"] = data["close"].astype(float)
            if data["close"].iloc[-1] > upper.iloc[-1]:
                signal = 2  # Vende
            elif data["close"].iloc[-1] < lower.iloc[-1]:
                signal = 1  # Compra
            else:
                signal = 0  # Neutral
            results.append((symbol, self.now, signal))
        return results

    async def check_ema_200(self, data: pd.DataFrame):
        results = []
        for symbol in self.symbols:
            data["close"] = data["close"].astype(float)
            indicators = Indicators(symbol, self.temporalidad)
            ema200 = await indicators.ema_200(data)
            if (
                data["close"].iloc[-1] > ema200.iloc[-1]
                and data["close"].iloc[-2] < ema200.iloc[-1]
            ):
                signal = 'SUPPORT'
            elif (
                data["close"].iloc[-1] < ema200.iloc[-1]
                and data["close"].iloc[-2] > ema200.iloc[-1]
            ):
                signal = 'RESISTANCE'
            else:
                signal = 0  # Neutral
            results.append((symbol, self.now, signal))
        return results

    async def check_moving_averages(self, data: pd.DataFrame):
        results = []
        for symbol in self.symbols:
            indicators = Indicators(symbol, self.temporalidad)
            moving_average = await indicators.moving_averages(data)
            if (
                moving_average["MM_5"].iloc[-2] and moving_average["MM_10"].iloc[-2]
            ) < moving_average["MM_20"].iloc[-2] and (
                moving_average["MM_5"].iloc[-1] and moving_average["MM_10"].iloc[-1]
            ) > moving_average[
                "MM_20"
            ].iloc[
                -1
            ]:
                signal = 1  # Compra
            elif (
                moving_average["MM_5"].iloc[-2] and moving_average["MM_10"].iloc[-2]
            ) > moving_average["MM_20"].iloc[-2] and (
                moving_average["MM_5"].iloc[-1] and moving_average["MM_10"].iloc[-1]
            ) < moving_average[
                "MM_20"
            ].iloc[
                -1
            ]:
                signal = 2  # Vende
            else:
                signal = 0  # Neutral
            results.append((symbol, self.now, signal))
        return results

    async def run(self, indicator):
        results = []
        for symbol in self.symbols:
            data = await self.fetch_data(symbol)
            if indicator == "1":
                results = await self.check_rsi(data)
            elif indicator == "2":
                results = await self.check_stochastic_rsi(data)
            elif indicator == "3":
                results = await self.check_macd(data)
            elif indicator == "4":
                results = await self.check_bollinger_bands(data)
            elif indicator == "5":
                results = await self.check_ema_200(data)
            elif indicator == "6":
                results = await self.check_moving_averages(data)
        return results
