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


    async def check_rsi(self, rsi_df):
       # -- = 0 -- BUY -- = 1 -- SELL -- = 2 -- 
        results = []
        """ now = datetime.now()
        now = str(now.strftime("%H:%M:%S")) """
        for symbol in self.symbols:
            indicator = pd.DataFrame(rsi_df)
            last_rsi = indicator.iloc[-1].round(2)
            if last_rsi <= 30:
                signal = 1
            elif last_rsi >= 70:
                signal = 2
            else:
                signal = 0
            # return symbol, date now, signal
            results.append((symbol, self.now, signal))
        return results

    async def check_stochastic_rsi(self, stochastic_rsi_df):
        results = []
        for symbol in self.symbols:
            indicator = pd.DataFrame(stochastic_rsi_df)
            k, d = indicator["k"], indicator["d"]
            if k.iloc[-1] <= 15 and k.iloc[-1] > d.iloc[-1] and k.iloc[-2] < d.iloc[-2]:
                signal = 1
            elif (k.iloc[-1] >= 85 and k.iloc[-1] < d.iloc[-1] and k.iloc[-2] > d.iloc[-2]):
                signal = 2
            else:
                signal = 0
            results.append((symbol, self.now, signal))

        # return symbol, date now, signal    
        return results

    async def check_macd(self, macd_df):
        results = []
        for symbol in self.symbols:
            indicator = pd.DataFrame(macd_df)
            macd_line, signal_line, histogram = indicator["macd_line"], indicator["signal_line"], indicator["histogram"]
            data_macd = pd.DataFrame()
            data_macd["MACD"] = macd_line
            data_macd["Signal"] = signal_line
            data_macd["Histogram"] = histogram
            if (data_macd["MACD"].iloc[-2] > data_macd["Signal"].iloc[-2] and data_macd["Histogram"].iloc[-2] > 0) and (data_macd["MACD"].iloc[-1] < data_macd["Signal"].iloc[-1]
                and data_macd["Histogram"].iloc[-1] < 0):
                signal = 2
            elif (
                data_macd["MACD"].iloc[-2] < data_macd["Signal"].iloc[-2]
                and data_macd["Histogram"].iloc[-2] < 0
            ) and (
                data_macd["MACD"].iloc[-1] > data_macd["Signal"].iloc[-1]
                and data_macd["Histogram"].iloc[-1] > 0
            ):
                signal = 1
            else:
                signal = 0
            # return symbol, date now, signal
            results.append((symbol, self.now, signal))
        return results

    async def check_bollinger_bands(self, bollinger_bands_df):
        results = []
        for symbol in self.symbols:
            indicator = pd.DataFrame(bollinger_bands_df)
            close, sma_21, lower, upper = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            close, sma_21, lower, upper = indicator["sma_21"], indicator["lower"], indicator["upper"]
            close["close"] = close["close"].astype(float)
            if close["close"].iloc[-1] > upper.iloc[-1]:
                signal = 2
            elif close["close"].iloc[-1] < lower.iloc[-1]:
                signal = 1
            else:
                signal = 0
            # return symbol, date now, signal
            results.append((symbol, self.now, signal))
        return results

    async def check_ema_200(self, ema_200_df):
        results = []
        for symbol in self.symbols:
            indicator = pd.DataFrame(ema_200_df)
            close = pd.DataFrame()
            close["close"] = indicator["close"]
            ema200 = pd.DataFrame()
            ema200["ema_200"] = indicator["ema_200"]
            if (
                close["close"].iloc[-1] > ema200.iloc[-1]
                and close["close"].iloc[-2] < ema200.iloc[-1]
            ):
                signal = 'SUPPORT'
            elif (
                close["close"].iloc[-1] < ema200.iloc[-1]
                and close["close"].iloc[-2] > ema200.iloc[-1]
            ):
                signal = 'RESISTANCE'
            else:
                signal = 0

            # return symbol, date now, signal
            results.append((symbol, self.now, signal))
        return results

    async def check_moving_averages(self, moving_average_df):
        results = []
        for symbol in self.symbols:
            moving_average = pd.DataFrame(moving_average_df)
            if (
                moving_average["MM_5"].iloc[-2] and moving_average["MM_10"].iloc[-2]
            ) < moving_average["MM_20"].iloc[-2] and (
                moving_average["MM_5"].iloc[-1] and moving_average["MM_10"].iloc[-1]
            ) > moving_average[
                "MM_20"
            ].iloc[
                -1
            ]:
                signal = 1
            elif (
                moving_average["MM_5"].iloc[-2] and moving_average["MM_10"].iloc[-2]
            ) > moving_average["MM_20"].iloc[-2] and (
                moving_average["MM_5"].iloc[-1] and moving_average["MM_10"].iloc[-1]
            ) < moving_average[
                "MM_20"
            ].iloc[
                -1
            ]:
                signal = 2
            else:
                signal = 0

            # return symbol, date now, signal
            results.append((symbol, self.now, signal))
        return results

    async def run(self, indicator):
        results = []
        if indicator == "1":
            results = await self.check_rsi()
        elif indicator == "2":
            results = await self.check_stochastic_rsi()
        elif indicator == "3":
            results = await self.check_macd()
        elif indicator == "4":
            results = await self.check_bollinger_bands()
        elif indicator == "5":
            results = await self.check_ema_200()
        elif indicator == "6":
            results = await self.check_moving_averages()
        return results
