import asyncio
from alert_indicators import AlertLive
from indicators import Indicators
from crypto_data import CryptoData

async def test_alert_live():
    alert = AlertLive()
    symbol = "BTCUSDT"
    crypto_data = CryptoData(symbol, alert.temporalidad)
    df = await crypto_data.get_live_data()
    
    # Prueba de Indicators
    indicators = Indicators(symbol, alert.timeinterval)
    rsi = await indicators.rsi(df)    
    print("RSI:", rsi)
    
    k, d = await indicators.stochastic_rsi(rsi)
    print("Stochastic RSI:", k, d)
    
    macd_line, signal_line, histogram = await indicators.macd(df)
    print("MACD:", macd_line, signal_line, histogram)
    
    sma_21, lower, upper = await indicators.bollinger_bands(df)
    print("Bollinger Bands:", sma_21, lower, upper)
    
    ema_200 = await indicators.ema_200(df)
    print("EMA 200:", ema_200)
    
    moving_averages = await indicators.moving_averages(df)
    print("Moving Averages:", moving_averages)
    
    # Prueba de AlertLive
    results = await alert.check_rsi()
    print("Alerta RSI:", results)
    
    results = await alert.check_stochastic_rsi()
    print("Alerta Stochastic RSI:", results)
    
    results = await alert.check_macd()
    print("Alerta MACD:", results)
    
    results = await alert.check_bollinger_bands()
    print("Alerta Bollinger Bands:", results)
    
    results = await alert.check_ema_200()
    print("Alerta EMA 200:", results)
    
    results = await alert.check_moving_averages()
    print("Alerta Moving Averages:", results)

asyncio.run(test_alert_live())