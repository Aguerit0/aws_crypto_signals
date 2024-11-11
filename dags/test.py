import pandas as pd
import asyncio
from datetime import datetime
from crypto_data import CryptoData
from indicators import Indicators  # Asegúrate de importar correctamente la clase Indicators

async def test_indicators_live(symbol="BTCUSDT", interval="1h"):
    # Obtención de datos en vivo usando tu clase CryptoData
    crypto_data = CryptoData(symbol, interval)
    
    # Asumimos que el método get_data() de CryptoData obtiene datos en vivo
    data = await crypto_data.get_live_data()  # Debe devolver un DataFrame con 'timestamp' y 'close'

    if data is None or data.empty:
        print("No se pudieron obtener datos.")
        return

    # Imprime las primeras filas para ver la estructura de los datos
    print(f"Datos obtenidos para {symbol} ({interval}):")
    print(data.head())  # Muestra las primeras filas de datos obtenidos

    # Asegúrate de que el índice es el timestamp, de lo contrario usa data.index
    print(f"Índice de los datos: {data.index}")  # Muestra el índice para confirmar que es la marca de tiempo

    # Inicializa la clase de indicadores
    indicators = Indicators(symbol, interval)

    # Calcula el RSI
    rsi = await indicators.rsi(data)

    # Calcula el MACD
    macd_line, signal_line, histogram = await indicators.macd(data)

    # Calcula las Bandas de Bollinger
    sma_21, lower, upper = await indicators.bollinger_bands(data)

    # Calcula el EMA de 200 períodos
    ema_200 = await indicators.ema_200(data)

    # Calcula las medias móviles de 5, 10 y 20 períodos

    # Combina todos los indicadores en un DataFrame
    result_df = pd.DataFrame({
        'timestamp': data.index,  # Usamos el índice como timestamp
        'close': data['close'],
        'RSI': rsi,
        'MACD': macd_line,
        'Signal_Line': signal_line,
        'MACD_Histogram': histogram,
        'SMA_21': sma_21,
        'Bollinger_Lower': lower,
        'Bollinger_Upper': upper,
        'EMA_200': ema_200,
    })

    print("\nDataFrame con todos los indicadores calculados:")
    print(result_df.tail())  # Muestra las últimas filas del DataFrame con los resultados

    print('\n\nCRYPTO DATA\n\n')
    print(data.columns)

# Ejecutar el test
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_indicators_live())