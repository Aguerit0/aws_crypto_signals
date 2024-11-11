""" 
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import boto3
import pandas as pd
from crypto_data import CryptoData
from indicators import Indicators
from alert_indicators import AlertLive
from io import StringIO
import asyncio

@dag(
    schedule_interval=timedelta(minutes=1),  # Ejecuta cada minuto
    start_date=datetime(2024, 11, 6),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),  # Tiempo límite para el dagrun
    max_active_runs=1,  # Limita el número de ejecuciones concurrentes
    dag_id="strata-lab-1-v03"
)
def crypto_trading_pipeline():
    symbol = "BTCUSDT"
    interval = "1m"

    @task
    def fetch_and_calculate_indicators():
        async def fetch_data_and_calculate():
            # Obteniendo datos de criptomonedas y calculando indicadores
            crypto_data = CryptoData(symbol, interval)
            data = await crypto_data.get_live_data()
            
            indicators = Indicators(symbol, interval)
            rsi, stoch_rsi, macd, bollinger_bands, mvg = await asyncio.gather(
                indicators.rsi(data),
                indicators.stochastic_rsi(data),
                indicators.macd(data),
                indicators.bollinger_bands(data),
                indicators.moving_averages(data)
            )
            
            return pd.DataFrame({
                'timestamp': data['timestamp'],
                'rsi': rsi,
                'stoch_rsi': stoch_rsi,
                'macd': macd,
                'bollinger_bands': bollinger_bands,
                'mvg': mvg
            })

        # Ejecuta la función asincrónica
        return asyncio.run(fetch_data_and_calculate())

    @task
    def check_alerts(indicator_data):
        async def validate_alerts():
            # Verifica señales de compra/venta según todos los indicadores
            alert_live = AlertLive()
            rsi_check, stoch_rsi_check, macd_check, bollinger_bands_check, mvg_check = await asyncio.gather(
                alert_live.check_rsi(indicator_data),
                alert_live.check_stochastic_rsi(indicator_data),
                alert_live.check_macd(indicator_data),
                alert_live.check_bollinger_bands(indicator_data),
                alert_live.check_moving_averages(indicator_data)
            )
            
            # Filtra solo las señales válidas y retorna en un DataFrame
            conditions = {
                'timestamp': indicator_data['timestamp'],
                'rsi': rsi_check,
                'stoch_rsi': stoch_rsi_check,
                'macd': macd_check,
                'bollinger_bands': bollinger_bands_check,
                'mvg': mvg_check
            }
            signals = pd.DataFrame(conditions).dropna()  # Conserva solo señales válidas
            return signals if not signals.empty else None

        # Ejecuta la función asincrónica
        return asyncio.run(validate_alerts())

    @task
    def export_signals_to_s3(signals):
        if signals is not None:
            s3 = boto3.client('s3')
            bucket_name = 'eaguerito-strata-lab-1'
            object_name = 'data_trade_signal.csv'
            
            try:
                response = s3.get_object(Bucket=bucket_name, Key=object_name)
                existing_df = pd.read_csv(response['Body'])
                updated_df = pd.concat([existing_df, signals], ignore_index=True)
            except s3.exceptions.NoSuchKey:
                updated_df = signals
            except Exception as e:
                print(f"Error al leer el archivo S3: {e}")
                return  # Si hay un error, se detiene la ejecución de la tarea

            csv_buffer = StringIO()
            updated_df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())

    data = fetch_and_calculate_indicators()
    signals = check_alerts(data)
    export_signals_to_s3(signals)

crypto_trading_pipeline()
 """