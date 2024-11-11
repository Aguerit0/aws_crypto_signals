# configurate AWS credentials: aws configure
# aws s3 mb s3://eaguerito-strata-lab-1


from airflow.decorators import dag, task
from datetime import datetime, timedelta
import boto3
import pandas as pd
from alert_indicators import AlertLive
from io import StringIO
import asyncio
from auth import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME
from crypto_data import CryptoData
from indicators import Indicators

@dag(
    schedule_interval=timedelta(minutes=1),  # executes every minute
    start_date=datetime(2024, 11, 6),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),  # limits the dagrun to 5 minutes
    max_active_runs=1,  # limits the number of concurrent dagruns
    dag_id="strata-lab-1-v00"
)

def crypto_trading_pipeline():
    SYMBOLS = [
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
    INTERVAL = "1m"
    NOW = datetime.now().strftime("%H:%M:%S")
 

    #task: get indicator from data_live
    @task
    def get_indicator():
        async def get_indicator_live():
            for symbol in SYMBOLS:
                crypto_data = CryptoData(symbol, INTERVAL)
                df_live_data = await crypto_data.get_live_data()
                indicators = Indicators(symbol, INTERVAL)
                rsi_indicator_live, stoch_rsi_, macd_, bollinger_bands_indi, mvg_indicator_live = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                rsi_indicator_live, stoch_rsi_, macd_, bollinger_bands_indi, mvg_indicator_live = await asyncio.gather(
                    indicators.rsi(df_live_data),
                    indicators.stochastic_rsi(df_live_data),
                    indicators.macd(df_live_data),
                    indicators.bollinger_bands(df_live_data),
                    indicators.moving_averages(df_live_data)
                )
                return rsi_indicator_live, stoch_rsi_, macd_, bollinger_bands_indi, mvg_indicator_live
        return asyncio.run(get_indicator_live())



    @task
    def check_alerts():
        async def validate_alerts( ):
            # Validate alerts
            alert_live = AlertLive()
            try:
                rsi_check, stoch_rsi_check, macd_check, bollinger_bands_check, mvg_check = await asyncio.gather(
                    alert_live.check_rsi(),
                    alert_live.check_stochastic_rsi(),
                    alert_live.check_macd(),
                    alert_live.check_bollinger_bands(),
                    alert_live.check_moving_averages()
                )
                df = {
                    'rsi': rsi_check,
                    'stoch_rsi': stoch_rsi_check,
                    'macd': macd_check,
                    'bollinger_bands': bollinger_bands_check,
                    'mvg': mvg_check
                }
                signals = pd.DataFrame(df).dropna()
                signals_json = signals.to_json(orient="records")  # Convert to JSON
                print("Signals JSON:", signals_json)  # Debugging
                return signals_json if signals_json else None
            except Exception as e:
                print(f"Error: {e}")
                return None

        # Run async function
        return asyncio.run(validate_alerts())

    @task
    def export_signals_to_s3(signals_json):
        if signals_json is not None:
            s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION_NAME)
            bucket_name = 'eaguerito-strata-lab-1'
            object_name = 'data_trade_signal.csv'

            # Convert JSON back to DataFrame
            signals_df = pd.read_json(signals_json, orient="records")
            
            try:
                # Check if file exists in S3 and load existing data
                response = s3.get_object(Bucket=bucket_name, Key=object_name)
                existing_df = pd.read_csv(response['Body'])
                updated_df = pd.concat([existing_df, signals_df], ignore_index=True)
            except s3.exceptions.NoSuchKey:
                updated_df = signals_df  # No existing file, use the current DataFrame
            except Exception as e:
                print(f"Error reading S3 file: {e}")
                return  # Stop if there's an error

            # Write updated DataFrame back to S3 as CSV
            csv_buffer = StringIO()
            updated_df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
            print("Signals exported to S3 successfully.")


    @task
    def read_signals_from_s3():
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION_NAME)
        bucket_name = 'eaguerito-strata-lab-1'
        object_name = 'data_trade_signal.csv'

        try:
            response = s3.get_object(Bucket=bucket_name, Key=object_name)
            return response['Body'].read().decode('utf-8')
        except s3.exceptions.NoSuchKey:
            return None


    # Execute tasks
    signals_json = check_alerts()
    if signals_json:
        export_signals_to_s3(signals_json)

    read_signals_from_s3()

crypto_trading_pipeline()
