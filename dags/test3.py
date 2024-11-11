from io import StringIO
import boto3
import pandas as pd

def test_export_signals_to_s3():
    # Datos de prueba
    signals = pd.DataFrame({
        'rsi': [30, 70],
        'stoch_rsi': [15, 85],
        'macd': [0.5, -0.5],
        'bollinger_bands': ['buy', 'sell'],
        'mvg': ['uptrend', 'downtrend']
    })

    # client s3 and configs
    s3 = boto3.client('s3')
    bucket_name = 'eaguerito-strata-lab-1'
    object_name = 'data_trade_signal.csv'

    # upload signals to s3
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
        existing_df = pd.read_csv(response['Body'])
        updated_df = pd.concat([existing_df, signals], ignore_index=True)
        print('Signals uploaded to S3 successfully.')
    except s3.exceptions.NoSuchKey:
        updated_df = signals

    # save updated signals to s3
    csv_buffer = StringIO()
    updated_df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())

test_export_signals_to_s3()
