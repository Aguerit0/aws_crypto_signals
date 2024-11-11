from datetime import datetime, timedelta
import pandas as pd
from alert_indicators import AlertLive
import asyncio

def check_alerts():
    async def validate_alerts():
        # Instancia de AlertLive para obtener y verificar las señales de compra/venta
        alert_live = AlertLive()

        try:
            # Ejecuta todas las verificaciones de manera concurrente
            rsi_check, stoch_rsi_check, macd_check, bollinger_bands_check, mvg_check = await asyncio.gather(
                alert_live.check_rsi(),
                alert_live.check_stochastic_rsi(),
                alert_live.check_macd(),
                alert_live.check_bollinger_bands(),
                alert_live.check_moving_averages()
            )

            # Verificar y mostrar las salidas de cada indicador
            print("RSI Check:", rsi_check)
            print("Stoch RSI Check:", stoch_rsi_check)
            print("MACD Check:", macd_check)
            print("Bollinger Bands Check:", bollinger_bands_check)
            print("Moving Averages Check:", mvg_check)

            # Filtra solo las señales válidas y retorna en un DataFrame
            conditions = {
                'rsi': rsi_check,
                'stoch_rsi': stoch_rsi_check,
                'macd': macd_check,
                'bollinger_bands': bollinger_bands_check,
                'mvg': mvg_check
            }
            signals = pd.DataFrame(conditions).dropna()  # Conserva solo señales válidas
            print("Señales generadas (DataFrame):\n", signals)
            return signals if not signals.empty else None

        except Exception as e:
            print(f"Error durante la validación de alertas: {e}")
            return None

    # Ejecuta la función asincrónica
    return asyncio.run(validate_alerts())

# Ejecución de la función para pruebas
check_alerts()
