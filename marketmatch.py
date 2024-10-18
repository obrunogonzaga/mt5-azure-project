import MetaTrader5 as mt5
import pandas as pd 
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import date, datetime, timedelta, time
from dotenv import load_dotenv

class MarketWatch():
    def __init__(self):
        try:
            load_dotenv()
            username = int(os.getenv("METATRADER_USERNAME"))
            password = os.getenv("METATRADER_PASSWORD")
            server = os.getenv("METATRADER_SERVER")
            account = os.getenv("METATRADER_ACCOUNT")
        except ValueError:
            print("Error: The value METATRADER_USERNAME is not a valid number.")
        except TypeError:
            print("Error: Missing a required environment variable.")
        except Exception as e:
            print(f"Unexpected error: {e}")

        # Initializing Metatrader 5
        if not mt5.initialize(login=username, password=password, server=server, account=account):
            print('Initialize() failed, error code = ', mt5.last_error())
            mt5.shutdown()

        self.timeframe_dict = {
            'TIMEFRAME_M1': [mt5.TIMEFRAME_M1, 60],
            'TIMEFRAME_M2': [mt5.TIMEFRAME_M2, 120],
            'TIMEFRAME_M3': [mt5.TIMEFRAME_M3, 180],
            'TIMEFRAME_M4': [mt5.TIMEFRAME_M4, 240],
            'TIMEFRAME_M5': [mt5.TIMEFRAME_M5, 300],
            'TIMEFRAME_M6': [mt5.TIMEFRAME_M6, 360],
            'TIMEFRAME_M10': [mt5.TIMEFRAME_M10, 600],
            'TIMEFRAME_M12': [mt5.TIMEFRAME_M12, 720],
            'TIMEFRAME_M15': [mt5.TIMEFRAME_M15, 900],
            'TIMEFRAME_M20': [mt5.TIMEFRAME_M20, 1200],
            'TIMEFRAME_M30': [mt5.TIMEFRAME_M30, 1800],
            'TIMEFRAME_H1': [mt5.TIMEFRAME_H1, 3600],
            'TIMEFRAME_H2': [mt5.TIMEFRAME_H2, 7200],
            'TIMEFRAME_H3': [mt5.TIMEFRAME_H3, 10800],
            'TIMEFRAME_H4': [mt5.TIMEFRAME_H4, 14400],
            'TIMEFRAME_H6': [mt5.TIMEFRAME_H6, 21600],
            'TIMEFRAME_H8': [mt5.TIMEFRAME_H8, 28800],
            'TIMEFRAME_H12': [mt5.TIMEFRAME_H12, 43200],
            'TIMEFRAME_D1': [mt5.TIMEFRAME_D1, 86400],
            'TIMEFRAME_W1': [mt5.TIMEFRAME_W1, 604800],
            'TIMEFRAME_MN1': [mt5.TIMEFRAME_MN1, 2592000],
        }

        if not os.path.isdir('ohlc'):
            print("'ohlc' folder doesn't exist. Creating...")
            os.mkdir('ohlc')
            for timeframe_dir in self.timeframe_dict.keys():
                try:
                    os.mkdir(f'ohlc\\{timeframe_dir}')
                    print(f"Folder '{timeframe_dir}' created inside 'ohlc' folder.")
                except FileExistsError:
                    print(f"Folder '{timeframe_dir}' exists. Skipping...")

        # Create 'ticks' directory if it doesn't exist
        if not os.path.isdir('ticks'):
            print("'ticks' folder doesn't exist. Creating...")
            try:
                os.mkdir('ticks')
                print("'ticks' folder created successfully.")
            except Exception as e:
                print(f"Error: folder 'ticks' not created: {e}")
        else:
            print("'ticks' folder already exists. No action required.")

    def update_ohlc(self, symbol, timeframe, start_date=datetime(2012, 1, 1), end_date=datetime.now()):
        # Check if the symbol is valid
        if not mt5.symbol_select(symbol):
            print(f"Error: Invalid symbol '{symbol}'.")
            return None
        
        # Check if the timeframe is supported
        if timeframe not in self.timeframe_dict.keys():
            print(f"Error: Invalid timeframe '{timeframe}'.")
            return None
        
        if not os.path.exists(f'ohlc\\{timeframe}\\{symbol}_{timeframe}.csv'):
            df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume'])
        else:
            df = pd.read_csv(f'ohlc\\{timeframe}\\{symbol}_{timeframe}.csv')
            df['time'] = pd.to_datetime(df['time'])
            if df['time'].max() < datetime.now() - timedelta(days=7): start_date = df['time'].max()
            else: return
        
        timedelta_default = timedelta(days=self.timeframe_dict[timeframe][1])
        end_date_aux = start_date + timedelta_default
        timeframe_name = timeframe
        mt5_timeframe = self.timeframe_dict[timeframe][0]

        while True:
            data_aux = mt5.copy_rates_range(symbol, mt5_timeframe, start_date, min(end_date_aux, end_date))
            df_aux = pd.DataFrame(data_aux)
            df_aux['time'] = pd.to_datetime(df_aux['time'], unit='s')
            df = pd.concat([df, df_aux], ignore_index=True)

            if end_date_aux >= end_date: break

            start_date = df_aux['time'].max()
            end_date_aux = start_date + timedelta_default

        # Save the updated DataFrame to the CSV file
        df.sort_values(by='time', inplace=True, ignore_index=True, ascending=False)
        #convert_csv_to_parquet(df, symbol, timeframe)
        df.to_csv(f'ohlc\\{timeframe_name}\\{symbol}_{timeframe_name}.csv', index=False)

    def get_next_actual_win_symbol(self):
        symbols = mt5.symbols_get()
        codigo_mini_indice = None
        for symbol in symbols:
            if symbol.name.startswith('WIN'):
                if symbol.select == 1 and symbol.visible == True: codigo_mini_indice = symbol.name

        return codigo_mini_indice

    def convert_csv_to_parquet(self, df, symbol, timeframe):
        table = pa.Table.from_pandas(df)
        writer = pq.ParquetWriter(f'ohlc\\{timeframe}\\{symbol}_{timeframe}.parquet', table.schema)
        writer.write_table(table)
        writer.close()