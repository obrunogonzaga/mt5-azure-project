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

    def update_ohlc(self, symbol, timeframe, start_date=None, end_date=None):
        # Check if the symbol is valid
        if not mt5.symbol_select(symbol):
            print(f"Error: Invalid symbol '{symbol}'.")
            return None
        
        # Check if the timeframe is supported
        if timeframe not in self.timeframe_dict.keys():
            print(f"Error: Invalid timeframe '{timeframe}'.")
            return None
        
        # Set default dates if not provided
        if start_date is None:
            start_date = datetime(2012, 1, 1)
        if end_date is None:
            end_date = datetime.now()
        
        # Ensure start_date and end_date are datetime objects
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        file_path = f'ohlc\\{timeframe}\\{symbol}_{timeframe}.csv'
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, parse_dates=['time'])
            if not df.empty and df['time'].max() >= end_date:
                print(f"Data already up to date for {symbol} in {timeframe}")
                return df
            start_date = max(start_date, df['time'].max() + pd.Timedelta(seconds=1))
        else:
            df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume'])

        mt5_timeframe = self.timeframe_dict[timeframe][0]
        
        # Fetch data in chunks
        chunk_size = timedelta(days=30)  # Adjust this value as needed
        current_start = start_date
        
        while current_start < end_date:
            chunk_end = min(current_start + chunk_size, end_date)
            print(f"Fetching data for {symbol} from {current_start} to {chunk_end}")
            
            rates = mt5.copy_rates_range(symbol, mt5_timeframe, current_start, chunk_end)
            if rates is None or len(rates) == 0:
                print(f"No data available for the period {current_start} to {chunk_end}")
                break
            
            chunk_df = pd.DataFrame(rates)
            chunk_df['time'] = pd.to_datetime(chunk_df['time'], unit='s')
            df = pd.concat([df, chunk_df], ignore_index=True)
            
            current_start = chunk_end + pd.Timedelta(seconds=1)
        
        if not df.empty:
            df.drop_duplicates(subset='time', keep='last', inplace=True)
            df.sort_values(by='time', inplace=True, ascending=False)
            df.to_csv(file_path, index=False)
            self.convert_pandas_to_parquet(df, symbol, timeframe)
            print(f"Data saved for {symbol} from {df['time'].min()} to {df['time'].max()}")
        else:
            print(f"No data was retrieved for {symbol} in {timeframe}")
        
        return df

    def get_next_actual_win_symbol(self):
        symbols = mt5.symbols_get()
        codigo_mini_indice = None
        for symbol in symbols:
            if symbol.name.startswith('WIN'):
                if symbol.select == 1 and symbol.visible == True: codigo_mini_indice = symbol.name

        return codigo_mini_indice

    def convert_pandas_to_parquet(self, df, symbol, timeframe):
        table = pa.Table.from_pandas(df)
        writer = pq.ParquetWriter(f'ohlc\\{timeframe}\\{symbol}_{timeframe}.parquet', table.schema)
        writer.write_table(table)
        writer.close()

    def convert_parquet_to_pandas(self, symbol, timeframe):
        df = None
        file_path = f'ohlc\\{timeframe}\\{symbol}_{timeframe}.parquet'
        if os.path.exists(file_path):
            df = pq.ParquetFile(file_path).read().to_pandas()
        return df