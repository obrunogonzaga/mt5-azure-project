import MetaTrader5 as mt5
import pandas as pd 
import numpy as np
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
            print("Error: Missing a require enriroment variable.")
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
            print("'ohlc' folder doesn't exists. Creating...")
            os.mkdir('ohlc')
            for timeframe_dir in self.timeframe_dict.keys():
                try:
                    os.mkdir(f'ohlc\\{timeframe_dir}')
                    print(f"Folder '{timeframe_dir}' created inside 'ohlc' folder.")
                except FileExistsError:
                    print(f"Folder '{timeframe_dir}' exists. Skipping...")

        # Criar diretório 'ticks' se não existir
        if not os.path.isdir('ticks'):
            print("'ticks' folder doesn't exists. Creating...")
            try:
                os.mkdir('ticks')
                print("'ticks' folder create successfully.")
            except Exception as e:
                print(f"Error: folder 'ticks' don't created: {e}")
        else:
            print("'ticks' folder already exists. No action required.")