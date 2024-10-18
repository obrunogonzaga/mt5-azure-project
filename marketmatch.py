import MetaTrader5 as mt5
import pandas as pd 
import numpy as np
import os
from datetime import date, datetime, timedelta, time
from dotenv import load_dotenv

class MarketWatch():
    def __init__(self):
        load_dotenv()
        username = int(os.getenv("METATRADER_USERNAME"))
        #password = os.getenv("METATRADER_PASSWORD")
        server = os.getenv("METATRADER_SERVER")
        account = os.getenv("METATRADER_ACCOUNT")
        print(f"Usuário: {username}, Servidor: {server}, Conta: {account}")