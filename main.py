from marketmatch import MarketWatch
import datetime
import pandas as pd

trader = MarketWatch()
codigo = trader.get_next_actual_win_symbol()
for timeframe_key in trader.timeframe_dict.keys():
  df = trader.update_ohlc(symbol=codigo, timeframe=timeframe_key)
  trader.convert_parquet_to_pandas(symbol=codigo, timeframe=timeframe_key)
  