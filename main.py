from marketmatch import MarketWatch
import datetime

trader = MarketWatch()
codigo = trader.get_next_actual_win_symbol()
for timeframe_key in trader.timeframe_dict.keys():
  trader.update_ohlc(symbol=codigo, timeframe=timeframe_key)
