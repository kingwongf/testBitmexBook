import pandas as pd
import numpy as np
import time

future_symbol = 'XBTU20'
futures = pd.read_csv(f"datasets/bitmex_book_snapshot_5_2020-09-01_{future_symbol}_FUTURES.csv",
                      names=['local_timestamp','bid4', 'bid3', 'bid2', 'bid1', 'bid0', 'bidSize4', 'bidSize3', 'bidSize2', 'bidSize1', 'bidSize0',
                             'ask0', 'ask1', 'ask2', 'ask3', 'ask4', 'askSize0', 'askSize1', 'askSize2', 'askSize3', 'askSize4',
                             'mid'])
spot = pd.read_csv('datasets/bitmex_book_snapshot_5_2020-09-01_XBTUSD.csv')
futures.index = pd.to_datetime(futures.local_timestamp, unit='us')

st = time.time()
min_futures = futures.resample('5min').last()
print(futures)
print(f"time elapsed: {(( time.time() - st) / 60)} mins")