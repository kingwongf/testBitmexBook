import pandas as pd
import numpy as np
from tqdm import tqdm
import csv
from reconstructOB import orderBook, snapshot


symbol = 'XBTZ20' # 'XBTU20'



futuresOrderBook = pd.read_csv('datasets/bitmex_incremental_book_L2_2020-09-01_FUTURES.csv') # , nrows=10000
futuresOrderBook = futuresOrderBook[futuresOrderBook.symbol==symbol]
futuresOrderBook.rename(columns={'amount':'size'}, inplace=True)

futuresOrderBook.sort_values('local_timestamp', inplace=True)
orderbook_future = orderBook()
snapshot_future = snapshot()



path_futures = f"datasets/bitmex_book_snapshot_5_2020-09-01_{symbol}_FUTURES.csv"

with open(path_futures, "w", newline='') as csv_file:
    writer = csv.writer(csv_file, delimiter=',')
    for local_timestamp, message in tqdm(futuresOrderBook.groupby('local_timestamp')):
        curBook = orderbook_future(message=message, symbol=symbol)
        snapshot_future(cur_ob=curBook, local_timestamp=local_timestamp)
        writer.writerow(snapshot_future.cur_snapshot)