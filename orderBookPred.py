import pandas as pd


orderBook = pd.read_csv('bitmex_book_snapshot_5_2020-09-01_XBTUSD.csv')
orderBook['mid'] = 0.5*(orderBook["asks[0].price"] + orderBook["bids[0].price"])

