import asyncio
from tardis_client import TardisClient, Channel
import pandas as pd
import numpy as np
import pickle

with open('xgb_clf.pkl', "rb") as f:
    xgb_clf = pickle.load(f)


class orderBook:

    def __init__(self):
        self._ob = None
        # self._curr_data = pd.DataFrame(message['data']).set_index('id') if message is not None else message

    def partial(self):
        self._ob = self._curr_data

    def update(self):
        self._ob.loc[self._curr_data.index, 'size'] = self._curr_data.size
        self._ob.loc[self._curr_data.index, 'side'] = self._curr_data.side

    def delete(self):
        self._ob.drop(self._curr_data.index, axis=0, inplace=True)

    def insert(self):
        self._ob = self._ob.append(self._curr_data)

    def __call__(self, message):
        self._curr_data = pd.DataFrame(message['data']).set_index('id')
        getattr(self, message['action'])()
        self._ob.sort_values('price', inplace=True)
        return self._ob


async def replay():
    tardis_client = TardisClient()

    # replay method returns Async Generator
    # https://rickyhan.com/jekyll/update/2018/01/27/python36.html
    messages = tardis_client.replay(
        exchange="bitmex",
        from_date="2020-09-01",
        to_date="2020-09-02",
        filters=[# Channel(name="trade", symbols=["XBTUSD"]),
                 # Channel("quote", ["XBTUSD"])],
                 Channel("orderBookL2_25", ["XBTUSD"])],
    )

    # this will print all trades and orderBookL2 messages for XBTUSD
    # and all trades for ETHUSD for bitmex exchange
    # between 2019-06-01T00:00:00.000Z and 2019-06-02T00:00:00.000Z (whole first day of June 2019)
    count=0
    bitMexOrderBook = orderBook()

    last_pred = None
    last_mid = None
    async for local_timestamp, message in messages:
        # local timestamp is a Python datetime that marks timestamp when given message has been received
        # message is a message object as provided by exchange real-time stream

        # print(message)

        ## id in orderBookL2_25 is a composite of price and symbol, and is always unique for any given price level.
        ## It should be used to apply update and delete actions.

        print('cur message:')
        print(message)
        if message['table'] == 'orderBookL2_25':

            curBook = bitMexOrderBook(message=message)

            print('curBook:')
            print(curBook)


            # print(f"local timestamp: {local_timestamp}")
            # print(curBook)
            top_bids = pd.Series(curBook[curBook.side=='Buy'].iloc[-3:][['price','size']].values.flatten(),
                                 index=['bid2', 'bid1', 'bid0', 'bidSize2','bidSize1','bidSize0'])

            top_asks = pd.Series(curBook[curBook.side=='Sell'].iloc[:3][['price','size']].values.flatten(),
                                 index=['ask0', 'ask1', 'ask2', 'askSize0', 'askSize1', 'askSize2'])
            mid = 0.5*(top_bids['bid0'] + top_asks['ask0'])
            spread = top_asks['ask0'] - top_bids['bid0']
            book_pressure = mid - (top_bids['bid0']*top_bids['bidSize0'] + top_asks['ask0']*top_asks['askSize0']) / (top_bids['bidSize0'] + top_asks['askSize0'])

            X = pd.Series(np.concatenate([top_bids.values, top_asks.values,np.array([mid, spread, book_pressure])]),
                          index=['bid2', 'bid1', 'bid0', 'bidSize2','bidSize1','bidSize0', 'ask0', 'ask1', 'ask2', 'askSize0', 'askSize1', 'askSize2', 'mid', 'spread', 'book_pressure'])

            last_pred = xgb_clf.predict(X.values.reshape((1,-1)))

            if count>=1:
                midDiff = mid - last_mid
                if midDiff!=0:
                    print(f"local timestamp: {local_timestamp}")
                    print(f"current price: {top_bids['bid0'], top_asks['ask0']}")
                    print(f"last pred: {last_pred[0]} actual diff: {np.sign(midDiff)}")



            last_mid = mid
            count+=1


asyncio.run(replay())