import pandas as pd
import numpy as np
class orderBook:

    def __init__(self):
        self._ob = None

    def partial(self):
        self._ob = self._curr_data

    def update(self, row):
        self.delete(row)
        self.insert(row)

    def delete(self, row):
        self._ob.drop(row.name, axis=0, inplace=True)

    def insert(self, row):
        self._ob = self._ob.append(row)

    def __call__(self, message, symbol='XBTU20'):


        message = message[message.symbol==symbol].set_index('price')
        if not message.empty:
            self._curr_data = message.sort_index()
            if self._curr_data.is_snapshot.all():
                self.partial()
            else:
                for i, row in self._curr_data.iterrows():
                    if row.size==0:
                        self.delete(row)
                    elif row.name in self._ob.index:
                        self.update(row)
                    else:
                        self.insert(row)

        ## Remove Size Zero
        self._ob = self._ob[self._ob['size']!=0.0]

        ## Sort Price
        self._ob.sort_index(inplace=True)
        return self._ob.copy()

class snapshot:

    def __init__(self):
        self._histSnapshot = pd.DataFrame()

    def __call__(self, cur_ob, local_timestamp):

        cur_ob.reset_index(inplace=True)
        top_bids = pd.Series(cur_ob[cur_ob.side == 'bid'].iloc[-5:][['price', 'size']].values.T.flatten(),
                             index=['bid4', 'bid3', 'bid2', 'bid1', 'bid0', 'bidSize4', 'bidSize3', 'bidSize2', 'bidSize1', 'bidSize0'])

        top_asks = pd.Series(cur_ob[cur_ob.side == 'ask'].iloc[:5][['price', 'size']].values.T.flatten(),
                             index=['ask0', 'ask1', 'ask2','ask3', 'ask4', 'askSize0', 'askSize1', 'askSize2', 'askSize3', 'askSize4'])
        mid = 0.5 * (top_bids['bid0'] + top_asks['ask0'])

        _X = pd.Series(np.concatenate([top_bids.values, top_asks.values, np.array([mid])]),
                      index=['bid4', 'bid3', 'bid2', 'bid1', 'bid0', 'bidSize4', 'bidSize3', 'bidSize2', 'bidSize1', 'bidSize0',
                             'ask0', 'ask1', 'ask2', 'ask3', 'ask4', 'askSize0', 'askSize1', 'askSize2', 'askSize3', 'askSize4',
                             'mid'], name=pd.to_datetime(local_timestamp, unit='us'))



        self._cur_snapshot = np.insert(_X.values,0 , local_timestamp)


    @property
    def histSnapshot(self):
        return self._histSnapshot

    @property
    def cur_snapshot(self):
        return self._cur_snapshot

