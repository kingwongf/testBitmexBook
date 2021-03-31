import pandas as pd
from functools import reduce
import numpy as np
from matplotlib import cm
import matplotlib.pyplot as plt
import seaborn as sns
from mpl_toolkits.mplot3d import Axes3D # <--- This is important for 3d plotting

futures_month  ={
'F':1,
'G':2,
'H':'2021-03-26',
'J':'2021-04-30',
'K':'2021-05-28',
'M':'2021-06-25',
'N':'2021-07-20',
'Q':'2021-08-27',
'U':9,
'V':10,
'X':11,
'Z':'2021-12-31',
'Z22':'2022-12-30',
}


# print(dict(zip (futures_month.keys(), (futures_month.values() - 2) /12 )))

futures_dfs = [pd.read_csv(f"datasets/dailyFutures/FUTURE_US_XCME_BTC{month}21.csv", index_col=['Date'])['Close'].rename(month).str.replace(',','') for month in ['H','J','K','M','N','Q', 'Z']] + \
              [pd.read_csv(f"datasets/dailyFutures/FUTURE_US_XCME_BTCZ22.csv", index_col=['Date'])['Close'].rename('Z22').str.replace(',','')]

futures = reduce( lambda X, x: pd.merge(X, x, how='left', left_index=True, right_index=True), futures_dfs)
futures.index = pd.to_datetime(futures.index)
futures = futures.astype('float64')
futures.rename(columns=futures_month, inplace=True)

futures.columns = pd.to_datetime(futures.columns)

# days = pd.DataFrame(np.tile(futures.columns.values.reshape(-1,1),
#                             futures.index.shape).T - futures.index,
#                     index=futures.index,
#                     columns=futures.columns)
# print(days)


fut_cols = futures.columns
fut_index = futures.index


spot = pd.read_csv("datasets/dailyFutures/SPOT.csv", index_col=['Date'], parse_dates=['Date'])['BTCUSD Close'].str.replace(',','').astype('float64')

print(spot)
futures['spot'] = spot

futures.ffill(inplace=True)


df = pd.DataFrame()

for dt in futures.index:
    yieldCurve = futures.loc[dt]
    tdy_spot = yieldCurve.pop('spot')
    yieldCurve.index = pd.to_datetime(yieldCurve.index)- dt

    tdy_imp_yield = ((yieldCurve / tdy_spot)**(365/ yieldCurve.index.days.values) -1)
    tdy_imp_yield.index = tdy_imp_yield.index.days

    tdy_imp_yield = tdy_imp_yield.reset_index().rename(columns={dt:'yield'})
    tdy_imp_yield['dt'] = dt


    df = df.append(tdy_imp_yield)

df.rename(columns={'index':'mat'}, inplace=True)
df.dt = df.dt.astype(int)
print(df)

sns.set(style = "darkgrid")

fig = plt.figure()
ax = fig.add_subplot(111, projection = '3d')

# Plot the surface.
ax.scatter(df['mat'], df['dt'], df['yield'], cmap=cm.coolwarm,
                       linewidth=0, antialiased=False)

ax.set_xlabel("mat")
ax.set_ylabel("dt")
ax.set_zlabel("yield")

plt.show()
exit()
