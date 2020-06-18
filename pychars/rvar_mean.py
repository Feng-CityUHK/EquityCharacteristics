# RVAR mean

import pandas as pd
import numpy as np
import datetime as dt
import wrds
import psycopg2
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime
import pickle as pkl

###################
# Connect to WRDS #
###################
conn=wrds.Connection()

# CRSP Block
crsp = conn.raw_sql("""
                    select permno, date, ret
                    from crsp.dsf
                    where date >= '01/01/1959'
                    """)

# sort variables by permno and date
crsp = crsp.sort_values(by=['permno', 'date'])

# change variable format to int
crsp['permno'] = crsp['permno'].astype(int)

# convert date format
crsp['date'] = pd.to_datetime(crsp['date'])
crsp['monthend'] = crsp['date'] + MonthEnd(0)

##########################
# calculate the variance #
##########################
df = crsp.groupby(['permno'])['ret'].rolling(60).var()  # variance of return in rolling trading days
df = df.reset_index()  # extract permno from index
df = df[['permno', 'ret']]
df[['date', 'monthend']] = crsp[['date', 'monthend']]
df.rename(columns={'ret': 'rvar_mean'}, inplace=True)

# find the closest trading day to the end of the month
df['date_diff'] = df['monthend'] - df['date']
date_temp = df.groupby(['permno', 'monthend'])['date_diff'].min()
date_temp = pd.DataFrame(date_temp)  # convert Series to DataFrame
date_temp.reset_index(inplace=True)
date_temp.rename(columns={'date_diff': 'min_diff'}, inplace=True)
df = pd.merge(df, date_temp, how='left', on=['permno', 'monthend'])
df['sig'] = np.where(df['date_diff'] == df['min_diff'], 1, np.nan)
df = df.dropna(subset=['sig'])
df = df[['permno', 'date', 'rvar_mean']]

with open('rvar_mean.pkl', 'wb') as f:
    pkl.dump(df, f)