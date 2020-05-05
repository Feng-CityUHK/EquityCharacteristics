# RVAR mean

import pandas as pd
import numpy as np
import datetime as dt
import wrds
import psycopg2
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime

###################
# Connect to WRDS #
###################
conn=wrds.Connection()

# CRSP Block
crsp = conn.raw_sql("""
                      select permno, date, ret
                      from crsp.dsf
                      where date >= '01/01/1963'
                      """)

crsp = crsp.sort_values(by=['permno', 'date'])

# change variable format to int
crsp['permno'] = crsp['permno'].astype(int)

# convert date format
crsp['date'] = pd.to_datetime(crsp['date'])

##########################
# calculate the variance #
##########################

df = crsp.groupby('permno')['ret'].rolling(60).var()  # 60 trading days

df.index.names = ['permno', 'index']  # rename the multiple keys in index

df = df.reset_index()  # extract permno from index
df.rename(columns={'ret': 'rvar_mean'}, inplace=True)
df = df[['permno', 'rvar_mean']]
df['date'] = crsp['date']
crsp = df.dropna()



