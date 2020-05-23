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
                    where date >= '01/01/1963'
                    """)

crsp = crsp.sort_values(by=['permno', 'date'])

# change variable format to int
crsp['permno'] = crsp['permno'].astype(int)

# convert date format
crsp['date'] = pd.to_datetime(crsp['date'])

crsp['monthend'] = crsp['date'] + MonthEnd(0)

##########################
# calculate the variance #
##########################
df = crsp.groupby(['permno', 'monthend'])['ret'].var()
df = df.reset_index()  # extract permno from index
df.rename(columns={'ret': 'rvar_mean'}, inplace=True)

with open('rvar_mean.pkl', 'wb') as f:
    pkl.dump(df, f)