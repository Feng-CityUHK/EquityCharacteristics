# RVAR_CAPM
# CAPM residual variance

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
                      select a.permno, a.date, a.ret, (a.ret - b.rf) as exret, b.mktrf
                      from crsp.dsf as a
                      left join ff.factors_daily as b
                      on a.date=b.date
                      where a.date >= '01/01/1959'
                      """)

crsp = crsp.sort_values(by=['permno', 'date'])

# change variable format to int
crsp['permno'] = crsp['permno'].astype(int)

# Line up date to be end of month
crsp['date'] = pd.to_datetime(crsp['date'])

################################
# Calculate the beta for mktrf #
################################

df = crsp.groupby('permno')['exret', 'mktrf'].rolling(60).cov()  # 60 trading days

df.index.names = ['permno', 'index', 'type']  # rename the multiple keys in index
df = df.xs('exret', level='type')  # takes a key argument to select data at a particular level of a MultiIndex
df.rename(columns={'exret': 'var', 'mktrf': 'cov'}, inplace=True)

df = df.reset_index()  # extract permno from index
df = df[['permno', 'var', 'cov']]
df['beta'] = df['cov']/df['var']

################################
#    Calculate the residual    #
################################
crsp['beta'] = df['beta']
crsp['monthend'] = crsp['date'] + MonthEnd(0)
crsp['date_diff'] = crsp['monthend'] - crsp['date']
date_temp = crsp.groupby(['permno', 'monthend'])['date_diff'].min()  # find the closest trading day to the end of the month
date_temp = pd.DataFrame(date_temp)  # convert Series to DataFrame
date_temp.reset_index(inplace=True)
date_temp.rename(columns={'date_diff': 'min_diff'}, inplace=True)

crsp = pd.merge(crsp, date_temp, how='left', on=['permno', 'monthend'])
crsp['sig'] = np.where(crsp['date_diff']==crsp['min_diff'], 1, np.nan)
crsp = crsp.dropna(subset=['beta'])
crsp['beta'] = np.where(crsp['sig'].notna(), crsp['beta'], np.nan)
crsp['beta'] = crsp['beta'].bfill()

crsp['residual'] = crsp['exret'] - crsp['beta']*crsp['mktrf']
crsp = crsp[['permno', 'date', 'monthend', 'residual']]

df = crsp.groupby(['permno', 'monthend'])['residual'].var()
df = df.reset_index()
df.columns = ['permno', 'date', 'rvar_capm']

with open('rvar_capm.pkl', 'wb') as f:
    pkl.dump(df, f)