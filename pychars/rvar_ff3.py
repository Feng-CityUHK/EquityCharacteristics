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
                      select a.permno, a.date, a.ret, (a.ret - b.rf) as exret, b.mktrf, b.smb, b.hml
                      from crsp.dsf as a
                      left join ff.factors_daily as b
                      on a.date=b.date
                      where a.date > '01/01/1959'
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
df['date'] = crsp['date']

crsp_final = df
crsp_final['beta_mktrf'] = crsp_final['cov']/crsp_final['var']
crsp_final = crsp_final[['permno', 'date', 'beta_mktrf']]

##############################
# Calculate the beta for smb #
##############################

df = crsp.groupby('permno')['exret', 'smb'].rolling(60).cov()  # 60 trading days

df.index.names = ['permno', 'index', 'type']  # rename the multiple keys in index
df = df.xs('exret', level='type')  # takes a key argument to select data at a particular level of a MultiIndex
df.rename(columns={'exret': 'var', 'smb': 'cov'}, inplace=True)

df = df.reset_index()  # extract permno from index
df = df[['permno', 'var', 'cov']]
df['beta_smb'] = df['cov']/df['var']

crsp_final['beta_smb'] = df['beta_smb']

##############################
# Calculate the beta for hml #
##############################

df = crsp.groupby('permno')['exret', 'hml'].rolling(60).cov()  # 60 trading days

df.index.names = ['permno', 'index', 'type']  # rename the multiple keys in index
df = df.xs('exret', level='type')  # takes a key argument to select data at a particular level of a MultiIndex
df.rename(columns={'exret': 'var', 'hml': 'cov'}, inplace=True)

df = df.reset_index()  # extract permno from index
df = df[['permno', 'var', 'cov']]
df['beta_hml'] = df['cov']/df['var']

crsp_final['beta_hml'] = df['beta_hml']

##############################
#     Calculate residual     #
##############################
crsp_final[['exret', 'mktrf', 'smb', 'hml']] = crsp[['exret', 'mktrf', 'smb', 'hml']]
crsp_final['monthend'] = crsp_final['date'] + MonthEnd(0)
crsp_final['date_diff'] = crsp_final['monthend'] - crsp_final['date']
date_temp = crsp_final.groupby(['permno', 'monthend'])['date_diff'].min()  # find the closest trading day to the end of the month
date_temp = pd.DataFrame(date_temp)  # convert Series to DataFrame
date_temp.reset_index(inplace=True)
date_temp.rename(columns={'date_diff': 'min_diff'}, inplace=True)

crsp_final = pd.merge(crsp_final, date_temp, how='left', on=['permno', 'monthend'])
crsp_final['sig'] = np.where(crsp_final['date_diff']==crsp_final['min_diff'], 1, np.nan)
crsp_final = crsp_final.dropna(subset=['beta_mktrf'])
crsp_final['beta_mktrf'] = np.where(crsp_final['sig'].notna(), crsp_final['beta_mktrf'], np.nan)
crsp_final['beta_smb'] = np.where(crsp_final['sig'].notna(), crsp_final['beta_smb'], np.nan)
crsp_final['beta_hml'] = np.where(crsp_final['sig'].notna(), crsp_final['beta_hml'], np.nan)
crsp_final['beta_mktrf'] = crsp_final['beta_mktrf'].bfill()
crsp_final['beta_smb'] = crsp_final['beta_smb'].bfill()
crsp_final['beta_hml'] = crsp_final['beta_hml'].bfill()

crsp_final['residual'] = crsp_final['exret'] - crsp_final['beta_mktrf']*crsp_final['mktrf'] - \
                         crsp_final['beta_smb']*crsp_final['smb'] - crsp_final['beta_hml']*crsp_final['hml']

df = crsp_final.groupby(['permno', 'monthend'])['residual'].var()
df = df.reset_index()
df.columns = ['permno', 'date', 'rvar_ff3']

with open('rvar_ff3.pkl', 'wb') as f:
    pkl.dump(df, f)