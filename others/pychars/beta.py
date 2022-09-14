# BETA monthly version
# Note: Please use the latest version of pandas, this version should support returning to pd.Series after rolling

import pandas as pd
import numpy as np
import datetime as dt
import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime
import pickle as pkl

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

# CRSP Block
crsp = conn.raw_sql("""
                    select a.permno, a.date, a.ret, (a.ret - b.rf) as exret, b.mktrf
                    from crsp.msf as a
                    left join ff.factors_daily as b
                    on a.date=b.date
                    where a.date > '01/01/1959'
                    """)

# sort variables by permno and date
crsp = crsp.sort_values(by=['permno', 'date'])

# change variable format to int
crsp['permno'] = crsp['permno'].astype(int)

# line up date to be end of month
crsp['date'] = pd.to_datetime(crsp['date'])

######################
# Calculate the beta #
######################
rolling_window = 60  # 60 months


# TODO: find a faster way to get rolling sub dataframe
def get_beta(df):
    """
    The original idea of calculate beta is using formula (X'MX)^(-1)X'MY,
    where M = I - 1(1'1)^{-1}1, I is a identity matrix.

    """
    temp = crsp.loc[df.index]  # extract the rolling sub dataframe from original dataframe
    X = np.mat(temp[['mktrf']])
    Y = np.mat(temp[['exret']])
    ones = np.mat(np.ones(rolling_window)).T
    M = np.identity(rolling_window) - ones.dot((ones.T.dot(ones)).I).dot(ones.T)
    beta = (X.T.dot(M).dot(X)).I.dot((X.T.dot(M).dot(Y)))
    return beta


# calculate beta through rolling window
crsp_temp = crsp.groupby('permno').rolling(rolling_window).apply(get_beta, raw=False)

# arrange final outcome
crsp_temp = crsp_temp[['mktrf']]  # all columns values are beta, we drop extra columns here
crsp_temp = crsp_temp.rename(columns={'mktrf': 'beta'})
crsp_temp = crsp_temp.reset_index()
crsp['beta'] = crsp_temp['beta']
crsp = crsp.dropna(subset=['beta'])  # drop NA due to rolling
crsp = crsp[['permno', 'date', 'beta']]

with open('beta.pkl', 'wb') as f:
    pkl.dump(crsp, f)