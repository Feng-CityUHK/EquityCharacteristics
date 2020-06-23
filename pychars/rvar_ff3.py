# Fama & French 3 factors residual variance
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
                      select a.permno, a.date, a.ret, (a.ret - b.rf) as exret, b.mktrf, b.smb, b.hml
                      from crsp.dsf as a
                      left join ff.factors_daily as b
                      on a.date=b.date
                      where a.date > '01/01/1959'
                      """)

# sort variables by permno and date
crsp = crsp.sort_values(by=['permno', 'date'])

# change variable format to int
crsp['permno'] = crsp['permno'].astype(int)

# Line up date to be end of month
crsp['date'] = pd.to_datetime(crsp['date'])

# find the closest trading day to the end of the month
crsp['monthend'] = crsp['date'] + MonthEnd(0)
crsp['date_diff'] = crsp['monthend'] - crsp['date']
date_temp = crsp.groupby(['permno', 'monthend'])['date_diff'].min()
date_temp = pd.DataFrame(date_temp)  # convert Series to DataFrame
date_temp.reset_index(inplace=True)
date_temp.rename(columns={'date_diff': 'min_diff'}, inplace=True)
crsp = pd.merge(crsp, date_temp, how='left', on=['permno', 'monthend'])
crsp['sig'] = np.where(crsp['date_diff'] == crsp['min_diff'], 1, np.nan)

# label every date of month end
crsp['month_count'] = crsp[crsp['sig'] == 1].groupby(['permno']).cumcount()
# label numbers of months for a firm
month_num = crsp[crsp['sig'] == 1].groupby(['permno'])['month_count'].tail(1)
month_num = month_num.astype(int)

# crate a firm list
df_firm = crsp.drop_duplicates(['permno'])
df_firm = df_firm[['permno']]
df_firm['permno'] = df_firm['permno'].astype(int)
df_firm = df_firm.reset_index(drop=True)

# mark the number of each month to each day of this month
crsp['month_count'] = crsp.groupby(['permno'])['month_count'].fillna(method='bfill')

######################
# Calculate the beta #
######################
# function that get multiple beta
''''
rolling_window = 60  # 60 trading days
crsp['beta_mktrf'] = np.nan
crsp['beta_smb'] = np.nan
crsp['beta_hml'] = np.nan


def get_beta(df):
    """
    The original idea of calculate beta is using formula (X'MX)^(-1)X'MY,
    where M = I - 1(1'1)^{-1}1, I is a identity matrix.

    """
    temp = crsp.loc[df.index]  # extract the rolling sub dataframe from original dataframe
    X = np.mat(temp[['mktrf', 'smb', 'hml']])
    Y = np.mat(temp[['exret']])
    ones = np.mat(np.ones(rolling_window)).T
    M = np.identity(rolling_window) - ones.dot((ones.T.dot(ones)).I).dot(ones.T)
    beta = (X.T.dot(M).dot(X)).I.dot((X.T.dot(M).dot(Y)))
    crsp['beta_mktrf'].loc[df.index[-1:]] = beta[0]
    crsp['beta_smb'].loc[df.index[-1:]] = beta[1]
    crsp['beta_hml'].loc[df.index[-1:]] = beta[2]
    return 0  # we do not need the rolling outcome since rolling cannot return different values in different columns


# calculate beta through rolling window
crsp_temp = crsp.groupby('permno').rolling(rolling_window).apply(get_beta, raw=False)
'''

######################
# Calculate residual #
######################


def get_res_var(df, firm_list):
    for firm, count, prog in zip(firm_list['permno'], month_num, range(firm_list['permno'].count()+1)):
        prog = prog + 1
        print('processing permno %s' % firm, '/', 'finished', '%.2f%%' % ((prog/firm_list['permno'].count())*100))
        for i in range(count + 1):
            # if you want to change the rolling window, please change here: i - 2 means 3 months is a window.
            temp = df[(df['permno'] == firm) & (i - 2 <= df['month_count']) & (df['month_count'] <= i)]
            if temp['permno'].count() < 60:
                pass
            else:
                rolling_window = temp['permno'].count()
                index = temp.tail(1).index
                X = pd.DataFrame()
                X[['mktrf', 'smb', 'hml']] = temp[['mktrf', 'smb', 'hml']]
                X['intercept'] = 1
                X = X[['intercept', 'mktrf', 'smb', 'hml']]
                X = np.mat(X)
                Y = np.mat(temp[['exret']])
                res = (np.identity(rolling_window) - X.dot(X.T.dot(X).I).dot(X.T)).dot(Y)
                res_var = res.var(ddof=1)
                crsp.loc[index, 'rvar'] = res_var
    return crsp


# calculate beta through rolling window
crsp = get_res_var(crsp, firm_list=df_firm)

# process dataframe
crsp = crsp.dropna(subset=['rvar'])  # drop NA due to rolling
crsp = crsp.rename(columns={'rvar': 'rvar_ff3'})
crsp = crsp[crsp['sig'] == 1]
crsp = crsp.reset_index(drop=True)
crsp = crsp[['permno', 'date', 'rvar_ff3']]

with open('rvar_ff3.pkl', 'wb') as f:
    pkl.dump(crsp, f)