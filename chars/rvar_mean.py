# RVAR mean

import pandas as pd
import numpy as np
import datetime as dt
import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime
import pickle as pkl
import pyarrow.feather as feather
import multiprocessing as mp

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

# CRSP Block
crsp = conn.raw_sql("""
                    select permno, date, ret, vol
                    from crsp.dsf
                    where date >= '01/01/1959'
                    """)

# sort variables by permno and date
crsp = crsp.sort_values(by=['permno', 'date'])

# change variable format to int
crsp['permno'] = crsp['permno'].astype(int)

# Line up date to be end of month
crsp['date'] = pd.to_datetime(crsp['date'])

# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.dsedelist
                     """)

dlret.permno = dlret.permno.astype(int)
dlret['dlstdt'] = pd.to_datetime(dlret['dlstdt'])
dlret['date'] = dlret['dlstdt']

# merge delisting return to crsp return
crsp = pd.merge(crsp, dlret, how='left', on=['permno', 'date'])
crsp['dlret'] = crsp['dlret'].fillna(0)
crsp['ret'] = crsp['ret'].fillna(0)
crsp['retadj'] = (1 + crsp['ret']) * (1 + crsp['dlret']) - 1
# crsp['exret'] = crsp['retadj'] - crsp['rf']

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
month_num = month_num.reset_index(drop=True)

# mark the number of each month to each day of this month
crsp['month_count'] = crsp.groupby(['permno'])['month_count'].fillna(method='bfill')

# crate a firm list
df_firm = crsp.drop_duplicates(['permno'])
df_firm = df_firm[['permno']]
df_firm['permno'] = df_firm['permno'].astype(int)
df_firm = df_firm.reset_index(drop=True)
df_firm = df_firm.reset_index()
df_firm = df_firm.rename(columns={'index': 'count'})
df_firm['month_num'] = month_num

######################
# Calculate variance #
######################


def get_ret_var(df, firm_list):
    """

    :param df: stock dataframe
    :param firm_list: list of firms matching stock dataframe
    :return: dataframe with variance of residual
    """
    for firm, count, prog in zip(firm_list['permno'], firm_list['month_num'], range(firm_list['permno'].count()+1)):
        prog = prog + 1
        print('processing permno %s' % firm, '/', 'finished', '%.2f%%' % ((prog/firm_list['permno'].count())*100))
        for i in range(count + 1):
            # if you want to change the rolling window, please change here: i - 2 means 3 months is a window.
            temp = df[(df['permno'] == firm) & (i - 2 <= df['month_count']) & (df['month_count'] <= i)]
            # if observations in last 3 months are less 21, we drop the rvar of this month
            if temp['permno'].count() < 21:
                pass
            else:
                if temp['vol'].notna().sum() < 21:
                    pass
                else:
                    index = temp.tail(1).index
                    ret_var = temp['ret'].var()
                    df.loc[index, 'rvar'] = ret_var
    return df

def sub_df(start, end, step):
    """

    :param start: the quantile to start cutting, usually it should be 0
    :param end: the quantile to end cutting, usually it should be 1
    :param step: quantile step
    :return: a dictionary including all the 'firm_list' dataframe and 'stock data' dataframe
    """
    # we use dict to store different sub dataframe
    temp = {}
    for i, h in zip(np.arange(start, end, step), range(int((end-start)/step))):
        print('processing splitting dataframe:', round(i, 2), 'to', round(i + step, 2))
        if i == 0:  # to get the left point
            temp['firm' + str(h)] = df_firm[df_firm['count'] <= df_firm['count'].quantile(i + step)]
            temp['crsp' + str(h)] = pd.merge(crsp, temp['firm' + str(h)], how='left',
                                             on='permno').dropna(subset=['count'])
        else:
            temp['firm' + str(h)] = df_firm[(df_firm['count'].quantile(i) < df_firm['count']) & (
                    df_firm['count'] <= df_firm['count'].quantile(i + step))]
            temp['crsp' + str(h)] = pd.merge(crsp, temp['firm' + str(h)], how='left',
                                             on='permno').dropna(subset=['count'])
    return temp


def main(start, end, step):
    """

    :param start: the quantile to start cutting, usually it should be 0
    :param end: the quantile to end cutting, usually it should be 1
    :param step: quantile step
    :return: a dataframe with calculated variance of residual
    """
    df = sub_df(start, end, step)
    pool = mp.Pool()
    p_dict = {}
    for i in range(int((end-start)/step)):
        p_dict['p' + str(i)] = pool.apply_async(get_ret_var, (df['crsp%s' % i], df['firm%s' % i],))
    pool.close()
    pool.join()
    result = pd.DataFrame()
    print('processing pd.concat')
    for h in range(int((end-start)/step)):
        result = pd.concat([result, p_dict['p%s' % h].get()])
    return result


# calculate variance of residual through rolling window
# Note: please split dataframe according to your CPU situation. For example, we split dataframe to (1-0)/0.05 = 20 sub
# dataframes here, so the function will use 20 cores to calculate variance of residual.
if __name__ == '__main__':
    crsp = main(0, 1, 0.05)

# process dataframe
crsp = crsp.dropna(subset=['rvar'])  # drop NA due to rolling
crsp = crsp.rename(columns={'rvar': 'rvar_mean'})
crsp = crsp.reset_index(drop=True)
crsp = crsp[['permno', 'date', 'rvar_mean']]

with open('rvar_mean.feather', 'wb') as f:
    feather.write_feather(crsp, f)