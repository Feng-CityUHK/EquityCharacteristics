# Calculate HSZ Replicating Anomalies
# SUE: Standardized Unexpected Earnings (Earnings surprise)

import pandas as pd
import numpy as np
import datetime as dt
import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from pandasql import *
import pickle as pkl
import pyarrow.feather as feather

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

###################
# Compustat Block #
###################
comp = conn.raw_sql("""
                        select gvkey, datadate, fyearq, fqtr, epspxq, ajexq
                        from comp.fundq
                        where indfmt = 'INDL' 
                        and datafmt = 'STD'
                        and popsrc = 'D'
                        and consol = 'C'
                        and datadate >= '01/01/1925'
                        """)

comp['datadate'] = pd.to_datetime(comp['datadate'])

###################
#    CCM Block    #
###################
ccm = conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim, 
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where linktype in ('LU', 'LC')
                  """)

ccm['linkdt'] = pd.to_datetime(ccm['linkdt'])
ccm['linkenddt'] = pd.to_datetime(ccm['linkenddt'])
# if linkenddt is missing then set to today date
ccm['linkenddt'] = ccm['linkenddt'].fillna(pd.to_datetime('today'))

ccm1 = pd.merge(comp, ccm, how='left', on=['gvkey'])

# set link date bounds
ccm2 = ccm1[(ccm1['datadate']>=ccm1['linkdt']) & (ccm1['datadate']<=ccm1['linkenddt'])]
ccm2 = ccm2[['gvkey', 'permno', 'datadate', 'fyearq', 'fqtr', 'epspxq', 'ajexq']]

# the time series of exspxq/ajexq
ccm2['eps'] = ccm2['epspxq']/ccm2['ajexq']
ccm2.drop_duplicates(['permno', 'datadate'], inplace=True)

######################### Fixed on 2025.04.22 #########################
# Sort at first to ensure correct cumcount
# merge lag1 to lag9, then calculate standard deviation
ccm2.sort_values(by=['permno', 'datadate'], inplace=True)
ccm2['eps_diff'] = ccm2.groupby('permno')['eps'].diff(4)
ccm2 = ccm2[(ccm2['eps'].notna()) & (ccm2['eps_diff'].notna())].reset_index(drop=True)
ccm2['count'] = ccm2.groupby('permno').cumcount() + 1

######################### Fixed on 2025.04.22 #########################
# Calculate lag1 to lag8, then calculate difference with lag 4 quarters
for i in range(1, 9):
    ccm2[f'e{i}'] = ccm2.groupby(['permno'])['eps'].shift(i)
    ccm2[f'e{i}_diff'] = ccm2.groupby('permno')[f'e{i}'].diff(4)

condlist = [ccm2['count']<=6,
            ccm2['count']==7,
            ccm2['count']==8,
            ccm2['count']>=9]

######################### Fixed on 2025.04.22 #########################
# More stricted constrain by non-empty terms >= 6 and round(4), because some has extremely small difference, e.g., at 10^(-7) level
def calculate_std(df, cols):
    valid_counts = df[cols].notna().sum(axis=1) >= 6 # Check whether there are at least 6 non-null values
    all_equal = df[cols].round(4).nunique(axis=1) == 1 # Prepare to check whether all values are equal
    std_values = df[cols].std(axis=1)
    std_values[~valid_counts] = np.nan
    std_values[all_equal] = 0.0
    return std_values

##### Fixed on 2025.04.22: Special case (count=7or8) calculation for std, and use diff rather than raw eps value #####
choicelist = [np.nan,
              calculate_std(ccm2, [f"{col}_diff" for col in ['e6', 'e5', 'e4', 'e3', 'e2', 'e1']]),
              calculate_std(ccm2, [f"{col}_diff" for col in ['e7', 'e6', 'e5', 'e4', 'e3', 'e2', 'e1']]),
              calculate_std(ccm2, [f"{col}_diff" for col in ['e8', 'e7', 'e6', 'e5', 'e4', 'e3', 'e2', 'e1']])]
ccm2['sue_std'] = np.select(condlist, choicelist, default=np.nan)

ccm2['sue'] = (ccm2['eps'] - ccm2['e4'])/ccm2['sue_std']

# populate the quarterly sue to monthly
crsp_msf = conn.raw_sql("""
                        select distinct date
                        from crsp.msf
                        where date >= '01/01/1925'
                        """)

ccm2['datadate'] = pd.to_datetime(ccm2['datadate'])
ccm2['plus12m'] = ccm2['datadate'] + np.timedelta64(12, 'M')
ccm2['plus12m'] = ccm2['plus12m'] + MonthEnd(0)

df = sqldf("""select a.*, b.date
              from ccm2 a left join crsp_msf b 
              on a.datadate <= b.date
              and a.plus12m >= b.date
              order by a.permno, b.date, a.datadate desc;""", globals())

df = df.drop_duplicates(['permno', 'date'])
df['datadate'] = pd.to_datetime(df['datadate'])
df = df[['gvkey', 'permno', 'datadate', 'date', 'sue']]

with open('sue.feather', 'wb') as f:
    feather.write_feather(df, f)