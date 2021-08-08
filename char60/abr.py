# Calculate HSZ Replicating Anomalies
# ABR: Cumulative abnormal stock returns around earnings announcements

import pandas as pd
import numpy as np
import datetime as dt
import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import pyarrow.feather as feather
import sqlite3

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

###################
# Compustat Block #
###################
comp = conn.raw_sql("""
                    select gvkey, datadate, rdq, fyearq, fqtr
                    from comp.fundq
                    where indfmt = 'INDL' 
                    and datafmt = 'STD'
                    and popsrc = 'D'
                    and consol = 'C'
                    and datadate >= '01/01/1959'
                    """)

comp['datadate'] = pd.to_datetime(comp['datadate'])

print('='*10, 'comp data is ready', '='*10)
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
# extract month and year of rdq
ccm1['rdq'] = pd.to_datetime(ccm1['rdq'])

# set link date bounds
ccm2 = ccm1[(ccm1['datadate'] >= ccm1['linkdt']) & (ccm1['datadate'] <= ccm1['linkenddt'])]
ccm2 = ccm2[['gvkey', 'datadate', 'rdq', 'fyearq', 'fqtr', 'permno']]

###################
#    CRSP Block   #
###################

# Report Date of Quarterly Earnings (rdq) may not be trading day, we need to get the first trading day on or after rdq
crsp_dsi = conn.raw_sql("""
                        select distinct date
                        from crsp.dsi
                        where date >= '01/01/1959'
                        """)

crsp_dsi['date'] = pd.to_datetime(crsp_dsi['date'])

ccm3 = ccm2.copy()
for i in range(6):  # we only consider the condition that the day after rdq is not a trading day, which is up to 5 days
    ccm3['trad_%s' % i] = ccm3['rdq'] + pd.DateOffset(days=i)  # set rdq + i days to match trading day
    crsp_dsi['trad_%s' % i] = crsp_dsi['date']  # set the merging key
    crsp_dsi = crsp_dsi[['date', 'trad_%s' % i]]  # reset trading day columns to avoid repeat merge
    ccm3 = pd.merge(ccm3, crsp_dsi, how='left', on='trad_%s' % i)
    ccm3['trad_%s' % i] = ccm3['date']  # reset rdq + i days to matched trading day
    ccm3 = ccm3.drop(['date'], axis=1)

# fill NA from rdq + 5 days to rdq + 0 days, then get trading day version of rdq
for i in range(5, 0, -1):
    count = i-1
    ccm3['trad_%s' % count] = np.where(ccm3['trad_%s' % count].isnull(), ccm3['trad_%s' % i], ccm3['trad_%s' % count])

ccm3['rdq_trad'] = ccm3['trad_0']

ccm3 = ccm3[['gvkey', 'permno', 'datadate', 'fyearq', 'fqtr', 'rdq', 'rdq_trad']]

print('='*10, 'crsp block is ready', '='*10)
#############################
#    CRSP abnormal return   #
#############################
crsp_d = conn.raw_sql("""
                      select a.prc, a.ret, a.shrout, a.vol, a.cfacpr, a.cfacshr, a.permno, a.permco, a.date,
                      b.siccd, b.ncusip, b.shrcd, b.exchcd
                      from crsp.dsf as a
                      left join crsp.dsenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date >= '01/01/1959'
                      and b.exchcd between 1 and 3
                      and b.shrcd in (10,11)
                      """)

# change variable format to int
crsp_d[['permco', 'permno', 'shrcd', 'exchcd']] = crsp_d[['permco', 'permno', 'shrcd', 'exchcd']].astype(int)

print('='*10, 'crsp abnormal return is ready', '='*10)

# convert the date format
crsp_d['date'] = pd.to_datetime(crsp_d['date'])

# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.dsedelist
                     where dlstdt >= '01/01/1959'
                     """)

dlret.permno = dlret.permno.astype(int)
dlret['dlstdt'] = pd.to_datetime(dlret['dlstdt'])

crsp_d = pd.merge(crsp_d, dlret, how='left', left_on=['permno', 'date'], right_on=['permno', 'dlstdt'])
# return adjusted for delisting
crsp_d['retadj'] = np.where(crsp_d['dlret'].notna(), (crsp_d['ret'] + 1)*(crsp_d['dlret'] + 1) - 1, crsp_d['ret'])
crsp_d['meq'] = crsp_d['prc'].abs()*crsp_d['shrout']  # market value of equity
crsp_d = crsp_d.sort_values(by=['date', 'permno', 'meq'])

# sprtrn
crspsp500d = conn.raw_sql("""
                          select date, sprtrn 
                          from crsp.dsi
                          where date >= '01/01/1959'
                          """)

crspsp500d['date'] = pd.to_datetime(crspsp500d['date'])

# abnormal return
crsp_d = pd.merge(crsp_d, crspsp500d, how='left', on='date')
crsp_d['abrd'] = crsp_d['retadj'] - crsp_d['sprtrn']
crsp_d = crsp_d[['date', 'permno', 'ret', 'retadj', 'sprtrn', 'abrd']]

# date count regarding to rdq
ccm3['minus10d'] = ccm3['rdq_trad'] - pd.Timedelta(days=10)
ccm3['plus5d'] = ccm3['rdq_trad'] + pd.Timedelta(days=5)

# df = sqldf("""select a.*, b.date, b.abrd
#               from ccm3 a left join crsp_d b
#               on a.permno=b.permno
#               and a.minus10d<=b.date
#               and b.date<=a.plus5d
#               order by a.permno, a.rdq_trad, b.date;""", globals())

sql = sqlite3.connect(':memory:')
ccm3.to_sql('ccm3', sql, index=False)
crsp_d.to_sql('crsp_d', sql, index=False)

qry = """select a.*, b.date, b.abrd 
              from ccm3 a left join crsp_d b 
              on a.permno=b.permno 
              and a.minus10d<=b.date 
              and b.date<=a.plus5d 
              order by a.permno, a.rdq_trad, b.date;"""
df = pd.read_sql_query(qry, sql)
df.drop(['plus5d', 'minus10d'], axis=1, inplace=True)

# delete missing return
df = df[df['abrd'].notna()]

# count
df.sort_values(by=['permno', 'rdq_trad', 'date'], inplace=True)
condlist = [df['date'] == df['rdq_trad'],
            df['date'] > df['rdq_trad'],
            df['date'] < df['rdq_trad']]
choicelist = [0, 1, -1]
df['c_1'] = np.select(condlist, choicelist, default=np.nan)

# trading days before rdq_trad
df_before = df[df['c_1'] == -1]
df_before['count'] = (df_before.groupby(['permno', 'rdq_trad'])['date'].cumcount(ascending=False) + 1) * -1

# trading days after rdq_trad
df_after = df[df['c_1'] >= 0]
df_after['count'] = df_after.groupby(['permno', 'rdq_trad'])['date'].cumcount()

df = pd.concat([df_before, df_after])

# calculate abr as the group sum
df = df[(df['count'] >= -2) & (df['count'] <= 1)]

df_temp = df.groupby(['permno', 'rdq_trad'])['abrd'].sum()
df_temp = pd.DataFrame(df_temp)
df_temp.reset_index(inplace=True)
df_temp.rename(columns={'abrd': 'abr'}, inplace=True)
df = pd.merge(df, df_temp, how='left', on=['permno', 'rdq_trad'], copy=False)  # add abr back to df
df = df[df['count'] == 1]
df.rename(columns={'date': 'rdq_plus_1d'}, inplace=True)
df = df[['gvkey', 'permno', 'datadate', 'rdq', 'rdq_plus_1d', 'abr']]

print('='*10, 'start populate', '='*10)

# populate the quarterly abr to monthly
crsp_msf = conn.raw_sql("""
                        select distinct date
                        from crsp.msf
                        where date >= '01/01/1959'
                        """)

df['datadate'] = pd.to_datetime(df['datadate'])
df['plus12m'] = df['datadate'] + np.timedelta64(12, 'M')
df['plus12m'] = df['plus12m'] + MonthEnd(0)

# df = sqldf("""select a.*, b.date
#               from df a left join crsp_msf b
#               on a.rdq_plus_1d < b.date
#               and a.plus12m >= b.date
#               order by a.permno, b.date, a.datadate desc;""", globals())

df.to_sql('df', sql, index=False)
crsp_msf.to_sql('crsp_msf', sql, index=False)

qry = """select a.*, b.date
              from df a left join crsp_msf b 
              on a.rdq_plus_1d < b.date
              and a.plus12m >= b.date
              order by a.permno, b.date, a.datadate desc;"""

df = pd.read_sql_query(qry, sql)

df = df.drop_duplicates(['permno', 'date'])
df['datadate'] = pd.to_datetime(df['datadate'])
df['rdq'] = pd.to_datetime(df['rdq'])
df['rdq_plus_1d'] = pd.to_datetime(df['rdq_plus_1d'])
df = df[['gvkey', 'permno', 'datadate', 'rdq', 'rdq_plus_1d', 'abr', 'date']]
df = df.dropna(subset=['date'])  # some firm will have records that report date is far away from actual datadate

with open('abr.feather', 'wb') as f:
    feather.write_feather(df, f)
