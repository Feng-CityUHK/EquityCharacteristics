# Since some firms only have annual recording before 80s, we need to use annual data as merging benchmark in case
# there are some recordings are missing.

# After expanding the data from 1926, we need to make sure every stock at least have corresponding return.

import pandas as pd
import pickle as pkl
import pyarrow.feather as feather
from pandas.tseries.offsets import *
import numpy as np
import wrds

######################################################################
# read return data and fill the missing value in accounting files
conn = wrds.Connection(wrds_username='gavinfen')

crsp = conn.raw_sql("""
                    select a.prc, a.ret, a.retx, a.shrout, a.vol, a.date, a.permno, a.permco,
                    b.shrcd, b.exchcd
                    from crsp.msf as a
                    left join crsp.msenames as b
                    on a.permno=b.permno
                    and b.namedt<=a.date
                    and a.date<=b.nameendt
                    where a.date >= '01/01/1925'
                    and b.exchcd between 1 and 3
                    """)

crsp = crsp.dropna(subset=['ret', 'retx', 'prc'])

# change variable format to int
crsp[['permco', 'permno']] = crsp[['permco', 'permno']].astype(int)

# Line up date to be end of month
crsp['date'] = pd.to_datetime(crsp['date'])
crsp['jdate'] = crsp['date'] + MonthEnd(0)  # set all the date to the standard end date of month

crsp = crsp.dropna(subset=['prc'])
crsp['me'] = crsp['prc'].abs() * crsp['shrout']  # calculate market equity

# Aggregate Market Cap
'''
There are cases when the same firm (permco) has two or more securities (permno) at same date.
For the purpose of ME for the firm, we aggregated all ME for a given permco, date.
This aggregated ME will be assigned to the permno with the largest ME.
'''
# sum of me across different permno belonging to same permco a given date
crsp_summe = crsp.groupby(['jdate', 'permco'])['me'].sum().reset_index()
# largest mktcap within a permco/date
crsp_maxme = crsp.groupby(['jdate', 'permco'])['me'].max().reset_index()
# join by monthend/maxme to find the permno
crsp1 = pd.merge(crsp, crsp_maxme, how='inner', on=['jdate', 'permco', 'me'])
# drop me column and replace with the sum me
crsp1 = crsp1.drop(['me'], axis=1)
# join with sum of me to get the correct market cap info
crsp2 = pd.merge(crsp1, crsp_summe, how='inner', on=['jdate', 'permco'])
# sort by permno and date and also drop duplicates
crsp2 = crsp2.sort_values(by=['permno', 'jdate']).drop_duplicates()

crsp = crsp2.copy()
crsp = crsp.sort_values(by=['permno', 'date'])

# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.msedelist
                     """)

dlret.permno = dlret.permno.astype(int)
dlret['dlstdt'] = pd.to_datetime(dlret['dlstdt'])
dlret['jdate'] = dlret['dlstdt'] + MonthEnd(0)

# merge delisting return to crsp return
crsp = pd.merge(crsp, dlret, how='left', on=['permno', 'jdate'])
crsp['dlret'] = crsp['dlret'].fillna(0)
crsp['ret'] = crsp['ret'].fillna(0)
crsp['retadj'] = (1 + crsp['ret']) * (1 + crsp['dlret']) - 1


crsp = crsp[['permno', 'jdate', 'ret', 'retx', 'retadj', 'me', 'shrcd', 'exchcd']]
crsp.columns = ['permno', 'jdate', 'ret_fill', 'retx_fill', 'retadj_fill', 'me_fill', 'shrcd_fill', 'exchcd_fill']
######################################################################

with open('chars_a_accounting.feather', 'rb') as f:
    chars_a = feather.read_feather(f)

chars_a = chars_a.dropna(subset=['permno'])
chars_a[['permno', 'gvkey']] = chars_a[['permno', 'gvkey']].astype(int)
chars_a['jdate'] = pd.to_datetime(chars_a['jdate'])
chars_a = chars_a.drop_duplicates(['permno', 'jdate'])

with open('beta.feather', 'rb') as f:
    beta = feather.read_feather(f)

beta['permno'] = beta['permno'].astype(int)
beta['jdate'] = pd.to_datetime(beta['date']) + MonthEnd(0)
beta = beta[['permno', 'jdate', 'beta']]
beta = beta.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, beta, how='outer', on=['permno', 'jdate'])

with open('rvar_capm.feather', 'rb') as f:
    rvar_capm = feather.read_feather(f)

rvar_capm['permno'] = rvar_capm['permno'].astype(int)
rvar_capm['jdate'] = pd.to_datetime(rvar_capm['date']) + MonthEnd(0)
rvar_capm = rvar_capm[['permno', 'jdate', 'rvar_capm']]
rvar_capm = rvar_capm.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, rvar_capm, how='outer', on=['permno', 'jdate'])

with open('rvar_mean.feather', 'rb') as f:
    rvar_mean = feather.read_feather(f)

rvar_mean['permno'] = rvar_mean['permno'].astype(int)
rvar_mean['jdate'] = pd.to_datetime(rvar_mean['date']) + MonthEnd(0)
rvar_mean = rvar_mean[['permno', 'jdate', 'rvar_mean']]
rvar_mean = rvar_mean.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, rvar_mean, how='outer', on=['permno', 'jdate'])

with open('rvar_ff3.feather', 'rb') as f:
    rvar_ff3 = feather.read_feather(f)

rvar_ff3['permno'] = rvar_ff3['permno'].astype(int)
rvar_ff3['jdate'] = pd.to_datetime(rvar_ff3['date']) + MonthEnd(0)
rvar_ff3 = rvar_ff3[['permno', 'jdate', 'rvar_ff3']]
rvar_ff3 = rvar_ff3.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, rvar_ff3, how='outer', on=['permno', 'jdate'])

with open('sue.feather', 'rb') as f:
    sue = feather.read_feather(f)

sue['permno'] = sue['permno'].astype(int)
sue['jdate'] = pd.to_datetime(sue['date']) + MonthEnd(0)
sue = sue[['permno', 'jdate', 'sue']]
sue = sue.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, sue, how='outer', on=['permno', 'jdate'])

with open('myre.feather', 'rb') as f:
    re = feather.read_feather(f)

re['permno'] = re['permno'].astype(int)
re['jdate'] = pd.to_datetime(re['date']) + MonthEnd(0)
re = re[['permno', 'jdate', 're']]
re = re.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, re, how='outer', on=['permno', 'jdate'])

with open('abr.feather', 'rb') as f:
    abr = feather.read_feather(f)

abr['permno'] = abr['permno'].astype(int)
abr['jdate'] = pd.to_datetime(abr['date']) + MonthEnd(0)
abr = abr[['permno', 'jdate', 'abr']]
abr = abr.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, abr, how='outer', on=['permno', 'jdate'])

with open('baspread.feather', 'rb') as f:
    baspread = feather.read_feather(f)

baspread['permno'] = baspread['permno'].astype(int)
baspread['jdate'] = pd.to_datetime(baspread['date']) + MonthEnd(0)
baspread = baspread[['permno', 'jdate', 'baspread']]
baspread = baspread.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, baspread, how='outer', on=['permno', 'jdate'])

with open('maxret.feather', 'rb') as f:
    maxret = feather.read_feather(f)

maxret['permno'] = maxret['permno'].astype(int)
maxret['jdate'] = pd.to_datetime(maxret['date']) + MonthEnd(0)
maxret = maxret[['permno', 'jdate', 'maxret']]
maxret = maxret.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, maxret, how='outer', on=['permno', 'jdate'])

with open('std_dolvol.feather', 'rb') as f:
    std_dolvol = feather.read_feather(f)

std_dolvol['permno'] = std_dolvol['permno'].astype(int)
std_dolvol['jdate'] = pd.to_datetime(std_dolvol['date']) + MonthEnd(0)
std_dolvol = std_dolvol[['permno', 'jdate', 'std_dolvol']]
std_dolvol = std_dolvol.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, std_dolvol, how='outer', on=['permno', 'jdate'])

with open('ill.feather', 'rb') as f:
    ill = feather.read_feather(f)

ill['permno'] = ill['permno'].astype(int)
ill['jdate'] = pd.to_datetime(ill['date']) + MonthEnd(0)
ill = ill[['permno', 'jdate', 'ill']]
ill = ill.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, ill, how='outer', on=['permno', 'jdate'])

with open('std_turn.feather', 'rb') as f:
    std_turn = feather.read_feather(f)

std_turn['permno'] = std_turn['permno'].astype(int)
std_turn['jdate'] = pd.to_datetime(std_turn['date']) + MonthEnd(0)
std_turn = std_turn[['permno', 'jdate', 'std_turn']]
std_turn = std_turn.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, std_turn, how='outer', on=['permno', 'jdate'])

with open('zerotrade.feather', 'rb') as f:
    zerotrade = feather.read_feather(f)

zerotrade['permno'] = zerotrade['permno'].astype(int)
zerotrade['jdate'] = pd.to_datetime(zerotrade['date']) + MonthEnd(0)
zerotrade = zerotrade[['permno', 'jdate', 'zerotrade']]
zerotrade = zerotrade.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, zerotrade, how='outer', on=['permno', 'jdate'])

# fill the return
chars_a = pd.merge(chars_a, crsp, how='left', on=['permno', 'jdate'])
chars_a['ret'] = np.where(chars_a['ret'].isnull(), chars_a['ret_fill'], chars_a['ret'])
chars_a['retx'] = np.where(chars_a['retx'].isnull(), chars_a['retx_fill'], chars_a['retx'])
chars_a['retadj'] = np.where(chars_a['retadj'].isnull(), chars_a['retadj_fill'], chars_a['retadj'])
chars_a['me'] = np.where(chars_a['me'].isnull(), chars_a['me_fill'], chars_a['me'])
chars_a['exchcd'] = np.where(chars_a['exchcd'].isnull(), chars_a['exchcd_fill'], chars_a['exchcd'])
chars_a['shrcd'] = np.where(chars_a['shrcd'].isnull(), chars_a['shrcd_fill'], chars_a['shrcd'])

chars_a = chars_a.dropna(subset=['permno', 'jdate', 'ret', 'retx', 'retadj'])
chars_a = chars_a[((chars_a['exchcd'] == 1) | (chars_a['exchcd'] == 2) | (chars_a['exchcd'] == 3)) &
                   ((chars_a['shrcd'] == 10) | (chars_a['shrcd'] == 11))]

# save data
with open('chars_a_raw.feather', 'wb') as f:
    feather.write_feather(chars_a, f)

########################################################################################################################
#     In order to keep the naming tidy, we need to make another chars_q_raw, which is just a temporary dataframe       #
########################################################################################################################

with open('chars_q_accounting.feather', 'rb') as f:
    chars_q = feather.read_feather(f)

chars_q = chars_q.dropna(subset=['permno'])
chars_q[['permno', 'gvkey']] = chars_q[['permno', 'gvkey']].astype(int)
chars_q['jdate'] = pd.to_datetime(chars_q['jdate'])
chars_q = chars_q.drop_duplicates(['permno', 'jdate'])

with open('beta.feather', 'rb') as f:
    beta = feather.read_feather(f)

beta['permno'] = beta['permno'].astype(int)
beta['jdate'] = pd.to_datetime(beta['date']) + MonthEnd(0)
beta = beta[['permno', 'jdate', 'beta']]
beta = beta.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, beta, how='outer', on=['permno', 'jdate'])

with open('rvar_capm.feather', 'rb') as f:
    rvar_capm = feather.read_feather(f)

rvar_capm['permno'] = rvar_capm['permno'].astype(int)
rvar_capm['jdate'] = pd.to_datetime(rvar_capm['date']) + MonthEnd(0)
rvar_capm = rvar_capm[['permno', 'jdate', 'rvar_capm']]
rvar_capm = rvar_capm.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_capm, how='outer', on=['permno', 'jdate'])

with open('rvar_mean.feather', 'rb') as f:
    rvar_mean = feather.read_feather(f)

rvar_mean['permno'] = rvar_mean['permno'].astype(int)
rvar_mean['jdate'] = pd.to_datetime(rvar_mean['date']) + MonthEnd(0)
rvar_mean = rvar_mean[['permno', 'jdate', 'rvar_mean']]
rvar_mean = rvar_mean.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_mean, how='outer', on=['permno', 'jdate'])

with open('rvar_ff3.feather', 'rb') as f:
    rvar_ff3 = feather.read_feather(f)

rvar_ff3['permno'] = rvar_ff3['permno'].astype(int)
rvar_ff3['jdate'] = pd.to_datetime(rvar_ff3['date']) + MonthEnd(0)
rvar_ff3 = rvar_ff3[['permno', 'jdate', 'rvar_ff3']]
rvar_ff3 = rvar_ff3.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_ff3, how='outer', on=['permno', 'jdate'])

with open('sue.feather', 'rb') as f:
    sue = feather.read_feather(f)

sue['permno'] = sue['permno'].astype(int)
sue['jdate'] = pd.to_datetime(sue['date']) + MonthEnd(0)
sue = sue[['permno', 'jdate', 'sue']]
sue = sue.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, sue, how='outer', on=['permno', 'jdate'])

with open('myre.feather', 'rb') as f:
    re = feather.read_feather(f)

re['permno'] = re['permno'].astype(int)
re['jdate'] = pd.to_datetime(re['date']) + MonthEnd(0)
re = re[['permno', 'jdate', 're']]
re = re.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, re, how='outer', on=['permno', 'jdate'])

with open('abr.feather', 'rb') as f:
    abr = feather.read_feather(f)

abr['permno'] = abr['permno'].astype(int)
abr['jdate'] = pd.to_datetime(abr['date']) + MonthEnd(0)
abr = abr[['permno', 'jdate', 'abr']]
abr = abr.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, abr, how='outer', on=['permno', 'jdate'])

with open('baspread.feather', 'rb') as f:
    baspread = feather.read_feather(f)

baspread['permno'] = baspread['permno'].astype(int)
baspread['jdate'] = pd.to_datetime(baspread['date']) + MonthEnd(0)
baspread = baspread[['permno', 'jdate', 'baspread']]
baspread = baspread.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, baspread, how='outer', on=['permno', 'jdate'])

with open('maxret.feather', 'rb') as f:
    maxret = feather.read_feather(f)

maxret['permno'] = maxret['permno'].astype(int)
maxret['jdate'] = pd.to_datetime(maxret['date']) + MonthEnd(0)
maxret = maxret[['permno', 'jdate', 'maxret']]
maxret = maxret.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, maxret, how='outer', on=['permno', 'jdate'])

with open('std_dolvol.feather', 'rb') as f:
    std_dolvol = feather.read_feather(f)

std_dolvol['permno'] = std_dolvol['permno'].astype(int)
std_dolvol['jdate'] = pd.to_datetime(std_dolvol['date']) + MonthEnd(0)
std_dolvol = std_dolvol[['permno', 'jdate', 'std_dolvol']]
std_dolvol = std_dolvol.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, std_dolvol, how='outer', on=['permno', 'jdate'])

with open('ill.feather', 'rb') as f:
    ill = feather.read_feather(f)

ill['permno'] = ill['permno'].astype(int)
ill['jdate'] = pd.to_datetime(ill['date']) + MonthEnd(0)
ill = ill[['permno', 'jdate', 'ill']]
ill = ill.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, ill, how='outer', on=['permno', 'jdate'])

with open('std_turn.feather', 'rb') as f:
    std_turn = feather.read_feather(f)

std_turn['permno'] = std_turn['permno'].astype(int)
std_turn['jdate'] = pd.to_datetime(std_turn['date']) + MonthEnd(0)
std_turn = std_turn[['permno', 'jdate', 'std_turn']]
std_turn = std_turn.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, std_turn, how='outer', on=['permno', 'jdate'])

with open('zerotrade.feather', 'rb') as f:
    zerotrade = feather.read_feather(f)

zerotrade['permno'] = zerotrade['permno'].astype(int)
zerotrade['jdate'] = pd.to_datetime(zerotrade['date']) + MonthEnd(0)
zerotrade = zerotrade[['permno', 'jdate', 'zerotrade']]
zerotrade = zerotrade.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, zerotrade, how='outer', on=['permno', 'jdate'])

# fill the return
chars_q = pd.merge(chars_q, crsp, how='left', on=['permno', 'jdate'])
chars_q['ret'] = np.where(chars_q['ret'].isnull(), chars_q['ret_fill'], chars_q['ret'])
chars_q['retx'] = np.where(chars_q['retx'].isnull(), chars_q['retx_fill'], chars_q['retx'])
chars_q['retadj'] = np.where(chars_q['retadj'].isnull(), chars_q['retadj_fill'], chars_q['retadj'])
chars_q['me'] = np.where(chars_q['me'].isnull(), chars_q['me_fill'], chars_q['me'])
chars_q['exchcd'] = np.where(chars_q['exchcd'].isnull(), chars_q['exchcd_fill'], chars_q['exchcd'])
chars_q['shrcd'] = np.where(chars_q['shrcd'].isnull(), chars_q['shrcd_fill'], chars_q['shrcd'])

chars_q = chars_q.dropna(subset=['permno', 'jdate', 'ret', 'retx', 'retadj'])
chars_q = chars_q[((chars_q['exchcd'] == 1) | (chars_q['exchcd'] == 2) | (chars_q['exchcd'] == 3)) &
                   ((chars_q['shrcd'] == 10) | (chars_q['shrcd'] == 11))]

# save data
with open('chars_q_raw.feather', 'wb') as f:
    feather.write_feather(chars_q, f)
