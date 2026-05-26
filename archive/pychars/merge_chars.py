import pandas as pd
import pickle as pkl
from pandas.tseries.offsets import *
import wrds

with open('chars_q.pkl', 'rb') as f:
    chars_q = pkl.load(f)

chars_q = chars_q.dropna(subset=['permno'])
chars_q[['permno', 'gvkey']] = chars_q[['permno', 'gvkey']].astype(int)
chars_q['jdate'] = pd.to_datetime(chars_q['jdate'])
chars_q = chars_q.drop_duplicates(['permno', 'jdate'])

with open('beta.pkl', 'rb') as f:
    beta = pkl.load(f)

beta['permno'] = beta['permno'].astype(int)
beta['jdate'] = pd.to_datetime(beta['date']) + MonthEnd(0)
beta = beta[['permno', 'jdate', 'beta']]
beta = beta.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, beta, how='left', on=['permno', 'jdate'])

with open('rvar_capm.pkl', 'rb') as f:
    rvar_capm = pkl.load(f)

rvar_capm['permno'] = rvar_capm['permno'].astype(int)
rvar_capm['jdate'] = pd.to_datetime(rvar_capm['date']) + MonthEnd(0)
rvar_capm = rvar_capm[['permno', 'jdate', 'rvar_capm']]
rvar_capm = rvar_capm.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_capm, how='left', on=['permno', 'jdate'])

with open('rvar_mean.pkl', 'rb') as f:
    rvar_mean = pkl.load(f)

rvar_mean['permno'] = rvar_mean['permno'].astype(int)
rvar_mean['jdate'] = pd.to_datetime(rvar_mean['date']) + MonthEnd(0)
rvar_mean = rvar_mean[['permno', 'jdate', 'rvar_mean']]
rvar_mean = rvar_mean.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_mean, how='left', on=['permno', 'jdate'])

with open('rvar_ff3.pkl', 'rb') as f:
    rvar_ff3 = pkl.load(f)

rvar_ff3['permno'] = rvar_ff3['permno'].astype(int)
rvar_ff3['jdate'] = pd.to_datetime(rvar_ff3['date']) + MonthEnd(0)
rvar_ff3 = rvar_ff3[['permno', 'jdate', 'rvar_ff3']]
rvar_ff3 = rvar_ff3.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_ff3, how='left', on=['permno', 'jdate'])

with open('sue.pkl', 'rb') as f:
    sue = pkl.load(f)

sue['permno'] = sue['permno'].astype(int)
sue['jdate'] = pd.to_datetime(sue['date']) + MonthEnd(0)
sue = sue[['permno', 'jdate', 'sue']]
sue = sue.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, sue, how='left', on=['permno', 'jdate'])

with open('re.pkl', 'rb') as f:
    re = pkl.load(f)

re['permno'] = re['permno'].astype(int)
re['jdate'] = pd.to_datetime(re['date']) + MonthEnd(0)
re = re[['permno', 'jdate', 're']]
re = re.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, re, how='left', on=['permno', 'jdate'])

with open('abr.pkl', 'rb') as f:
    abr = pkl.load(f)

abr['permno'] = abr['permno'].astype(int)
abr['jdate'] = pd.to_datetime(abr['date']) + MonthEnd(0)
abr = abr[['permno', 'jdate', 'abr']]
abr = abr.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, abr, how='left', on=['permno', 'jdate'])

# save data
with open('chars_q_raw.pkl', 'wb') as f:
    pkl.dump(chars_q, f, protocol=4)