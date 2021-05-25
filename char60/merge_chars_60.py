# Since some firms only have annual recording before 80s, we need to use annual data as merging benchmark in case
# there are some recordings are missing

import pandas as pd
import pickle as pkl
import pyarrow.feather as feather
from pandas.tseries.offsets import *

with open('chars_a_60.feather', 'rb') as f:
    chars_a = feather.read_feather(f)

chars_a = chars_a.dropna(subset=['permno'])
chars_a[['permno', 'gvkey']] = chars_a[['permno', 'gvkey']].astype(int)
chars_a['jdate'] = pd.to_datetime(chars_a['jdate'])
chars_a = chars_a.drop_duplicates(['permno', 'jdate'])

with open(beta.feather', 'rb') as f:
    beta = feather.read_feather(f)

beta['permno'] = beta['permno'].astype(int)
beta['jdate'] = pd.to_datetime(beta['date']) + MonthEnd(0)
beta = beta[['permno', 'jdate', 'beta']]
beta = beta.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, beta, how='left', on=['permno', 'jdate'])

with open(rvar_capm.feather', 'rb') as f:
    rvar_capm = feather.read_feather(f)

rvar_capm['permno'] = rvar_capm['permno'].astype(int)
rvar_capm['jdate'] = pd.to_datetime(rvar_capm['date']) + MonthEnd(0)
rvar_capm = rvar_capm[['permno', 'jdate', 'rvar_capm']]
rvar_capm = rvar_capm.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, rvar_capm, how='left', on=['permno', 'jdate'])

with open(rvar_mean.feather', 'rb') as f:
    rvar_mean = feather.read_feather(f)

rvar_mean['permno'] = rvar_mean['permno'].astype(int)
rvar_mean['jdate'] = pd.to_datetime(rvar_mean['date']) + MonthEnd(0)
rvar_mean = rvar_mean[['permno', 'jdate', 'rvar_mean']]
rvar_mean = rvar_mean.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, rvar_mean, how='left', on=['permno', 'jdate'])

with open(rvar_ff3.feather', 'rb') as f:
    rvar_ff3 = feather.read_feather(f)

rvar_ff3['permno'] = rvar_ff3['permno'].astype(int)
rvar_ff3['jdate'] = pd.to_datetime(rvar_ff3['date']) + MonthEnd(0)
rvar_ff3 = rvar_ff3[['permno', 'jdate', 'rvar_ff3']]
rvar_ff3 = rvar_ff3.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, rvar_ff3, how='left', on=['permno', 'jdate'])

with open(sue.feather', 'rb') as f:
    sue = feather.read_feather(f)

sue['permno'] = sue['permno'].astype(int)
sue['jdate'] = pd.to_datetime(sue['date']) + MonthEnd(0)
sue = sue[['permno', 'jdate', 'sue']]
sue = sue.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, sue, how='left', on=['permno', 'jdate'])

with open(re.feather', 'rb') as f:
    re = feather.read_feather(f)

re['permno'] = re['permno'].astype(int)
re['jdate'] = pd.to_datetime(re['date']) + MonthEnd(0)
re = re[['permno', 'jdate', 're']]
re = re.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, re, how='left', on=['permno', 'jdate'])

with open(abr.feather', 'rb') as f:
    abr = feather.read_feather(f)

abr['permno'] = abr['permno'].astype(int)
abr['jdate'] = pd.to_datetime(abr['date']) + MonthEnd(0)
abr = abr[['permno', 'jdate', 'abr']]
abr = abr.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, abr, how='left', on=['permno', 'jdate'])

with open('baspread.feather', 'rb') as f:
    baspread = feather.read_feather(f)

baspread['permno'] = baspread['permno'].astype(int)
baspread['jdate'] = pd.to_datetime(baspread['date']) + MonthEnd(0)
baspread = baspread[['permno', 'jdate', 'baspread']]
baspread = baspread.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, baspread, how='left', on=['permno', 'jdate'])

with open('maxret.feather', 'rb') as f:
    maxret = feather.read_feather(f)

maxret['permno'] = maxret['permno'].astype(int)
maxret['jdate'] = pd.to_datetime(maxret['date']) + MonthEnd(0)
maxret = maxret[['permno', 'jdate', 'maxret']]
maxret = maxret.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, maxret, how='left', on=['permno', 'jdate'])

with open('std_dolvol.feather', 'rb') as f:
    std_dolvol = feather.read_feather(f)

std_dolvol['permno'] = std_dolvol['permno'].astype(int)
std_dolvol['jdate'] = pd.to_datetime(std_dolvol['date']) + MonthEnd(0)
std_dolvol = std_dolvol[['permno', 'jdate', 'std_dolvol']]
std_dolvol = std_dolvol.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, std_dolvol, how='left', on=['permno', 'jdate'])

with open('ill.feather', 'rb') as f:
    ill = feather.read_feather(f)

ill['permno'] = ill['permno'].astype(int)
ill['jdate'] = pd.to_datetime(ill['date']) + MonthEnd(0)
ill = ill[['permno', 'jdate', 'ill']]
ill = ill.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, ill, how='left', on=['permno', 'jdate'])

with open('std_turn.feather', 'rb') as f:
    std_turn = feather.read_feather(f)

std_turn['permno'] = std_turn['permno'].astype(int)
std_turn['jdate'] = pd.to_datetime(std_turn['date']) + MonthEnd(0)
std_turn = std_turn[['permno', 'jdate', 'std_turn']]
std_turn = std_turn.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, std_turn, how='left', on=['permno', 'jdate'])

with open('zerotrade.feather', 'rb') as f:
    zerotrade = feather.read_feather(f)

zerotrade['permno'] = zerotrade['permno'].astype(int)
zerotrade['jdate'] = pd.to_datetime(zerotrade['date']) + MonthEnd(0)
zerotrade = zerotrade[['permno', 'jdate', 'zerotrade']]
zerotrade = zerotrade.drop_duplicates(['permno', 'jdate'])

chars_a = pd.merge(chars_a, zerotrade, how='left', on=['permno', 'jdate'])

# save data
with open('chars_a_raw.feather', 'wb') as f:
    feather.write_feather(chars_a, f)

########################################################################################################################
#     In order to keep the naming tidy, we need to make another chars_q_raw, which is just a temporary dataframe       #
########################################################################################################################

with open('chars_q_60.feather', 'rb') as f:
    chars_q = feather.read_feather(f)

chars_q = chars_q.dropna(subset=['permno'])
chars_q[['permno', 'gvkey']] = chars_q[['permno', 'gvkey']].astype(int)
chars_q['jdate'] = pd.to_datetime(chars_q['jdate'])
chars_q = chars_q.drop_duplicates(['permno', 'jdate'])

with open(beta.feather', 'rb') as f:
    beta = feather.read_feather(f)

beta['permno'] = beta['permno'].astype(int)
beta['jdate'] = pd.to_datetime(beta['date']) + MonthEnd(0)
beta = beta[['permno', 'jdate', 'beta']]
beta = beta.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, beta, how='left', on=['permno', 'jdate'])

with open(rvar_capm.feather', 'rb') as f:
    rvar_capm = feather.read_feather(f)

rvar_capm['permno'] = rvar_capm['permno'].astype(int)
rvar_capm['jdate'] = pd.to_datetime(rvar_capm['date']) + MonthEnd(0)
rvar_capm = rvar_capm[['permno', 'jdate', 'rvar_capm']]
rvar_capm = rvar_capm.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_capm, how='left', on=['permno', 'jdate'])

with open(rvar_mean.feather', 'rb') as f:
    rvar_mean = feather.read_feather(f)

rvar_mean['permno'] = rvar_mean['permno'].astype(int)
rvar_mean['jdate'] = pd.to_datetime(rvar_mean['date']) + MonthEnd(0)
rvar_mean = rvar_mean[['permno', 'jdate', 'rvar_mean']]
rvar_mean = rvar_mean.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_mean, how='left', on=['permno', 'jdate'])

with open(rvar_ff3.feather', 'rb') as f:
    rvar_ff3 = feather.read_feather(f)

rvar_ff3['permno'] = rvar_ff3['permno'].astype(int)
rvar_ff3['jdate'] = pd.to_datetime(rvar_ff3['date']) + MonthEnd(0)
rvar_ff3 = rvar_ff3[['permno', 'jdate', 'rvar_ff3']]
rvar_ff3 = rvar_ff3.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, rvar_ff3, how='left', on=['permno', 'jdate'])

with open(sue.feather', 'rb') as f:
    sue = feather.read_feather(f)

sue['permno'] = sue['permno'].astype(int)
sue['jdate'] = pd.to_datetime(sue['date']) + MonthEnd(0)
sue = sue[['permno', 'jdate', 'sue']]
sue = sue.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, sue, how='left', on=['permno', 'jdate'])

with open(myre.feather', 'rb') as f:
    re = feather.read_feather(f)

re['permno'] = re['permno'].astype(int)
re['jdate'] = pd.to_datetime(re['date']) + MonthEnd(0)
re = re[['permno', 'jdate', 're']]
re = re.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, re, how='left', on=['permno', 'jdate'])

with open(abr.feather', 'rb') as f:
    abr = feather.read_feather(f)

abr['permno'] = abr['permno'].astype(int)
abr['jdate'] = pd.to_datetime(abr['date']) + MonthEnd(0)
abr = abr[['permno', 'jdate', 'abr']]
abr = abr.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, abr, how='left', on=['permno', 'jdate'])

with open('baspread.feather', 'rb') as f:
    baspread = feather.read_feather(f)

baspread['permno'] = baspread['permno'].astype(int)
baspread['jdate'] = pd.to_datetime(baspread['date']) + MonthEnd(0)
baspread = baspread[['permno', 'jdate', 'baspread']]
baspread = baspread.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, baspread, how='left', on=['permno', 'jdate'])

with open('maxret.feather', 'rb') as f:
    maxret = feather.read_feather(f)

maxret['permno'] = maxret['permno'].astype(int)
maxret['jdate'] = pd.to_datetime(maxret['date']) + MonthEnd(0)
maxret = maxret[['permno', 'jdate', 'maxret']]
maxret = maxret.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, maxret, how='left', on=['permno', 'jdate'])

with open('std_dolvol.feather', 'rb') as f:
    std_dolvol = feather.read_feather(f)

std_dolvol['permno'] = std_dolvol['permno'].astype(int)
std_dolvol['jdate'] = pd.to_datetime(std_dolvol['date']) + MonthEnd(0)
std_dolvol = std_dolvol[['permno', 'jdate', 'std_dolvol']]
std_dolvol = std_dolvol.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, std_dolvol, how='left', on=['permno', 'jdate'])

with open('ill.feather', 'rb') as f:
    ill = feather.read_feather(f)

ill['permno'] = ill['permno'].astype(int)
ill['jdate'] = pd.to_datetime(ill['date']) + MonthEnd(0)
ill = ill[['permno', 'jdate', 'ill']]
ill = ill.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, ill, how='left', on=['permno', 'jdate'])

with open('std_turn.feather', 'rb') as f:
    std_turn = feather.read_feather(f)

std_turn['permno'] = std_turn['permno'].astype(int)
std_turn['jdate'] = pd.to_datetime(std_turn['date']) + MonthEnd(0)
std_turn = std_turn[['permno', 'jdate', 'std_turn']]
std_turn = std_turn.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, std_turn, how='left', on=['permno', 'jdate'])

with open('zerotrade.feather', 'rb') as f:
    zerotrade = feather.read_feather(f)

zerotrade['permno'] = zerotrade['permno'].astype(int)
zerotrade['jdate'] = pd.to_datetime(zerotrade['date']) + MonthEnd(0)
zerotrade = zerotrade[['permno', 'jdate', 'zerotrade']]
zerotrade = zerotrade.drop_duplicates(['permno', 'jdate'])

chars_q = pd.merge(chars_q, zerotrade, how='left', on=['permno', 'jdate'])

# save data
with open('chars_q_raw.feather', 'wb') as f:
    feather.write_feather(chars_q, f)