import pandas as pd
import pickle as pkl
import numpy as np
import wrds
from functions import *

####################
#    All Stocks    #
####################

with open('chars_a_60.pkl', 'rb') as f:
    chars_a = pkl.load(f)

chars_a = chars_a.dropna(subset=['permno'])
chars_a[['permno', 'gvkey']] = chars_a[['permno', 'gvkey']].astype(int)
chars_a['jdate'] = pd.to_datetime(chars_a['jdate'])
chars_a = chars_a.drop_duplicates(['permno', 'jdate'])

with open('chars_q_raw.pkl', 'rb') as f:
    chars_q = pkl.load(f)

# use annual variables to fill na of quarterly variables
chars_q = fillna_atq(df_q=chars_q, df_a=chars_a)

# merge annual variables to quarterly variables
chars_a_var = chars_a[['permno', 'jdate', 'dy', 'hire', 'herf', 'me_ia', 'bm_ia']]
chars_q = pd.merge(chars_q, chars_a_var, how='left', on=['permno', 'jdate'])

# adm is annual variable
adm = chars_a[['permno', 'jdate', 'adm']]
chars_q = pd.merge(chars_q, adm, how='left', on=['permno', 'jdate'])

# impute missing values, you can choose different func form functions, such as ffi49/ffi10
chars_q_impute = chars_q.copy()
chars_q_impute['sic'] = chars_q_impute['sic'].astype(int)
chars_q_impute['jdate'] = pd.to_datetime(chars_q_impute['jdate'])

chars_q_impute['ffi49'] = ffi49(chars_q_impute)
chars_q_impute['ffi49'] = chars_q_impute['ffi49'].fillna(49)  # we treat na in ffi49 as 'other'
chars_q_impute['ffi49'] = chars_q_impute['ffi49'].astype(int)

# there are two ways to impute: industrial median or mean
chars_q_impute = fillna_ind(chars_q_impute, method='median', ffi=49)
# we use all stocks' mean or median to fill na that are not filled by value of ffi
chars_q_impute = fillna_all(chars_q_impute, method='median')
chars_q_impute['re'] = chars_q_impute['re'].fillna(0)  # re use IBES database, there are lots of missing data

chars_q_impute['year'] = chars_q_impute['jdate'].dt.year
chars_q_impute = chars_q_impute[chars_q_impute['year'] >= 1972]
chars_q_impute = chars_q_impute.drop(['year'], axis=1)

chars_q_impute = chars_q_impute[['permno', 'gvkey', 'datadate', 'jdate', 'ffi49', 'sic', 'exchcd', 'shrcd', 'ret',
                                 'retx', 'retadj', 'me', 'abr', 'acc', 'adm', 'agr',
                                 'alm', 'ato', 'baspread', 'beta', 'bm', 'bm_ia',
                                 'cash',  'cashdebt', 'cfp', 'chcsho', 'chpm', 'chtx',
                                 'cinvest', 'depr', 'dolvol', 'dy', 'ep', 'gma',
                                 'grltnoa', 'herf', 'hire', 'ill', 'lev', 'lgr',
                                 'maxret', 'me_ia', 'mom12m', 'mom1m', 'mom36m', 'mom60m',
                                 'mom6m', 'ni', 'nincr', 'noa', 'op', 'pctacc', 'pm',
                                 'pscore', 'rd_sale', 'rdm', 're', 'rna', 'roa',
                                 'roe', 'rsup', 'rvar_capm', 'rvar_ff3', 'rvar_mean',
                                 'seas1a', 'sgr', 'sp', 'std_dolvol', 'std_turn', 'sue',
                                 'turn', 'zerotrade']]

# process me and shift data
chars_q_impute['log_me'] = np.log(chars_q_impute['me'])
chars_q_impute = chars_q_impute.rename(columns={'me': 'lag_me'})
chars_q_impute['ret'] = chars_q_impute.groupby(['permno'])['ret'].shift(-1)
chars_q_impute['retx'] = chars_q_impute.groupby(['permno'])['retx'].shift(-1)
chars_q_impute['retadj'] = chars_q_impute.groupby(['permno'])['retadj'].shift(-1)

# drop na due to lag
chars_q_impute = chars_q_impute.dropna()

with open('chars_impute_60.pkl', 'wb') as f:
    pkl.dump(chars_q_impute, f, protocol=4)

# standardize characteristics
chars_q_rank = chars_q.copy()
chars_q_rank['sic'] = chars_q_rank['sic'].astype(int)
chars_q_rank['jdate'] = pd.to_datetime(chars_q_rank['jdate'])

chars_q_rank['ffi49'] = ffi49(chars_q_rank)
chars_q_rank['ffi49'] = chars_q_rank['ffi49'].fillna(49)  # we treat na in ffi49 as 'other'
chars_q_rank['ffi49'] = chars_q_rank['ffi49'].astype(int)

# process me and shift data
chars_q_rank['log_me'] = np.log(chars_q_rank['me'])
chars_q_rank = chars_q_rank.rename(columns={'me': 'lag_me'})
chars_q_rank['ret'] = chars_q_rank.groupby(['permno'])['ret'].shift(-1)
chars_q_rank['retx'] = chars_q_rank.groupby(['permno'])['retx'].shift(-1)
chars_q_rank['retadj'] = chars_q_rank.groupby(['permno'])['retadj'].shift(-1)

# standardize data
chars_q_rank = standardize(chars_q_rank)
chars_q_rank['year'] = chars_q_rank['jdate'].dt.year
chars_q_rank = chars_q_rank[chars_q_rank['year'] >= 1972]
chars_q_rank = chars_q_rank.drop(['year'], axis=1)

chars_q_rank = chars_q_rank[['permno', 'gvkey', 'datadate', 'jdate', 'ffi49', 'sic', 'exchcd', 'shrcd', 'ret',
                             'retx', 'retadj', 'lag_me', 'rank_log_me', 'rank_abr', 'rank_acc', 'rank_adm', 'rank_agr',
                             'rank_alm', 'rank_ato', 'rank_baspread', 'rank_beta', 'rank_bm', 'rank_bm_ia', 'rank_cash',
                             'rank_cashdebt', 'rank_cfp', 'rank_chcsho', 'rank_chpm', 'rank_chtx', 'rank_cinvest',
                             'rank_depr', 'rank_dolvol', 'rank_dy', 'rank_ep', 'rank_gma', 'rank_grltnoa', 'rank_herf',
                             'rank_hire', 'rank_ill', 'rank_lev', 'rank_lgr', 'rank_maxret', 'rank_me_ia',
                             'rank_mom12m', 'rank_mom1m', 'rank_mom36m', 'rank_mom60m', 'rank_mom6m', 'rank_ni',
                             'rank_nincr', 'rank_noa', 'rank_op', 'rank_pctacc', 'rank_pm', 'rank_pscore',
                             'rank_rd_sale', 'rank_rdm', 'rank_re', 'rank_rna', 'rank_roa', 'rank_roe', 'rank_rsup',
                             'rank_rvar_capm', 'rank_rvar_ff3', 'rank_rvar_mean', 'rank_seas1a', 'rank_sgr', 'rank_sp',
                             'rank_std_dolvol', 'rank_std_turn', 'rank_sue', 'rank_turn', 'rank_zerotrade']]

with open('chars_rank_60.pkl', 'wb') as f:
    pkl.dump(chars_q_rank, f, protocol=4)

####################
#      SP1500      #
####################
with open('/home/jianxinma/chars/data/sp1500_impute_benchmark.pkl', 'rb') as f:
    sp1500_index = pkl.load(f)

sp1500_index = sp1500_index[['gvkey', 'jdate']]

sp1500_impute = pd.merge(sp1500_index, chars_q_impute, how='left', on=['gvkey', 'jdate'])

# for test
# test = sp1500_rank.groupby(['jdate'])['gvkey'].nunique()

with open('sp1500_impute_60.pkl', 'wb') as f:
    pkl.dump(sp1500_impute, f, protocol=4)

# standardize characteristics
sp1500_rank = pd.merge(sp1500_index, chars_q_rank, how='left', on=['gvkey', 'jdate'])

with open('sp1500_rank_60.pkl', 'wb') as f:
    pkl.dump(sp1500_rank, f, protocol=4)