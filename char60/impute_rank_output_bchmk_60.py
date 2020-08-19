import pandas as pd
import pickle as pkl
import numpy as np
import wrds
from functions import *

####################
#    All Stocks    #
####################

with open('chars_a.pkl', 'rb') as f:
    chars_a = pkl.load(f)

chars_a = chars_a.dropna(subset=['permno'])
chars_a[['permno', 'gvkey']] = chars_a[['permno', 'gvkey']].astype(int)
chars_a['jdate'] = pd.to_datetime(chars_a['jdate'])
chars_a = chars_a.drop_duplicates(['permno', 'jdate'])

with open('chars_q_raw.pkl', 'rb') as f:
    chars_q = pkl.load(f)

# use annual variables to fill na of quarterly variables
chars_q = fillna_atq(df_q=chars_q, df_a=chars_a)

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

with open('chars_impute.pkl', 'wb') as f:
    pkl.dump(chars_q_impute, f, protocol=4)

# standardize characteristics
chars_q_rank = standardize(chars_q)
chars_q_rank['year'] = chars_q_rank['jdate'].dt.year
chars_q_rank = chars_q_rank[chars_q_rank['year'] >= 1972]
chars_q_rank = chars_q_rank.drop(['year'], axis=1)

with open('chars_rank.pkl', 'wb') as f:
    pkl.dump(chars_q_rank, f, protocol=4)

####################
#      SP1500      #
####################
with open('sp1500_impute_benchmark.pkl', 'rb') as f:
    sp1500_index = pkl.load(f)

sp1500_index = sp1500_index[['gvkey', 'jdate']]

sp1500_impute = pd.merge(sp1500_index, chars_q_impute, how='left', on=['gvkey', 'jdate'])

# for test
# test = sp1500_rank.groupby(['jdate'])['gvkey'].nunique()

with open('sp1500_impute.pkl', 'wb') as f:
    pkl.dump(sp1500_impute, f, protocol=4)

# standardize characteristics
sp1500_rank = pd.merge(sp1500_index, chars_q_rank, how='left', on=['gvkey', 'jdate'])

with open('sp1500_rank.pkl', 'wb') as f:
    pkl.dump(sp1500_rank, f, protocol=4)