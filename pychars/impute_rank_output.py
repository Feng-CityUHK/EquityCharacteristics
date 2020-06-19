import pandas as pd
import pickle as pkl
import numpy as np
from functions import *

with open('sp1500.pkl', 'rb')as f:
    sp1500 = pkl.load(f)

# impute missing values, you can choose different func form functions, such as ffi49/ffi10
sp1500_impute['sic'] = sp1500_impute['sic'].astype(int)
sp1500_impute['jdate'] = pd.to_datetime(sp1500_impute['jdate'])

sp1500_impute['ffi49'] = ffi49(sp1500_impute)
sp1500_impute['ffi49'] = sp1500_impute['ffi49'].fillna(49)  # we treat na in ffi49 as 'other'
sp1500_impute['ffi49'] = sp1500_impute['ffi49'].astype(int)

# there are two ways to impute: industrial median or mean
sp1500_impute = fillna_ind(sp1500, method='median', ffi=49)

with open('sp1500_impute.pkl', 'wb') as f:
    pkl.dump(sp1500_impute, f, protocol=4)

# normalize characteristics
'''
Note: Please use rank_chars in sp1500_rank and ignore other raw chars. Since I use 0 to fill all the N/A in sp1500_rank. 
If you want to use raw chars with N/A values already filled, pleas use fillna_ind function.
'''
sp1500_rank = standardize(sp1500)

with open('sp1500_rank.pkl', 'wb') as f:
    pkl.dump(sp1500_rank, f, protocol=4)