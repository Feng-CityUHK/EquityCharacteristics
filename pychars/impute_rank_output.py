import pandas as pd
import pickle as pkl
import numpy as np
from functions import *

with open('sp1500.pkl', 'rb')as f:
    sp1500 = pkl.load(f)

# impute missing values, you can choose different func form functions, such as ffi49/ffi10
sp1500['sic'] = sp1500['sic'].astype(int)
sp1500['jdate'] = pd.to_datetime(sp1500['jdate'])

sp1500['ffi49'] = ffi49(sp1500)
sp1500['ffi49'] = sp1500['ffi49'].fillna(49)  # we treat na in ffi49 as 'other'
sp1500['ffi49'] = sp1500['ffi49'].astype(int)

# there are two ways to impute: industrial median or mean
sp1500 = fillna_ind(sp1500, method='median', ffi=49)

# normalize characteristics
sp1500 = normalize(sp1500, ffi=49)

with open('sp1500_final.pkl', 'wb') as f:
    pkl.dump(sp1500, f, protocol=4)