# Xin He - Nov 28, 2019
import wrds
db = wrds.Connection(wrds_username='xinhe97')

import pandas as pd
import numpy as np
import datetime as dt
import psycopg2
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from scipy import stats

import matplotlib as mpl
import os
if os.environ.get('DISPLAY','') == '':
    print('no display found. Using non-interactive Agg backend')
    mpl.use('Agg')

import matplotlib.pyplot as plt

start_date = dt.datetime(2010,1,1)
#end_date = datetime.datetime(2019,11,30)
end_date = dt.datetime.now()

from bm import *
