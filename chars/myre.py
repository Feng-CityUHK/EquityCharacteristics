# Calculate HSZ Replicating Anomalies
# RE: Revisions in analysts’ earnings forecasts

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

#########################################################################
# Merging IBES and CRSP by using ICLINK table. Merging last month price #
#########################################################################

with open('iclink.feather', 'rb')as f:
    iclink = feather.read_feather(f)

ibes = conn.raw_sql("""
                         select
                         ticker, statpers, meanest, fpedats, anndats_act, curr_act, fpi, medest
                         from ibes.statsum_epsus
                         where
                         /* filtering IBES */
                         statpers<ANNDATS_ACT      /*only keep summarized forecasts prior to earnings annoucement*/
                         and measure='EPS'
                         and (fpedats-statpers)>=0
                         and CURCODE='USD'
                         and fpi in ('1','2')""")

# filtering IBES
ibes = ibes[(ibes['medest'].notna()) & (ibes['fpedats'].notna())]
ibes = ibes[(ibes['curr_act']=='USD') | (ibes['curr_act'].isnull())]
ibes['statpers'] = pd.to_datetime(ibes['statpers'])
ibes['merge_date'] = ibes['statpers']+MonthEnd(0)

crsp_msf = conn.raw_sql("""
                        select permno, date, prc, cfacpr
                        from crsp.msf
                        """)

crsp_msf['date'] = pd.to_datetime(crsp_msf['date'])
crsp_msf['date'] = crsp_msf['date']+MonthEnd(0)
crsp_msf['merge_date'] = crsp_msf['date']+MonthEnd(1)

ibes_iclink = pd.merge(ibes, iclink, how='left', on='ticker')
ibes_crsp = pd.merge(ibes_iclink, crsp_msf, how='inner', on=['permno', 'merge_date'])
ibes_crsp.sort_values(by=['ticker', 'fpedats', 'statpers'], inplace=True)
ibes_crsp.reset_index(inplace=True, drop=True)

###############################
# Merging last month forecast #
###############################
ibes_crsp['statpers_last_month'] = np.where((ibes_crsp['ticker'] == ibes_crsp['ticker'].shift(1)) &
                                            (ibes_crsp['permno'] == ibes_crsp['permno'].shift(1)) &
                                            (ibes_crsp['fpedats'] == ibes_crsp['fpedats'].shift(1)),
                                            ibes_crsp['statpers'].shift(1).astype(str), np.nan)

ibes_crsp['meanest_last_month'] = np.where((ibes_crsp['ticker'] == ibes_crsp['ticker'].shift(1)) &
                                            (ibes_crsp['permno'] == ibes_crsp['permno'].shift(1)) &
                                            (ibes_crsp['fpedats'] == ibes_crsp['fpedats'].shift(1)),
                                            ibes_crsp['meanest'].shift(1), np.nan)

ibes_crsp.sort_values(by=['ticker', 'permno', 'fpedats', 'statpers'], inplace=True)
ibes_crsp.reset_index(inplace=True, drop=True)

###########################
# Drop empty "last month" #
# Calculate HXZ RE        #
###########################

ibes_crsp = ibes_crsp[ibes_crsp['statpers_last_month'].notna()]
ibes_crsp['prc_adj'] = ibes_crsp['prc']/ibes_crsp['cfacpr']
ibes_crsp = ibes_crsp[ibes_crsp['prc_adj']>0]
ibes_crsp['monthly_revision'] = (ibes_crsp['meanest'] - ibes_crsp['meanest_last_month'])/ibes_crsp['prc_adj']

ibes_crsp['permno'] = ibes_crsp['permno'].astype(int)
ibes_crsp['permno'] = ibes_crsp['permno'].astype(str)
ibes_crsp['fpedats'] = ibes_crsp['fpedats'].astype(str)
ibes_crsp['permno_fpedats'] = ibes_crsp['permno'].str.cat(ibes_crsp['fpedats'], sep='-')

ibes_crsp = ibes_crsp.drop_duplicates(['permno_fpedats', 'statpers'])
ibes_crsp['count'] = ibes_crsp.groupby('permno_fpedats').cumcount() + 1

########################
# Calculate RE (CJL)   #
########################

ibes_crsp['monthly_revision_l1'] = ibes_crsp.groupby(['permno'])['monthly_revision'].shift(1)
ibes_crsp['monthly_revision_l2'] = ibes_crsp.groupby(['permno'])['monthly_revision'].shift(2)
ibes_crsp['monthly_revision_l3'] = ibes_crsp.groupby(['permno'])['monthly_revision'].shift(3)
ibes_crsp['monthly_revision_l4'] = ibes_crsp.groupby(['permno'])['monthly_revision'].shift(4)
ibes_crsp['monthly_revision_l5'] = ibes_crsp.groupby(['permno'])['monthly_revision'].shift(5)
ibes_crsp['monthly_revision_l6'] = ibes_crsp.groupby(['permno'])['monthly_revision'].shift(6)

condlist = [ibes_crsp['count']==4,
            ibes_crsp['count']==5,
            ibes_crsp['count']==6,
            ibes_crsp['count']>=7]
choicelist = [(ibes_crsp['monthly_revision_l1'] + ibes_crsp['monthly_revision_l2'] + ibes_crsp['monthly_revision_l3'])/3,
              (ibes_crsp['monthly_revision_l1'] + ibes_crsp['monthly_revision_l2'] + ibes_crsp['monthly_revision_l3'] + ibes_crsp['monthly_revision_l4'])/4,
              (ibes_crsp['monthly_revision_l1'] + ibes_crsp['monthly_revision_l2'] + ibes_crsp['monthly_revision_l3'] + ibes_crsp['monthly_revision_l4'] + ibes_crsp['monthly_revision_l5'])/5,
              (ibes_crsp['monthly_revision_l1'] + ibes_crsp['monthly_revision_l2'] + ibes_crsp['monthly_revision_l3'] + ibes_crsp['monthly_revision_l4'] + ibes_crsp['monthly_revision_l5'] + ibes_crsp['monthly_revision_l6'])/6]
ibes_crsp['re'] = np.select(condlist, choicelist, default=np.nan)

ibes_crsp = ibes_crsp[ibes_crsp['count']>=4]
ibes_crsp = ibes_crsp.sort_values(by=['ticker', 'statpers', 'fpedats'])
ibes_crsp = ibes_crsp.drop_duplicates(['ticker', 'statpers'])

ibes_crsp = ibes_crsp[['ticker', 'statpers', 'fpedats', 'anndats_act', 'curr_act', 'permno', 're']]
ibes_crsp.rename(columns={'statpers': 'date'}, inplace=True)

with open('myre.feather', 'wb') as f:
    feather.write_feather(ibes_crsp, f)