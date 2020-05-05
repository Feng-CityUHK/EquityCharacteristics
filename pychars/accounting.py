import pandas as pd
import numpy as np
import datetime as dt
import wrds
import psycopg2
from dateutil.relativedelta import *
from pandas.tseries.offsets import *

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

#######################################################################################################################
#                                                    TTM functions                                                    #
#######################################################################################################################
def ttm4(series, df):
    '''

    :param series: variables' name
    :param df: dataframe
    :return: ttm4
    '''
    lag = pd.DataFrame()
    for i in range(1, 4):
        lag['%(series)s%(lag)s' % {'series': series, 'lag': i}] = np.where(df['gvkey'] == df['gvkey'].shift(i), df['%s' % series].shift(i), np.nan)
    result = np.sum([df['%s' % series], lag['%s1' % series], lag['%s2' % series], lag['%s3' % series]])
    return result

def ttm12(series, df):
    '''

    :param series: variables' name
    :param df: dataframe
    :return: ttm12
    '''
    lag = pd.DataFrame()
    for i in range(1, 12):
        lag['%(series)s%(lag)s' % {'series': series, 'lag': i}] = np.where(df['gvkey'] == df['gvkey'].shift(i), df['%s' % series].shift(i), np.nan)
    result = np.sum([df['%s' % series], lag['%s1' % series], lag['%s2' % series], lag['%s3' % series], lag['%s4' % series],
                    lag['%s5' % series], lag['%s6' % series], lag['%s7' % series], lag['%s8' % series], lag['%s9' % series],
                    lag['%s10' % series], lag['%s11' % series]])
    return result

#######################################################################################################################
#                                                  Compustat Block                                                    #
#######################################################################################################################
'''
gvkey: Compustat’s permanent company identifier
at: Assets - Total
pstkl: Preferred Stock/Liquidating Value
txditc: Deferred Taxes and Investment Tax Credit
pstkrv: Preferred Stock/Redemption Value
seq: Stockholders' Equity - Total
pstk: Preferred/Preference Stock (Capital) - Total
'''

comp = conn.raw_sql("""
                    /*header info*/
                    select c.gvkey, f.cusip, f.datadate, f.fyear, c.cik, substr(c.sic,1,2) as sic2, c.sic, c.naics,
                    
                    /*firm variables*/
                    /*income statement*/
                    f.sale, f.revt, f.cogs, f.xsga, f.dp, f.xrd, f.xad, f.ib, f.ebitda,
                    f.ebit, f.nopi, f.spi, f.pi, f.txp, f.ni, f.txfed, f.txfo, f.txt, f.xint,
                    
                    /*CF statement and others*/
                    f.capx, f.oancf, f.dvt, f.ob, f.gdwlia, f.gdwlip, f.gwo, f.mib, f.oiadp, f.ivao,
                    
                    /*assets*/
                    f.rect, f.act, f.che, f.ppegt, f.invt, f.at, f.aco, f.intan, f.ao, f.ppent, f.gdwl, f.fatb, f.fatl,
                    
                    /*liabilities*/
                    f.lct, f.dlc, f.dltt, f.lt, f.dm, f.dcvt, f.cshrc, 
                    f.dcpstk, f.pstk, f.ap, f.lco, f.lo, f.drc, f.drlt, f.txdi,
                    
                    /*equity and other*/
                    f.ceq, f.scstkc, f.emp, f.csho, f.seq, f.txditc, f.pstkrv, f.pstkl, f.np, f.txdc, f.dpc, f.ajex,
                    
                    /*market*/
                    abs(f.prcc_f) as prcc_f, f.csho*prcc_f as mve_f
                    
                    from comp.funda as f
                    left join comp.company as c
                    on f.gvkey = c.gvkey
                    
                    /*get consolidated, standardized, industrial format statements*/
                    where f.indfmt = 'INDL' 
                    and f.datafmt = 'STD'
                    and f.popsrc = 'D'
                    and f.consol = 'C'
                    and f.datadate >= '01/01/1959'
                    """)
comp['cnum'] = comp['cusip'].str.strip().str[0:6]

# sort and clean up
comp=comp.sort_values(by=['gvkey','datadate']).drop_duplicates()

# prep for clean-up and using time series of variables
comp['count']=comp.groupby(['gvkey']).cumcount() # number of years in Compustat

# do some clean up. several of these variables have lots of missing values
condlist = [comp['drc'].notna() & comp['drlt'].notna(),
            comp['drc'].notna() & comp['drlt'].isnull(),
            comp['drlt'].notna() & comp['drc'].isnull()]
choicelist = [comp['drc']+comp['drlt'],
              comp['drc'],
              comp['drlt']]
comp['dr'] = np.select(condlist, choicelist, default=np.nan)

condlist = [comp['dcvt'].isnull() & comp['dcpstk'].notna() & comp['pstk'].notna() & comp['dcpstk']>comp['pstk'],
            comp['dcvt'].isnull() & comp['dcpstk'].notna() & comp['pstk'].isnull()]
choicelist = [comp['dcpstk']-comp['pstk'],
              comp['dcpstk']]
comp['dc'] = np.select(condlist, choicelist, default=np.nan)
comp['dc'] = np.where(comp['dc'].isnull(), comp['dcvt'], comp['dc'])

comp['xint0'] = np.where(comp['xint'].isnull(), 0, comp['xint'])
comp['xsga0'] = np.where(comp['xsga'].isnull, 0, 0)

# convert datadate to date fmt
comp['datadate'] = pd.to_datetime(comp['datadate'])

#######################################################################################################################
#                                                       CRSP Block                                                    #
#######################################################################################################################
# Create a CRSP Subsample with Monthly Stock and Event Variables
# Restrictions will be applied later
# Select variables from the CRSP monthly stock and event datasets
crsp_m = conn.raw_sql("""
                      select a.prc, a.ret, a.retx, a.shrout, a.vol, a.cfacpr, a.cfacshr, a.date, a.permno, a.permco,
                      b.ticker, b.ncusip, b.shrcd, b.exchcd
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/1959' and '12/31/2018'
                      and b.exchcd between 1 and 3
                      """)

'''
permno: a unique permanent security identification number assigned by CRSP to each security
permco: a unique permanent company identification number assigned by CRSP to all companies with issues on a CRSP File
shrcd: share type code
exchcd: exchange code
ret: returns on income
retx: returns without dividends
shrout: share outstanding
prc: price
'''

# change variable format to int
crsp_m[['permco', 'permno', 'shrcd', 'exchcd']] = crsp_m[['permco', 'permno', 'shrcd', 'exchcd']].astype(int)

# Line up date to be end of month
crsp_m['date'] = pd.to_datetime(crsp_m['date'])
crsp_m['jdate'] = crsp_m['date']+MonthEnd(0)  # set all the date to the standard end date of month

# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.msedelist
                     """)

dlret.permno = dlret.permno.astype(int)
dlret['dlstdt'] = pd.to_datetime(dlret['dlstdt'])
dlret['jdate'] = dlret['dlstdt']+MonthEnd(0)

crsp = pd.merge(crsp_m, dlret, how='left', on=['permno','jdate'])
crsp['dlret'] = crsp['dlret'].fillna(0)
crsp['ret'] = crsp['ret'].fillna(0)
crsp['retadj'] = (1+crsp['ret'])*(1+crsp['dlret'])-1
crsp['me'] = crsp['prc'].abs()*crsp['shrout']  # calculate market equity

# if me is Nan then let return equals to 0
crsp['ret'] = np.where(crsp['me'].isnull(), 0, crsp['ret'])
crsp['retx'] = np.where(crsp['me'].isnull(), 0, crsp['retx'])

# impute prc and me
crsp = crsp.sort_values(by=['permno', 'date']).drop_duplicates()
crsp['prc'] = np.where(crsp['permno'] == crsp['permno'].shift(1), crsp['prc'].fillna(method='ffill'), crsp['prc'])
crsp['me'] = np.where(crsp['permno'] == crsp['permno'].shift(1), crsp['me'].fillna(method='ffill'), crsp['me'])


### Aggregate Market Cap ###
# There are cases when the same firm (permco) has two or more securities (permno) at same date.
# For the purpose of ME for the firm, we aggregated all ME for a given permco, date.
# Thisaggregated ME will be assigned to the permno with the largest ME.

# sum of me across different permno belonging to same permco a given date
crsp_summe = crsp.groupby(['jdate','permco'])['me'].sum().reset_index()
# largest mktcap within a permco/date
crsp_maxme = crsp.groupby(['jdate','permco'])['me'].max().reset_index()
# join by jdate/maxme to find the permno
crsp1 = pd.merge(crsp, crsp_maxme, how='inner', on=['jdate','permco','me'])
# drop me column and replace with the sum me
crsp1 = crsp1.drop(['me'], axis=1)
# join with sum of me to get the correct market cap info
crsp2 = pd.merge(crsp1, crsp_summe, how='inner', on=['jdate','permco'])
# sort by permno and date and also drop duplicates
crsp2 = crsp2.sort_values(by=['permno', 'jdate']).drop_duplicates()

#######################################################################################################################
#                                                        CCM Block                                                    #
#######################################################################################################################
# CRSP/Compustat Merged Database
# reference: https://wrds-www.wharton.upenn.edu/pages/support/applications/linking-databases/linking-crsp-and-compustat/
ccm = conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim, 
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where substr(linktype,1,1)='L'
                  and (linkprim ='C' or linkprim='P')
                  """)
'''
gvkey: Compustat’s permanent company identifier
lpermno: Linked CRSP PERMCO, 0 if no CRSP company link exists
linktype: Link type code. Each link is given a code describing the connection between the CRSP and Compustat data
linkprim: Primary issue marker for the link
linkdt: First effective calendar date of link record range
linkenddt: Last effective calendar date of link record range
'''

ccm['linkdt'] = pd.to_datetime(ccm['linkdt'])
ccm['linkenddt'] = pd.to_datetime(ccm['linkenddt'])
# if linkenddt is missing then set to today date
ccm['linkenddt'] = ccm['linkenddt'].fillna(pd.to_datetime('today'))

ccm1=pd.merge(comp, ccm, how='left', on=['gvkey'])
# we can only get the accounting data after the firm public their report
ccm1['yearend'] = ccm1['datadate']+YearEnd(0)
ccm1['jdate'] = ccm1['yearend']+MonthEnd(6)

# set link date bounds
ccm2 = ccm1[(ccm1['jdate']>=ccm1['linkdt']) & (ccm1['jdate']<=ccm1['linkenddt'])]

# link comp and crsp
data_rawa = pd.merge(crsp2, ccm2, how='inner', on=['permno', 'jdate'])

# filter exchcd & shrcd
data_rawa = data_rawa[((data_rawa['exchcd']==1) | (data_rawa['exchcd']==2) | (data_rawa['exchcd']==3)) &
                   ((data_rawa['shrcd']==10) | (data_rawa['shrcd']==11))]

# Note: data_rawa['me'] actully is the crsp me

# process crsp me
data_rawa['mve_f'] = data_rawa['me']/1000

# update count after merging
data_rawa['count'] = data_rawa.groupby(['gvkey']).cumcount() + 1

# deal with the duplicates
data_rawa.loc[data_rawa.groupby(['datadate', 'permno', 'linkprim'], as_index=False).nth([0]).index, 'temp' ] = 1
data_rawa = data_rawa[data_rawa['temp'].notna()]
data_rawa.loc[data_rawa.groupby(['permno', 'yearend', 'datadate'], as_index=False).nth([-1]).index, 'temp' ] = 1
data_rawa = data_rawa[data_rawa['temp'].notna()]

#######################################################################################################################
#                                                  Annual Variables                                                   #
#######################################################################################################################
chars_a = pd.DataFrame()
chars_a[['cusip', 'ncusip', 'cnum', 'gvkey', 'permno', 'exchcd', 'datadate', 'jdate', 'fyear', 'sic2', 'sic']] = \
    data_rawa[['cusip', 'ncusip', 'cnum', 'gvkey', 'permno', 'exchcd', 'datadate', 'jdate', 'fyear', 'sic2', 'sic']]
lag_a = pd.DataFrame()
lag_a[['permno', 'gvkey', 'cusip', 'datadate', 'jdate']] = data_rawa[['permno', 'gvkey', 'cusip', 'datadate', 'jdate']]

# preferrerd stock
data_rawa['ps'] = np.where(data_rawa['pstkrv'].isnull(), data_rawa['pstkl'], data_rawa['pstkrv'])
data_rawa['ps'] = np.where(data_rawa['ps'].isnull(), data_rawa['pstk'], data_rawa['ps'])
data_rawa['ps'] = np.where(data_rawa['ps'].isnull(), 0, data_rawa['ps'])

data_rawa['txditc'] = data_rawa['txditc'].fillna(0)

# book equity
chars_a['be'] = data_rawa['seq']+data_rawa['txditc']-data_rawa['ps']
chars_a['be'] = np.where(chars_a['be']>0, chars_a['be'], np.nan)

# ac
lag_a['act'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['act'].shift(1), np.nan)
lag_a['lct'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['lct'].shift(1), np.nan)

condlist = [data_rawa['np'].isnull(),
          data_rawa['act'].isnull() | data_rawa['lct'].isnull()]
choicelist = [((data_rawa['act']-data_rawa['lct'])-(lag_a['act']-lag_a['lct'])/(10*chars_a['be'])),
              (data_rawa['ib']-data_rawa['oancf'])/(10*chars_a['be'])]
chars_a['ac'] = np.select(condlist, choicelist, default=((data_rawa['act']-data_rawa['lct']+data_rawa['np'])-
                  (lag_a['act']-lag_a['lct']+data_rawa['np'].shift(1)))/(10*chars_a['be']))

# inv
lag_a['at'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['at'].shift(1), np.nan)
chars_a['inv'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1), np.nan, -(lag_a['at']-data_rawa['at'])/lag_a['at'])

# bm
chars_a['bm'] = chars_a['be']
chars_a['bm_n'] = chars_a['be']

# cfp
condlist = [data_rawa['dp'].isnull(),
            data_rawa['ib'].isnull()]
choicelist = [data_rawa['ib']/data_rawa['mve_f'],
              np.nan]
chars_a['cfp'] = np.select(condlist, choicelist, default=(data_rawa['ib']+data_rawa['dp'])/data_rawa['mve_f'])

condlist = [data_rawa['dp'].isnull(),
            data_rawa['ib'].isnull()]
choicelist = [data_rawa['ib'],
              np.nan]
chars_a['cfp_n'] = np.select(condlist, choicelist, default=data_rawa['ib']+data_rawa['dp'])

# ep
chars_a['ep'] = data_rawa['ib']/data_rawa['mve_f']
chars_a['ep_n'] = data_rawa['ib']

# ni
lag_a['csho'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['csho'].shift(1), np.nan)
lag_a['ajex'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['ajex'].shift(1), np.nan)
chars_a['ni'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1), np.nan, np.log(data_rawa['csho']*data_rawa['ajex']).replace(-np.inf, 0)-
                         np.log(lag_a['csho']*lag_a['ajex']).replace(-np.inf, 0))

# op
chars_a['cogs0'] = np.where(data_rawa['cogs'].isnull(), 0, data_rawa['cogs'])
chars_a['xint0'] = np.where(data_rawa['xint'].isnull(), 0, data_rawa['xint'])
chars_a['xsga0'] = np.where(data_rawa['xsga'].isnull(), 0, data_rawa['xsga'])

condlist = [data_rawa['revt'].isnull(), chars_a['be'].isnull()]
choicelist = [np.nan, np.nan]
chars_a['op'] = np.select(condlist, choicelist,
                          default=(data_rawa['revt'] - chars_a['cogs0'] - chars_a['xsga0'] - chars_a['xint0'])/chars_a['be'])

# rsup
lag_a['sale'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['sale'].shift(1), np.nan)
chars_a['rsup'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1), np.nan, (data_rawa['sale']-lag_a['sale'])/data_rawa['mve_f'])

# sue
chars_a['sue'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1), np.nan, (data_rawa['ib']-lag_a['ib'])/data_rawa['mve_f'])

# cash
chars_a['cash'] = data_rawa['che']/data_rawa['at']

# lev
chars_a['lev'] = data_rawa['lt']/data_rawa['mve_f']

# sp
chars_a['sp'] = data_rawa['sale']/data_rawa['mve_f']
chars_a['sp_n'] = data_rawa['sale']

# rd_sale
chars_a['rd_sale'] = data_rawa['xrd']/data_rawa['sale']

# rd_mve
chars_a['rd_mve'] = data_rawa['xrd']/data_rawa['mve_f']

# rmd hxz rdm
chars_a['rdm'] = chars_a['rd_mve']
chars_a['rdm_n'] = data_rawa['xrd']

# adm hxz adm
chars_a['adm'] = data_rawa['xad']/data_rawa['mve_f']
chars_a['adm_n'] = data_rawa['xad']

# gma
chars_a['gma'] = (data_rawa['revt']-data_rawa['cogs'])/lag_a['at']

# chcsho
chars_a['chcsho'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1), np.nan,
                             (data_rawa['csho']/lag_a['csho'])-1)

# lgr
lag_a['lt'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['lt'].shift(1), np.nan)
chars_a['lgr'] = (data_rawa['lt']/lag_a['lt'])-1

# pctacc
lag_a['che'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['che'].shift(1), np.nan)
lag_a['dlc'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['dlc'].shift(1), np.nan)
lag_a['txp'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['txp'].shift(1), np.nan)

condlist = [data_rawa['ib']==0,
            data_rawa['oancf'].isnull(),
            data_rawa['oancf'].isnull() & data_rawa['ib']==0]
choicelist = [(data_rawa['ib']-data_rawa['oancf'])/0.01,
              ((data_rawa['act']-lag_a['act'])-(data_rawa['che']-lag_a['che']))-
              ((data_rawa['lct']-lag_a['lct'])-(data_rawa['dlc'])-lag_a['dlc']-
               ((data_rawa['txp']-lag_a['txp'])-data_rawa['dp']))/data_rawa['ib'].abs(),
              ((data_rawa['act'] - lag_a['act']) - (data_rawa['che'] - lag_a['che'])) -
              ((data_rawa['lct'] - lag_a['lct']) - (data_rawa['dlc']) - lag_a['dlc'] -
               ((data_rawa['txp'] - lag_a['txp']) - data_rawa['dp']))]
chars_a['pctacc'] = np.select(condlist, choicelist, default=(data_rawa['ib']-data_rawa['oancf'])/data_rawa['ib'].abs())

# age
chars_a['age'] = data_rawa['count']

# sgr
chars_a['sgr'] = (data_rawa['sale']/lag_a['sale'])-1

# chpm
lag_a['ib'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['ib'].shift(1), np.nan)
chars_a['chpm'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1), np.nan,
                           (data_rawa['ib']/data_rawa['sale'])-(lag_a['ib']/lag_a['sale']))

# chato
lag_a['at2'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(2), data_rawa['at'].shift(2), np.nan)
chars_a['chato'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1), np.nan,
                            (data_rawa['sale']/((data_rawa['at']+lag_a['at'])/2))-
                            (lag_a['sale']/((data_rawa['at']+lag_a['at2'])/2)))

# chtx
lag_a['txt'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['txt'].shift(1), np.nan)
chars_a['chtx'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1), np.nan,
                           (data_rawa['txt']-lag_a['txt'])/lag_a['at'])

# ala
chars_a['ala'] = data_rawa['che']+0.75*(data_rawa['act']-data_rawa['che'])-0.5*(data_rawa['at']-data_rawa['act']-data_rawa['gdwl']-data_rawa['intan'])

# alm
chars_a['alm'] = chars_a['ala']/(data_rawa['at']+data_rawa['prcc_f']*data_rawa['csho']-data_rawa['ceq'])

# noa
chars_a['noa'] = ((data_rawa['at']-data_rawa['che']-data_rawa['ivao'].fillna(0))-
                  (data_rawa['at']-data_rawa['dlc'].fillna(0)-data_rawa['dltt'].fillna(0)-data_rawa['mib'].fillna(0)
                   -data_rawa['pstk'].fillna(0)-data_rawa['ceq'])/lag_a['at'])

# rna
lag_a['noa'] = np.where(chars_a['gvkey'] == chars_a['gvkey'].shift(1), chars_a['noa'].shift(1), np.nan)
chars_a['rna'] = data_rawa['oiadp']/lag_a['noa']

# pm
chars_a['pm'] = data_rawa['oiadp']/data_rawa['sale']

# ato
chars_a['ato'] = data_rawa['sale']/lag_a['noa']

# depr
chars_a['depr'] = data_rawa['dp']/data_rawa['ppent']

# invest
lag_a['ppent'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['ppent'].shift(1), np.nan)
lag_a['invt'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['invt'].shift(1), np.nan)

chars_a['invest'] = np.where(data_rawa['ppegt'].isnull(), ((data_rawa['ppent']-lag_a['ppent'])+
                                                          (data_rawa['invt']-lag_a['invt']))/lag_a['at'],
                             ((data_rawa['ppegt']-lag_a['ppent'])+(data_rawa['invt']-lag_a['invt']))/lag_a['at'])

# egr
lag_a['ceq'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['ceq'].shift(1), np.nan)
chars_a['egr'] = ((data_rawa['ceq']-lag_a['ceq'])/lag_a['ceq'])

# cashdebt
chars_a['cashdebt'] = (data_rawa['ib']+data_rawa['dp'])/((data_rawa['lt']+lag_a['lt'])/2)

# # grltnoa
# lag_a['aco'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['aco'].shift(1), np.nan)
# lag_a['intan'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['intan'].shift(1), np.nan)
# lag_a['ao'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['ao'].shift(1), np.nan)
# lag_a['ap'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['ap'].shift(1), np.nan)
# lag_a['lco'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['lco'].shift(1), np.nan)
# lag_a['lo'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['lo'].shift(1), np.nan)
# lag_a['rect'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['rect'].shift(1), np.nan)
#
# chars_a['grltnoa'] = ((data_rawa['rect']+data_rawa['invt']+data_rawa['ppent']+data_rawa['aco']+data_rawa['intan']+
#                        data_rawa['ao']-data_rawa['ap']-data_rawa['lco']-data_rawa['lo'])-
#                       (lag_a['rect']+lag_a['invt']+lag_a['ppent']+lag_a['aco']+lag_a['intan']+lag_a['ao']-lag_a['ap']-
#                        lag_a['lco']-lag_a['lo'])-\
#                       (data_rawa['rect']-lag_a['rect']+data_rawa['invt']-lag_a['invt']+data_rawa['aco']-lag_a['aco']-
#                        (data_rawa['ap']-lag_a['ap']+data_rawa['lco']-lag_a['lco'])-data_rawa['dp']))/((data_rawa['at']+lag_a['at'])/2)

# rd
lag_a['xrd/at1'] = np.where(data_rawa['gvkey'] == data_rawa['gvkey'].shift(1), data_rawa['xrd']/lag_a['at'], np.nan)
chars_a['rd'] = np.where(((data_rawa['xrd']/data_rawa['at'])-(lag_a['xrd/at1']))/lag_a['xrd/at1']>0.05, 1, 0)

# roa
chars_a['roa'] = data_rawa['ni']/((data_rawa['at']+lag_a['at'])/2)

# dy
chars_a['dy'] = data_rawa['dvt']/data_rawa['mve_f']

# Annual Accounting Variables
chars_a = chars_a[['cusip', 'ncusip', 'cnum', 'gvkey', 'permno', 'exchcd', 'datadate', 'jdate', 'fyear', 'sic2', 'sic',
                  'ac', 'inv', 'bm', 'bm_n', 'cfp', 'cfp_n', 'ep', 'ep_n', 'ni', 'op', 'rsup', 'cash', 'chcsho',
                  'rd', 'cashdebt', 'pctacc', 'gma', 'lev', 'rd_mve', 'rdm', 'rdm_n', 'adm', 'adm_n', 'sgr', 'sp', 'sp_n',
                  'invest', 'rd_sale', 'lgr', 'roa', 'depr', 'egr', 'chpm', 'chato', 'chtx',
                  'ala', 'alm', 'noa', 'rna', 'pm', 'ato', 'dy']]

#######################################################################################################################
#                                              Compustat Quarterly Raw Infor                                          #
#######################################################################################################################
comp = conn.raw_sql("""
                    /*header info*/
                    select c.gvkey, f.cusip, f.datadate, f.fyearq,  substr(c.sic,1,2) as sic2, f.fqtr, f.rdq,

                    /*income statement*/
                    f.ibq, f.saleq, f.txtq, f.revtq, f.cogsq, f.xsgaq, f.revty, f.cogsy, f.saley,

                    /*balance sheet items*/
                    f.atq, f.actq, f.cheq, f.lctq, f.dlcq, f.ppentq, f.ppegtq,

                    /*others*/
                    abs(f.prccq) as prccq, abs(f.prccq)*f.cshoq as mveq, f.ceqq, f.seqq, f.pstkq, f.ltq,
                    f.pstkrq, f.gdwlq, f.intanq, f.mibq, f.oiadpq, f.ivaoq,
                    
                    /* v3 my formula add*/
                    f.ajexq, f.cshoq, f.txditcq, f.npq, f.xrdy, f.xrdq, f.dpq, f.xintq, f.invtq, f.scstkcy, f.niq,
                    f.oancfy, f.dlttq

                    from comp.fundq as f
                    left join comp.company as c
                    on f.gvkey = c.gvkey

                    /*get consolidated, standardized, industrial format statements*/
                    where f.indfmt = 'INDL' 
                    and f.datafmt = 'STD'
                    and f.popsrc = 'D'
                    and f.consol = 'C'
                    and f.datadate >= '01/01/1959'
                    """)
comp['cusip6'] = comp['cusip'].str.strip().str[0:6]
comp = comp[comp['ibq'].notna()]

# sort and clean up
comp = comp.sort_values(by=['gvkey','datadate']).drop_duplicates()

# prep for clean-up and using time series of variables
comp['count'] = comp.groupby(['gvkey']).cumcount() # number of years in Compustat

# convert datadate to date fmt
comp['datadate'] = pd.to_datetime(comp['datadate'])

# merge ccm and comp
ccm1 = pd.merge(comp, ccm, how='left', on=['gvkey'])
ccm1['yearend'] = ccm1['datadate']+YearEnd(0)
ccm1['jdate'] = ccm1['datadate']+MonthEnd(3)
# ccm1['jdate'] = ccm1['datadate']+MonthEnd(4)

# set link date bounds
ccm2 = ccm1[(ccm1['jdate']>=ccm1['linkdt']) & (ccm1['jdate']<=ccm1['linkenddt'])]

# merge ccm2 and crsp2
data_rawq = pd.merge(crsp2, ccm2, how='inner', on=['permno', 'jdate'])

# filter exchcd & shrcd
data_rawq = data_rawq[((data_rawq['exchcd']==1) | (data_rawq['exchcd']==2) | (data_rawq['exchcd']==3)) &
                   ((data_rawq['shrcd']==10) | (data_rawq['shrcd']==11))]

# process crsp me
data_rawq['mveq'] = data_rawq['me']/1000

# update count after merging
data_rawq['count'] = data_rawq.groupby(['gvkey']).cumcount() + 1

# deal with the duplicates
data_rawq.loc[data_rawq.groupby(['datadate', 'permno', 'linkprim'], as_index=False).nth([0]).index, 'temp' ] = 1
data_rawq = data_rawq[data_rawq['temp'].notna()]
data_rawq.loc[data_rawq.groupby(['permno', 'yearend', 'datadate'], as_index=False).nth([-1]).index, 'temp' ] = 1
data_rawq = data_rawq[data_rawq['temp'].notna()]

#######################################################################################################################
#                                                   Quarterly Variables                                               #
#######################################################################################################################
chars_q = pd.DataFrame()
chars_q[['cusip', 'ncusip', 'cusip6', 'gvkey', 'permno', 'exchcd', 'datadate', 'jdate', 'sic2']] = \
    data_rawq[['cusip', 'ncusip', 'cusip6', 'gvkey', 'permno', 'exchcd', 'datadate', 'jdate', 'sic2']]
lag_q = pd.DataFrame()
lag_q[['permno', 'gvkey', 'cusip', 'datadate', 'jdate']] = data_rawq[['permno', 'gvkey', 'cusip', 'datadate', 'jdate']]

# prepare be
chars_q['beq'] = np.where(data_rawq['seqq']>0, data_rawq['seqq']+data_rawq['txditcq']-data_rawq['pstkq'], np.nan)
chars_q['beq'] = np.where(chars_q['beq']<=0, np.nan, chars_q['beq'])

# dy
# data_rawq['me'] actually is the crsp monthly me
lag_q['me'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(1), data_rawq['me'].shift(1), np.nan)
data_rawq['retdy'] = data_rawq['ret'] - data_rawq['retx']
data_rawq['mdivpay'] = data_rawq['retdy']*lag_q['me']

chars_q['dy'] = ttm12(series='mdivpay', df=data_rawq)/data_rawq['me']

# # pstk
# chars_q['pstk'] = np.where(data_rawq['pstkrq'].notna(), data_rawq['pstkrq'], data_rawq['pstkq'])
#
# # scal
# condlist = [data_rawq['seqq'].isnull(),
#             data_rawq['seqq'].isnull() & (data_rawq['ceqq'].isnull() | chars_q['pstk'].isnull())]
# choicelist = [data_rawq['ceqq']+chars_q['pstk'],
#               data_rawq['atq']-data_rawq['ltq']]
# chars_q['scal'] = np.select(condlist, choicelist, default=data_rawq['seqq'])

# chtx
lag_q['txtq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['txtq'].shift(4), np.nan)
lag_q['atq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['atq'].shift(4), np.nan)
chars_q['chtx'] = (data_rawq['txtq']-lag_q['txtq4'])/lag_q['atq4']

# roa
lag_q['atq'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(1), data_rawq['txtq'].shift(1), np.nan)
chars_q['roa'] = data_rawq['ibq']/lag_q['atq']

# cash
chars_q['cash'] = data_rawq['cheq']/data_rawq['atq']

# ac
lag_q['actq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['actq'].shift(4), np.nan)
lag_q['lctq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['lctq'].shift(4), np.nan)
lag_q['npq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['npq'].shift(4), np.nan)
condlist = [data_rawq['npq'].isnull(),
            data_rawq['actq'].isnull() | data_rawq['lctq'].isnull()]
choicelist = [((data_rawq['actq']-data_rawq['lctq'])-(lag_q['actq4']-lag_q['lctq4']))/(10*chars_q['beq']),
              np.nan]
chars_q['ac'] = np.select(condlist, choicelist,
                          default=((data_rawq['actq']-data_rawq['lctq']+data_rawq['npq'])-
                                   (lag_q['actq4']-lag_q['lctq4']+lag_q['npq4']))/(10*chars_q['beq']))

# bm
chars_q['bm'] = chars_q['beq']/data_rawq['mveq']
chars_q['bm_n'] = chars_q['beq']

# cfp
chars_q['cfp'] = np.where(data_rawq['dpq'].isnull(),
                          ttm4('ibq', data_rawq)/data_rawq['mveq'],
                          (ttm4('ibq', data_rawq)+ttm4('dpq', data_rawq))/data_rawq['mveq'])
chars_q['cfp_n'] = chars_q['cfp']*data_rawq['mveq']

# ep
chars_q['ep'] = ttm4('ibq', data_rawq)/data_rawq['mveq']
chars_q['ep_n'] = chars_q['ep']*data_rawq['mveq']

# inv
chars_q['inv'] = -(lag_q['atq4']-data_rawq['atq'])/lag_q['atq4']

# ni
lag_q['cshoq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['cshoq'].shift(4), np.nan)
lag_q['ajexq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['ajexq'].shift(4), np.nan)
chars_q['ni'] = np.where(data_rawq['cshoq'].isnull(), np.nan,
                         np.log(data_rawq['cshoq']*data_rawq['ajexq']).replace(-np.inf, 0)-np.log(lag_q['cshoq4']*lag_q['ajexq4']))

# op
chars_q['xintq0'] = np.where(data_rawq['xintq'].isnull(), 0, data_rawq['xintq'])
chars_q['xsgaq0'] = np.where(data_rawq['xsgaq'].isnull(), 0, data_rawq['xsgaq'])
lag_q['beq4'] = np.where(chars_q['gvkey'] == chars_q['gvkey'].shift(4), chars_q['beq'].shift(4), np.nan)

chars_q['op'] = (ttm4('revtq', data_rawq)-ttm4('cogsq', data_rawq)-ttm4('xsgaq0', chars_q)-ttm4('xintq0', chars_q))/lag_q['beq4']

# sue
lag_q['ibq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['ibq'].shift(4), np.nan)
chars_q['sue'] = (data_rawq['ibq']-lag_q['ibq4'])/data_rawq['mveq'].abs()

# csho
chars_q['chcsho'] = (data_rawq['cshoq']-lag_q['cshoq4'])-1

# cashdebt
lag_q['ltq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['ltq'].shift(4), np.nan)
chars_q['cashdebt'] = (ttm4('ibq', data_rawq) + ttm4('dpq', data_rawq))/((data_rawq['ltq']+lag_q['ltq4'])/2)

# rd
chars_q['xrdq4'] = ttm4('xrdq', data_rawq)
chars_q['xrdq4'] = np.where(chars_q['xrdq4'].isnull(), data_rawq['xrdy'], chars_q['xrdq4'])

chars_q['xrdq4/atq4'] = chars_q['xrdq4']/lag_q['atq4']
lag_q['xrdq4/atq4'] = np.where(chars_q['gvkey'] == chars_q['gvkey'].shift(4), chars_q['xrdq4/atq4'].shift(4), np.nan)
chars_q['rd'] = np.where(((chars_q['xrdq4']/data_rawq['atq'])-lag_q['xrdq4/atq4'])/lag_q['xrdq4/atq4']>0.05, 1, 0)

# pctacc
condlist = [data_rawq['npq'].isnull(),
            data_rawq['actq'].isnull() | data_rawq['lctq'].isnull()]
choicelist = [((data_rawq['actq']-data_rawq['lctq'])-(lag_q['actq4']-lag_q['lctq4']))/abs(ttm4('ibq', data_rawq)), np.nan]
chars_q['pctacc'] = np.select(condlist, choicelist,
                              default=((data_rawq['actq']-data_rawq['lctq']+data_rawq['npq'])-(lag_q['actq4']-lag_q['lctq4']+lag_q['npq4']))/
                                      abs(ttm4('ibq', data_rawq)))

# gma
lag_q['revtq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['revtq'].shift(4), np.nan)
lag_q['cogsq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['cogsq'].shift(4), np.nan)
chars_q['gma'] = (lag_q['revtq4']-lag_q['cogsq4'])/lag_q['atq4']

# lev
chars_q['lev'] = data_rawq['ltq']/data_rawq['mveq']

# rd_mve
chars_q['rd_mve'] = chars_q['xrdq4']/data_rawq['mveq']

# sgr
chars_q['saleq4'] = ttm4('saleq', data_rawq)
chars_q['saleq4'] = np.where(chars_q['saleq4'].isnull(), data_rawq['saley'], chars_q['saleq4'])

lag_q['saleq4_4'] = np.where(chars_q['gvkey'] == chars_q['gvkey'].shift(4), chars_q['saleq4'].shift(4), np.nan)
chars_q['sgr'] = (chars_q['saleq4']/lag_q['saleq4_4'])-1

# sp
chars_q['sp'] = chars_q['saleq4']/data_rawq['mveq']
chars_q['sp_n'] = chars_q['saleq4']

# invest
lag_q['ppentq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['ppentq'].shift(4), np.nan)
lag_q['invtq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['invtq'].shift(4), np.nan)
lag_q['ppegtq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['ppegtq'].shift(4), np.nan)

chars_q['invest'] = np.where(data_rawq['ppegtq'].isnull(), ((data_rawq['ppentq']-lag_q['ppentq4'])+
                                                            (data_rawq['invtq']-lag_q['invtq4']))/lag_q['atq4'],
                             ((data_rawq['ppegtq']-lag_q['ppegtq4'])+(data_rawq['invtq']-lag_q['invtq4']))/lag_q['atq4'])

# rd_sale
chars_q['rd_sale'] = chars_q['xrdq4']/chars_q['saleq4']

# lgr
chars_q['lgr'] = (data_rawq['ltq']/lag_q['ltq4'])-1

# depr
chars_q['depr'] = ttm4('dpq', data_rawq)/data_rawq['ppentq']

# egr
lag_q['ceqq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['ceqq'].shift(4), np.nan)

chars_q['egr'] = (data_rawq['ceqq']-lag_q['ceqq4'])/lag_q['ceqq4']

# grltnoa
# lag_q['rectq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['rectq'].shift(4), np.nan)
# lag_q['acoq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['acoq'].shift(4), np.nan)
# lag_q['apq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['apq'].shift(4), np.nan)
# lag_q['lcoq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['lcoq'].shift(4), np.nan)
# lag_q['loq4'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(4), data_rawq['loq'].shift(4), np.nan)
#
# chars_q['grltnoa'] = ((data_rawq['rectq']+data_rawq['invtq']+data_rawq['ppentq']+data_rawq['acoq']+data_rawq['intanq']+
#                        data_rawq['aoq']-data_rawq['apq']-data_rawq['lcoq']-data_rawq['loq'])-
#                       (lag_q['rectq4']+lag_q['invtq4']+lag_q['ppentq4']+lag_q['acoq4']-lag_q['apq4']-lag_q['lcoq4']-lag_q['loq4'])-\
#                      (data_rawq['rectq']-lag_q['rectq4']+data_rawq['invtq']-lag_q['invtq4']+data_rawq['acoq']-
#                       (data_rawq['apq']-lag_q['apq4']+data_rawq['lcoq']-lag_q['lcoq4'])-
#                       ttm4('dpq', data_rawq)))/((data_rawq['atq']+lag_q['atq4'])/2)

# chpm
chars_q['ibq4'] = ttm4('ibq', data_rawq)
lag_q['ibq4_4'] = np.where(chars_q['gvkey'] == chars_q['gvkey'].shift(4), chars_q['ibq4'].shift(4), np.nan)

chars_q['chpm'] = (chars_q['ibq4']/chars_q['saleq4'])-(lag_q['ibq4_4']/lag_q['saleq4_4'])

# chato
lag_q['atq8'] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(8), data_rawq['atq'].shift(8), np.nan)
chars_q['chato'] = (chars_q['saleq4']/(data_rawq['atq']+lag_q['atq4'])/2)-(lag_q['saleq4_4']/((lag_q['atq4']+lag_q['atq8'])/2))

# ala
chars_q['ala'] = data_rawq['cheq'] + 0.75*(data_rawq['actq']-data_rawq['cheq'])+\
                 0.5*(data_rawq['atq']-data_rawq['actq']-data_rawq['gdwlq']-data_rawq['intanq'])

# alm
chars_q['alm'] = chars_q['ala']/(data_rawq['atq']+data_rawq['mveq']-data_rawq['ceqq'])

# noa
chars_q['ivaoq'] = np.where(data_rawq['ivaoq'].isnull(), 0, 1)
chars_q['dlcq'] = np.where(data_rawq['dlcq'].isnull(), 0 , 1)
chars_q['dlttq']= np.where(data_rawq['dlttq'].isnull(), 0, 1)
chars_q['mibq'] = np.where(data_rawq['mibq'].isnull(), 0, 1)
chars_q['pstkq'] = np.where(data_rawq['pstkq'].isnull(), 0, 1)
chars_q['noa'] = (data_rawq['atq']-data_rawq['cheq']-chars_q['ivaoq'])-\
                 (data_rawq['atq']-chars_q['dlcq']-chars_q['dlttq']-chars_q['mibq']-chars_q['pstkq']-data_rawq['ceqq'])/lag_q['atq4']

# rna
lag_q['noa4'] = np.where(chars_q['gvkey'] == chars_q['gvkey'].shift(4), chars_q['noa'].shift(4), np.nan)

chars_q['rna'] = data_rawq['oiadpq']/lag_q['noa4']

# pm
chars_q['pm'] = data_rawq['oiadpq']/data_rawq['saleq']

# ato
chars_q['ato'] = data_rawq['saleq']/lag_q['noa4']

# Quarterly Accounting Variables
chars_q = chars_q[['gvkey', 'permno', 'datadate', 'jdate', 'cusip6', 'sue',
                   'ac', 'bm', 'cfp', 'ep', 'inv', 'ni', 'op', 'bm_n', 'ep_n', 'cfp_n',
                   'sp_n', 'cash', 'chcsho', 'rd', 'cashdebt', 'pctacc', 'gma', 'lev',
                   'rd_mve', 'sgr', 'sp', 'invest', 'rd_sale', 'lgr', 'roa', 'depr', 'egr',
                   'chato', 'chpm', 'chtx', 'ala', 'alm', 'noa', 'rna', 'pm', 'ato']]


#######################################################################################################################
#                                                       Momentum                                                      #
#######################################################################################################################
def mom(start, end):
    '''

    :param start: Order of starting lag
    :param end: Order of ending lag
    :return: Momentum factor
    '''
    lag = pd.DataFrame()
    result = 1
    for i in range(start, end):
        lag['mom%s' % i] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(i), data_rawq['ret'].shift(i), np.nan)
        result = result * (1+lag['mom%s' % i])
    result = result - 1
    return result

chars_q['mom60m'] = mom(12, 60)
chars_q['mom12m'] = mom(1, 12)
chars_q['mom1m'] = data_rawq['ret']
chars_q['mom6m'] = mom(1, 6)
chars_q['mom36m'] = mom(1, 36)

def moms(start, end):
    '''

    :param start: Order of starting lag
    :param end: Order of ending lag
    :return: Momentum factor
    '''
    lag = pd.DataFrame()
    result = 1
    for i in range(start, end):
        lag['moms%s' % i] = np.where(data_rawq['gvkey'] == data_rawq['gvkey'].shift(i), data_rawq['ret'].shift(i), np.nan)
        result = np.sum([result, lag['moms%s' % i]])
    result = result/11
    return result

chars_q['moms12m'] = moms(1, 12)