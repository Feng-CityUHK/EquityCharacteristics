import pandas as pd
import numpy as np
import datetime as dt
import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import pickle as pkl
from functions import *

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

#######################################################################################################################
#                                                    TTM functions                                                    #
#######################################################################################################################


def ttm4(series, df):
    """

    :param series: variables' name
    :param df: dataframe
    :return: ttm4
    """
    lag = pd.DataFrame()
    for i in range(1, 4):
        lag['%(series)s%(lag)s' % {'series': series, 'lag': i}] = df.groupby('gvkey')['%s' % series].shift(i)
    result = df['%s' % series] + lag['%s1' % series] + lag['%s2' % series] + lag['%s3' % series]
    return result


def ttm12(series, df):
    """

    :param series: variables' name
    :param df: dataframe
    :return: ttm12
    """
    lag = pd.DataFrame()
    for i in range(1, 12):
        lag['%(series)s%(lag)s' % {'series': series, 'lag': i}] = df.groupby('permno')['%s' % series].shift(i)
    result = df['%s' % series] + lag['%s1' % series] + lag['%s2' % series] + lag['%s3' % series] +\
             lag['%s4' % series] + lag['%s5' % series] + lag['%s6' % series] + lag['%s7' % series] +\
             lag['%s8' % series] + lag['%s9' % series] + lag['%s10' % series] + lag['%s11' % series]
    return result

print('TTM')
#######################################################################################################################
#                                                  Compustat Block                                                    #
#######################################################################################################################
comp = conn.raw_sql("""
                    /*header info*/
                    select c.gvkey, f.cusip, f.datadate, f.fyear, c.cik, substr(c.sic,1,2) as sic2, c.sic, c.naics,
                    
                    /*firm variables*/
                    /*income statement*/
                    f.sale, f.revt, f.cogs, f.xsga, f.dp, f.xrd, f.xad, f.ib, f.ebitda,
                    f.ebit, f.nopi, f.spi, f.pi, f.txp, f.ni, f.txfed, f.txfo, f.txt, f.xint, f.xpp, f.xacc,
                    
                    /*CF statement and others*/
                    f.capx, f.oancf, f.dvt, f.ob, f.gdwlia, f.gdwlip, f.gwo, f.mib, f.oiadp, f.ivao, f.ivst,
                    
                    /*assets*/
                    f.rect, f.act, f.che, f.ppegt, f.invt, f.at, f.aco, f.intan, f.ao, f.ppent, f.gdwl, f.fatb, f.fatl,
                    
                    /*liabilities*/
                    f.lct, f.dlc, f.dltt, f.lt, f.dm, f.dcvt, f.cshrc, 
                    f.dcpstk, f.pstk, f.ap, f.lco, f.lo, f.drc, f.drlt, f.txdi, f.dltis, f.dltr, f.dlcch,
                    
                    /*equity and other*/
                    f.ceq, f.scstkc, f.emp, f.csho, f.seq, f.txditc, f.pstkrv, f.pstkl, f.np, f.txdc, 
                    f.dpc, f.ajex, f.tstkp, f.oibdp, f.capxv, f.dvpa, f.epspx,
                    
                    /*market*/
                    abs(f.prcc_f) as prcc_f, abs(f.prcc_c) as prcc_c, f.dvc, f.prstkc, f.sstk, f.fopt, f.wcap
                    
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

# convert datadate to date fmt
comp['datadate'] = pd.to_datetime(comp['datadate'])

# sort and clean up
comp = comp.sort_values(by=['gvkey', 'datadate']).drop_duplicates()

# clean up csho
comp['csho'] = np.where(comp['csho'] == 0, np.nan, comp['csho'])

# calculate Compustat market equity
comp['mve_f'] = comp['csho'] * comp['prcc_f']

# do some clean up. several variables have lots of missing values
condlist = [comp['drc'].notna() & comp['drlt'].notna(),
            comp['drc'].notna() & comp['drlt'].isnull(),
            comp['drlt'].notna() & comp['drc'].isnull()]
choicelist = [comp['drc']+comp['drlt'],
              comp['drc'],
              comp['drlt']]
comp['dr'] = np.select(condlist, choicelist, default=np.nan)

condlist = [comp['dcvt'].isnull() & comp['dcpstk'].notna() & comp['pstk'].notna() & comp['dcpstk'] > comp['pstk'],
            comp['dcvt'].isnull() & comp['dcpstk'].notna() & comp['pstk'].isnull()]
choicelist = [comp['dcpstk']-comp['pstk'],
              comp['dcpstk']]
comp['dc'] = np.select(condlist, choicelist, default=np.nan)
comp['dc'] = np.where(comp['dc'].isnull(), comp['dcvt'], comp['dc'])

comp['xint0'] = np.where(comp['xint'].isnull(), 0, comp['xint'])
comp['xsga0'] = np.where(comp['xsga'].isnull, 0, 0)

comp['ceq'] = np.where(comp['ceq'] == 0, np.nan, comp['ceq'])
comp['at'] = np.where(comp['at'] == 0, np.nan, comp['at'])
comp = comp.dropna(subset=['at'])
print('compustat')
#######################################################################################################################
#                                                       CRSP Block                                                    #
#######################################################################################################################
# Create a CRSP Subsample with Monthly Stock and Event Variables
# Restrictions will be applied later
# Select variables from the CRSP monthly stock and event datasets
crsp = conn.raw_sql("""
                      select a.prc, a.ret, a.retx, a.shrout, a.vol, a.cfacpr, a.cfacshr, a.date, a.permno, a.permco,
                      b.ticker, b.ncusip, b.shrcd, b.exchcd
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date >= '01/01/1959'
                      and b.exchcd between 1 and 3
                      """)

# change variable format to int
crsp[['permco', 'permno', 'shrcd', 'exchcd']] = crsp[['permco', 'permno', 'shrcd', 'exchcd']].astype(int)

# Line up date to be end of month
crsp['date'] = pd.to_datetime(crsp['date'])
crsp['monthend'] = crsp['date'] + MonthEnd(0)  # set all the date to the standard end date of month

crsp = crsp.dropna(subset=['prc'])
crsp['me'] = crsp['prc'].abs() * crsp['shrout']  # calculate market equity 

# if Market Equity is Nan then let return equals to 0
crsp['ret'] = np.where(crsp['me'].isnull(), 0, crsp['ret'])
crsp['retx'] = np.where(crsp['me'].isnull(), 0, crsp['retx'])

# impute me
crsp = crsp.sort_values(by=['permno', 'date']).drop_duplicates()
crsp['me'] = np.where(crsp['permno'] == crsp['permno'].shift(1), crsp['me'].fillna(method='ffill'), crsp['me'])

# Aggregate Market Cap
'''
There are cases when the same firm (permco) has two or more securities (permno) at same date.
For the purpose of ME for the firm, we aggregated all ME for a given permco, date.
This aggregated ME will be assigned to the permno with the largest ME.
'''
# sum of me across different permno belonging to same permco a given date
crsp_summe = crsp.groupby(['monthend', 'permco'])['me'].sum().reset_index()
# largest mktcap within a permco/date
crsp_maxme = crsp.groupby(['monthend', 'permco'])['me'].max().reset_index()
# join by monthend/maxme to find the permno
crsp1 = pd.merge(crsp, crsp_maxme, how='inner', on=['monthend', 'permco', 'me'])
# drop me column and replace with the sum me
crsp1 = crsp1.drop(['me'], axis=1)
# join with sum of me to get the correct market cap info
crsp2 = pd.merge(crsp1, crsp_summe, how='inner', on=['monthend', 'permco'])
# sort by permno and date and also drop duplicates
crsp2 = crsp2.sort_values(by=['permno', 'monthend']).drop_duplicates()
print('crsp')
#######################################################################################################################
#                                                        CCM Block                                                    #
#######################################################################################################################
# merge CRSP and Compustat
# reference: https://wrds-www.wharton.upenn.edu/pages/support/applications/linking-databases/linking-crsp-and-compustat/
ccm = conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim, 
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where substr(linktype,1,1)='L'
                  and (linkprim ='C' or linkprim='P')
                  """)

ccm['linkdt'] = pd.to_datetime(ccm['linkdt'])
ccm['linkenddt'] = pd.to_datetime(ccm['linkenddt'])

# if linkenddt is missing then set to today date
ccm['linkenddt'] = ccm['linkenddt'].fillna(pd.to_datetime('today'))

# merge ccm and comp
ccm1 = pd.merge(comp, ccm, how='left', on=['gvkey'])

# we can only get the accounting data after the firm public their report
# for annual data, we use 5 or 6 months lagged data
ccm1['yearend'] = ccm1['datadate'] + YearEnd(0)
ccm1['jdate'] = ccm1['datadate'] + MonthEnd(4)

# set link date bounds
ccm2 = ccm1[(ccm1['jdate'] >= ccm1['linkdt']) & (ccm1['jdate'] <= ccm1['linkenddt'])]

# link comp and crsp
crsp2 = crsp2.rename(columns={'monthend': 'jdate'})
data_rawa = pd.merge(crsp2, ccm2, how='inner', on=['permno', 'jdate'])

# filter exchcd & shrcd
data_rawa = data_rawa[((data_rawa['exchcd'] == 1) | (data_rawa['exchcd'] == 2) | (data_rawa['exchcd'] == 3)) &
                   ((data_rawa['shrcd'] == 10) | (data_rawa['shrcd'] == 11))]

# process Market Equity
'''
Note: me is CRSP market equity, mve_f is Compustat market equity. Please choose the me below.
'''
data_rawa['me'] = data_rawa['me']/1000  # CRSP ME
# data_rawa['me'] = data_rawa['mve_f']  # Compustat ME

# there are some ME equal to zero since this company do not have price or shares data, we drop these observations
data_rawa['me'] = np.where(data_rawa['me'] == 0, np.nan, data_rawa['me'])
data_rawa = data_rawa.dropna(subset=['me'])

# count single stock years
# data_rawa['count'] = data_rawa.groupby(['gvkey']).cumcount()

# deal with the duplicates
data_rawa.loc[data_rawa.groupby(['datadate', 'permno', 'linkprim'], as_index=False).nth([0]).index, 'temp'] = 1
data_rawa = data_rawa[data_rawa['temp'].notna()]
data_rawa.loc[data_rawa.groupby(['permno', 'yearend', 'datadate'], as_index=False).nth([-1]).index, 'temp'] = 1
data_rawa = data_rawa[data_rawa['temp'].notna()]

data_rawa = data_rawa.sort_values(by=['permno', 'jdate'])
print('ccm')
#######################################################################################################################
#                                                  Annual Variables                                                   #
#######################################################################################################################
# stockholders' equity 
data_rawa['se'] = np.where(data_rawa['seq'].isnull(), data_rawa['ceq']+data_rawa['pstk'], data_rawa['seq'])
data_rawa['se'] = np.where(data_rawa['se'].isnull(), data_rawa['at']-data_rawa['lt'], data_rawa['se'])

data_rawa['txditc'] = data_rawa['txditc'].fillna(0)

# preferrerd stock
data_rawa['ps'] = np.where(data_rawa['pstkrv'].isnull(), data_rawa['pstkl'], data_rawa['pstkrv'])
data_rawa['ps'] = np.where(data_rawa['ps'].isnull(), data_rawa['pstk'], data_rawa['ps'])
data_rawa['ps'] = np.where(data_rawa['ps'].isnull(), 0, data_rawa['ps'])

# book equity
data_rawa['be'] = data_rawa['seq'] + data_rawa['txditc'] - data_rawa['ps']
data_rawa['be'] = np.where(data_rawa['be'] > 0, data_rawa['be'], np.nan)

# acc
data_rawa['act_l1'] = data_rawa.groupby(['permno'])['act'].shift(1)
data_rawa['lct_l1'] = data_rawa.groupby(['permno'])['lct'].shift(1)

condlist = [data_rawa['np'].isnull(),
          data_rawa['act'].isnull() | data_rawa['lct'].isnull()]
choicelist = [((data_rawa['act']-data_rawa['lct'])-(data_rawa['act_l1']-data_rawa['lct_l1'])/(10*data_rawa['be'])),
              (data_rawa['ib']-data_rawa['oancf'])/(10*data_rawa['be'])]
data_rawa['acc'] = np.select(condlist,
                            choicelist,
                            default=((data_rawa['act']-data_rawa['lct']+data_rawa['np'])-
                                     (data_rawa['act_l1']-data_rawa['lct_l1']+data_rawa['np'].shift(1)))/(10*data_rawa['be']))

# agr
data_rawa['at_l1'] = data_rawa.groupby(['permno'])['at'].shift(1)
data_rawa['agr'] = (data_rawa['at']-data_rawa['at_l1'])/data_rawa['at_l1']

# bm
# data_rawa['bm'] = data_rawa['be'] / data_rawa['me']

# cfp
# condlist = [data_rawa['dp'].isnull(),
#             data_rawa['ib'].isnull()]
# choicelist = [data_rawa['ib']/data_rawa['me'],
#               np.nan]
# data_rawa['cfp'] = np.select(condlist, choicelist, default=(data_rawa['ib']+data_rawa['dp'])/data_rawa['me'])

# ep, checked from Hou and change 'ME' from compustat to crsp
#data_rawa['ep'] = data_rawa['ib']/data_rawa['me']
#data_rawa['ep_n'] = data_rawa['ib']

# ni
data_rawa['csho_l1'] = data_rawa.groupby(['permno'])['csho'].shift(1)
data_rawa['ajex_l1'] = data_rawa.groupby(['permno'])['ajex'].shift(1)
data_rawa['ni'] = np.where(data_rawa['gvkey'] != data_rawa['gvkey'].shift(1),
                           np.nan,
                           np.log(data_rawa['csho']*data_rawa['ajex']).replace(-np.inf, 0)-
                           np.log(data_rawa['csho_l1']*data_rawa['ajex_l1']).replace(-np.inf, 0))

# op: the formula seems different from Hou Page 74?
data_rawa['cogs0'] = np.where(data_rawa['cogs'].isnull(), 0, data_rawa['cogs'])
data_rawa['xint0'] = np.where(data_rawa['xint'].isnull(), 0, data_rawa['xint'])
data_rawa['xsga0'] = np.where(data_rawa['xsga'].isnull(), 0, data_rawa['xsga'])

condlist = [data_rawa['revt'].isnull(), data_rawa['be'].isnull()]
choicelist = [np.nan, np.nan]
data_rawa['op'] = np.select(condlist, choicelist,
                          default=(data_rawa['revt'] - data_rawa['cogs0'] - data_rawa['xsga0'] - data_rawa['xint0'])/data_rawa['be'])



# rsup
data_rawa['sale_l1'] = data_rawa.groupby(['permno'])['sale'].shift(1)
# data_rawa['rsup'] = (data_rawa['sale']-data_rawa['sale_l1'])/data_rawa['me']

# cash
data_rawa['cash'] = data_rawa['che']/data_rawa['at']

# lev
# data_rawa['lev'] = data_rawa['lt']/data_rawa['me']

# sp, checked
# data_rawa['sp'] = data_rawa['sale']/data_rawa['me']
#data_rawa['sp_n'] = data_rawa['sale']

# rd_sale
data_rawa['rd_sale'] = data_rawa['xrd']/data_rawa['sale']

# rdm
# data_rawa['rdm'] = data_rawa['xrd']/data_rawa['me']

# adm hxz adm, checked
# data_rawa['adm'] = data_rawa['xad']/data_rawa['me']

# gma
data_rawa['gma'] = (data_rawa['revt']-data_rawa['cogs'])/data_rawa['at_l1']

# chcsho
data_rawa['chcsho'] = (data_rawa['csho']/data_rawa['csho_l1'])-1

# lgr
data_rawa['lt_l1'] = data_rawa.groupby(['permno'])['lt'].shift(1)
data_rawa['lgr'] = (data_rawa['lt']/data_rawa['lt_l1'])-1

# pctacc
data_rawa['che_l1'] = data_rawa.groupby(['permno'])['che'].shift(1)
data_rawa['dlc_l1'] = data_rawa.groupby(['permno'])['dlc'].shift(1)
data_rawa['txp_l1'] = data_rawa.groupby(['permno'])['txp'].shift(1)

condlist = [data_rawa['ib']==0,
            data_rawa['oancf'].isnull(),
            data_rawa['oancf'].isnull() & data_rawa['ib']==0]
choicelist = [(data_rawa['ib']-data_rawa['oancf'])/0.01,
              ((data_rawa['act'] - data_rawa['act_l1']) - (data_rawa['che'] - data_rawa['che_l1']))-
              ((data_rawa['lct'] - data_rawa['lct_l1']) - (data_rawa['dlc']) - data_rawa['dlc_l1']-
               ((data_rawa['txp'] - data_rawa['txp_l1']) - data_rawa['dp']))/data_rawa['ib'].abs(),
              ((data_rawa['act'] - data_rawa['act_l1']) - (data_rawa['che'] - data_rawa['che_l1'])) -
              ((data_rawa['lct'] - data_rawa['lct_l1']) - (data_rawa['dlc']) - data_rawa['dlc_l1'] -
               ((data_rawa['txp'] - data_rawa['txp_l1']) - data_rawa['dp']))]
data_rawa['pctacc'] = np.select(condlist, choicelist, default=(data_rawa['ib']-data_rawa['oancf'])/data_rawa['ib'].abs())

# sgr
data_rawa['sgr'] = (data_rawa['sale']/data_rawa['sale_l1'])-1

# chato
data_rawa['at_l2'] = data_rawa.groupby(['permno'])['at'].shift(2)
data_rawa['chato'] = (data_rawa['sale']/((data_rawa['at']+data_rawa['at_l1'])/2))-\
                     (data_rawa['sale_l1']/((data_rawa['at']+data_rawa['at_l2'])/2))

# chtx
data_rawa['txt_l1'] = data_rawa.groupby(['permno'])['txt'].shift(1)
data_rawa['chtx'] = (data_rawa['txt']-data_rawa['txt_l1'])/data_rawa['at_l1']

# noa,checked
data_rawa['noa'] = ((data_rawa['at']-data_rawa['che']-data_rawa['ivao'].fillna(0))-
                  (data_rawa['at']-data_rawa['dlc'].fillna(0)-data_rawa['dltt'].fillna(0)-data_rawa['mib'].fillna(0)
                   -data_rawa['pstk'].fillna(0)-data_rawa['ceq'])/data_rawa['at_l1'])

# rna
data_rawa['noa_l1'] = data_rawa.groupby(['permno'])['noa'].shift(1)
data_rawa['rna'] = data_rawa['oiadp']/data_rawa['noa_l1']

# pm
data_rawa['pm'] = data_rawa['oiadp']/data_rawa['sale']

# ato
data_rawa['ato'] = data_rawa['sale']/data_rawa['noa_l1']

# depr
data_rawa['depr'] = data_rawa['dp']/data_rawa['ppent']

# invest
data_rawa['ppent_l1'] = data_rawa.groupby(['permno'])['ppent'].shift(1)
data_rawa['invt_l1'] = data_rawa.groupby(['permno'])['invt'].shift(1)

data_rawa['invest'] = np.where(data_rawa['ppegt'].isnull(), ((data_rawa['ppent']-data_rawa['ppent_l1'])+
                                                             (data_rawa['invt']-data_rawa['invt_l1']))/data_rawa['at_l1'],
                             ((data_rawa['ppegt']-data_rawa['ppent_l1'])+(data_rawa['invt']-data_rawa['invt_l1']))/data_rawa['at_l1'])

# egr
data_rawa['ceq_l1'] = data_rawa.groupby(['permno'])['ceq'].shift(1)
data_rawa['egr'] = ((data_rawa['ceq']-data_rawa['ceq_l1'])/data_rawa['ceq_l1'])

# cashdebt
data_rawa['cashdebt'] = (data_rawa['ib']+data_rawa['dp'])/((data_rawa['lt']+data_rawa['lt_l1'])/2)

# rd
# if ((xrd/at)-(lag(xrd/lag(at))))/(lag(xrd/lag(at))) >.05 then rd=1 else rd=0
data_rawa['xrd/at_l1'] = data_rawa['xrd']/data_rawa['at_l1']
data_rawa['xrd/at_l1_l1'] = data_rawa.groupby(['permno'])['xrd/at_l1'].shift(1)
data_rawa['rd'] = np.where(((data_rawa['xrd']/data_rawa['at'])-
                            (data_rawa['xrd/at_l1_l1']))/data_rawa['xrd/at_l1_l1']>0.05, 1, 0)

# roa
data_rawa['roa'] = data_rawa['ni']/((data_rawa['at']+data_rawa['at_l1'])/2)

# roe
data_rawa['roe'] = data_rawa['ib']/data_rawa['ceq_l1']

# dy
# data_rawa['dy'] = data_rawa['dvt']/data_rawa['me']

################## Added on 2020.07.28 ##################

# roic
data_rawa['roic'] = (data_rawa['ebit'] - data_rawa['nopi'])/(data_rawa['ceq'] + data_rawa['lt'] - data_rawa['che'])

# chinv
data_rawa['chinv'] = (data_rawa['invt'] - data_rawa['invt_l1'])/((data_rawa['at'] + data_rawa['at_l2'])/2)

# pchsale_pchinvt
data_rawa['pchsale_pchinvt'] = ((data_rawa['sale'] - data_rawa['sale_l1'])/data_rawa['sale_l1'])\
                               - ((data_rawa['invt']-data_rawa['invt_l1'])/data_rawa['invt_l1'])

# pchsale_pchrect
data_rawa['rect_l1'] = data_rawa.groupby(['permno'])['rect'].shift(1)
data_rawa['pchsale_pchrect'] = ((data_rawa['sale']-data_rawa['sale_l1'])/data_rawa['sale_l1'])\
                               - ((data_rawa['rect']-data_rawa['rect_l1'])/data_rawa['rect_l1'])

# pchgm_pchsale
data_rawa['cogs_l1'] = data_rawa.groupby(['permno'])['cogs'].shift(1)
data_rawa['pchgm_pchsale'] = (((data_rawa['sale']-data_rawa['cogs'])
                               - (data_rawa['sale_l1']-data_rawa['cogs_l1']))/(data_rawa['sale_l1']-data_rawa['cogs_l1']))\
                             - ((data_rawa['sale']-data_rawa['sale_l1'])/data_rawa['sale'])

# pchsale_pchxsga
data_rawa['xsga_l1'] = data_rawa.groupby(['permno'])['xsga'].shift(1)
data_rawa['pchsale_pchxsga'] = ((data_rawa['sale']-data_rawa['sale_l1'])/data_rawa['sale_l1'])\
                               - ((data_rawa['xsga']-data_rawa['xsga_l1'])/data_rawa['xsga_l1'])

# pchdepr
data_rawa['dp_l1'] = data_rawa.groupby(['permno'])['dp'].shift(1)
data_rawa['pchdepr'] = ((data_rawa['dp']/data_rawa['ppent'])-(data_rawa['dp_l1']
                                                              /data_rawa['ppent_l1']))\
                       / (data_rawa['dp_l1']/data_rawa['ppent'])

# chadv
data_rawa['xad_l1'] = data_rawa.groupby(['permno'])['xad'].shift(1)
data_rawa['chadv'] = np.log(data_rawa['xad'] + 1) - np.log(data_rawa['xad_l1'] + 1)

# pchcapx
data_rawa['capx_l1'] = data_rawa.groupby(['permno'])['capx'].shift(1)
data_rawa['pchcapx'] = (data_rawa['capx']-data_rawa['capx_l1'])/data_rawa['capx_l1']

# grcapx
data_rawa['capx_l2'] = data_rawa.groupby(['permno'])['capx'].shift(2)
data_rawa['grcapx'] = (data_rawa['capx']-data_rawa['capx_l2'])/data_rawa['capx_l2']

# grGW
data_rawa['gdwl_l1'] = data_rawa.groupby(['permno'])['gdwl'].shift(1)
data_rawa['grGW'] = (data_rawa['gdwl']-data_rawa['gdwl_l1'])/data_rawa['gdwl']
condlist = [(data_rawa['gdwl']==0) | (data_rawa['gdwl'].isnull()),
            (data_rawa['gdwl'].notna()) & (data_rawa['gdwl'] != 0) & (data_rawa['grGW'].isnull())]
choicelist = [0, 1]
data_rawa['grGW'] = np.select(condlist, choicelist, default=data_rawa['grGW'])

# currat
data_rawa['currat'] = data_rawa['act']/data_rawa['lct']

# pchcurrat
data_rawa['pchcurrat'] = ((data_rawa['act']/data_rawa['lct'])-(data_rawa['act_l1']/data_rawa['lct_l1']))\
                         /(data_rawa['act_l1']/data_rawa['lct_l1'])

# quick
data_rawa['quick'] = (data_rawa['act']-data_rawa['invt'])/data_rawa['lct']

# pchquick
data_rawa['pchquick'] = ((data_rawa['act']-data_rawa['invt'])/data_rawa['lct']
                         -(data_rawa['act_l1']-data_rawa['invt_l1'])/data_rawa['lct_l1'])\
                        /((data_rawa['act_l1']-data_rawa['invt_l1'])/data_rawa['lct_l1'])

# salecash
data_rawa['salecash'] = data_rawa['sale']/data_rawa['che']

# salerec
data_rawa['salerec']= data_rawa['sale']/data_rawa['rect']

# saleinv
data_rawa['saleinv'] = data_rawa['sale']/data_rawa['invt']

# pchsaleinv
data_rawa['pchsaleinv'] = ((data_rawa['sale']/data_rawa['invt'])-(data_rawa['sale_l1']/data_rawa['invt_l1']))\
                          /(data_rawa['sale_l1']/data_rawa['invt_l1'])

# realestate
data_rawa['realestate'] = (data_rawa['fatb']+data_rawa['fatl'])/data_rawa['ppegt']
data_rawa['realestate'] = np.where(data_rawa['ppegt'].isnull(),
                                  (data_rawa['fatb']+data_rawa['fatl'])/data_rawa['ppent'], data_rawa['realestate'])

# obklg
data_rawa['obklg'] = data_rawa['ob']/((data_rawa['at']+data_rawa['at_l1'])/2)

# chobklg
data_rawa['ob_l1'] = data_rawa.groupby(['permno'])['ob'].shift(1)
data_rawa['chobklg'] = (data_rawa['ob'] - data_rawa['ob_l1'])/((data_rawa['at']+data_rawa['at_l1'])/2)

# grltnoa
data_rawa['aco_l1'] = data_rawa.groupby(['permno'])['aco'].shift(1)
data_rawa['intan_l1'] = data_rawa.groupby(['permno'])['intan'].shift(1)
data_rawa['ao_l1'] = data_rawa.groupby(['permno'])['ao'].shift(1)
data_rawa['ap_l1'] = data_rawa.groupby(['permno'])['ap'].shift(1)
data_rawa['lco_l1'] = data_rawa.groupby(['permno'])['lco'].shift(1)
data_rawa['lo_l1'] = data_rawa.groupby(['permno'])['lo'].shift(1)
data_rawa['rect_l1'] = data_rawa.groupby(['permno'])['rect'].shift(1)

data_rawa['grltnoa'] = ((data_rawa['rect']+data_rawa['invt']+data_rawa['ppent']+data_rawa['aco']+data_rawa['intan']+
                       data_rawa['ao']-data_rawa['ap']-data_rawa['lco']-data_rawa['lo'])
                        -(data_rawa['rect_l1']+data_rawa['invt_l1']+data_rawa['ppent_l1']+data_rawa['aco_l1']
                       +data_rawa['intan_l1']+data_rawa['ao_l1']-data_rawa['ap_l1']-data_rawa['lco_l1']
                       -data_rawa['lo_l1'])
                        -(data_rawa['rect']-data_rawa['rect_l1']+data_rawa['invt']-data_rawa['invt_l1']
                          +data_rawa['aco']-data_rawa['aco_l1']
                          -(data_rawa['ap']-data_rawa['ap_l1']+data_rawa['lco']-data_rawa['lco_l1'])-data_rawa['dp']))\
                       /((data_rawa['at']+data_rawa['at_l1'])/2)

# conv
data_rawa['conv'] = data_rawa['dc']/data_rawa['dltt']

# chdrc
data_rawa['dr_l1'] = data_rawa.groupby(['permno'])['dr'].shift(1)
data_rawa['chdrc'] = (data_rawa['dr']-data_rawa['dr_l1'])/((data_rawa['at']+data_rawa['at_l1'])/2)

# rdbias
data_rawa['xrd_l1'] = data_rawa.groupby(['permno'])['xrd'].shift(1)
data_rawa['rdbias'] = (data_rawa['xrd']/data_rawa['xrd_l1'])-1-data_rawa['ib']/data_rawa['ceq_l1']

# operprof
data_rawa['operprof'] = (data_rawa['revt']-data_rawa['cogs']-data_rawa['xsga0']-data_rawa['xint0'])/data_rawa['ceq_l1']

# cfroa
data_rawa['cfroa'] = data_rawa['oancf']/((data_rawa['at']+data_rawa['at_l1'])/2)
data_rawa['cfroa'] = np.where(data_rawa['oancf'].isnull(),
                              (data_rawa['ib'] + data_rawa['dp'])/((data_rawa['at']+data_rawa['at_l1'])/2),
                              data_rawa['cfroa'])

# xrdint
data_rawa['xrdint'] = data_rawa['xrd']/((data_rawa['at']+data_rawa['at_l1'])/2)

# capxint
data_rawa['capxint'] = data_rawa['capx']/((data_rawa['at']+data_rawa['at_l1'])/2)

# xadint
data_rawa['xadint'] = data_rawa['xad']/((data_rawa['at']+data_rawa['at_l1'])/2)

# chpm
data_rawa['ib_l1'] = data_rawa.groupby(['permno'])['ib'].shift(1)
data_rawa['chpm'] = (data_rawa['ib']/data_rawa['sale'])-(data_rawa['ib_l1']/data_rawa['sale_l1'])

# ala
data_rawa['ala'] = data_rawa['che']+0.75*(data_rawa['act']-data_rawa['che'])-\
                   0.5*(data_rawa['at']-data_rawa['act']-data_rawa['gdwl']-data_rawa['intan'])

# alm
data_rawa['alm'] = data_rawa['ala']/(data_rawa['at']+data_rawa['prcc_f']*data_rawa['csho']-data_rawa['ceq'])

# hire
data_rawa['emp_l1'] = data_rawa.groupby(['permno'])['emp'].shift(1)
data_rawa['hire'] = (data_rawa['emp'] - data_rawa['emp_l1'])/data_rawa['emp_l1']
data_rawa['hire'] = np.where((data_rawa['emp'].isnull()) | (data_rawa['emp_l1'].isnull()), 0, data_rawa['hire'])

# herf
data_rawa['sic'] = data_rawa['sic'].astype(int)
data_rawa['ffi49'] = ffi49(data_rawa)
data_rawa['ffi49'] = data_rawa['ffi49'].fillna(49)
data_rawa['ffi49'] = data_rawa['ffi49'].astype(int)
df_temp = data_rawa.groupby(['datadate', 'ffi49'], as_index=False)['sale'].sum()
df_temp = df_temp.rename(columns={'sale': 'indsale'})
data_rawa = pd.merge(data_rawa, df_temp, how='left', on=['datadate', 'ffi49'])
data_rawa['herf'] = (data_rawa['sale']/data_rawa['indsale'])*(data_rawa['sale']/data_rawa['indsale'])
df_temp = data_rawa.groupby(['datadate', 'ffi49'], as_index=False)['herf'].sum()
data_rawa = data_rawa.drop(['herf'], axis=1)
data_rawa = pd.merge(data_rawa, df_temp, how='left', on=['datadate', 'ffi49'])

################################## Added on 2020.10.29 ##################################
# Bmj
data_rawa['be_per'] = data_rawa['be'] / data_rawa['csho']
data_rawa['bmj'] = data_rawa['be_per'] / data_rawa['prc'] 
############### *Q*: used prc as  share price  from crsp ##########

# Cp
data_rawa['cf'] = data_rawa['ib'] + data_rawa['dp']
#data_rawa['cp'] = data_rawa['cf'] / data_rawa['me']

# Dp
###### *Q* difference return with without divident

# Dur
# me = data_rawa['me_comp']


# Ebp
data_rawa['dvpa'] = np.where(data_rawa['dvpa'].isnull(), 0, data_rawa['dvpa'])
data_rawa['tstkp'] = np.where(data_rawa['tstkp'].isnull(), 0, data_rawa['tstkp'])
data_rawa['f_liab'] = data_rawa['dltt'] + data_rawa['dlc'] + data_rawa['pstk'] + data_rawa['dvpa'] - data_rawa['tstkp']
data_rawa['f_asse'] = data_rawa['che']
# net debt : = ﬁnancial liabilities - ﬁnancial assets.
data_rawa['n_debt'] = data_rawa['f_liab'] - data_rawa['f_asse']
data_rawa['ber'] = data_rawa['ceq'] + data_rawa['tstkp'] - data_rawa['dvpa']
#data_rawa['ebp'] = (data_rawa['n_debt']+data_rawa['ber']) / (data_rawa['n_debt']+data_rawa['me'])


# Em
#data_rawa['enteprs_v'] = data_rawa['me'] + data_rawa['dlc'] + data_rawa['dltt'] + data_rawa['pstkrv'] - data_rawa['che']
#data_rawa['em'] = data_rawa['enteprs_v'] / data_rawa['oibdp']

############### Investment ###############
# Aci
data_rawa['ce'] = data_rawa['capx'] / data_rawa['sale']
data_rawa['ce1'] = data_rawa['ce'].shift(1)
data_rawa['ce2'] = data_rawa['ce'].shift(2)
data_rawa['ce3'] = data_rawa['ce'].shift(3)
data_rawa['aci'] = data_rawa['ce']/ (data_rawa['ce1']+data_rawa['ce2']+data_rawa['ce3'])-1

# Cei
#data_rawa['lg_me'] = np.log(data_rawa['me']/data_rawa['me'].shift(6))
#data_rawa['lg_ret'] =  np.log(data_rawa['ret']*data_rawa['ret'].shift(1)*data_rawa['ret'].shift(2)*data_rawa['ret'].shift(3)*data_rawa['ret'].shift(5)*data_rawa['ret'].shift(6)) 
#data_rawa['cei'] = data_rawa['lg_me'] - data_rawa['lg_ret']


# Dac



# dCoa
data_rawa['coa'] = data_rawa['act'] - data_rawa['che']
data_rawa['dcoa'] = (data_rawa['coa']-data_rawa['coa'].shift(1)) / data_rawa['at'].shift(1)


# dBe
data_rawa['dBe'] = (data_rawa['ceq'] - data_rawa['ceq'].shift(1)) / data_rawa['at'].shift(1)


# dFnl & dFin
data_rawa['fna'] = data_rawa['ivst'] + data_rawa['ivao']
data_rawa['fnl'] = data_rawa['dltt'] + data_rawa['dlc'] + data_rawa['pstk']

data_rawa['d_dlc'] = data_rawa['dlc'] - data_rawa['dlc'].shift(1)
data_rawa['d_dlc'] = np.where(data_rawa['d_dlc'].isnull(), 0, data_rawa['d_dlc'])
data_rawa['d_pstk'] = data_rawa['pstk'] - data_rawa['pstk'].shift(1)
data_rawa['d_pstk'] = np.where(data_rawa['d_pstk'].isnull(), 0, data_rawa['d_pstk'])

data_rawa['dfnl'] = (data_rawa['dltt']-data_rawa['dltt'].shift(1)) + data_rawa['d_dlc'] + data_rawa['d_pstk']

data_rawa['d_ivst'] = data_rawa['ivst'] - data_rawa['ivst'].shift(1)
data_rawa['d_ivst'] = np.where(data_rawa['d_ivst'].isnull(), 0, data_rawa['d_ivst'])
data_rawa['d_ivao'] = data_rawa['ivao'] - data_rawa['ivao'].shift(1)
data_rawa['d_ivao'] = np.where(data_rawa['d_ivao'].isnull(), 0, data_rawa['d_ivao'])

data_rawa['dfna'] = data_rawa['d_ivst'] + data_rawa['d_ivao']
data_rawa['dfin'] = data_rawa['dfna'] - data_rawa['dfnl']

data_rawa['dfin'] = data_rawa['dfin'] / data_rawa['at'].shift(1)
data_rawa['dfnl'] = data_rawa['dfnl'] / data_rawa['at'].shift(1)




# dIi
data_rawa['e_invt'] = (data_rawa['capxv'] + data_rawa['capxv'].shift(1))/2
data_rawa['dinvt'] = (data_rawa['capxv'] - data_rawa['e_invt']) / data_rawa['e_invt']

data_rawa['ind'] = data_rawa['capxv']
s = data_rawa.groupby(['jdate', 'sic2'])['ind'].sum()
data_rawa = pd.merge(data_rawa, s, on=['jdate', 'sic2'])
# new industry investment will be named as ind_y, cause it's been grouped by ind
data_rawa['e_ind'] = (data_rawa['ind_y'] + data_rawa['ind_y'].shift(1))/2
data_rawa['dind'] = (data_rawa['ind_y']-data_rawa['e_ind']) / data_rawa['e_ind']
data_rawa['dIi'] = data_rawa['dinvt'] - data_rawa['dind']

# dLno
data_rawa['dlno'] = (data_rawa['ppent']-data_rawa['ppent'].shift(1)) + (data_rawa['intan']-data_rawa['intan'].shift(1)) + (data_rawa['ao']-data_rawa['ao'].shift(1)) - (data_rawa['lo']-data_rawa['lo'].shift(1)) + data_rawa['dp'] 
avg_at = []
for i in range(data_rawa.shape[0]):
  avg_at.append(data_rawa.loc[0:i, 'at'].mean())
data_rawa['avg_at'] = pd.DataFrame(avg_at)
data_rawa['dlno'] = data_rawa['dlno'] / data_rawa['avg_at']


# dNco
data_rawa['nca'] = data_rawa['at'] - data_rawa['act'] - data_rawa['ivao']
data_rawa['ncl'] = data_rawa['lt'] - data_rawa['lct'] - data_rawa['dltt'] 
data_rawa['nco'] = data_rawa['nca'] - data_rawa['ncl']
data_rawa['dnco'] = data_rawa['nco'] - data_rawa['nco'].shift(1)


# dNca
data_rawa['ivao_0'] = np.where(data_rawa['ivao'].isnull(), 0, data_rawa['ivao'])
data_rawa['dltt_0'] = np.where(data_rawa['dltt'].isnull(), 0, data_rawa['dltt'])

data_rawa['nca'] = data_rawa['at'] - data_rawa['act'] - data_rawa['ivao_0']
data_rawa['ncl'] = data_rawa['lt'] - data_rawa['lct'] - data_rawa['dltt_0'] 
data_rawa['nco'] = data_rawa['nca'] - data_rawa['ncl']
data_rawa['dnca'] = data_rawa['nco'] - data_rawa['nco'].shift(1)



# dNoa
data_rawa['dlc_0'] = np.where(data_rawa['dlc'].isnull(), 0, data_rawa['dlc'])
data_rawa['mib_0'] = np.where(data_rawa['mib'].isnull(), 0, data_rawa['mib'])
data_rawa['pstk_0'] = np.where(data_rawa['pstk'].isnull(), 0, data_rawa['pstk'])

data_rawa['op_at'] = data_rawa['at'] - data_rawa['che']
data_rawa['op_lia'] = data_rawa['at'] - data_rawa['dlc_0'] - data_rawa['dltt_0'] - data_rawa['mib_0'] - data_rawa['pstk_0'] - data_rawa['ceq']
data_rawa['net_op'] = data_rawa['op_at'] - data_rawa['op_lia']
data_rawa['dnoa'] = (data_rawa['net_op']-data_rawa['net_op'].shift(1))/ data_rawa['at'].shift(1)


# dPia
data_rawa['c_propty'] = data_rawa['ppegt'] - data_rawa['ppegt'].shift(1)
data_rawa['c_invt'] = data_rawa['invt'] - data_rawa['invt'].shift(1)
data_rawa['dpia'] = (data_rawa['c_propty'] + data_rawa['c_invt']) / data_rawa['at'].shift(1)





######### Profitability ##########
# Ato,repeated
#data_rawa['op_at'] = data_rawa['at'] - data_rawa['che'] - data_rawa['ivao_0']
#data_rawa['op_lia'] = data_rawa['dlc_0'] - data_rawa['dltt_0'] - data_rawa['mib_0'] - data_rawa['pstk_0'] - data_rawa['ceq']
#data_rawa['noa'] = data_rawa['op_at'] - data_rawa['op_lia']
#data_rawa['ato'] = data_rawa['sale'] / data_rawa['noa'].shift(1)


# Cla
data_rawa['d_rect'] = data_rawa['rect'] - data_rawa['rect'].shift(1)
data_rawa['d_invt'] = data_rawa['invt'] - data_rawa['invt'].shift(1)
data_rawa['d_xpp'] = data_rawa['xpp'] - data_rawa['xpp'].shift(1)
data_rawa['d_dr'] = (data_rawa['drc']-data_rawa['drc'].shift(1)) + (data_rawa['drlt']-data_rawa['drlt'].shift(1))
data_rawa['d_ap'] = data_rawa['ap'] - data_rawa['ap'].shift(1)
data_rawa['d_xacc'] = data_rawa['xacc'] - data_rawa['xacc'].shift(1)

data_rawa['xrd_0'] = np.where(data_rawa['xrd'].isnull(), 0, data_rawa['xrd'])
data_rawa['d_rect_0'] = np.where(data_rawa['d_rect'].isnull(), 0, data_rawa['d_rect'])
data_rawa['d_invt_0'] = np.where(data_rawa['d_invt'].isnull(), 0, data_rawa['d_invt'])
data_rawa['d_xpp_0'] = np.where(data_rawa['d_xpp'].isnull(), 0, data_rawa['d_xpp'])
data_rawa['d_dr_0'] = np.where(data_rawa['d_dr'].isnull(), 0, data_rawa['d_dr'])
data_rawa['d_ap_0'] = np.where(data_rawa['d_ap'].isnull(), 0, data_rawa['d_ap'])
data_rawa['d_xacc_0'] = np.where(data_rawa['d_xacc'].isnull(), 0, data_rawa['d_xacc'])

data_rawa['cla'] = data_rawa['revt'] - data_rawa['cogs'] - data_rawa['xsga'] + data_rawa['xrd_0']\
                 - data_rawa['d_rect_0'] - data_rawa['d_invt_0'] - data_rawa['d_xpp_0']\
                 + data_rawa['d_dr_0'] + data_rawa['d_ap_0'] + data_rawa['d_xacc_0']
data_rawa['cla'] = data_rawa['cla'] / data_rawa['at'].shift(1)


# Cop
data_rawa['cop'] = data_rawa['revt'] - data_rawa['cogs'] - data_rawa['xsga'] + data_rawa['xrd_0']\
                 - data_rawa['d_rect_0'] - data_rawa['d_invt_0'] - data_rawa['d_xpp_0']\
                 + data_rawa['d_dr_0'] + data_rawa['d_ap_0'] + data_rawa['d_xacc_0']
data_rawa['cop'] = data_rawa['cop'] / data_rawa['at'] 


# Cto
data_rawa['cto'] = data_rawa['sale'] / data_rawa['at'].shift(1)

#ir
'''
#First calculate r(t-5,t). Then rb(t-5,t) and use Bm to perform linear regression and get residue
'''
#r(t-5,t):sum ret from t-5 to t (which is calendar year t-6 to t-1)
lag = pd.DataFrame()
for i in range(1,6):
        lag['ret%s' % i] = data_rawa.groupby(['permno'])['ret'].shift(i)

data_rawa['ret5'] = lag['ret1']+lag['ret2']+lag['ret3']+lag['ret4']+lag['ret5']

#bm_t-5 (bm of year t-5)
#data_rawa['bm5'] = data_rawa.groupby(['permno'])['bm'].shift(5)

#rB (five year log book return)
#Reference: jf_06 page8 by KENT DANIEL
#data_rawa['rB'] = data_rawa['bm'] - data_rawa['bm5'] + data_rawa['ret5']

#Regression and get ir
#First get unique datelist
#datelist = data_rawa['jdate'].unique()
#for date in datelist:
#    temp = data_rawa['jdate' == date]
#    n_row = temp.shape[0]
#    index = temp.index
#    X = pd.DataFrame()
#    X['bm5'] = temp['bm5']
#    X['rB'] = temp['rB']
#    X['intercept'] = 1
#    X = X[['intercept','rB','bm5']]
#    X = np.mat(X)
#    Y = np.mat(temp[['ret5']])
    #These are residuals on one date
#    res = (np.identity(n_row) - X.dot(X.T.dot(X).I).dot(X.T)).dot(Y)
#    #put residuals back into data_rawa
#    data_rawa.loc[index,'ir'] = res

#nop
#data_rawa['net_p'] = data_rawa['dvc'] + data_rawa['prstkc'] + 2*data_rawa['pstkrv'] - data_rawa['sstk']
#data_rawa['nop'] = data_rawa['net_p'] / data_rawa['me']
#data_rawa['nop'] = np.where(data_rawa['nop']<=0, np.nan, data_rawa['nop'] )

#ocp
#data_rawa['ocy'] = np.where(data_rawa['jdate'] < '1988-06-30', data_rawa['fopt'] - data_rawa['wcap'], data_rawa['fopt'] - data_rawa['oancf'])
#data_rawa['ocp'] = data_rawa['ocy'] / data_rawa['me']
#data_rawa['ocp'] = np.where(data_rawa['ocp']<=0, np.nan, data_rawa['ocp'] )

#dwc
data_rawa['dwc'] = (data_rawa['act'] - data_rawa['che']) - (data_rawa['lct'] - data_rawa['dlc'])
#data_rawa['dwc'] = data_rawa['dwc']/data_rawa['at_l1']

#I/A
data_rawa['ia'] = (data_rawa['at']/data_rawa['at_l1'])-1

#Ig
data_rawa['capx_l1'] = data_rawa.groupby('permno')['capx'].shift(1)
data_rawa['ig'] = data_rawa['capx']/data_rawa['capx_l1']

#2Ig
data_rawa['capx_l2'] = data_rawa.groupby('permno')['capx'].shift(2)
data_rawa['2ig'] = data_rawa['capx']/data_rawa['capx_l2']

#Ivc
data_rawa['atAvg'] = (data_rawa['at']+data_rawa['at_l1'])/2
data_rawa['ivc'] = data_rawa['invt'] / data_rawa['atAvg']

#Ndf
data_rawa['ndf'] = data_rawa['dltis'] - data_rawa['dltr'] + data_rawa['dlcch'] 

#nsi
data_rawa['sps'] = data_rawa['csho'] * data_rawa['ajex']
data_rawa['sps_l1'] = data_rawa.groupby('permno')['sps'].shift(1)
data_rawa['nsi'] = np.log(data_rawa['sps']/data_rawa['sps_l1'])

#oa
data_rawa['txp'] = np.where(data_rawa['txp'].isnull(), 0, data_rawa['txp'])
data_rawa['oa'] = (data_rawa['act'] - data_rawa['che']) - (data_rawa['lct'] - data_rawa['dlc'] - data_rawa['txp']) - data_rawa['dp']

#Poa
data_rawa['poa'] = data_rawa['oa']/data_rawa['ni']

#Ta
data_rawa['ta'] = data_rawa['dwc'] + data_rawa['dnco'] + data_rawa['dfin']

#Ol
data_rawa['ol'] = (data_rawa['cogs'] + data_rawa['xsga'])/data_rawa['at']

#etr
data_rawa['txtpi'] = data_rawa['txt'] / data_rawa['pi']
data_rawa['txtpi_l1'] = data_rawa.groupby('permno')['txtpi'].shift(1)
data_rawa['txtpi_l2'] = data_rawa.groupby('permno')['txtpi'].shift(2)
data_rawa['txtpi_l3'] = data_rawa.groupby('permno')['txtpi'].shift(3)
data_rawa['deps'] = data_rawa['epspx']/(data_rawa['ajex'] * data_rawa['prcc_f'])
data_rawa['etr'] = (data_rawa['txtpi'] - (data_rawa['txtpi_l1'] + data_rawa['txtpi_l2'] + data_rawa['txtpi_l3'])/3) * data_rawa['deps']

print('annual')
#######################################################################################################################
#                                              Compustat Quarterly Raw Info                                           #
#######################################################################################################################
comp = conn.raw_sql("""
                    /*header info*/
                    select c.gvkey, f.cusip, f.datadate, f.fyearq,  substr(c.sic,1,2) as sic2, c.sic, f.fqtr, f.rdq,

                    /*income statement*/
                    f.ibq, f.saleq, f.txtq, f.revtq, f.cogsq, f.xsgaq, f.revty, f.cogsy, f.saley,

                    /*balance sheet items*/
                    f.atq, f.actq, f.cheq, f.lctq, f.dlcq, f.ppentq, f.ppegtq,

                    /*others*/
                    abs(f.prccq) as prccq, abs(f.prccq)*f.cshoq as mveq_f, f.ceqq, f.seqq, f.pstkq, f.ltq,
                    f.pstkrq, f.gdwlq, f.intanq, f.mibq, f.oiadpq, f.ivaoq,
                    
                    /* v3 my formula add*/
                    f.ajexq, f.cshoq, f.txditcq, f.npq, f.xrdy, f.xrdq, f.dpq, f.xintq, f.invtq, f.scstkcy, f.niq,
                    f.oancfy, f.dlttq, f.rectq, f.acoq, f.apq, f.lcoq, f.loq, f.aoq

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

# comp['cusip6'] = comp['cusip'].str.strip().str[0:6]
comp = comp.dropna(subset=['ibq'])

# sort and clean up
comp = comp.sort_values(by=['gvkey', 'datadate']).drop_duplicates()
comp['cshoq'] = np.where(comp['cshoq'] == 0, np.nan, comp['cshoq'])
comp['ceqq'] = np.where(comp['ceqq'] == 0, np.nan, comp['ceqq'])
comp['atq'] = np.where(comp['atq'] == 0, np.nan, comp['atq'])
comp = comp.dropna(subset=['atq'])

# convert datadate to date fmt
comp['datadate'] = pd.to_datetime(comp['datadate'])

# merge ccm and comp
ccm1 = pd.merge(comp, ccm, how='left', on=['gvkey'])
ccm1['yearend'] = ccm1['datadate'] + YearEnd(0)
ccm1['jdate'] = ccm1['datadate'] + MonthEnd(3)  # we change quarterly lag here
# ccm1['jdate'] = ccm1['datadate']+MonthEnd(4)

# set link date bounds
ccm2 = ccm1[(ccm1['jdate'] >= ccm1['linkdt']) & (ccm1['jdate'] <= ccm1['linkenddt'])]

# merge ccm2 and crsp2
# crsp2['jdate'] = crsp2['monthend']
data_rawq = pd.merge(crsp2, ccm2, how='inner', on=['permno', 'jdate'])

# filter exchcd & shrcd
data_rawq = data_rawq[((data_rawq['exchcd'] == 1) | (data_rawq['exchcd'] == 2) | (data_rawq['exchcd'] == 3)) &
                   ((data_rawq['shrcd'] == 10) | (data_rawq['shrcd'] == 11))]

# process Market Equity
'''
Note: me is CRSP market equity, mveq_f is Compustat market equity. Please choose the me below.
'''
data_rawq['me'] = data_rawq['me']/1000  # CRSP ME
# data_rawq['me'] = data_rawq['mveq_f']  # Compustat ME

# there are some ME equal to zero since this company do not have price or shares data, we drop these observations
data_rawq['me'] = np.where(data_rawq['me'] == 0, np.nan, data_rawq['me'])
data_rawq = data_rawq.dropna(subset=['me'])

# count single stock years
# data_rawq['count'] = data_rawq.groupby(['gvkey']).cumcount()

# deal with the duplicates
data_rawq.loc[data_rawq.groupby(['datadate', 'permno', 'linkprim'], as_index=False).nth([0]).index, 'temp'] = 1
data_rawq = data_rawq[data_rawq['temp'].notna()]
data_rawq.loc[data_rawq.groupby(['permno', 'yearend', 'datadate'], as_index=False).nth([-1]).index, 'temp'] = 1
data_rawq = data_rawq[data_rawq['temp'].notna()]

data_rawq = data_rawq.sort_values(by=['permno', 'jdate'])
print('quarterly raw')
#######################################################################################################################
#                                                   Quarterly Variables                                               #
#######################################################################################################################
# prepare be
data_rawq['beq'] = np.where(data_rawq['seqq']>0, data_rawq['seqq']+data_rawq['txditcq']-data_rawq['pstkq'], np.nan)
data_rawq['beq'] = np.where(data_rawq['beq']<=0, np.nan, data_rawq['beq'])

# dy
# data_rawq['me_l1'] = data_rawq.groupby(['permno'])['me'].shift(1)
# data_rawq['retdy'] = data_rawq['ret'] - data_rawq['retx']
# data_rawq['mdivpay'] = data_rawq['retdy']*data_rawq['me_l1']
#
# data_rawq['dy'] = ttm12(series='mdivpay', df=data_rawq)/data_rawq['me']

# chtx
data_rawq['txtq_l4'] = data_rawq.groupby(['permno'])['txtq'].shift(4)
data_rawq['atq_l4'] = data_rawq.groupby(['permno'])['atq'].shift(4)
data_rawq['chtx'] = (data_rawq['txtq']-data_rawq['txtq_l4'])/data_rawq['atq_l4']

# roa
data_rawq['atq_l1'] = data_rawq.groupby(['permno'])['atq'].shift(1)
data_rawq['roa'] = data_rawq['ibq']/data_rawq['atq_l1']

# cash
data_rawq['cash'] = data_rawq['cheq']/data_rawq['atq']

# acc
data_rawq['actq_l4'] = data_rawq.groupby(['permno'])['actq'].shift(4)
data_rawq['lctq_l4'] = data_rawq.groupby(['permno'])['lctq'].shift(4)
data_rawq['npq_l4'] = data_rawq.groupby(['permno'])['npq'].shift(4)
condlist = [data_rawq['npq'].isnull(),
            data_rawq['actq'].isnull() | data_rawq['lctq'].isnull()]
choicelist = [((data_rawq['actq']-data_rawq['lctq'])-(data_rawq['actq_l4']-data_rawq['lctq_l4']))/(10*data_rawq['beq']),
              np.nan]
data_rawq['acc'] = np.select(condlist, choicelist,
                          default=((data_rawq['actq']-data_rawq['lctq']+data_rawq['npq'])-
                                   (data_rawq['actq_l4']-data_rawq['lctq_l4']+data_rawq['npq_l4']))/(10*data_rawq['beq']))

# bm
# data_rawq['bm'] = data_rawq['beq']/data_rawq['me']

# cfp
data_rawq['ibq4'] = ttm4('ibq', data_rawq)
data_rawq['dpq4'] = ttm4('dpq', data_rawq)
# data_rawq['cfp'] = np.where(data_rawq['dpq'].isnull(),
#                             data_rawq['ibq4']/data_rawq['me'],
#                             (data_rawq['ibq4']+data_rawq['dpq4'])/data_rawq['me'])

# ep
# data_rawq['ep'] = data_rawq['ibq4']/data_rawq['me']

# agr
data_rawq['agr'] = (data_rawq['atq']-data_rawq['atq_l4'])/data_rawq['atq_l4']

# ni
data_rawq['cshoq_l4'] = data_rawq.groupby(['permno'])['cshoq'].shift(4)
data_rawq['ajexq_l4'] = data_rawq.groupby(['permno'])['ajexq'].shift(4)
data_rawq['ni'] = np.where(data_rawq['cshoq'].isnull(), np.nan,
                         np.log(data_rawq['cshoq']*data_rawq['ajexq']).replace(-np.inf, 0)-np.log(data_rawq['cshoq_l4']*data_rawq['ajexq_l4']))

# op
data_rawq['xintq0'] = np.where(data_rawq['xintq'].isnull(), 0, data_rawq['xintq'])
data_rawq['xsgaq0'] = np.where(data_rawq['xsgaq'].isnull(), 0, data_rawq['xsgaq'])
data_rawq['beq_l4'] = data_rawq.groupby(['permno'])['beq'].shift(4)

data_rawq['op'] = (ttm4('revtq', data_rawq)-ttm4('cogsq', data_rawq)-ttm4('xsgaq0', data_rawq)-ttm4('xintq0', data_rawq))/data_rawq['beq_l4']

# csho
data_rawq['chcsho'] = (data_rawq['cshoq']/data_rawq['cshoq_l4'])-1

# cashdebt
data_rawq['ltq_l4'] = data_rawq.groupby(['permno'])['ltq'].shift(4)
data_rawq['cashdebt'] = (ttm4('ibq', data_rawq) + ttm4('dpq', data_rawq))/((data_rawq['ltq']+data_rawq['ltq_l4'])/2)

# rd
data_rawq['xrdq4'] = ttm4('xrdq', data_rawq)
data_rawq['xrdq4'] = np.where(data_rawq['xrdq4'].isnull(), data_rawq['xrdy'], data_rawq['xrdq4'])

data_rawq['xrdq4/atq_l4'] = data_rawq['xrdq4']/data_rawq['atq_l4']
data_rawq['xrdq4/atq_l4_l4'] = data_rawq.groupby(['permno'])['xrdq4/atq_l4'].shift(4)
data_rawq['rd'] = np.where(((data_rawq['xrdq4']/data_rawq['atq'])-data_rawq['xrdq4/atq_l4_l4'])/data_rawq['xrdq4/atq_l4_l4']>0.05, 1, 0)

# pctacc
condlist = [data_rawq['npq'].isnull(),
            data_rawq['actq'].isnull() | data_rawq['lctq'].isnull()]
choicelist = [((data_rawq['actq']-data_rawq['lctq'])-(data_rawq['actq_l4']-data_rawq['lctq_l4']))/abs(ttm4('ibq', data_rawq)), np.nan]
data_rawq['pctacc'] = np.select(condlist, choicelist,
                              default=((data_rawq['actq']-data_rawq['lctq']+data_rawq['npq'])-(data_rawq['actq_l4']-data_rawq['lctq_l4']+data_rawq['npq_l4']))/
                                      abs(ttm4('ibq', data_rawq)))

# gma
data_rawq['revtq4'] = ttm4('revtq', data_rawq)
data_rawq['cogsq4'] = ttm4('cogsq', data_rawq)
data_rawq['gma'] = (data_rawq['revtq4']-data_rawq['cogsq4'])/data_rawq['atq_l4']

# lev
# data_rawq['lev'] = data_rawq['ltq']/data_rawq['me']

# rdm
# data_rawq['rdm'] = data_rawq['xrdq4']/data_rawq['me']

# sgr
data_rawq['saleq4'] = ttm4('saleq', data_rawq)
data_rawq['saleq4'] = np.where(data_rawq['saleq4'].isnull(), data_rawq['saley'], data_rawq['saleq4'])

data_rawq['saleq4_l4'] = data_rawq.groupby(['permno'])['saleq4'].shift(4)
data_rawq['sgr'] = (data_rawq['saleq4']/data_rawq['saleq4_l4'])-1

# sp
# data_rawq['sp'] = data_rawq['saleq4']/data_rawq['me']

# invest
data_rawq['ppentq_l4'] = data_rawq.groupby(['permno'])['ppentq'].shift(4)
data_rawq['invtq_l4'] = data_rawq.groupby(['permno'])['invtq'].shift(4)
data_rawq['ppegtq_l4'] = data_rawq.groupby(['permno'])['ppegtq'].shift(4)

data_rawq['invest'] = np.where(data_rawq['ppegtq'].isnull(), ((data_rawq['ppentq']-data_rawq['ppentq_l4'])+
                                                            (data_rawq['invtq']-data_rawq['invtq_l4']))/data_rawq['atq_l4'],
                             ((data_rawq['ppegtq']-data_rawq['ppegtq_l4'])+(data_rawq['invtq']-data_rawq['invtq_l4']))/data_rawq['atq_l4'])

# rd_sale
data_rawq['rd_sale'] = data_rawq['xrdq4']/data_rawq['saleq4']

# lgr
data_rawq['lgr'] = (data_rawq['ltq']/data_rawq['ltq_l4'])-1

# depr
data_rawq['depr'] = ttm4('dpq', data_rawq)/data_rawq['ppentq']

# egr
data_rawq['ceqq_l4'] = data_rawq.groupby(['permno'])['ceqq'].shift(4)
data_rawq['egr'] = (data_rawq['ceqq']-data_rawq['ceqq_l4'])/data_rawq['ceqq_l4']

# chpm
data_rawq['ibq4_l1'] = data_rawq.groupby(['permno'])['ibq4'].shift(1)
data_rawq['saleq4_l1'] = data_rawq.groupby(['permno'])['saleq4'].shift(1)

data_rawq['chpm'] = (data_rawq['ibq4']/data_rawq['saleq4'])-(data_rawq['ibq4_l1']/data_rawq['saleq4_l1'])

# chato
data_rawq['atq_l8'] = data_rawq.groupby(['permno'])['atq'].shift(8)
data_rawq['chato'] = (data_rawq['saleq4']/((data_rawq['atq']+data_rawq['atq_l4'])/2))-(data_rawq['saleq4_l4']/((data_rawq['atq_l4']+data_rawq['atq_l8'])/2))

# noa
data_rawq['ivaoq'] = np.where(data_rawq['ivaoq'].isnull(), 0, 1)
data_rawq['dlcq'] = np.where(data_rawq['dlcq'].isnull(), 0, 1)
data_rawq['dlttq'] = np.where(data_rawq['dlttq'].isnull(), 0, 1)
data_rawq['mibq'] = np.where(data_rawq['mibq'].isnull(), 0, 1)
data_rawq['pstkq'] = np.where(data_rawq['pstkq'].isnull(), 0, 1)
data_rawq['noa'] = (data_rawq['atq']-data_rawq['cheq']-data_rawq['ivaoq'])-\
                 (data_rawq['atq']-data_rawq['dlcq']-data_rawq['dlttq']-data_rawq['mibq']-data_rawq['pstkq']-data_rawq['ceqq'])/data_rawq['atq_l4']

# rna
data_rawq['noa_l4'] = data_rawq.groupby(['permno'])['noa'].shift(4)
data_rawq['rna'] = data_rawq['oiadpq']/data_rawq['noa_l4']

# pm
data_rawq['pm'] = data_rawq['oiadpq']/data_rawq['saleq']

# ato
data_rawq['ato'] = data_rawq['saleq']/data_rawq['noa_l4']

# roe
data_rawq['ceqq_l1'] = data_rawq.groupby(['permno'])['ceqq'].shift(1)
data_rawq['roe'] = data_rawq['ibq']/data_rawq['ceqq_l1']

################################## New Added ##################################

# grltnoa
data_rawq['rectq_l4'] = data_rawq.groupby(['permno'])['rectq'].shift(4)
data_rawq['acoq_l4'] = data_rawq.groupby(['permno'])['acoq'].shift(4)
data_rawq['apq_l4'] = data_rawq.groupby(['permno'])['apq'].shift(4)
data_rawq['lcoq_l4'] = data_rawq.groupby(['permno'])['lcoq'].shift(4)
data_rawq['loq_l4'] = data_rawq.groupby(['permno'])['loq'].shift(4)
data_rawq['invtq_l4'] = data_rawq.groupby(['permno'])['invtq'].shift(4)
data_rawq['ppentq_l4'] = data_rawq.groupby(['permno'])['ppentq'].shift(4)
data_rawq['atq_l4'] = data_rawq.groupby(['permno'])['atq'].shift(4)

data_rawq['grltnoa'] = ((data_rawq['rectq']+data_rawq['invtq']+data_rawq['ppentq']+data_rawq['acoq']+data_rawq['intanq']+
                       data_rawq['aoq']-data_rawq['apq']-data_rawq['lcoq']-data_rawq['loq'])-
                      (data_rawq['rectq_l4']+data_rawq['invtq_l4']+data_rawq['ppentq_l4']+data_rawq['acoq_l4']-data_rawq['apq_l4']-data_rawq['lcoq_l4']-data_rawq['loq_l4'])-\
                     (data_rawq['rectq']-data_rawq['rectq_l4']+data_rawq['invtq']-data_rawq['invtq_l4']+data_rawq['acoq']-
                      (data_rawq['apq']-data_rawq['apq_l4']+data_rawq['lcoq']-data_rawq['lcoq_l4'])-
                      ttm4('dpq', data_rawq)))/((data_rawq['atq']+data_rawq['atq_l4'])/2)

# scal
# condlist = [data_rawq['seqq'].isnull(),
#             data_rawq['seqq'].isnull() & (data_rawq['ceqq'].isnull() | data_rawq['pstk'].isnull())]
# choicelist = [data_rawq['ceqq']+data_rawq['pstk'],
#               data_rawq['atq']-data_rawq['ltq']]
# data_rawq['scal'] = np.select(condlist, choicelist, default=data_rawq['seqq'])

# ala
data_rawq['ala'] = data_rawq['cheq'] + 0.75*(data_rawq['actq']-data_rawq['cheq'])+\
                 0.5*(data_rawq['atq']-data_rawq['actq']-data_rawq['gdwlq']-data_rawq['intanq'])

# alm
# data_rawq['alm'] = data_rawq['ala']/(data_rawq['atq']+data_rawq['me']-data_rawq['ceqq'])

# rsup
data_rawq['saleq_l4'] = data_rawq.groupby(['permno'])['saleq'].shift(4)
# data_rawq['rsup'] = (data_rawq['saleq'] - data_rawq['saleq_l4'])/data_rawq['me']

# stdsacc
data_rawq['actq_l1'] = data_rawq.groupby(['permno'])['actq'].shift(1)
data_rawq['cheq_l1'] = data_rawq.groupby(['permno'])['cheq'].shift(1)
data_rawq['lctq_l1'] = data_rawq.groupby(['permno'])['lctq'].shift(1)
data_rawq['dlcq_l1'] = data_rawq.groupby(['permno'])['dlcq'].shift(1)

data_rawq['sacc'] = ((data_rawq['actq']-data_rawq['actq_l1'] - (data_rawq['cheq']-data_rawq['cheq_l1']))
                     -((data_rawq['lctq']-data_rawq['lctq_l1'])-(data_rawq['dlcq']-data_rawq['dlcq_l1'])))/data_rawq['saleq']
data_rawq['sacc'] = np.where(data_rawq['saleq']<=0, ((data_rawq['actq']-data_rawq['actq_l1'] - (data_rawq['cheq']-data_rawq['cheq_l1']))
                     -((data_rawq['lctq']-data_rawq['lctq_l1'])-(data_rawq['dlcq']-data_rawq['dlcq_l1'])))/0.01, data_rawq['sacc'])


def chars_std(start, end, df, chars):
    """

    :param start: Order of starting lag
    :param end: Order of ending lag
    :param df: Dataframe
    :param chars: lag chars
    :return: std of factor
    """
    lag = pd.DataFrame()
    lag_list = []
    for i in range(start, end):
        lag['chars_l%s' % i] = df.groupby(['permno'])['%s' % chars].shift(i)
        lag_list.append('chars_l%s' % i)
    result = lag[lag_list].std(axis=1)
    return result

data_rawq['stdacc'] = chars_std(0, 16, data_rawq, 'sacc')

# sgrvol
# data_rawq['sgrvol'] = chars_std(0, 15, data_rawq, 'rsup')

# roavol
data_rawq['roavol'] = chars_std(0, 16, data_rawq, 'roa')

# stdcf
data_rawq['scf'] = (data_rawq['ibq']/data_rawq['saleq']) - data_rawq['sacc']
data_rawq['scf'] = np.where(data_rawq['saleq']<=0, (data_rawq['ibq']/0.01) - data_rawq['sacc'], data_rawq['sacc'])

data_rawq['stdcf'] = chars_std(0, 16, data_rawq, 'scf')

# cinvest
data_rawq['ppentq_l1'] = data_rawq.groupby(['permno'])['ppentq'].shift(1)
data_rawq['ppentq_l2'] = data_rawq.groupby(['permno'])['ppentq'].shift(2)
data_rawq['ppentq_l3'] = data_rawq.groupby(['permno'])['ppentq'].shift(3)
data_rawq['ppentq_l4'] = data_rawq.groupby(['permno'])['ppentq'].shift(4)
data_rawq['saleq_l1'] = data_rawq.groupby(['permno'])['saleq'].shift(1)
data_rawq['saleq_l2'] = data_rawq.groupby(['permno'])['saleq'].shift(2)
data_rawq['saleq_l3'] = data_rawq.groupby(['permno'])['saleq'].shift(3)

data_rawq['c_temp1'] = (data_rawq['ppentq_l1'] - data_rawq['ppentq_l2']) / data_rawq['saleq_l1']
data_rawq['c_temp2'] = (data_rawq['ppentq_l2'] - data_rawq['ppentq_l3']) / data_rawq['saleq_l2']
data_rawq['c_temp3'] = (data_rawq['ppentq_l3'] - data_rawq['ppentq_l4']) / data_rawq['saleq_l3']

data_rawq['cinvest'] = ((data_rawq['ppentq'] - data_rawq['ppentq_l1']) / data_rawq['saleq'])\
                       -(data_rawq[['c_temp1', 'c_temp2', 'c_temp3']].mean(axis=1))

data_rawq['c_temp1'] = (data_rawq['ppentq_l1'] - data_rawq['ppentq_l2']) / 0.01
data_rawq['c_temp2'] = (data_rawq['ppentq_l2'] - data_rawq['ppentq_l3']) / 0.01
data_rawq['c_temp3'] = (data_rawq['ppentq_l3'] - data_rawq['ppentq_l4']) / 0.01

data_rawq['cinvest'] = np.where(data_rawq['saleq']<=0, ((data_rawq['ppentq'] - data_rawq['ppentq_l1']) / 0.01)
                                -(data_rawq[['c_temp1', 'c_temp2', 'c_temp3']].mean(axis=1)), data_rawq['cinvest'])

data_rawq = data_rawq.drop(['c_temp1', 'c_temp2', 'c_temp3'], axis=1)

# nincr
data_rawq['ibq_l1'] = data_rawq.groupby(['permno'])['ibq'].shift(1)
data_rawq['ibq_l2'] = data_rawq.groupby(['permno'])['ibq'].shift(2)
data_rawq['ibq_l3'] = data_rawq.groupby(['permno'])['ibq'].shift(3)
data_rawq['ibq_l4'] = data_rawq.groupby(['permno'])['ibq'].shift(4)
data_rawq['ibq_l5'] = data_rawq.groupby(['permno'])['ibq'].shift(5)
data_rawq['ibq_l6'] = data_rawq.groupby(['permno'])['ibq'].shift(6)
data_rawq['ibq_l7'] = data_rawq.groupby(['permno'])['ibq'].shift(7)
data_rawq['ibq_l8'] = data_rawq.groupby(['permno'])['ibq'].shift(8)

data_rawq['nincr_temp1'] = np.where(data_rawq['ibq'] > data_rawq['ibq_l1'], 1, 0)
data_rawq['nincr_temp2'] = np.where(data_rawq['ibq_l1'] > data_rawq['ibq_l2'], 1, 0)
data_rawq['nincr_temp3'] = np.where(data_rawq['ibq_l2'] > data_rawq['ibq_l3'], 1, 0)
data_rawq['nincr_temp4'] = np.where(data_rawq['ibq_l3'] > data_rawq['ibq_l4'], 1, 0)
data_rawq['nincr_temp5'] = np.where(data_rawq['ibq_l4'] > data_rawq['ibq_l5'], 1, 0)
data_rawq['nincr_temp6'] = np.where(data_rawq['ibq_l5'] > data_rawq['ibq_l6'], 1, 0)
data_rawq['nincr_temp7'] = np.where(data_rawq['ibq_l6'] > data_rawq['ibq_l7'], 1, 0)
data_rawq['nincr_temp8'] = np.where(data_rawq['ibq_l7'] > data_rawq['ibq_l8'], 1, 0)

data_rawq['nincr'] = (data_rawq['nincr_temp1']
                      + (data_rawq['nincr_temp1']*data_rawq['nincr_temp2'])
                      + (data_rawq['nincr_temp1']*data_rawq['nincr_temp2']*data_rawq['nincr_temp3'])
                      + (data_rawq['nincr_temp1']*data_rawq['nincr_temp2']*data_rawq['nincr_temp3']*data_rawq['nincr_temp4'])
                      + (data_rawq['nincr_temp1']*data_rawq['nincr_temp2']*data_rawq['nincr_temp3']*data_rawq['nincr_temp4']*data_rawq['nincr_temp5'])
                      + (data_rawq['nincr_temp1']*data_rawq['nincr_temp2']*data_rawq['nincr_temp3']*data_rawq['nincr_temp4']*data_rawq['nincr_temp5']*data_rawq['nincr_temp6'])
                      + (data_rawq['nincr_temp1']*data_rawq['nincr_temp2']*data_rawq['nincr_temp3']*data_rawq['nincr_temp4']*data_rawq['nincr_temp5']*data_rawq['nincr_temp6']*data_rawq['nincr_temp7'])
                      + (data_rawq['nincr_temp1']*data_rawq['nincr_temp2']*data_rawq['nincr_temp3']*data_rawq['nincr_temp4']*data_rawq['nincr_temp5']*data_rawq['nincr_temp6']*data_rawq['nincr_temp7']*data_rawq['nincr_temp8']))

data_rawq = data_rawq.drop(['ibq_l1', 'ibq_l2', 'ibq_l3', 'ibq_l4', 'ibq_l5', 'ibq_l6', 'ibq_l7', 'ibq_l8', 'nincr_temp1',
                            'nincr_temp2', 'nincr_temp3', 'nincr_temp4', 'nincr_temp5', 'nincr_temp6', 'nincr_temp7',
                            'nincr_temp8'], axis=1)

# performance score
data_rawq['niq4'] = ttm4(series='niq', df=data_rawq)
data_rawq['niq4_l4'] = data_rawq.groupby(['permno'])['niq4'].shift(4)
data_rawq['dlttq_l4'] = data_rawq.groupby(['permno'])['dlttq'].shift(4)
data_rawq['p_temp1'] = np.where(data_rawq['niq4']>0, 1, 0)
data_rawq['p_temp2'] = np.where(data_rawq['oancfy']>0, 1, 0)
data_rawq['p_temp3'] = np.where(data_rawq['niq4']/data_rawq['atq']>data_rawq['niq4_l4']/data_rawq['atq_l4'], 1, 0)
data_rawq['p_temp4'] = np.where(data_rawq['oancfy']>data_rawq['niq4'], 1, 0)
data_rawq['p_temp5'] = np.where(data_rawq['dlttq']/data_rawq['atq']<data_rawq['dlttq_l4']/data_rawq['atq_l4'], 1, 0)
data_rawq['p_temp6'] = np.where(data_rawq['actq']/data_rawq['lctq'] > data_rawq['actq_l4']/data_rawq['lctq_l4'], 1, 0)
data_rawq['cogsq4_l4'] = data_rawq.groupby(['permno'])['cogsq4'].shift(4)
data_rawq['p_temp7'] = np.where((data_rawq['saleq4']-data_rawq['cogsq4']/data_rawq['saleq4'])>(data_rawq['saleq4_l4']-data_rawq['cogsq4_l4']/data_rawq['saleq4_l4']), 1, 0)
data_rawq['p_temp8'] = np.where(data_rawq['saleq4']/data_rawq['atq']>data_rawq['saleq4_l4']/data_rawq['atq_l4'], 1, 0)
data_rawq['p_temp9'] = np.where(data_rawq['scstkcy']==0, 1, 0)

data_rawq['pscore'] = data_rawq['p_temp1']+data_rawq['p_temp2']+data_rawq['p_temp3']+data_rawq['p_temp4']\
                      +data_rawq['p_temp5']+data_rawq['p_temp6']+data_rawq['p_temp7']+data_rawq['p_temp8']\
                      +data_rawq['p_temp9']

data_rawq = data_rawq.drop(['p_temp1', 'p_temp2', 'p_temp3', 'p_temp4', 'p_temp5', 'p_temp6', 'p_temp7', 'p_temp8',
                            'p_temp9'], axis=1)

################################## Added on 2020.10.29 ##################################
#Iaq
data_rawq['atqlag'] = ttm4('atq',data_rawq)
data_rawq['iaq'] = (data_rawq['atq']/data_rawq['atqlag'])-1

#Almq
data_rawq['intanq'] = np.where(data_rawq['intanq'].isnull(), 0, data_rawq['intanq'])
data_rawq['qal'] = data_rawq['cheq'] + 0.75*(data_rawq['actq']-data_rawq['cheq']) + 0.5*(data_rawq['atq'] - data_rawq['actq'] - data_rawq['intanq'])
data_rawq['mveqa'] = data_rawq['atq'] + data_rawq['mveq_f'] - data_rawq['ceqq']
data_rawq['mveqa_1'] = data_rawq.groupby(['permno'])['mveqa'].shift(1)
data_rawq['almq'] = data_rawq['qal']/data_rawq['mveqa_1']

#Olq, needs atq
data_rawq['olq'] = (data_rawq['cogsq'] + data_rawq['xsgaq'])/data_rawq['atq']

# rds
data_rawq['rds'] = data_rawq['xrdq4']/data_rawq['saleq']

print('quarterly variables')
#######################################################################################################################
#                                                       Momentum                                                      #
#######################################################################################################################
crsp_mom = conn.raw_sql("""
                        select permno, date, ret, retx, prc, shrout, vol
                        from crsp.msf
                        where date >= '01/01/1959'
                        """)

crsp_mom['permno'] = crsp_mom['permno'].astype(int)
crsp_mom['date'] = pd.to_datetime(crsp_mom['date'])
crsp_mom['jdate'] = pd.to_datetime(crsp_mom['date']) + MonthEnd(0)
crsp_mom = crsp_mom.dropna(subset=['ret', 'retx', 'prc'])

# add delisting return
dlret = conn.raw_sql("""
                     select permno, dlret, dlstdt 
                     from crsp.msedelist
                     """)

dlret.permno = dlret.permno.astype(int)
dlret['dlstdt'] = pd.to_datetime(dlret['dlstdt'])
dlret['jdate'] = dlret['dlstdt'] + MonthEnd(0)

# merge delisting return to crsp return
crsp_mom = pd.merge(crsp_mom, dlret, how='left', on=['permno', 'jdate'])
crsp_mom['dlret'] = crsp_mom['dlret'].fillna(0)
crsp_mom['ret'] = crsp_mom['ret'].fillna(0)
crsp_mom['retadj'] = (1 + crsp_mom['ret']) * (1 + crsp_mom['dlret']) - 1
crsp_mom['me'] = crsp_mom['prc'].abs() * crsp_mom['shrout']  # calculate market equity
crsp_mom['retx'] = np.where(crsp_mom['me'].isnull(), 0, crsp_mom['retx'])
crsp_mom = crsp_mom.drop(['dlret', 'dlstdt'], axis=1)#delete prc,shrout

#Seasonality

#Rla
crsp_mom['rla'] = crsp_mom.groupby(['permno'])['ret'].shift(12)

#Rln
lag = pd.DataFrame()
result = 0
for i in range(1, 12):
    lag['mom%s' % i] = crsp_mom.groupby(['permno'])['ret'].shift(i)
    result = result + lag['mom%s' % i]
crsp_mom['rln'] = result/11

#R[2,5]a
#R[2,5]n
lag = pd.DataFrame()
result = 0
for i in range(13,61):
    lag['mom%s' % i] = crsp_mom.groupby(['permno'])['ret'].shift(i)
    if i not in [24,36,48,60]:
        result = result + lag['mom%s' % i]

crsp_mom['r25a'] = (lag['mom24']+lag['mom36']+lag['mom48']+lag['mom60'])/4
crsp_mom['r25n'] = result/44

#R[6,10]a
#R[6,10]n
lag = pd.DataFrame()
result = 0
for i in range(61,121):
    lag['mom%s' % i] = crsp_mom.groupby(['permno'])['ret'].shift(i)
    if i not in [72,84,96,108,120]:
        result = result + lag['mom%s' % i]

crsp_mom['r610a'] = (lag['mom72']+lag['mom84']+lag['mom96']+lag['mom108']+lag['mom120'])/5
crsp_mom['r610n'] = result/55

#R[11,15]a
lag = pd.DataFrame()
result = 0
for i in [132,144,156,168,180]:
    lag['mom%s' % i] = crsp_mom.groupby(['permno'])['ret'].shift(i)
    result = result + lag['mom%s' % i]
crsp_mom['r1115a'] = result/5

#R[16,20]a
lag = pd.DataFrame()
result = 0
for i in [192,204,216,228,240]:
    lag['mom%s' % i] = crsp_mom.groupby(['permno'])['ret'].shift(i)
    result = result + lag['mom%s' % i]
crsp_mom['r1620a'] = result/5


def mom(start, end, df):
    """
    :param start: Order of starting lag
    :param end: Order of ending lag
    :param df: Dataframe
    :return: Momentum factor
    """
    lag = pd.DataFrame()
    result = 1
    for i in range(start, end):
        lag['mom%s' % i] = df.groupby(['permno'])['ret'].shift(i)
        result = result * (1+lag['mom%s' % i])
    result = result - 1
    return result


crsp_mom['mom60m'] = mom(12, 60, crsp_mom)
crsp_mom['mom12m'] = mom(1, 12, crsp_mom)
crsp_mom['mom1m'] = crsp_mom['ret']
crsp_mom['mom6m'] = mom(1, 6, crsp_mom)
crsp_mom['mom36m'] = mom(1, 36, crsp_mom)
crsp_mom['seas1a'] = crsp_mom.groupby(['permno'])['ret'].shift(11)

crsp_mom['vol_l1'] = crsp_mom.groupby(['permno'])['vol'].shift(1)
crsp_mom['vol_l2'] = crsp_mom.groupby(['permno'])['vol'].shift(2)
crsp_mom['vol_l3'] = crsp_mom.groupby(['permno'])['vol'].shift(3)
crsp_mom['prc_l2'] = crsp_mom.groupby(['permno'])['prc'].shift(2)
crsp_mom['dolvol'] = np.log(crsp_mom['vol_l2']*crsp_mom['prc_l2']).replace([np.inf, -np.inf], np.nan)
crsp_mom['turn'] = ((crsp_mom['vol_l1']+crsp_mom['vol_l2']+crsp_mom['vol_l3'])/3)/crsp_mom['shrout']

# dy
crsp_mom['me_l1'] = crsp_mom.groupby(['permno'])['me'].shift(1)
crsp_mom['retdy'] = crsp_mom['ret'] - crsp_mom['retx']
crsp_mom['mdivpay'] = crsp_mom['retdy']*crsp_mom['me_l1']

crsp_mom['dy'] = ttm12(series='mdivpay', df=crsp_mom)/crsp_mom['me']

# def moms(start, end, df):
#     """
#
#     :param start: Order of starting lag
#     :param end: Order of ending lag
#     :param df: Dataframe
#     :return: Momentum factor
#     """
#     lag = pd.DataFrame()
#     result = 1
#     for i in range(start, end):
#         lag['moms%s' % i] = df.groupby['permno']['ret'].shift(i)
#         result = result + lag['moms%s' % i]
#     result = result/11
#     return result
#
#
# crsp_mom['moms12m'] = moms(1, 12, crsp_mom)

# populate the chars to monthly
print('momentum')
# data_rawa
data_rawa = data_rawa.drop(['date', 'ret', 'retx', 'me'], axis=1)
data_rawa = pd.merge(crsp_mom, data_rawa, how='left', on=['permno', 'jdate'])
data_rawa['datadate'] = data_rawa.groupby(['permno'])['datadate'].fillna(method='ffill')
data_rawa = data_rawa.groupby(['permno', 'datadate'], as_index=False).fillna(method='ffill')
data_rawa = data_rawa[((data_rawa['exchcd'] == 1) | (data_rawa['exchcd'] == 2) | (data_rawa['exchcd'] == 3)) &
                      ((data_rawa['shrcd'] == 10) | (data_rawa['shrcd'] == 11))]
print('data_rawa')
# data_rawq
data_rawq = data_rawq.drop(['date', 'ret', 'retx', 'me'], axis=1)
data_rawq = pd.merge(crsp_mom, data_rawq, how='left', on=['permno', 'jdate'])
data_rawq['datadate'] = data_rawq.groupby(['permno'])['datadate'].fillna(method='ffill')
data_rawq = data_rawq.groupby(['permno', 'datadate'], as_index=False).fillna(method='ffill')
data_rawq = data_rawq[((data_rawq['exchcd'] == 1) | (data_rawq['exchcd'] == 2) | (data_rawq['exchcd'] == 3)) &
                      ((data_rawq['shrcd'] == 10) | (data_rawq['shrcd'] == 11))]
print('data_rawq')
#######################################################################################################################
#                                                    Monthly ME                                                       #
#######################################################################################################################

########################################
#                Annual                #
########################################

# bm
data_rawa['bm'] = data_rawa['be'] / data_rawa['me']
#data_rawa['bm_n'] = data_rawa['be']

# bm_ia
df_temp = data_rawa.groupby(['datadate', 'ffi49'], as_index=False)['bm'].mean()
df_temp = df_temp.rename(columns={'bm': 'bm_ind'})
data_rawa = pd.merge(data_rawa, df_temp, how='left', on=['datadate', 'ffi49'])
data_rawa['bm_ia'] = data_rawa['bm']/data_rawa['bm_ind']

# me_ia
df_temp = data_rawa.groupby(['datadate', 'ffi49'], as_index=False)['me'].mean()
df_temp = df_temp.rename(columns={'me': 'me_ind'})
data_rawa = pd.merge(data_rawa, df_temp, how='left', on=['datadate', 'ffi49'])
data_rawa['me_ia'] = data_rawa['me']/data_rawa['me_ind']

# cfp
condlist = [data_rawa['dp'].isnull(),
            data_rawa['ib'].isnull()]
choicelist = [data_rawa['ib']/data_rawa['me'],
              np.nan]
data_rawa['cfp'] = np.select(condlist, choicelist, default=(data_rawa['ib']+data_rawa['dp'])/data_rawa['me'])

# ep, checked from Hou and change 'ME' from compustat to crsp，checked
data_rawa['ep'] = data_rawa['ib']/data_rawa['me']
#data_rawa['ep_n'] = data_rawa['ib']

# rsup
# data_rawa['sale_l1'] = data_rawa.groupby(['permno'])['sale'].shift(1)
data_rawa['rsup'] = (data_rawa['sale']-data_rawa['sale_l1'])/data_rawa['me']

# lev
data_rawa['lev'] = data_rawa['lt']/data_rawa['me']

# sp, checked
data_rawa['sp'] = data_rawa['sale']/data_rawa['me']
#data_rawa['sp_n'] = data_rawa['sale']

# rdm
data_rawa['rdm'] = data_rawa['xrd']/data_rawa['me']

# adm hxz adm,checked
data_rawa['adm'] = data_rawa['xad']/data_rawa['me']

# dy
data_rawa['dy'] = data_rawa['dvt']/data_rawa['me']

# Cp
#data_rawa['cf'] = data_rawa['ib'] + data_rawa['dp']
data_rawa['cp'] = data_rawa['cf'] / data_rawa['me']

# Ebp
#data_rawa['dvpa'] = np.where(data_rawa['dvpa'].isnull(), 0, data_rawa['dvpa'])
#data_rawa['tstkp'] = np.where(data_rawa['tstkp'].isnull(), 0, data_rawa['tstkp'])
#data_rawa['f_liab'] = data_rawa['dltt'] + data_rawa['dlc'] + data_rawa['pstk'] + data_rawa['dvpa'] - data_rawa['tstkp']
#data_rawa['f_asse'] = data_rawa['che']
# net debt : = ﬁnancial liabilities - ﬁnancial assets.
#data_rawa['n_debt'] = data_rawa['f_liab'] - data_rawa['f_asse']
#data_rawa['ber'] = data_rawa['ceq'] + data_rawa['tstkp'] - data_rawa['dvpa']
data_rawa['ebp'] = (data_rawa['n_debt']+data_rawa['ber']) / (data_rawa['n_debt']+data_rawa['me'])

# Em
data_rawa['enteprs_v'] = data_rawa['me'] + data_rawa['dlc'] + data_rawa['dltt'] + data_rawa['pstkrv'] - data_rawa['che']
data_rawa['em'] = data_rawa['enteprs_v'] / data_rawa['oibdp']

# Cei
data_rawa['lg_me'] = np.log(data_rawa['me']/data_rawa['me'].shift(6))
data_rawa['lg_ret'] =  np.log(data_rawa['ret']*data_rawa['ret'].shift(1)*data_rawa['ret'].shift(2)*data_rawa['ret'].shift(3)*data_rawa['ret'].shift(5)*data_rawa['ret'].shift(6)) 
data_rawa['cei'] = data_rawa['lg_me'] - data_rawa['lg_ret']

#nop
data_rawa['net_p'] = data_rawa['dvc'] + data_rawa['prstkc'] + 2*data_rawa['pstkrv'] - data_rawa['sstk']
data_rawa['nop'] = data_rawa['net_p'] / data_rawa['me']
data_rawa['nop'] = np.where(data_rawa['nop']<=0, np.nan, data_rawa['nop'] )

#ocp
data_rawa['ocy'] = np.where(data_rawa['jdate'] < '1988-06-30', data_rawa['fopt'] - data_rawa['wcap'], data_rawa['fopt'] - data_rawa['oancf'])
data_rawa['ocp'] = data_rawa['ocy'] / data_rawa['me']
data_rawa['ocp'] = np.where(data_rawa['ocp']<=0, np.nan, data_rawa['ocp'] )

#bm_t-5 (bm of year t-5)
data_rawa['bm5'] = data_rawa.groupby(['permno'])['bm'].shift(5)

#rB (five year log book return)
#Reference: jf_06 page8 by KENT DANIEL
data_rawa['rB'] = data_rawa['bm'] - data_rawa['bm5'] + data_rawa['ret5']

#Regression and get ir
#First get unique datelist
datelist = data_rawa['jdate'].unique()
for date in datelist:
    temp = data_rawa[data_rawa['jdate'] == date]
    n_row = temp.shape[0]
    index = temp.index
    X = pd.DataFrame()
    X['bm5'] = temp['bm5']
    X['rB'] = temp['rB']
    X['intercept'] = 1
    X = X[['intercept','rB','bm5']]
    X = np.mat(X)
    Y = np.mat(temp[['ret5']])
    #These are residuals on one date
    res = (np.identity(n_row) - X.dot(X.T.dot(X).I).dot(X.T)).dot(Y)
    #put residuals back into data_rawa
    data_rawa.loc[index,'ir'] = res

# Annual Accounting Variables
chars_a = data_rawa[['cusip', 'ncusip', 'gvkey', 'permno', 'exchcd', 'shrcd', 'datadate', 'jdate',
                     'sic', 'retadj', 'acc', 'agr', 'bm', 'cfp', 'ep', 'ni', 'op', 'rsup', 'cash', 'chcsho',
                     'rd', 'cashdebt', 'pctacc', 'gma', 'lev', 'rdm', 'adm', 'sgr', 'sp', 'invest', 'roe',
                     'rd_sale', 'lgr', 'roa', 'depr', 'egr', 'chato', 'chtx', 'noa', 'rna', 'pm', 'ato', 'dy',
                     'roic', 'chinv', 'pchsale_pchinvt', 'pchsale_pchrect', 'pchgm_pchsale', 'pchsale_pchxsga',
                     'pchdepr', 'chadv', 'pchcapx', 'grcapx', 'grGW', 'currat', 'pchcurrat', 'quick', 'pchquick',
                     'salecash', 'salerec', 'saleinv', 'pchsaleinv', 'realestate', 'obklg', 'chobklg', 'grltnoa',
                     'conv', 'chdrc', 'rdbias', 'operprof', 'capxint', 'xadint', 'chpm', 'ala', 'alm',
                     'mom1m', 'mom6m', 'mom12m', 'mom60m', 'mom36m', 'seas1a', 'me', 'hire', 'herf', 'bm_ia',
                     'me_ia', 'bmj','cp', 'ebp', 'em', 'dp', 'aci', 'dpia', 'dBe', 'dfnl', 'dfin', 'dcoa',
                     'dlno', 'dnoa', 'cla', 'cop', 'cto', 'dIi', 'dnco', 'dnca', 'ir', 'nop', 'ocp',
                     'ia', 'ig','2ig','ivc','ndf','nsi','oa','poa','ta','ol','etr']]

chars_a.reset_index(drop=True, inplace=True)
print(chars_a)
print('ME annual')
########################################
#               Quarterly              #
########################################
# bm
data_rawq['bm'] = data_rawq['beq']/data_rawq['me']

# cfp
data_rawq['cfp'] = np.where(data_rawq['dpq'].isnull(),
                            data_rawq['ibq4']/data_rawq['me'],
                            (data_rawq['ibq4']+data_rawq['dpq4'])/data_rawq['me'])

# ep
data_rawq['ep'] = data_rawq['ibq4']/data_rawq['me']

# lev
data_rawq['lev'] = data_rawq['ltq']/data_rawq['me']

# rdm
data_rawq['rdm'] = data_rawq['xrdq4']/data_rawq['me']

# sp
data_rawq['sp'] = data_rawq['saleq4']/data_rawq['me']

# alm
data_rawq['alm'] = data_rawq['ala']/(data_rawq['atq']+data_rawq['me']-data_rawq['ceqq'])

# rsup
# data_rawq['saleq_l4'] = data_rawq.groupby(['permno'])['saleq'].shift(4)
data_rawq['rsup'] = (data_rawq['saleq'] - data_rawq['saleq_l4'])/data_rawq['me']

# sgrvol
data_rawq['sgrvol'] = chars_std(0, 15, data_rawq, 'rsup')

# Quarterly Accounting Variables
chars_q = data_rawq[['gvkey', 'permno', 'datadate', 'jdate', 'sic', 'exchcd', 'shrcd','retadj' ,'acc', 'bm', 'cfp',
                     'ep', 'agr', 'ni', 'op', 'cash', 'chcsho', 'rd', 'cashdebt', 'pctacc', 'gma', 'lev',
                     'rdm', 'sgr', 'sp', 'invest', 'rd_sale', 'lgr', 'roa', 'depr', 'egr', 'roe',
                     'chato', 'chpm', 'chtx', 'noa', 'rna', 'pm', 'ato', 'stdcf',
                     'grltnoa', 'ala', 'alm', 'rsup', 'stdacc', 'sgrvol', 'roavol', 'scf', 'cinvest',
                     'mom1m', 'mom6m', 'mom12m', 'mom60m', 'mom36m', 'seas1a', 'me', 'pscore', 'nincr',
                     'turn', 'dolvol', 'iaq', 'almq', 'olq', 'rds']]

chars_q.reset_index(drop=True, inplace=True)
print(chars_q)
print('ME quarterly')
with open('chars_a_60.pkl', 'wb') as f:
    pkl.dump(chars_a, f)
print('pkl a')
with open('chars_q_60.pkl', 'wb') as f:
    pkl.dump(chars_q, f)
print('pkl q')
print('Finished')