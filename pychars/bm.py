# book-to-market ratio, refer to HXZ & financial ratio
global db
###################
# COMP Block      #
###################
# compustat quarterly database
# beq: book equity (quarterly)
compq = db.raw_sql(
    """
    SELECT
        gvkey, datadate, fyr, fyearq, fqtr,
        ATq,
        CEQq, SEQq, TXDITCq, PSTKq
    FROM comp.fundq
    WHERE
        indfmt='INDL' AND
        datafmt='STD' AND
        popsrc='D'    AND
        consol='C'    AND
        datadate BETWEEN '%s' AND '%s'
    ORDER BY
        gvkey, fyr, fyearq, fqtr
    """ \
    % (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    )
# all variables are in lowwer case
compq['beq']=compq['seqq']+compq['txditcq']-compq['pstkq']
compq['beq']=np.where(compq['beq']>0, compq['beq'], np.nan)

###################
# CRSP Block      #
###################
# crsp monthly
# me: market equity (monthly)
# retadj: monthly return adjusted by delist return
crsp_m = db.raw_sql(
    """
    SELECT
        a.permno, a.permco, a.date, b.shrcd, b.exchcd,
        a.ret, a.retx, a.shrout, a.prc
    FROM
        crsp.msf as a
    LEFT JOIN
        crsp.msenames as b
    ON
        a.permno=b.permno AND
        b.namedt<=a.date AND
        a.date<=b.nameendt
    WHERE
        a.date >= '2000-01-01' AND
        b.exchcd BETWEEN 1 and 3 AND
        b.shrcd BETWEEN 10 and 11
    """
    )

# change variable format to int
crsp_m[['permco','permno','shrcd','exchcd']]=crsp_m[['permco','permno','shrcd','exchcd']].astype(int)
# Line up date to be end of month
crsp_m['date']=pd.to_datetime(crsp_m['date'])
# mdate is the month end date (by calendar not by trading date)
crsp_m['mdate']=crsp_m['date']+MonthEnd(0)

# add delisting return
dlret = db.raw_sql(
    """
    SELECT permno, dlret, dlstdt
    FROM crsp.msedelist
    """
    )
dlret.permno=dlret.permno.astype(int)
dlret['dlstdt']=pd.to_datetime(dlret['dlstdt'])
dlret['mdate']=dlret['dlstdt']+MonthEnd(0)

crsp = pd.merge(crsp_m, dlret, how='left',on=['permno','mdate'])
crsp['dlret']=crsp['dlret'].fillna(0)
crsp['ret']=crsp['ret'].fillna(0)
crsp['retadj']=(1+crsp['ret'])*(1+crsp['dlret'])-1
crsp['me']=crsp['prc'].abs()*crsp['shrout'] # calculate market equity
crsp=crsp.drop(['dlret','dlstdt','prc','shrout'], axis=1)
crsp=crsp.sort_values(by=['mdate','permco','me'])

### Aggregate Market Cap ###
# sum of me across different permno belonging to same permco a given date
crsp_summe = crsp.groupby(['mdate','permco'])['me'].sum().reset_index()
# largest mktcap within a permco/date
crsp_maxme = crsp.groupby(['mdate','permco'])['me'].max().reset_index()
# join by jdate/maxme to find the permno
crsp1=pd.merge(crsp, crsp_maxme, how='inner', on=['mdate','permco','me'])
# drop me column and replace with the sum me
crsp1=crsp1.drop(['me'], axis=1)
# join with sum of me to get the correct market cap info
crsp2=pd.merge(crsp1, crsp_summe, how='inner', on=['mdate','permco'])
# sort by permno and date and also drop duplicates
crsp2=crsp2.sort_values(by=['permno','mdate']).drop_duplicates()

#######################
# CCM Block           #
#######################
# link crsp and comp
ccm = db.raw_sql(
    """
    SELECT
        gvkey, lpermno as permno, linktype, linkprim,
        linkdt, linkenddt
    FROM
        crsp.ccmxpf_linktable
    WHERE
        substr(linktype,1,1)='L' AND
        (linkprim ='C' or linkprim='P')
    """
    )

ccm['linkdt']=pd.to_datetime(ccm['linkdt'])
ccm['linkenddt']=pd.to_datetime(ccm['linkenddt'])
# if linkenddt is missing then set to today date
ccm['linkenddt']=ccm['linkenddt'].fillna(pd.to_datetime('today'))

ccm1=pd.merge(compq[['gvkey','datadate','beq']],ccm,how='left',on=['gvkey'])
## jdate A.K.A next year june end date, is for Annual Sorting
## or the first holding date for the sorted portfolio
#ccm1['yearend']=ccm1['datadate']+YearEnd(0)
#ccm1['jdate']=ccm1['yearend']+MonthEnd(6)
# here we use qdate A.K.A quarter end date
# to merge the right me(monthly)
ccm1['qdate']=ccm1['datadate']+MonthEnd(1)

# set link date bounds
ccm2=ccm1[(ccm1['qdate']>=ccm1['linkdt'])&(ccm1['qdate']<=ccm1['linkenddt'])]
ccm2=ccm2[['gvkey','permno','datadate', 'qdate','beq']]

# link comp and crsp
crsp['qdate'] = crsp2['mdate'].copy()
ccmq=pd.merge(crsp2, ccm2, how='inner', on=['permno', 'qdate'])
ccmq['bemeq']=ccmq['beq']*1000/ccmq['me']
