'''
see the hist of 'rdq-fdateq', 'rdq-pdateq'
'''
compq = db.raw_sql(
    """
    SELECT
        gvkey, datadate,
        rdq, fdateq, pdateq
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
    % (dt.datetime(2000,1,1).strftime('%Y-%m-%d'), \
       dt.datetime(2018,12,31).strftime('%Y-%m-%d'))
    )

print(compq.shape)
print(compq.head())

compq['fmr'] = compq['fdateq']-compq['rdq']
compq['pmr'] = compq['pdateq']-compq['rdq']

# proportion of no-missing data
compq.notna().sum()/compq.shape[0]*100

# plot histogram of date gaps
plt.figure('figsize=(10,10)')
fmr = pd.Series([ i.days for i in compq['fmr'].dropna()])
sum(fmr>=0)/len(fmr)*100
fmr = fmr[fmr>=0]
# plt.hist(np.log(fmr+1), density=True)
# plt.hist(fmr, density=True)
# plt.hist(fmr, density=True, range=(0,100))
plt.hist(fmr, density=True, bins=50 , range=(0,100))
plt.title('final date - rdq')
plt.savefig('finaldateq-rdq.png')
plt.close()

plt.figure('figsize=(10,10)')
pmr = pd.Series([ i.days for i in compq['pmr'].dropna()])
sum(pmr>=0)/len(pmr)*100
pmr = pmr[pmr>=0]
# plt.hist(np.log(pmr+1), density=True)
# plt.hist(pmr, density=True)
# plt.hist(pmr, density=True, range=(0,100))
plt.hist(pmr, density=True, bins=50 , range=(0,100))
plt.title('preliminary date - rdq')
plt.savefig('preliminarydateq-rdq.png')
plt.close()

# maximum date gap
compq.iloc[compq['fmr'].idxmax(),]
compq.iloc[compq['pmr'].idxmax(),]
