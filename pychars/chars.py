# Xin He - Nov 28, 2019
import wrds
import pandas as pd
import numpy as np
db = wrds.Connection(wrds_username='xinhe97')

crsp_m = db.raw_sql(
    """
    select
        a.permno, a.permco, a.date, b.shrcd, b.exchcd,
        a.ret, a.retx, a.shrout, a.prc
    from
        crsp.msf as a left join crsp.msenames as b
    on
        a.permno=b.permno and
        b.namedt<=a.date and
        a.date<=b.nameendt
    where
        a.date between '01/01/2018' and '12/31/2018' and
        b.exchcd between 1 and 3
    """)
print(crsp_m.shape)
print(crsp_m.head())

# '''
# https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-python/querying-wrds-data-python/
# '''
# db.raw_sql(
#     '''
#     select
#         a.gvkey, a.datadate, a.tic, a.conm, a.at, a.lt,
#         b.prccm, b.cshoq
#     from
#         comp.funda a join comp.secm b
#     on
#         a.gvkey = b.gvkey and
#         a.datadate = b.datadate
#     where
#         a.tic = 'IBM' and
#         a.datafmt = 'STD' and
#         a.consol = 'C' and
#         a.indfmt = 'INDL'
#     ''')
