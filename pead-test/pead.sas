
/* IBES Unadjusted Summary File  */
proc sql;
create table ibessum as select
TICKER,	CUSIP,	OFTIC,	CNAME,	STATPERS,
MEASURE,	FISCALP,	FPI,	ESTFLAG,	CURCODE,
NUMEST,	NUMUP,	NUMDOWN,	MEDEST,	MEANEST,	STDEV,
HIGHEST,	LOWEST,	USFIRM,	FPEDATS,	ACTUAL,
ANNDATS_ACT,	ANNTIMS_ACT,	CURR_ACT
from ibes.statsum_epsus
where
statpers<ANNDATS_ACT /*only keep summarized forecasts prior to earnings annoucement*/
and measure='EPS'
and not missing(medest) and not missing(fpedats)
and (fpedats-statpers)>=0
and CURCODE='USD'
and CURR_ACT='USD'
and fpi in ('6','7','8')
and ticker in ('AAPL','BABA','GOOG','AMZN')
order by ticker, ANNDATS_ACT, fpedats, statpers;
quit;

proc export data = ibessum
outfile='ibessum.csv' dbms=csv replace; run;

/* Prepare Compustat-IBES translation file; */
proc sort data=crsp.msenames(where=(ncusip ne '')) out=names nodupkey;
by permno ncusip;
run;

/* Add current cusip to IBES (IBES cusip is historical); */
proc sql;
create table ibessum2 as select
a.*, substr(compress(b.cusip),1,6) as cusip6
from ibessum a left join names b on
(a.cusip = b.ncusip)
order by ticker, anndats_act, fpedats, statpers
;
quit;

proc export data = ibessum2
outfile='ibessum2.csv' dbms=csv replace; run;

/* merge CRSP price(adjusted) to IBES */
proc sql;
create table ibessum3 as
  select a.*,
  b.prc,
  b.cfacpr,
  b.date as crsp_date,
  b.cusip as crsp_cusip
from
  ibessum2 a left join crsp.msf b
on
  a.cusip=b.cusip and
  intnx('MONTH',a.statpers,-1,'end') =   intnx('MONTH',b.date,0,'end')
order by ticker, anndats_act, fpedats, statpers
;
quit;

data ibessum3;
set ibessum3;
prc_adj = prc/cfacpr;
run;

/* sort */
proc sort data=ibessum3;
by cusip fpedats statpers;
run;

proc export data = ibessum3
outfile='ibessum3.csv' dbms=csv replace; run;

data ibessum4; set ibessum3;
group = catx('-', cusip, fpedats);
run;

proc sort data=ibessum4;
by group statpers;
run;

proc export data = ibessum4
outfile='ibessum4.csv' dbms=csv replace; run;

/* calculate dif */
data ibessum5;
   set ibessum4;
   by group;
   delta_f = dif(meanest);
   if first.group then delta_f=0;
run;

proc export data = ibessum5
outfile='ibessum5.csv' dbms=csv replace; run;
