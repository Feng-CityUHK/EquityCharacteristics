/* ********************************************* */
/* ********************************************* */
/* Calculate HXZ Replicating Anormalies          */
/* Revision in analyst forecast                  */
/* ********************************************* */
/* ********************************************* */

/* ********************************************* */
/*       load iclink                             */
/* ********************************************* */
libname chars '/scratch/cityuhk/xinhe/eqchars';
data iclink; set chars.iclink; run;

proc export data = iclink
outfile='iclink.csv' dbms=csv replace; run;

/* ********************************************* */
/*  Merging IBES and CRSP using ICLINK table     */
/*  Merging last month price                     */
/* ********************************************* */
proc sql;
create table IBES_CRSP as select
  a.ticker, a.statpers, a.meanest, a.fpedats, a.anndats_act, a.curr_act, a.fpi,
  c.permno, c.date, c.prc, c.cfacpr
from
  ibes.statsum_epsus as a, work.ICLINK as b, crsp.msf as c
where
  /* merging rules */
  a.ticker=b.ticker and
  b.permno=c.permno and
  intnx('month',a.STATPERS,0,'E') = intnx('month',c.date,1,'E') and
  /* filtering IBES */
  a.statpers<a.ANNDATS_ACT and     /*only keep summarized forecasts prior to earnings annoucement*/
  a.measure='EPS' and
  not missing(a.medest) and
  not missing(a.fpedats) and
  (a.fpedats-a.statpers)>=0 and
  a.CURCODE='USD' and
  a.CURR_ACT='USD' and
  a.FISCALP = 'QTR' and
  a.fpi in ('6','7','8') /* and */
  /* a.ticker in ('AAPL','BABA','GOOG','AMZN') */
order by
  a.ticker, a.fpedats, a.statpers
;
quit;

data ic; set IBES_CRSP; run;  /* a short name */

proc export data = ic
outfile='ic.csv' dbms=csv replace; run;

/* ********************************************* */
/*  Merging last month forecast                  */
/* ********************************************* */

proc sql;
create table ic1 as select
  a.*,
  b.statpers as statpers_last_month,
  b.meanest as meanest_last_month
from
  ic a left join ic b
on
  a.ticker=b.ticker and
  a.permno=b.permno and
  intnx('month',a.statpers,0,'E') = intnx('month',b.statpers,1,'E')
order by
  a.ticker, a.permno, a.fpedats, a.statpers
;
quit;

proc sort data=ic1 nodupkey; by ticker fpedats statpers; run;

proc export data = ic1
outfile='ic1.csv' dbms=csv replace; run;

/* ********************************************* */
/*  Drop empty "last month"                      */
/*  Drop tow far forecasts (larger than 6 month ago) */
/*  calculate HXZ RE                             */
/* ********************************************* */
data ic2; set ic1;
if missing(statpers_last_month) then delete;
/* atmost 6 months */
if intnx('month',statpers,7,'E') <= fpedats then delete;
/* remove the most recent month */
if intnx('month',statpers,0,'E') = intnx('month',fpedats,0,'E') then delete;
prc_adj = prc/cfacpr;
monthly_revision = (meanest - meanest_last_month)/prc_adj;
run;

proc export data = ic2
outfile='ic2.csv' dbms=csv replace; run;

/* ********************************************* */
/*  Count the number of obs for each rdq         */
/* ********************************************* */
/*Count number of estimates reported on primary/diluted basis */
proc sql;
create table ic3 as select
  a.*,
  sum(curr_act='USD') as n_count,
  mean(monthly_revision) as hxz_re
from
  ic2 a
group by
  ticker, fpedats
order by
  ticker,fpedats,statpers
;
quit;

proc export data = ic3
outfile='ic3.csv' dbms=csv replace; run;

/* ********************************************* */
/* retain one obs for each ticker-fpedats        */
/* ********************************************* */

data ic4;
set ic3(drop=STATPERS CURR_ACT FPI DATE PRC CFACPR MEANEST
statpers_last_month	meanest_last_month monthly_revision);
if n_count<4 then delete;
run;

proc sort data=ic4 nodupkey; by ticker fpedats; run;

proc export data = ic4
outfile='ic4.csv' dbms=csv replace; run;

/* ********************************************* */
/*  save re                                      */
/* ********************************************* */
data chars.revision_in_analyst_forecast;
set ic4;
run;
