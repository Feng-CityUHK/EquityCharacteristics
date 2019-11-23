/* ********************************************************************************* */
/* ************** W R D S   R E S E A R C H   A P P L I C A T I O N S ************** */
/* ********************************************************************************* */
/* Summary   : Construct Daniel Grinblatt Titman and Wermers(1997) Benchmarks        */
/* Date      : January, 2011                                                         */
/* Author    : Rabih Moussawi and Gjergji Cici                                       */
/* Variables : - BEGDATE: Sample Start Date                                          */
/*             - ENDDATE: Sample End Date                                            */
/* ********************************************************************************* */
 
/* Step 1. Specifying Options */
%let begdate = 01JAN1970;
%let enddate = &sysdate9.;
 
/* Create a CRSP Subsample with Monthly Stock and Event Variables */
/* Restriction on the type of shares (common stocks only) */
%let sfilter = (shrcd in (10,11));
/* Selected variables from the CRSP monthly data file (crsp.msf file) */
%let msfvars = permco prc ret vol shrout cfacpr cfacshr;
%let msevars = ncusip exchcd shrcd siccd ;
/* This procedure creates a Monthly CRSP dataset named "CRSP_M"  */
%crspmerge(s=m,start=&begdate,end=&enddate,sfvars=&msfvars,sevars=&msevars,filters=&sfilter);
 
/* Adjust Share and Price in Monthly Data */
data crsp_m;
set crsp_m;
DATE = INTNX("MONTH",date,0,"E");
P = abs(prc)/cfacpr;
TSO=shrout*cfacshr*1000;
if TSO<=0 then TSO=.;
ME = P*TSO/1000000;
label P = "Price at Period End, Adjusted";
label TSO = "Total Shares Outstanding, Adjusted";
label ME = "Issue-Level Market Capitalization, x$1m";
drop ncusip prc cfacpr shrout shrcd;
format ret percentn8.4 ME P dollar12.3 TSO comma12.;
run;
 
/* Create Total Market Capitalization at the Company Level */
proc sql  undo_policy=none;
create table crsp_m
as select *, sum(me) as me_comp "Company-Level Market Cap, $million" format dollar12.3
from crsp_m
group by permco,date
order by permno,date;
quit;
 
/* Get Book Value of Equity from Compustat to Create B/P Rankings */
data comp1;
set comp.funda (keep=gvkey datadate cusip indfmt datafmt consol popsrc
    SICH SEQ PSTKRV PSTKL PSTK TXDB ITCB);
where indfmt='INDL' and datafmt='STD' and consol='C' and popsrc='D'
  and datadate>="&begdate"d;
  if SEQ>0;                         /* Shareholders' Equity */
  PREF=PSTKRV;                      /* Preferred stock - Redemption Value */
  if missing(pref) then PREF=PSTKL; /* Preferred stock - Liquidating Value */
  if missing(pref) then PREF=PSTK;  /* Preferred stock - Carrying Value, Stock (Capital) - Total */
  BE = sum(SEQ, TXDB, ITCB, -PREF); /* Deferred taxes  and Investment Tax Credit */
  label BE = "Book Value of Equity";
  if BE>=0;
  /* Daniel and Titman (JF 1997):                                                   */
  /* BE = stockholders' equity + deferred taxes + investment tax credit - Preferred Stock */
 label datadate = "Fiscal Year End Date";
 keep gvkey sich datadate BE;
run;
 
/* Add Historical PERMCO identifier */
proc sql;
  create table comp2
  as select a.*, b.lpermco as permco, b.linkprim
  from comp1 as a, crsp.ccmxpf_linktable as b
  where a.gvkey = b.gvkey and
  b.LINKTYPE in ("LU","LC") and
 (b.LINKDT <= a.datadate) and (a.datadate <= b.LINKENDDT or missing(b.LINKENDDT));
quit;
 
/* Sorting into Buckets is done in July of Each Year t               */
/* Additional Requirements:                                          */
/* - Compustat data is available for at least 2 years                */
/* - CRSP data available on FYE of year t-1 and June of year t       */
/* - at least 6 months of returns in CRSP between t-1 and t          */
/* - size weights are constructed using the market value in June     */
/* - B/M Ratio uses the market cap at FYE of the year t-1            */
/* - Momentum factor is the 12 month return with 1 month reversal    */
 
/* Construct Book to Market Ratio Each Fiscal Year End               */
proc sql;
  create table comp3
  as select distinct b.permno,a.gvkey,year(a.datadate) as YEAR,a.datadate,a.linkprim,
  a.BE,b.me,a.sich,b.siccd,a.be/b.me_comp as BM "Book-to-Market Ratio" format comma8.2
  from comp2 as a, crsp_m as b
  where a.permco=b.permco and datadate=intnx("month",date,0,"E")
order by permno,datadate;
quit;
 
/* Use linkprim='P' for selecting just one permno-gvkey combination   */
/* Also, if a company changes its FYE month, choose the last report   */
proc sort data=comp3 nodupkey; by permno year datadate linkprim bm; run;
data comp3;
set comp3;
by permno year datadate;
if last.year;
drop linkprim;
run;
 
/* Industry-Adjust the B/M Ratios using F&F(1997) 48-Industries */
data comp4;
set comp3;
/* First, use historical Compustat SIC Code */
if sich>0 then SIC=sich;
/* Then, if missing, use historical CRSP SIC Code */
else if siccd>0 then sic=siccd;
/* and adjust some SIC code to fit F&F 48 ind delineation */
if SIC in (3990,9995,9997) and siccd>0 and siccd ne SIC then SIC = siccd;
if SIC in (3990,3999) then SIC = 3991;
/* F&F 48 Industry Classification Macro */
%FFI48(sic);
if missing (FFI48) or missing(BM) then delete;
drop sich siccd datadate;
run;
 
 /* Calculate BM Industry Average Each Period */
proc sort data=comp4; by FFI48 year; run;
proc means data = comp4 noprint;
where FFI48>0 and bm>=0;
  by FFI48 year;
  var bm;
  output out = BM_IND (drop=_Type_ _freq_)  mean=bmind;
run;
 
/* Calculate Long-Term Industry BtM Average */
data BM_IND;
  set BM_IND;
  by FFI48 year;
  retain avg n;
  if first.FFI48 then do;
  avg=bmind;
  n=1;
  bmavg=avg;
end;
  else do;
  bmavg=((avg*n)+bmind)/(n+1);
  n+1;
  avg=bmavg;
end;
format bmavg comma8.2;
drop avg n bmind;
run;
 
/* Adjust Firm-Specific BtM with Industry Averages */
proc sql;
create table comp5
as select a.*, (a.bm-b.bmavg) as BM_ADJ "Adjusted Book-to-Market Ratio"
 format comma8.2
from comp4 as a, BM_IND as b
where a.year=b.year and a.FFI48=b.FFI48;
quit;
 
proc printto log=junk; run;
/* Create (12,1) Momentum Factor with at least 6 months of returns */
proc expand data=crsp_m (keep=permno date ret me exchcd) out=sizmom method=none;
by permno;
id date;
convert ret = cret_12m / transformin=(+1) transformout=(MOVPROD 12 -1 trimleft 6);
quit;
proc printto; run;
 
/* Keep Momentum Factor and Size at the End of June - which is the formation date */
data sizmom;
set sizmom;
by permno date;
/* First, add the one month reversal gap */
MOM=lag(cret_12m);
if first.permno then MOM=.;
/* Then, keep Momentum Factor at the End of June */
if month(date)=6;
label MOM="12-Month Momentum Factor with one month reversal";
label date="Formation Date"; format MOM RET percentn8.2;
drop cret_12m; rename me=SIZE;
run;
 
/* Get Size Breakpoints for NYSE firms */
proc sort data=sizmom nodupkey; by date permno; run;
 
proc univariate data=sizmom noprint;
where exchcd=1;
by date;
var size;
output out=NYSE pctlpts = 20 to 80 by 20 pctlpre=dec;
run;
 
/* Add NYSE Size Breakpoints to the Data*/
data sizmom;
merge sizmom NYSE;
by date;
if size>0 and size < dec20 then group = 1;
else if size >= dec20 and size < dec40 then group =2;
else if size >= dec40 and size < dec60 then group =3;
else if size >= dec60 and size < dec80 then group =4;
else if size >= dec80                  then group =5;
drop dec20 dec40 dec60 dec80;
label group = "Size Portfolio Group";
run;
 
/* Adjusted BtM from the calendar year preceding the formation date */
proc sql;
  create table comp6
  as select distinct a.permno, a.gvkey, b.date, b.group, b.size, b.mom, a.year, a.bm_adj
  from comp5 as a, sizmom as b
  where a.permno=b.permno and year(date)=year+1
   and not missing(size+mom+bm_adj+ret);
quit;
 
/* Start the Triple Sort on Size, Book-to-Market, and Momentum */
proc sort data=comp6 out=port1 nodupkey; by date group permno; run;
proc rank data=port1 out=port2 group=5;
  by date group;
  var bm_adj;
  ranks bmr;
run;
proc sort data=port2; by date group bmr; run;
proc rank data=port2 out=port3 group=5;
  by date group bmr;
  var mom;
  ranks momr;
run;
 
/* DGTW_PORT 1 for Bottom Quintile, 5 for Top Quintile */
data port4;
set port3;
bmr=bmr+1;
momr=momr+1;
DGTW_PORT=put(group,1.)||put(bmr,1.)||put(momr,1.);
drop group bmr momr year;
if index(DGTW_PORT, '.') then delete;
label DGTW_PORT="Size, BtM, and Momentum Portfolio Number";
run;
 
/* Use Size in June as Weights in the Value-Weighted Portfolios */
proc sql;
  create table crsp_m1
  as select a.*, b.date as formdate "Formation Date", b.dgtw_port, b.size as sizew
  from crsp_m (keep=permno date ret) as a, port4 as b
  where a.permno=b.permno and intnx('month', b.date,1,'e')<=a.date<=intnx('month', b.date,12,'e');
quit;

/* Calculate Weighted Average Returns */
proc sort data=crsp_m1 nodupkey;  by date dgtw_port permno; run;
proc means data = crsp_m1 noprint;
by date dgtw_port;
where sizew>0;
var ret / weight=sizew ;
output out = dgtw_vwret(drop=_type_ _freq_)  mean= dgtw_vwret;
run;

/* Calculate DGTW Excess Return */
proc sql;
  create table work.dgtw_returns (index=(perm_dat=(permno date)))
  as select a.*,b.DGTW_VWRET format percentn8.4 "DGTW Benchmark Return",
    (a.ret-b.DGTW_VWRET) as DGTW_XRET "DGTW Excess Return" format percentn8.4
  from crsp_m1(drop=sizew) as a left join dgtw_vwret as b
  on a.dgtw_port=b.dgtw_port and a.date=b.date
  order by permno, date;
quit;

proc export data=dgtw_returns
outfile="dgtw-sas-xret.csv" dbms=csv replace; run;

/* House Cleaning */
proc sql;
drop table port1, port2, port3, port4, sizmom,
comp1, comp2, comp3, comp4, comp5, comp6,
crsp_m, crsp_m1, dgtw_vwret, nyse, bm_ind;
quit;
 
/* END */
 
/* Reference: Daniel , Kent , Mark Grinblatt, Sheridan Titman, and Russ Wermers,     */
/*   1997, "Measuring Mutual Fund Performance with Characteristic-Based Benchmarks," */
/*   Journal of Finance , 52, pp. 1035-1058.                                         */
 
/* ********************************************************************************* */
/* *************  Material Copyright Wharton Research Data Services  *************** */
/* ****************************** All Rights Reserved ****************************** */
/* ********************************************************************************* */
