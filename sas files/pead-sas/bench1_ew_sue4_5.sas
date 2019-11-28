
 

	/*PEAD part*/

	/* PART1 data*/
%let bdate=01jan2002;        /*start calendar date of fiscal period end*/
%let edate=31dec2006;        /*end calendar date of fiscal period end  */
       
/*CRSP-IBES link*/
%iclink  (ibesid=ibes.id, crspid=crspq.stocknames, outset=work.iclink);
    
/* Step 1. All companies that were ever included in S&P 500 index as an example  */
/* Linking Compustat GVKEY and IBES Tickers using ICLINK                         */
/* For unmatched GVKEYs, use header IBTIC link in Compustat Security file        */ 
proc sql; create table gvkeys
  as select distinct a.gvkey, b.lpermco as permco, b.lpermno as permno,
  coalesce (b.linkenddt,'31dec9999'd) as linkenddt format date9.,
  coalesce (d.ticker, c.ibtic) as ticker, b.linkdt format date9.
  from comp.idxcst_his a
  left join crsp.ccmxpf_linktable
           (where=(usedflag=1 and linkprim in ('P','C'))) b
  on a.gvkey=b.gvkey
  left join comp.security c
  on a.gvkey=c.gvkey
  left join iclink (where=(score in (0,1))) d
  on b.lpermno=d.permno
 order by gvkey, linkdt, ticker;
quit;
data gvkeys; set gvkeys;
  by gvkey linkdt ticker;
  if last.linkdt ;
run;
    
/* Extract estimates from IBES Unadjusted file and select    */
/* the latest estimate for a firm within broker-analyst group*/
/* "fpi in (6,7)" selects quarterly forecast for the current */
/* and the next fiscal quarter                               */
proc sql;
 create view ibes_temp
   as select a.*, b.permno
   from ibes.detu_epsus  a,
      (select distinct ticker,permno,
       min(linkdt) as mindt,max(linkenddt) as maxdt
       from gvkeys group by ticker, permno) b
   where a.ticker=b.ticker and b.mindt<=a.anndats<=b.maxdt
        and "&bdate"d<=fpedats <="&edate"d and fpi in ('6','7');
    
/*Count number of estimates reported on primary/diluted basis */
  create table ibes
    as select a.*, sum(pdf='P') as p_count, sum(pdf='D') as d_count
    from ibes_temp a
    group by ticker, fpedats
  order by ticker,fpedats,estimator,analys,anndats,revdats,anntims,revtims; 
quit;
    
/* Determine whether most analysts report estimates on primary/diluted basis*/
/* following Livnat and Mendenhall (2006)                                   */       
data ibes; set ibes;
  by ticker fpedats estimator analys;
  if nmiss(p_count, d_count)=0  then do;
  if p_count>d_count then basis='P'; else basis='D'; end;
  if last.analys; /*Keep the latest observation for a given analyst*/
  keep ticker value fpedats anndats revdats estimator
       analys revtims anntims permno basis;
run;
    
/* Link Unadjusted estimates with Unadjusted actuals and CRSP permnos  */
/* Keep only the estimates issued within 90 days before the report date*/
proc sql;
  create table ibes1
  (where=(nmiss(repdats, anndats)=0 and 0<=repdats-anndats<=90))
      as select a.*, b.anndats as repdats, b.value as act 
      from ibes as a left join ibes.actu_epsus as b
      on a.ticker=b.ticker and a.fpedats=b.pends and b.pdicity='QTR';
    
/* select all relevant combinations of Permnos and Date*/
   create table ibes_anndats
      as select distinct permno, anndats
      from ibes1
      union
      select distinct permno, repdats as anndats
      from ibes1;
    
/* Adjust all estimate and earnings announcement dates to the closest    */
/* preceding trading date in CRSP to ensure that adjustment factors wont */
/* be missing after the merge                                            */
   create view tradedates
    as select a.anndats, b.date  format=date9.
    from (select distinct anndats from ibes_anndats
          where not missing(anndats)) a
    left join (select distinct date from crspq.dsi ) b
    on 5>=a.anndats-b.date>=0
    group by a.anndats
    having a.anndats-b.date=min(a.anndats-b.date);
    
/* merge the CRSP adjustment factors for all estimate and report dates   */
    create table ibes_anndats
    as select a.*, c.cfacshr
    from ibes_anndats a left join tradedates b
    on a.anndats=b.anndats
    left join crspq.dsf  (keep=permno date cfacshr) c
    on a.permno=c.permno and b.date=c.date;
    
/* Put the estimate on the same per share basis as */
/* company reported EPS using CRSP Adjustment factors. New_value is the       */
/* estimate adjusted to be on the same basis with reported earnings           */
    create table ibes1  
    as select a.*, (c.cfacshr/b.cfacshr)*a.value as new_value
    from ibes1 a, ibes_anndats b, ibes_anndats c
    where (a.permno=b.permno and a.anndats=b.anndats)
     and (a.permno=c.permno and a.repdats=c.anndats);
quit;
    
/* Sanity check: there should be one most recent estimate for */
/* a given firm-fiscal period end combination                 */
proc sort data=ibes1 nodupkey; by ticker fpedats estimator analys;run; 
           
/* Compute the median forecast based on estimates in the 90 days prior to the EAD*/
proc means data=ibes1 noprint;
   by ticker fpedats; id basis;
   var new_value; id repdats act permno;
   output out= medest (drop=_type_ _freq_)        
   median=medest n=numest mean=mean_sue;
run;
    
/* Extracting Compustat Data and merging it with IBES consensus */
proc sql;
  create table comp
  (keep=gvkey fyearq fqtr conm datadate rdq epsfxq epspxq
        prccq ajexq spiq cshoq prccq ajexq spiq cshoq mcap /*Compustat variables*/
        cshprq cshfdq rdq saleq atq fyr datafqtr          
        permno ticker medest numest repdats act basis mean_sue)     /*CRSP and IBES vars */
as select *, abs((a.cshoq*a.prccq)) as mcap
from comp.fundq
     (where=((not missing(saleq) or atq>0) and nmiss(epsfxq)<10 and nmiss(epspxq)<10 
    and nmiss(rdq)<10 and consol='C' and
    popsrc='D' and indfmt='INDL' and datafmt='STD' and not missing(datafqtr))) a
    inner join
    (select distinct gvkey, ticker, min(linkdt) as mindate,
    max(linkenddt) as maxdate from gvkeys group by gvkey, ticker) b
    on a.gvkey=b.gvkey and b.mindate<=a.datadate<=b.maxdate
    left join medest c
    on b.ticker=c.ticker and put(a.datadate,yymmn6.)=put(c.fpedats,yymmn6.);
quit;
    
/* PART2 SUE */
/* Process Compustat Data on a seasonal year-quarter basis*/
proc sort data=comp nodupkey; by gvkey fqtr fyearq;run;
data sue/view=sue; set comp;
 by gvkey fqtr fyearq;
    if dif(fyearq)=1 then do;
      lagadj=lag(ajexq); lageps_p=lag(epspxq);lageps_d=lag(epsfxq);
      lagshr_p=lag(cshprq);lagshr_d=lag(cshfdq);lagspiq=lag(spiq);
    end;
    if first.gvkey then do;
    lageps_d=.;lagadj=.; lageps_p=.;
    lagshr_p=.;lagshr_d=.;lagspiq=.;end;
    if basis='P' then do;
       actual1=epspxq/ajexq; expected1=lageps_p/lagadj;
       actual2=sum(epspxq,-0.65*spiq/cshprq)/ajexq;
       expected2=sum(lageps_p,-0.65*lagspiq/lagshr_p)/lagadj;end;
    else if basis='D' then do;
        actual1=epsfxq/ajexq; expected1=lageps_d/lagadj;
        actual2=sum(epsfxq,-0.65*spiq/cshfdq)/ajexq;
        expected2=sum(lageps_d,-0.65*lagspiq/lagshr_d)/lagadj;end;
    else do;
        actual1=epspxq/ajexq; expected1=lageps_p/lagadj;
        actual2=sum(epspxq,-0.65*spiq/cshprq)/ajexq;
        expected2=sum(lageps_p,-0.65*lagspiq/lagshr_p)/lagadj;end;
    sue1=(actual1-expected1)/(prccq/ajexq);
    sue2=(actual2-expected2)/(prccq/ajexq);
    sue3=(act-medest)/prccq;
    format sue1 sue2 sue3 percent7.4 rdq date9.;
  label datadate='Calendar date of fiscal period end';
  keep ticker permno gvkey conm fyearq fqtr fyr datadate
       repdats rdq sue1 sue2 sue3 basis
       act medest numest prccq mcap mean_sue;
run;
/* Shifting the announcement date to be the next trading day;     */
/* Defining the day after the following quarterly EA as leadrdq1  */
proc sql;
  create view eads1
     as select a.*, b.date as rdq1 format=date9.
     from (select distinct rdq from comp) a
     left join (select distinct date from crspq.dsi) b
     on 5>=b.date-a.rdq>=0
     group by rdq
     having b.date-a.rdq=min(b.date-a.rdq);
  create table sue_final
     as select a.*, b.rdq1
     label='Adjusted Report Date of Quarterly Earnings'
     from sue a left join eads1 b
     on a.rdq=b.rdq
     order by a.gvkey, a.fyearq desc, a.fqtr desc;
quit;
/* Sanity Check: there should be no duplicates. Descending sort is intentional  */
/* to define the consecutive earnings announcement date                         */
                             
proc sort data=sue_final nodupkey; by gvkey descending fyearq descending fqtr;run;


proc expand data=sue_final out=sue_final1;
convert rdq1=lagrdq1/transformout=(lead1);/*the last consecutive EAD*/
convert rdq1=leadrdq1/transformout=(lag1);/*the next consecutive EAD*/
run;

/* Filter from Livnat & Mendenhall (2006):                                */
/*- earnings announcement date is reported in Compustat                   */
/*- the price per share is available from Compustat at fiscal quarter end */ 
/*- price is greater than $1                                              */
/*- the market (book) equity at fiscal quarter end is available and is    */
/* EADs in Compustat and in IBES (if available)should not differ by more  */
/* than one calendar day larger than $5 mil.                              */

data sue_final1;
   retain gvkey ticker permno conm fyearq fqtr datadate fyr rdq rdq1 lagrdq1 leadrdq1
          repdats mcap medest act numest basis sue1 sue2 sue3 mean_sue;
   set sue_final1; 
   by gvkey descending fyearq descending fqtr;
   if last.gvkey then lagrdq1=intnx('month',rdq1,-3,'sameday'); 
   if lagrdq1=rdq1 then delete;   
   if first.gvkey then leadrdq1=intnx('month',rdq1,3,'sameday'); 
   if leadrdq1=rdq1 then delete;  
   if ((nmiss(sue1,sue2)=0 and missing(repdats)) 
   or (not missing(repdats) and abs(intck('day',repdats,rdq))<=1)); 
   if (not missing(rdq) and prccq>1 and mcap>5.0);
   keep gvkey ticker permno conm fyearq fqtr datadate fyr rdq rdq1 lagrdq1 leadrdq1
          repdats mcap medest act numest basis sue1 sue2 sue3 mean_sue;
   label
      lagrdq1='Lag Adjusted Report Date of Quarterly Earnings'
	  leadrdq1='Lead Adjusted Report Date of Quarterly Earnings'
      basis='Primary/Diluted Basis'
      act='Actual Reported Earnings per Share'
      medest='EPS consensus forecast (median)'
      ticker='Historical IBES Ticker'
      sue1='Earnings Surprise (Seasonal Random Walk)'
      sue2='Earnings Surprise (Excluding Special items)'
      sue3='Earnings Surprise (Analyst Forecast-based)'
      numest='Number of analyst forecasts used in Analyst-based SUE';
   format rdq1 lagrdq1 leadrdq1 date9.; 
run; 




/*NYSE part*/
/*NYSE part for sizeport*/
%let uni_begdt = 01jan1995;
%let uni_enddt = 31dec2007;

/* libname comp '/wrds/comp/sasdata/d_na'; */
libname crsp ('/wrds/crsp/sasdata/a_stock' '/wrds/crsp/sasdata/a_ccm');


/************************ Part 1: Compustat ****************************/
/* Compustat XpressFeed Variables:                                     */
/* AT      = Total Assets                                              */
/* PSTKL   = Preferred Stock Liquidating Value                         */
/* TXDITC  = Deferred Taxes and Investment Tax Credit                  */
/* PSTKRV  = Preferred Stock Redemption Value                          */
/* SEQ     = Total Parent Stockholders' Equity                         */
/* PSTK    = Preferred/Preference Stock (Capital) - Total              */

/* In calculating Book Equity, incorporate Preferred Stock (PS) values */
/*  use the redemption value of PS, or the liquidation value           */
/*    or the par value (in that order) (FF,JFE, 1993, p. 8)            */
/* USe Balance Sheet Deferred Taxes TXDITC if available                */
/* Flag for number of years in Compustat (<2 likely backfilled data)   */

%let vars = AT PSTKL TXDITC PSTKRV SEQ PSTK ;
data comp;
  set comp.funda
  (keep= gvkey datadate &vars indfmt datafmt popsrc consol);
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and datadate >='01Jan1950'd;
  PS = coalesce(PSTKRV,PSTKL,PSTK,0);
  if missing(TXDITC) then TXDITC = 0 ;
  BE = SEQ + TXDITC - PS ;
  if BE<0 then BE=.;
  year = year(datadate);
  label BE='Book Value of Equity FYear t-1' ;
  drop indfmt datafmt popsrc consol ps &vars;
  retain count;
  if first.gvkey then count=1;
  else count = count+1;
run;

/************************ Part 2: CRSP **********************************/
/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;

%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01jan1962,end=31dec2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3) and shrcd in (10,11));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from Crsp_m a left join crsp.msedelist(where=(missing(dlret)=0)) b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DLSTDT,0,'E')
 order by a.date, a.permco, MEq;
quit;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */
data crspm2a (drop = Meq); set crspm2;
  by date permco Meq;
  retain ME;
  if first.permco and last.permco then do;
    ME=meq;
  output; /* most common case where a firm has a unique permno*/
  end;
  else do ;
    if  first.permco then ME=meq;
    else ME=sum(meq,ME);
    if last.permco then output;
  end;
run;

/* There should be no duplicates*/
proc sort data=crspm2a nodupkey; by permno date;run;



proc sql;
create table crspdec as
select * from crspm2a
where month(date)=12;
quit;


/***************   Part 3: Merging CRSP and Compustat ***********/
/* Add Permno to Compustat sample */
proc sql;
  create table ccm1 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('year',a.datadate,0,'E') >= b.linkdt)
   and (b.linkenddt >= intnx('year',a.datadate,0,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;
quit;

/*  Cleaning Compustat Data for no relevant duplicates                      */
/*  Eliminating overlapping matching : few cases where different gvkeys     */
/*  for same permno-date --- some of them are not 'primary' matches in CCM  */
/*  Use linkprim='P' for selecting just one gvkey-permno-date combination   */
data ccm1a; set ccm1;
  by datadate permno descending linkprim;
  if first.permno;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm1a nodupkey; by permno year datadate; run;

/* 2. However, there other type of duplicates within the year                */
/* Some companiess change fiscal year end in the middle of the calendar year */
/* In these cases, there are more than one annual record for accounting data */
/* We will be selecting the last annual record in a given calendar year      */
data ccm2a ; set ccm1a;
  by permno year datadate;
  if last.year;
run;

/* Sanity Check -- No Duplicates */
proc sort data=ccm2a nodupkey; by permno datadate; run;



/***************   Part 3B: Lock 1982 stock universe ***********/
/* Add Permno to Compustat sample */

data ccm2a1982;
set ccm2a;
where year=1982;
run;



data ccm2a1982_stocks;
set ccm2a1982;
keep gvkey permno count base_year;
base_year=1;
run;



/* **** ccm with base_year label **** */
proc sql;
create table ccm3a as
select a.*, b.base_year
from
ccm2a a left join ccm2a1982_stocks b
on a.permno=b.permno and a.gvkey=b.gvkey
order by permno, datadate;
quit;

data ccm3a; set ccm3a;
if missing(base_year) then base_year=0;
run;



/* Finalize Compustat Sample                              */
/* Merge CRSP with Compustat data, at Dec of every year  */
proc sql; create table ccm3_dec as
  select a.*, b.BE, (1000*b.BE)/a.ME as BEME, b.count,
  b.datadate, b.base_year,
  intck('month',b.datadate, a.date) as dist
  from crspdec a, ccm3a b
  where a.permno=b.permno and intnx('month',a.date,0,'E')=
  intnx('year',b.datadate,0,'E')
  order by a.date;
quit;



/************************ Part 4: Size and Book to Market Portfolios ***/
/* Forming Portolio by ME and BEME as of each Dec                      */
/* Calculate NYSE Breakpoints for Market Equity (ME) and               */

proc univariate data=ccm3_dec noprint;
  where exchcd=1 and
  shrcd in (10,11) and me>0 and count>=2 ;
  var ME ;
  by date;
  output out=nyse_breaks median = SIZEMEDN pctlpre=ME pctlpts=10 20 30 40 50 60 70 80 90;
run;

/* Use Breakpoints to classify stock only at end of all Dec's */
proc sql;
  create table ccm4_dec as
  select a.*, b.me10, b.me20, b.me30, b.me40, b.me50, b.me60, b.me70, b.me80, b.me90
  from ccm3_dec as a, nyse_breaks as b
  where a.date=b.date;
quit;


data dec; set ccm4_dec;
/* If beme>0 and me>0 and count>=2 then do; */
If me>5000 and count>=2 then do;
 positivebeme=1;

 if 5000 <= me <= me10 then        sizeport = '0' ;
 else if me10 < me <= me20 then sizeport = '1' ;
 else if me20 < me <= me30 then sizeport = '2' ;
 else if me30 < me <= me40 then sizeport = '3' ;
 else if me40 < me <= me50 then sizeport = '4' ;
 else if me50 < me <= me60 then sizeport = '5' ;
 else if me60 < me <= me70 then sizeport = '6' ;
 else if me70 < me <= me80 then sizeport = '7' ;
 else if me80 < me <= me90 then sizeport = '8' ;
 else if me  > me90 then        sizeport = '9' ;
 else sizeport='';
 end;
else positivebeme=0;
if cmiss(sizeport)=0 then nonmissport=1; else nonmissport=0;
keep permno date sizeport positivebeme exchcd shrcd nonmissport base_year;
run;


data mydec; set dec;
datesort=date;
year=YEAR(date)+1;
if nmiss(sizeport)=0;
keep permno year sizeport datesort base_year;
format datesort date9.;
run;

/* *** 1 *** */
%crspmerge(s=d,start=01jan1962,end=31dec2018,
	sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3) and shrcd in (10,11));
proc sql; create table mydaily1
	as select a.*, b.dlret,
	  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
	  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
	 from Crsp_d a left join crsp.dsedelist(where=(missing(dlret)=0)) b
	 on a.permno=b.permno and a.date=b.DLSTDT
	 order by a.date, a.permco, MEq;
quit;

proc sql;
create table mydaily
as select a.permno, a.date, a.ret,a.prc, b.*
from
mydaily1 (where=((not missing(ret)) and ("&uni_begdt"d<=date<="&uni_enddt"d) and (-1<=ret))) a
inner join
mydec b
on a.permno=b.permno and YEAR(a.date)=b.year
order by a.permno,a.date;
quit;

/************************   ****************************/
/*** set two mydaily   ****************************/
/*** but only use 1982 conditional mydaily ****************************/


/* all firm */
data mydaily_all;
set mydaily;
if nmiss(ret)=0;
run;

/* 1982 condition */
/*data mydaily_cond1982;
set mydaily(where=(base_year=1)); 
if nmiss(ret)=0;
run;*/


/* 2 calculating benchmark 1 ewret */
proc sort data=mydaily_all nodupkey; by date sizeport permno;run;
proc means data=mydaily_all noprint;
  by date;
  var ret;
  output out=ewret1_daily (drop=_type_ _freq_)
  median=ewret_median mean=ewret_mean n=n_firms;
run;

/*2 calculating benchmark 10 ew version */
proc sort data=mydaily_all nodupkey; by date sizeport permno;run;
proc means data=mydaily_all noprint;
  by date sizeport;
  var ret;
  output out=ewret10_daily (drop=_type_ _freq_)
  median=ewret_median mean=ewret_mean n=n_firms;
run;


/*3*/

proc sql;
create table abret
    as select a.*, b.ewret_mean,(a.ret-b.ewret_mean) as abret
    from mydaily_all a
    left join ewret1_daily b
    on a.date=b.date;
	quit;
 
/*4*/
/*merge*/

proc sql; 
   create view crsprets 
   as select a.*,
             b.rdq1, b.leadrdq1, b.sue1, b.sue2, b.sue3, b.lagrdq1,b.mean_sue,b.act
   from abret a inner join
   sue_final1 (where=(nmiss(rdq, leadrdq1, permno)=0 and leadrdq1-lagrdq1>110)) b 
   on a.permno=b.permno and b.lagrdq1-5<=a.date<=b.leadrdq1+5
      order by a.permno, b.rdq1, a.date;
quit;



/*count*/
data temp0; set crsprets;
  by permno rdq1 date;
  if date=rdq1 then c_1=0;
  else if date>rdq1 then c_1=1;
  else if date<rdq1 then c_1=(-1);
  format date date9. abret percent7.4;
run;
data temp_1; set temp0;
if c_1=-1;
run;
proc sort data=temp_1;
by permno rdq1 descending date;
run;

data temp1;set temp_1;
by permno rdq1;
if first.rdq1 then count=c_1;
else count + c_1;
run;

data temp_2; set temp0;
if c_1>=0;
run;
proc sort data=temp_2;
by permno rdq1 date;
run;

data temp2;set temp_2;
by permno rdq1;
if first.rdq1 then count=c_1;
else count + c_1;
run;

proc sql;
create table temp3
as select * from temp1
union
select * from temp2;
quit;
proc sort data=temp3; by permno rdq1 count;run;

proc sql;
create table temp4_1 as select
a.*,b.prc as prc1 from temp3 (where=(count=0)) a left join temp3 b
on a.permno=b.permno and a.rdq1=b.rdq1  and b.count=-10;
create table temp4_2 as select
a.*,b.prc as prc2 from temp4_1 a left join temp3 b
on a.permno=b.permno and a.rdq1=b.rdq1  and b.count=-9;
create table temp4_3 as select
a.*,b.prc as prc3 from temp4_2 a left join temp3 b
on a.permno=b.permno and a.rdq1=b.rdq1  and b.count=-8;
create table temp4_4 as select
a.*,b.prc as prc4 from temp4_3 a left join temp3 b
on a.permno=b.permno and a.rdq1=b.rdq1  and b.count=-7;
create table temp4_5 as select
a.*,b.prc as prc5 from temp4_4 a left join temp3 b
on a.permno=b.permno and a.rdq1=b.rdq1  and b.count=-6;
quit;

data temp5;
set temp4_5;
prc0=prc1;
if prc0=. then prc0=prc2;
if prc2=. then prc0=prc3;
if prc3=. then prc0=prc4;
if prc4=. then prc0=prc5;
sue4=(act-mean_sue)/prc0;
format sue4 percent7.4;
run;

proc sql;
create table temp as select
a.*,b.prc1,b.sue4  from temp3 a left join temp5 b
on a.permno=b.permno and a.rdq1=b.rdq1  ;
quit;

proc sort data=temp; by permno rdq1 count;run;

/* PART 3 sorting*/
proc sort data=temp out=peadrets nodupkey; by count permno rdq1;run;
proc rank data=peadrets out=peadrets groups=5;
  by count; var sue1 sue2 sue3 sue4;
  ranks sue1r sue2r sue3r sue4r;
run;
/*form portfolios on Compustat-based SUEs (=sue1 or =sue2) or IBES-based SUE (=sue3)*/
%let sue=sue4;
proc sort data=peadrets (where=(not missing(&sue))) out=pead&sue; by count &sue.r;run;
    
proc means data=pead&sue noprint;
  by count &sue.r;
  var abret; 
  output out=pead&sue.port mean=abret_mean;
run;
proc transpose data=pead&sue.port out=pead&sue.port;
  by count; id &sue.r;
  var abret_mean;
run;
    
data pead&sue.port; set pead&sue.port ;
   if count=-60 then do;
  _0=0;_1=0;_2=0;_3=0;_4=0;end;
  label
  _0='Rets of Most negative port' _1='Rets of SUE P2'
  _2='Rets of SUE P3'   _3='Rets of SUE P#4'
  _4='Rets of Most positive port';
   drop _name_;
run;
/*Cumulating Excess Returns*/
proc expand data=pead&sue.port out=pead&sue.port;
  id count; where -60<=count<=60;
  convert _0=sueport1/transformout=(sum);
  convert _1=sueport2/transformout=(sum);
  convert _2=sueport3/transformout=(sum);
  convert _3=sueport4/transformout=(sum);
  convert _4=sueport5/transformout=(sum);
quit;


/* PART4 plot */
options nodate orientation=landscape;
ods pdf file="Bench1_ew_SUE4_5_0206.pdf";
goptions device=pdfc; /* Plot Saved in Home Directory */
axis1 label=(angle=90 "Cumulative Equally Weighted Excess Returns");
axis2 label=("Event time, t=0 is Earnings Announcement Date");
symbol interpol=join w=4 l=1;
proc gplot data =pead&sue.port;
 Title 'SUE4,all stocks,2002-2006,all sorting,benchmark1(equally),mcap5';
 plot (sueport1 sueport2 sueport3 sueport4 sueport5 )*count
  /overlay legend vaxis=axis1 haxis=axis2 href=0;
run;quit;
ods pdf close;
    


 
/*house cleaning*/
proc sql; 
   drop view crsprets, ibes_temp, temp, tradedates, sue, eads1;
   drop table iclink, medest, ibes, ibes1, comp, ibes_anndats, pead&sue.;
quit;
