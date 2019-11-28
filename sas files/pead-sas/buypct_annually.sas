       
/*CRSP-IBES link*/
%iclink (ibesid=ibes.id, crspid=crspq.stocknames, outset=work.iclink);

/*PART1 Buypct*/
/* Linking Compustat GVKEY and IBES Tickers using ICLINK                 */
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
  if last.linkdt;
run;

proc sql;
create table ibes_sum
as select a.*,b.permno
from ibes.recdsum a,
 (select distinct ticker,permno,
       min(linkdt) as mindt,max(linkenddt) as maxdt
       from gvkeys group by ticker, permno) b
   where a.ticker=b.ticker and b.mindt<=a.statpers<=b.maxdt;
   quit;

proc sort data=ibes_sum; by permno statpers; run;

data ibes_sum_june(rename=(buypct=pct_june));
set ibes_sum;
if month(statpers)=6;
format statpers date9.;
run;


/*proc export data=ibes_sum_june outfile="ibes_sum_june.csv"
dbms=csv replace;
run;*/

/*PART2 CRSP return*/
/*Create a CRSP Subsample with Monthly Stock and Event Variables */

%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout cfacpr cfacshr;
    
%include '/wrds/crsp/samples/crspmerge.sas';
    
%crspmerge(s=m,start=01jan1980,end=30jun2019,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3) and shrcd in (10,11));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
/* Add CRSP delisting returns */
proc sql; create table crspm2
 as select a.*, b.dlret,
  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
 from crsp_m  a left join crsp.msedelist(where=(missing(dlret)=0)) b
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

/* - Create a File with Market Equity (ME)                */
data crspm3 (keep=permno date retadj exchcd shrcd me LME weight_port cumretx);
     set crspm2a;
 by permno date;
 retain me_base weight_port cumretx;
 Lpermno=lag(permno);
 LME=lag(me);
 weight_port = LME;
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
output crspm3;
run;

proc sort data=crspm3 nodupkey;by permno date;run;

/*Part3: Merge buypct and return*/

proc sql; 
   create table variables
   as select a.date,a.shrcd,a.exchcd,a.me,a.retadj,a.weight_port,
   b.*
   from crspm3  a inner join ibes_sum_june b 
   on a.permno=b.permno and 
   intnx('month', b.statpers, 1, 'E') <= intnx('month', a.date, 0, 'E')<= intnx('month', b.statpers, 12, 'E')
   order by a.permno,a.date;
   quit;

/* identify every stock */
data portmember; set variables;
 if me>0 then do;
 positiveme=1;
 if pct_june>=10 then pct10=1;
 if pct_june>=20 then pct20=1;
 if pct_june>=30 then pct30=1;
 if pct_june>=40 then pct40=1;
 if pct_june>=50 then pct50=1;
 if pct_june>=60 then pct60=1;
 if pct_june>=70 then pct70=1;
 if pct_june>=80 then pct80=1;
 if pct_june>=90 then pct90=1;
 end;
 else positiveme=0;
 keep permno date pct_june pct10 pct20 pct30 pct40 pct50 pct60 pct70 pct80 pct90 positiveme exchcd shrcd ;
 run;


proc sql;
  create table vars as
  select a.*,b.pct_june,b.pct10, b.pct20, b.pct30, b.pct40, b.pct50, b.pct60,
b.pct70, b.pct80, b.pct90,b.positiveme
  from variables as a, portmember as b
  where a.permno=b.permno and a.date=b.date;
quit;


/* Calculate monthly time series of weighted average portfolio returns */
/*loop*/
%let portlist=pct10 pct20 pct30 pct40 pct50 pct60 pct70 pct80 pct90;

%macro loop(portlist);
%local i port;
%do i=1 %to %sysfunc(countw(&portlist));
   %let port = %scan(&portlist, &i);

proc sort data=vars (where=(not missing(&port))) out=vars0&port ; by date;run;

data vars&port (keep=date permno ticker shrcd exchcd retadj weight_port ME pct_june
meanrec medrec stdev numrec numup numdown buypct sellpct holdpct USfirm) ;
retain date permno ticker shrcd exchcd retadj weight_port ME pct_june
meanrec medrec stdev numrec numup numdown buypct sellpct holdpct USfirm;
set vars0&port;
format date date9.;
run;

proc export data=vars&port outfile="vars&port..csv"
dbms=csv replace;
run;

/*vwret*/
proc means data=vars0&port noprint;
 where weight_port>0 and positiveme=1 and exchcd in (1,2,3) and shrcd in (10,11);
 by date;
 var retadj;
 weight weight_port;
 output out=vwret_&port (drop= _type_ _freq_ ) mean=vwret_&port. n=n_firms_&port.;
run;
/*ewret*/
proc means data=vars0&port noprint;
 where weight_port>0 and positiveme=1 and exchcd in (1,2,3) and shrcd in (10,11);
 by date;
 var retadj;
 output out=ewret_&port (drop= _type_ _freq_ ) mean=ewret_&port. n=n_firms_&port.;
run;


%end;
%mend;

%loop(&portlist);

data monthly_vwret;
merge vwret_pct10 vwret_pct20 vwret_pct30 vwret_pct40 vwret_pct50
vwret_pct60 vwret_pct70 vwret_pct80 vwret_pct90;
by date;
run;

data monthly_ewret;
merge ewret_pct10 ewret_pct20 ewret_pct30 ewret_pct40 ewret_pct50
ewret_pct60 ewret_pct70 ewret_pct80 ewret_pct90;
by date;
run;




proc export data=monthly_vwret outfile="buypct_annually_vwret.csv"
dbms=csv replace;
run;

proc export data=monthly_ewret outfile="buypct_annually_ewret.csv"
dbms=csv replace;
run;