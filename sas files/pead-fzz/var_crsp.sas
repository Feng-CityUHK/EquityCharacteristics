/* Set Universal Input Variables */
%let comp_begdt = 01JAN1958;
%let comp_enddt = 31DEC2018;

/************************ Part 1: Compustat ****************************/



data comp0; set comp.funda 
  (keep= gvkey datadate indfmt datafmt popsrc consol );
  by gvkey datadate;
  where indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
  and "&comp_begdt"d<= datadate <="&comp_enddt"d;
  drop indfmt datafmt popsrc consol;
  year=year(datadate);
  retain count_year;
  if first.gvkey then count_year=1;
  else count_year = count_year+1;
run;


proc sql;
create table comp1 as select a.*,b.sic,substr(b.sic,1,1) as sic1,substr(b.sic,1,2) as sic2
 from comp0 as a , comp.company as b
 where a.gvkey = b.gvkey;

create table comp2 as
  select a.*, b.lpermno as permno, b.linkprim
  from comp1 as a, crsp.ccmxpf_linktable as b
  where a.gvkey=b.gvkey
  and substr(b.linktype,1,1)='L' and linkprim in ('P','C')
  and (intnx('month',intnx('year',a.datadate,0,'E'),6,'E') >= b.linkdt)
  and (b.linkenddt >= intnx('month',intnx('year',a.datadate,0,'E'),6,'E')
  or missing(b.linkenddt))
  order by a.datadate, permno, b.linkprim desc;

/*at least 2 years' obs in the period we want*/
create table comp2 as select a.*, max(count_year) as max_count_year
from  comp2 a
group by permno
order by datadate, permno, linkprim desc;
quit;

data comp2;set comp2;by datadate permno descending linkprim;
/* if sic1 ^=6; /*exclude one-digit sic =6
if max_count_year>=2;*/run;

data ccm1a; set comp2;
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
proc sort data=ccm2a nodupkey; by gvkey datadate; run;





/************************ Part 2: CRSP monthly return**********************************/

/*Create a CRSP Subsample with Monthly Stock and Event Variables */

%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx BIDLO ASKHI VOL BID ASK SPREAD shrout cfacpr cfacshr;
    
%include '/wrds/crsp/samples/crspmerge.sas';
    
%crspmerge(s=m,start=01jan1940,end=31mar2019,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3) and shrcd in (10,11));

/* CRSP_M is sorted by date and permno and has historical returns     */
/* as well as historical share codes and exchange codes               */
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

/* - Create a File (retadj)             */
data crspm3 (keep=permno date retadj exchcd shrcd LME weight_port cumretx);
     set crspm2a;
 by permno date;
 retain me_base weight_port cumretx;
 Lpermno=lag(permno);
 LME=lag(me);
     if first.permno then do;
     LME=me/(1+retx); cumretx=sum(1,retx); me_base=LME;weight_port=.;end;
     else do;
     if month(date)=7 then do;
        weight_port= LME;
        me_base=LME; /* lag ME also at the end of June */
        cumretx=sum(1,retx);
     end;
	 else do;
	 if LME>0 then weight_port=cumretx*me_base;
        else weight_port=.;
        cumretx=cumretx*sum(1,retx);
     end; end;
output crspm3;
run;

data crspjune(rename=(date=date_june));set crspm3;
if month(date)=6;run;
proc export data= crspjune outfile="crspjune.csv"
dbms=csv replace;
run;

/* Add the crsp variables we want */
proc sql; create table crspm4
 as select a.*,b.divamt,b.facpr,b.facshr
 from crsp_m a left join crsp.msedist b
 on a.permno=b.permno and
    intnx('month',a.date,0,'E')=intnx('month',b.DCLRDT,0,'E') 
 order by a.date, a.PERMNO;

create table crspm4a
as select b.permno, b.date_june, a.date,a.ret, a.retx, a.vol,a.prc,(a.ret-a.retx) as dy , (a.ASKHI - a.BIDLO) as prc_range
from crspm4 a left join crspjune b
on a.permno=b.permno 
and intnx('month',a.date,1,'E') <=intnx('month',b.date_june,0,'E')<=intnx('month',a.date,62,'E')
order by a.permno, b.date_june, a.date;
quit;
proc sort data=crspm4a nodupkey;by permno date;run;

/* loop for SUM -60 to -1*/
%macro loop;
%local i ;
%do i=1 %to 60;
   
proc sql;
create table sret_&i.
as select permno, date_june,ret,(exp(sum(log(1+b.ret)))-1) as sret_&i.
from ( 
select a.*,b.ret from crspm4a a left join crspm4a b
on a.permno=b.permno and  1<=INTCK('MONTH',b.date,a.date_june)<= &i
)
group by permno,date_june,ret;
quit;

proc sort data=sret_&i. nodupkey;by permno date_june;run;

%end;
%mend;

%loop;



%macro loop;
%local i ;
%do i=1 %to 12;
/*for -12 to -1 sum dy*/  
proc sql;
create table sdy_&i.
as select permno, date_june,dy,(sum(dy)) as sdy_&i.
from ( 
select a.*,b.dy  from crspm4a a left join crspm4a b
on a.permno=b.permno and  1<=INTCK('MONTH',b.date,a.date_june)<= &i
)
group by permno,date_june,dy;
quit;

proc sort data=sdy_&i. nodupkey;by permno date_june;run;

/*for -12 to -1 avr(vol)*/  
proc sql;
create table mvol_&i.
as select permno,date_june,vol,mean(b.vol) as mvol_&i.
from ( 
select a.*,b.vol  from crspm4a a left join crspm4a b
on a.permno=b.permno and  1<=INTCK('MONTH',b.date,a.date_june)<= &i
)
group by permno,date_june,vol;
quit;

proc sort data=mvol_&i. nodupkey;by permno date_june;run;


/*for -12 to -1 average prc*/  
proc sql;
create table mprc_&i.
as select permno, date_june,retx,mean(b.prc) as mprc_&i.
from ( 
select a.*,b.prc  from crspm4a a left join crspm4a b
on a.permno=b.permno and  1<=INTCK('MONTH',b.date,a.date_june)<= &i
)
group by permno,date_june,prc;
quit;

proc sort data=mprc_&i. nodupkey;by permno date_june;run;

/*for -12 to -1 average prc*/  
proc sql;
create table mprc_range_&i.
as select permno, date_june,retx,mean(b.prc_range) as mprc_range_&i.
from ( 
select a.*,b.prc_range  from crspm4a a left join crspm4a b
on a.permno=b.permno and  1<=INTCK('MONTH',b.date,a.date_june)<= &i
)
group by permno,date_june,prc_range;
quit;

proc sort data=mprc_range_&i. nodupkey;by permno date_june;run;


%end;
%mend;

%loop;

data crsp_final;
merge sret_1-sret_60 sdy_1-sdy_12 mvol_1-mvol_12 mprc_1-mprc_12 mprc_range_1-mprc_range_12;
by permno date_june;
run;


 proc export data=crsp_final outfile="crsp_final.csv" dbms=csv replace;



proc sql; 
   create table variables
   as select a.permno,a.date,a.shrcd,a.exchcd,a.shrout,abs(a.prc*a.shrout) as annual_me,
   a.BIDLO,a.ASKHI,a.BID, a.ASK,a.SPREAD, a.divamt,a.facpr,a.facshr,
             b.*
   from crspm4  a inner join crsp_final   b 
   on a.permno=b.permno and year(b.date_june)=year(a.date) and month(b.date_june)=month(a.date)
   order by a.permno,b.date_june;
   quit;

proc sql; 
   create table variables
   as select a.*,b.*
   from ccm2a a inner join variables  b 
   on a.permno=b.permno and 
   intnx('month', b.date_june, -15, 'E') <= a.datadate <= intnx('month', b.date_june, -3, 'E')
   order by a.permno,b.date_june;
   quit;

proc sort data=variables;by  date_june;run;
/*data variables(drop = count_year max_count_year linkprim year ret retx vol prc);
    retain permno gvkey shrcd exchcd sic sic1 sic2 datadate date annual_me;
set variables;
by gvkey  date_june ;
run;
*/

%let vars_sorting=
SHROUT	BIDLO	ASKHI	BID	ASK	SPREAD	DIVAMT	FACPR	FACSHR		

sret_1	sret_2	sret_3	sret_4	sret_5	sret_6	sret_7	sret_8	
sret_9	sret_10	sret_11	sret_12	sret_13	sret_14	sret_15	sret_16	sret_17	sret_18	
sret_19	sret_20	sret_21	sret_22	sret_23	sret_24	sret_25	sret_26	sret_27	sret_28	
sret_29	sret_30	sret_31	sret_32	sret_33	sret_34	sret_35	sret_36	sret_37	sret_38	
sret_39	sret_40	sret_41	sret_42	sret_43	sret_44	sret_45	sret_46	sret_47	sret_48	
sret_49	sret_50	sret_51	sret_52	sret_53	sret_54	sret_55	sret_56	sret_57	sret_58	
sret_59	sret_60		

sdy_1	sdy_2	sdy_3	sdy_4	sdy_5	sdy_6	sdy_7	sdy_8	sdy_9	sdy_10 sdy_11 sdy_12	

mvol_1	mvol_2	mvol_3	mvol_4	mvol_5	mvol_6	mvol_7	mvol_8	mvol_9	mvol_10	mvol_11	mvol_12	

mprc_1	mprc_2	mprc_3	mprc_4	mprc_5	mprc_6	mprc_7	mprc_8	mprc_9	mprc_10	mprc_11	mprc_12

mprc_range_1 mprc_range_2 mprc_range_3 mprc_range_4 mprc_range_5 mprc_range_6 mprc_range_7 mprc_range_8
mprc_range_9-mprc_range_10 mprc_range_11 mprc_range_12


;

 

/************************ Part 4:  Portfolios ***/
/* Calculate NYSE Breakpoints           */

proc univariate data=variables noprint;
  where exchcd=1 and annual_me>0 /*and count_year>=2*/;
  var annual_me &vars_sorting; 
  by date_june; /*at june;*/
   output out=nyse_breaks median = SIZEMEDN pctlpre=ME &vars_sorting pctlpts=30 70;
 run;


proc export data=nyse_breaks outfile="nyse_breaks.csv"
dbms=csv replace;
run;



/*another loop for sorting*/
%macro loop(vars_sorting);
%local i next_name;
%do i=1 %to %sysfunc(countw(&vars_sorting));
   %let next_name = %scan(&vars_sorting, &i);
  

/* Use Breakpoints to classify stock only at end of all June's*/
proc sql;
  create table vars_&next_name as
  select a.*,  b.sizemedn, b.&next_name.30, b.&next_name.70
  from variables as a, nyse_breaks as b
  where a.date_june=b.date_june;
quit;

/* Create portfolios as of June                       */
/* the accounting-item Portfolios */
data june_&next_name ; set vars_&next_name;
 If annual_me>0 /*and count_year>=2*/ then do;
 positiveme=1;
 if 0 <= annual_me <= sizemedn     then sizeport = 'S_';
 else if annual_me > sizemedn      then sizeport = 'B_';
 else sizeport='';
 if  &next_name<= &next_name.30 then           &next_name.port = "&next_name.1";
 else if &next_name.30 < &next_name <= &next_name.70 then &next_name.port = "&next_name.2" ;
 else if &next_name  > &next_name.70 then          &next_name.port ="&next_name.3";
 else &next_name.port='';
end;
else positiveme=0;
if cmiss(sizeport,&next_name.port)=0  then nonmissport=1; else nonmissport=0;
keep permno date sizeport &next_name.port positiveme exchcd shrcd nonmissport;
run;
 /* Identifying each month the securities of              */
/* Buy and hold June portfolios from July t to June t+1  */
proc sql; 
create table ccm4_&next_name as
 select a.*,b.sizeport, b.&next_name.port, b.date as portdate format date9.,
        b.positiveme , b.nonmissport
 from crspm3 as a, june_&next_name as b
 where a.permno=b.permno and  1 <= intck('month',b.date,a.date) <= 12
 order by date,sizeport, &next_name.port;
quit;


/*************** Part 5: Calculating  Factors  **************/
/* Calculate monthly time series of weighted average portfolio returns */
proc means data=ccm4_&next_name noprint;
 where weight_port>0 and positiveme=1 and exchcd in (1,2,3)
      and shrcd in (10,11) and nonmissport=1;
 by date  sizeport &next_name.port;
 var retadj;
 weight weight_port;
 output out=vwret_&next_name (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;

/* Monthly Factor Returns */
proc transpose data=vwret_&next_name(keep=date sizeport &next_name.port vwret)
 out=vwret2_&next_name (drop=_name_ _label_);
 by date ;
 ID  sizeport &next_name.port;
 Var vwret;
run;


data vwret2_&next_name;
retain S_&next_name.1 S_&next_name.2 S_&next_name.3 B_&next_name.1 B_&next_name.2 B_&next_name.3;
set vwret2_&next_name;
by date;
format date date9.;
run;

data vwret2_&next_name;
set vwret2_&next_name;
if date>='01JUN1964'd;
run;

proc export data= vwret2_&next_name outfile="vwret_&next_name..csv"
dbms=csv replace;
run;



%end;
%mend;

%loop(&vars_sorting);