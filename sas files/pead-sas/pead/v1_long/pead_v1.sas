/* ********************************************************************************* */
/* ************** W R D S   R E S E A R C H   A P P L I C A T I O N S ************** */
/* ********************************************************************************* */
/* Summary   : Calculates quarterly standardized earnings surprises (SUE) based      */
/*             on time-series (seasonal random walk model) and analyst EPS forecasts */
/*             using methodology in Livnat and Mendenhall (JAR, 2006)                */
/*             Forms SUE-based portfolios,compares drift across Compustat and IBES   */
/*             based earnings surprise definitions and across different time periods */
/*                                                                                   */
/* Date      : February 2008, Modified: Sep, 2011                                    */
/* Author    : Denys Glushkov, WRDS                                                  */
/* Input     : SAS dataset containing a set of gvkeys which constitute the universe  */
/*             of interest. Application uses members of S&P 500 index as             */
/*             an illustrative example. Users can easily substitute their own file   */
/*             E.g., gvkeyx='030824' is index for S&P 600 SmallCap index             */
/*                                                                                   */
/* Variables : - SUE1: SUE based on a rolling seasonal random walk model (LM,p. 185) */
/*             - SUE2: SUE accounting for  exclusion of special items                */
/*             - SUE3: SUE based on IBES reported analyst forecasts and actuals      */
/*             - BEGINDATE: Sample Start Date                                        */
/*             - ENDDATE: Sample End Date                                            */
/*                                                                                   */
/* To run the program, a user should have access to CRSP daily and monthly stock,    */
/* Compustat Annual and Quarterly sets, IBES and CRSP/Compustat Merged database      */
/* ********************************************************************************* */

%let bdate=01jan1980;        /*start calendar date of fiscal period end*/
%let edate=30jun2018;        /*end calendar date of fiscal period end  */

/*CRSP-IBES link*/
%iclink (ibesid=ibes.id, crspid=crspq.stocknames, outset=work.iclink);

/* Step 1. All companies that were ever included in S&P 500 index as an example  */
/* Linking Compustat GVKEY and IBES Tickers using ICLINK                         */
/* For unmatched GVKEYs, use header IBTIC link in Compustat Security file        */
proc sql; create table gvkeys
    as select distinct a.gvkey, b.lpermco as permco, b.lpermno as permno,
        coalesce (b.linkenddt,'31dec9999'd) as linkenddt format date9.,
        coalesce (d.ticker, c.ibtic) as ticker, b.linkdt format date9.
        from comp.idxcst_his (where=(gvkeyx='000003')) a
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

/* Extract estimates from IBES Unadjusted file and select    */
/* the latest estimate for a firm within broker-analyst group*/
/* "fpi in (6,7)" selects quarterly forecast for the current */
/* and the next fiscal quarter                               */
proc sql;
    create view ibes_temp
        as select a.*, b.permno
            from ibes.detu_epsus a,
                (select distinct ticker,permno,
                 min(linkdt) as mindt,max(linkenddt) as maxdt
                 from gvkeys group by ticker, permno) b
                    where a.ticker=b.ticker and b.mindt<=a.anndats<=b.maxdt
and "&bdate"d<=fpedats<="&edate"d and fpi in ('6','7');

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
        if nmiss(p_count, d_count)=0 then do;
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
    as select a.anndats, b.date format=date9.
    from (select distinct anndats from ibes_anndats
          where not missing(anndats)) a
          left join (select distinct date from crspq.dsi) b
          on 5>=a.anndats-b.date>=0
          group by a.anndats
          having a.anndats-b.date=min(a.anndats-b.date);

/* merge the CRSP adjustment factors for all estimate and report dates   */
    create table ibes_anndats
    as select a.*, c.cfacshr
    from ibes_anndats a left join tradedates b
    on a.anndats=b.anndats
    left join crspq.dsf (keep=permno date cfacshr) c
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
            median=medest n=numest;
run;

/* Extracting Compustat Data and merging it with IBES consensus */
proc sql;
    create table comp
        (keep=gvkey fyearq fqtr conm datadate rdq epsfxq epspxq
         prccq ajexq spiq cshoq prccq ajexq spiq cshoq mcap /*Compustat variables*/
         cshprq cshfdq rdq saleq atq fyr datafqtr
         permno ticker medest numest repdats act basis)     /*CRSP and IBES vars */
as select *, (a.cshoq*a.prccq) as mcap
from comp.fundq
    (where=((not missing(saleq) or atq>0) and consol='C' and
            popsrc='D' and indfmt='INDL' and datafmt='STD' and not missing(datafqtr))) a
            inner join
            (select distinct gvkey, ticker, min(linkdt) as mindate,
             max(linkenddt) as maxdate from gvkeys group by gvkey, ticker) b
            on a.gvkey=b.gvkey and b.mindate<=a.datadate<=b.maxdate
            left join medest c
            on b.ticker=c.ticker and put(a.datadate,yymmn6.)=put(c.fpedats,yymmn6.);
quit;

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
                act medest numest prccq mcap;
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


/* Filter from Livnat & Mendenhall (2006):                                */
/*- earnings announcement date is reported in Compustat                   */
/*- the price per share is available from Compustat at fiscal quarter end */
/*- price is greater than $1                                              */
/*- the market (book) equity at fiscal quarter end is available and is    */
/* EADs in Compustat and in IBES (if available)should not differ by more  */
/* than one calendar day larger than $5 mil.                              */
data sue_final;
    retain gvkey ticker permno conm fyearq fqtr datadate fyr rdq rdq1 leadrdq1
        repdats mcap medest act numest basis sue1 sue2 sue3;
            set sue_final;
                by gvkey descending fyearq descending fqtr;
                    leadrdq1=lag(rdq1); /*the next consecutive EAD*/
                    if first.gvkey then leadrdq1=intnx('month',rdq1,3,'sameday');
                    if leadrdq1=rdq1 then delete;
                        if ((nmiss(sue1,sue2)=0 and missing(repdats))
                            or (not missing(repdats) and abs(intck('day',repdats,rdq))<=1));
                            if (not missing(rdq) and prccq>1 and mcap>5.0);
                                keep gvkey ticker permno conm fyearq fqtr datadate fyr rdq rdq1 leadrdq1
                                    repdats mcap medest act numest basis sue1 sue2 sue3;
                                        label
                                            leadrdq1='Lead Adjusted Report Date of Quarterly Earnings'
                                                basis='Primary/Diluted Basis'
                                                    act='Actual Reported Earnings per Share'
                                                        medest='EPS consensus forecast (median)'
                                                            ticker='Historical IBES Ticker'
                                                                sue1='Earnings Surprise (Seasonal Random Walk)'
                                                                    sue2='Earnings Surprise (Excluding Special items)'
                                                                        sue3='Earnings Surprise (Analyst Forecast-based)'
                                                                            numest='Number of analyst forecasts used in Analyst-based SUE';
                                                                                format rdq1 leadrdq1 date9.;
run;


/* Extract file of raw daily returns around and between EADs and link them */
/* to Standardized Earnings Surprises for forming SUE-based portfolios     */
proc sql;
    create view crsprets
        as select a.permno, a.prc, a.date, abs(a.prc*a.shrout) as mcap,
            b.rdq1, b.leadrdq1, b.sue1, b.sue2, b.sue3, a.ret,
                c.vwretd as mkt, (a.ret-c.vwretd) as exret
                    from crsp.dsf (where=("&bdate"d<=date<="&edate"d)) a inner join
                    sue_final (where=(nmiss(rdq, leadrdq1, permno)=0 and leadrdq1-rdq1>30)) b
                    on a.permno=b.permno and b.rdq1-5<=a.date<=b.leadrdq1+5
                        left join crspq.dsi (keep=date vwretd) c
                        on a.date=c.date
                            order by a.permno, b.rdq1, a.date;
quit;


/* To estimate the drift, sum daily returns over the period from  */
/* 1 day after the earnings announcement through the day of       */
/* the following quarterly earnings announcement                  */
data temp/view=temp; set crsprets;
    by permno rdq1 date;
        lagmcap=lag(mcap);
        if first.permno then lagmcap=.;
            if date=rdq1 then count=0;
                else if date>rdq1 then count+1;
                    format date date9. exret percent7.4;
                        if rdq1<=date<=leadrdq1;
run;

proc sort data=temp out=peadrets nodupkey; by count permno rdq1;run;

/* export */
proc export data=temp
outfile="pead_temp.csv" dbms=csv replace; run;

proc rank data=peadrets out=peadrets groups=5;
    by count; var sue1 sue2 sue3;
        ranks sue1r sue2r sue3r;
run;

/* export */
proc export data=peadrets
outfile="peadrets_rank.csv" dbms=csv replace; run;

/*form portfolios on Compustat-based SUEs (=sue1 or =sue2) or IBES-based SUE (=sue3)*/
/* sue 1*/
%let sue=sue1;
proc sort data=peadrets (where=(not missing(&sue))) out=pead&sue; by count &sue.r;run;

proc means data=pead&sue noprint;
by count &sue.r;
var exret; weight lagmcap;
output out=pead&sue.port mean=/autoname;
run;
proc transpose data=pead&sue.port out=pead&sue.port;
by count; id &sue.r;
var exret_mean;
run;

data pead&sue.port; set pead&sue.port ;
if count=0 then do;
_0=0;_1=0;_2=0;_3=0;_4=0;end;
label
_0='Rets of Most negative SUE port' _1='Rets of SUE Portfolio #2'
_2='Rets of SUE Portfolio #3'   _3='Rets of SUE Portfolio #4'
_4='Rets of most positive SUE port';
drop _name_;
run;
/*Cumulating Excess Returns*/
proc expand data=pead&sue.port out=pead&sue.port;
id count; where count<=50;
convert _0=sueport1/transformout=(sum);
convert _1=sueport2/transformout=(sum);
convert _2=sueport3/transformout=(sum );
convert _3=sueport4/transformout=(sum);
convert _4=sueport5/transformout=(sum);
quit;

options nodate orientation=landscape;
ods pdf file="PEAD_&sue..pdf";
goptions device=pdfc; /* Plot Saved in Home Directory */
axis1 label=(angle=90 "Cumulative Value-Weighted Excess Returns");
axis2 label=("Event time, t=0 is Earnings Announcement Date");
symbol interpol=join w=4 l=1;
proc gplot data =pead&sue.port;
Title 'CARs following EAD for Analyst-based SUE portfolios';
Title2 'Sample: S&P 500 members, Period: 1980-2018';
plot (sueport1 sueport2 sueport3 sueport4 sueport5)*count
/overlay legend vaxis=axis1 haxis=axis2;
run;quit;
ods pdf close;

/* sue 2*/
%let sue=sue2;
proc sort data=peadrets (where=(not missing(&sue))) out=pead&sue; by count &sue.r;run;

proc means data=pead&sue noprint;
by count &sue.r;
var exret; weight lagmcap;
output out=pead&sue.port mean=/autoname;
run;
proc transpose data=pead&sue.port out=pead&sue.port;
by count; id &sue.r;
var exret_mean;
run;

data pead&sue.port; set pead&sue.port ;
if count=0 then do;
_0=0;_1=0;_2=0;_3=0;_4=0;end;
label
_0='Rets of Most negative SUE port' _1='Rets of SUE Portfolio #2'
_2='Rets of SUE Portfolio #3'   _3='Rets of SUE Portfolio #4'
_4='Rets of most positive SUE port';
drop _name_;
run;
/*Cumulating Excess Returns*/
proc expand data=pead&sue.port out=pead&sue.port;
id count; where count<=50;
convert _0=sueport1/transformout=(sum);
convert _1=sueport2/transformout=(sum);
convert _2=sueport3/transformout=(sum );
convert _3=sueport4/transformout=(sum);
convert _4=sueport5/transformout=(sum);
quit;

options nodate orientation=landscape;
ods pdf file="PEAD_&sue..pdf";
goptions device=pdfc; /* Plot Saved in Home Directory */
axis1 label=(angle=90 "Cumulative Value-Weighted Excess Returns");
axis2 label=("Event time, t=0 is Earnings Announcement Date");
symbol interpol=join w=4 l=1;
proc gplot data =pead&sue.port;
Title 'CARs following EAD for Analyst-based SUE portfolios';
Title2 'Sample: S&P 500 members, Period: 1980-2018';
plot (sueport1 sueport2 sueport3 sueport4 sueport5)*count
/overlay legend vaxis=axis1 haxis=axis2;
run;quit;
ods pdf close;

/* sue 3*/
%let sue=sue3;
proc sort data=peadrets (where=(not missing(&sue))) out=pead&sue; by count &sue.r;run;

proc means data=pead&sue noprint;
    by count &sue.r;
        var exret; weight lagmcap;
            output out=pead&sue.port mean=/autoname;
run;
proc transpose data=pead&sue.port out=pead&sue.port;
    by count; id &sue.r;
        var exret_mean;
run;

data pead&sue.port; set pead&sue.port ;
    if count=0 then do;
        _0=0;_1=0;_2=0;_3=0;_4=0;end;
            label
                _0='Rets of Most negative SUE port' _1='Rets of SUE Portfolio #2'
                    _2='Rets of SUE Portfolio #3'   _3='Rets of SUE Portfolio #4'
                        _4='Rets of most positive SUE port';
                            drop _name_;
run;
/*Cumulating Excess Returns*/
proc expand data=pead&sue.port out=pead&sue.port;
    id count; where count<=50;
        convert _0=sueport1/transformout=(sum);
        convert _1=sueport2/transformout=(sum);
        convert _2=sueport3/transformout=(sum );
        convert _3=sueport4/transformout=(sum);
        convert _4=sueport5/transformout=(sum);
quit;

options nodate orientation=landscape;
ods pdf file="PEAD_&sue..pdf";
goptions device=pdfc; /* Plot Saved in Home Directory */
axis1 label=(angle=90 "Cumulative Value-Weighted Excess Returns");
axis2 label=("Event time, t=0 is Earnings Announcement Date");
symbol interpol=join w=4 l=1;
proc gplot data =pead&sue.port;
    Title 'CARs following EAD for Analyst-based SUE portfolios';
        Title2 'Sample: S&P 500 members, Period: 1980-2018';
            plot (sueport1 sueport2 sueport3 sueport4 sueport5)*count
                /overlay legend vaxis=axis1 haxis=axis2;
run;quit;
ods pdf close;

/*house cleaning - skip*/


/* ********************************************************************************* */
/* *************  Material Copyright Wharton Research Data Services  *************** */
/* ****************************** All Rights Reserved ****************************** */
/* ********************************************************************************* */
