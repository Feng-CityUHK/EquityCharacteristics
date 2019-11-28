
/*  my_idiovol  RVAR_CAPM   */
/* capm residual variance */
proc sql;
	create table dcrsp
	as select permno,date,ret
	from crsp.dsf
	quit;

proc sql;
		create table ddcrsp as
		select a.*, (a.ret-b.rf) as ERET, b.mktrf, b.smb, b.hml from
		dcrsp a left join ff.factors_daily b
		on
		a.date = b.date;
		quit;

proc sort data=ddcrsp; by permno date; run;


/*proc export data=work.ddcrsp outfile="ddcrsp.csv" */
/*dbms=csv replace;run;*/


%macro RRLOOP(year1=1950, year2=2018, in_ds=ddcrsp, out_ds = work.out_ds);

%local date1 date2 date1f date2f yy mm;

/*Extra step to be sure to start with clean, null datasets for appending*/

proc datasets nolist lib=work;
	delete all_ds oreg_ds1;
run;

%do yy = &year1 %to &year2;
	%do mm = 1 %to 12;

%let date2= %sysfunc(mdy(&mm,1,&yy));
%let date2 = %sysfunc (intnx(month,&date2,0,end));
%let date1 = %sysfunc (intnx(month,&date2,-3,end));

/*An extra step to be sure the loop starts with a clean (empty) dataset for combining results*/
proc datasets nolist lib=work;
	delete oreg_ds1;
run;


/*Regression model estimation -- creates output set with residual*/
proc reg noprint data=&in_ds outest=oreg_ds1 edf sse;
	where date between &date1 and &date2;
	model ERET = mktrf;
	by permno;
run;

/*Store DATE1 and DATE2 as dataset variables;*/
data oreg_ds1;
	set oreg_ds1;
	date1=&date1;
	date2=&date2;
	date=&date2;
	rename _SSE_=rvar;
	nobs= _p_ + _edf_;
	format date1 date2 date yymmdd10.;
run;

/*Append loop results to dataset with all date1-date2 observations*/
proc datasets lib=work;
	append base=all_ds data=oreg_ds1;
run;

 %end;   /*MM month loop*/

 %end;  /*YY year loop*/

/*Save results in final dataset*/
data &out_ds;
	set all_ds;
run;

%mend RRLOOP;

%RRLOOP (year1= 1950, year2= 2018,  in_ds=ddcrsp, out_ds=work.out_ds);
proc sort data=work.out_ds nodupkey; by permno date; run;
/* my_idiovol RVAR_CAPM end */


libname chars '/scratch/cityuhk/xinchars/';
data chars.v6_3_rvar_capm;
set work.out_ds;
run;

proc export data=work.out_ds
outfile='/scratch/cityuhk/xinchars/v6_3_rvar_capm' dbms=csv replace; run;
