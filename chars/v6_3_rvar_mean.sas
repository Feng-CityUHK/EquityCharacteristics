
/* RVAR mean */
proc sql;
	create table dcrsp
	as select permno,date,ret
	from crsp.dsf
	quit;

%macro DATELOOP (year1= 1963, year2= 2018, in_ds=dcrsp, out_ds=work.out_ds);

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

proc sql;
	create table oreg_ds1
	as select permno,date,ret,
	var(ret) as SVAR
	from &in_ds
  where date between &date1 and &date2
  group by permno;
	quit;

data oreg_ds1;
  set oreg_ds1;
  date1 = &date1;
  date2 = &date2;
  format date1 date2 date9.;
run;

proc datasets lib=work;
  append base=all_ds data=oreg_ds1;
run;

%end;
%end;

/*Save results in final dataset*/
data &out_ds;
  set all_ds;
run;

%mend DATELOOP;

%DATELOOP (year1= 1950, year2= 2018,  in_ds=dcrsp, out_ds=work.out_ds);
proc sort data=work.out_ds nodupkey; by permno date; run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.v6_3_rvar_mean;
set work.out_ds;
run;

proc export data=work.out_ds
outfile='/scratch/cityuhk/xinchars/v6_3_rvar_mean.csv' dbms=csv replace; run;
