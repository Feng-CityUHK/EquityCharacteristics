/* load zt */
libname chars '/scratch/cityuhk/xinchars/';

%let begdt = 01JAN1950;
%let enddt = 31DEC2018;

data zt;
  set chars.firmchars_v6_2_final;
  where "&begdt"d<=public_date<="&enddt"d;
run;

PROC PRINT DATA=zt(obs=10);RUN;

/* load dsf */
data mydsf;
set crsp.dsf;
keep permno date ret;
where "&begdt"d<=date<="&enddt"d;
run;

PROC PRINT DATA=mydsf(obs=10);RUN;

/* merge zt & dret */
proc sql;
create table zt_d as
select a.*, b.ret as dret, b.date from
zt a left join mydsf b on
a.permno=b.permno and
intnx('month',a.public_date,1,'b')<=b.date<=intnx('month',a.public_date,1,'e')
order by permno, date;
quit;

PROC PRINT DATA=zt_d(obs=10);RUN;

/* output */

proc export data = zt_d(where=(2004<=year(date)<=2013))
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_04_13.csv' dbms=csv replace; run;

proc export data = zt_d
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_50_18.csv' dbms=csv replace; run;

proc export data = zt_d(where=(1950<=year(date)<=1959))
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_50_59.csv' dbms=csv replace; run;

proc export data = zt_d(where=(1960<=year(date)<=1969))
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_60_69.csv' dbms=csv replace; run;

proc export data = zt_d(where=(1970<=year(date)<=1979))
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_70_79.csv' dbms=csv replace; run;

proc export data = zt_d(where=(1980<=year(date)<=1989))
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_80_89.csv' dbms=csv replace; run;

proc export data = zt_d(where=(1990<=year(date)<=1999))
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_90_99.csv' dbms=csv replace; run;

proc export data = zt_d(where=(2000<=year(date)<=2009))
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_00_09.csv' dbms=csv replace; run;

proc export data = zt_d(where=(2010<=year(date)<=2018))
outfile='/scratch/cityuhk/xinchars/daily_ret_zt_10_18.csv' dbms=csv replace; run;

