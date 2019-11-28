libname chars '/scratch/cityuhk/xinchars/';

/* accounting info */
data temp7; set chars.temp7; run;

/* rvar ff3 */
data out_ds_ff3; set chars.v6_3_rvar_ff3; run;

/* rvar capm */
data out_ds_capm; set chars.v6_3_rvar_capm; run;

/* rvar mean */
data out_ds_mean; set chars.v6_3_rvar_mean; run;

/* beta */
data out_ds_beta; set chars.v6_3_beta; run;

*==============================================================================================================

						merge all parts

==============================================================================================================;

proc sql;
create table temp7 as
select a.*, b.rvar as z_rvar_ff3 from
temp7 a left join work.out_ds_ff3 b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;

proc sql;
create table temp7 as
select a.*, b.rvar as z_rvar_capm from
temp7 a left join work.out_ds_capm b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;

proc sql;
create table temp7 as
select a.*, b.svar as z_rvar_mean from
temp7 a left join work.out_ds_mean b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;


proc sql;
create table temp7 as
select a.*, b.beta as z_beta from
temp7 a left join work.out_ds_beta b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;


proc sort data=temp7 nodupkey;
	where  year(date)>=1950;
	by permno date;
run;

data chars.temp7_rvars; set temp7; run;
proc export data = temp7(where=(year(date)=2018))
outfile='/scratch/cityuhk/xintempv6/temp7_rvars.csv' dbms=csv replace; run;
