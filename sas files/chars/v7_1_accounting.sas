
/* v4 some macros defined by xin he*/
%macro ttm12(var); (&var + lag1(&var) + lag2(&var) + lag3(&var) + lag4(&var) + lag5(&var) + lag6(&var) + lag7(&var) + lag8(&var) + lag9(&var) + lag10(&var) + lag11(&var)) %mend;
%macro ttm4(var); (&var + lag1(&var) + lag2(&var) + lag3(&var)) %mend;

*==============================================================================================================

													CRSP ME DY RET

==============================================================================================================;

/* Create a CRSP Subsample with Monthly Stock and Event Variables       */
/* This procedure creates a SAS dataset named "CRSP_M"                  */
/* Restrictions will be applied later                                   */
/* Select variables from the CRSP monthly stock and event datasets      */
%let msevars=ticker ncusip shrcd exchcd;
%let msfvars =  prc ret retx shrout vol cfacpr cfacshr;
%include '/wrds/crsp/samples/crspmerge.sas';

%crspmerge(s=m,start=01JAN1950,end=31DEC2018,
sfvars=&msfvars,sevars=&msevars,filters=exchcd in (1,2,3));

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

/* impute prc me */

proc sort data=crspm2 nodupkey; by permno date; run;

data crspm2; set crspm2;
	by permno date;
	retain lastprc;
	if first.permno then do;
		lastprc = prc;
	end;
	else do;
		if not missing(prc) then lastprc = prc;
		if missing(prc) then prc=lastprc;
	end;
run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.crspm2_prc; set crspm2; run;
proc export data = crspm2(where=(year(date)=2018))
outfile='/scratch/cityuhk/xintempv6/crspm2_prc.csv' dbms=csv replace; run;

proc sort data=crspm2 nodupkey; by permno date; run;

data crspm2; set crspm2;
	by permno date;
	retain lastmeq;
	if first.permno then do;
		lastmeq = meq;
	end;
	else do;
		if not missing(meq) then lastmeq = meq;
		if missing(meq) then do;
			meq=lastmeq;
			ret=0;
			retx=0;
		end;
	end;
run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.crspm2_me; set crspm2; run;
proc export data = crspm2(where=(year(date)=2018))
outfile='/scratch/cityuhk/xintempv6/crspm2_me.csv' dbms=csv replace; run;

/* There are cases when the same firm (permco) has two or more         */
/* securities (permno) at same date. For the purpose of ME for         */
/* the firm, we aggregated all ME for a given permco, date. This       */
/* aggregated ME will be assigned to the Permno with the largest ME    */

proc sort data=crspm2; by date permco meq; run;

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
/* crspm2a is a monthly table:  */
/* DATE	NCUSIP	TICKER	PERMNO	PERMCO	SHRCD	EXCHCD	PRC	RET	SHROUT	CFACPR	CFACSHR	RETX	DLRET	retadj	ME */

libname chars '/scratch/cityuhk/xinchars/';
data chars.crspm2a; set crspm2a; run;
proc export data = crspm2a(where=(year(date)=2018))
outfile='/scratch/cityuhk/xintempv6/crspm2a.csv' dbms=csv replace; run;


*==============================================================================================================

				Comp Annual Info
				Get raw information

==============================================================================================================;

proc sql;
create table data
	  as select 		/*header info*/
				substr(compress(cusip),1,6) as cnum,c.gvkey,datadate,fyear,c.cik,substr(sic,1,2) as sic2,
				sic,naics,

						/*firm variables*/
						/*income statement*/
		sale,revt,cogs,xsga,dp,xrd,xad,ib,ebitda,ebit,nopi,spi,pi,txp,ni,txfed,txfo,txt,xint,

						/*CF statement and others*/
		capx,oancf,dvt,ob,gdwlia,gdwlip,gwo,mib,oiadp,ivao,

						/*assets*/
		rect,act,che,ppegt,invt,at,aco,intan,ao,ppent,gdwl,fatb,fatl,

						/*liabilities*/
		lct,dlc,dltt,lt,dm,dcvt,cshrc,dcpstk,pstk,ap,lco,lo,drc,drlt,txdi,

						/*equity and other*/
		ceq,scstkc,emp,csho,SEQ,TXDITC,PSTKRV,NP,TXDC,DPC,AJEX,

						/*market*/
		abs(prcc_f) as prcc_f,csho*calculated prcc_f as mve_f


	  from comp.company as c, comp.funda as f
	  where f.gvkey = c.gvkey

	  				/*require some reasonable amount of information*/
	 /* and not missing(at)  and not missing(prcc_f) and not missing(ni) and datadate>='01JAN1950'd */
	 and datadate>='01JAN1950'd
	  				/*get consolidated, standardized, industrial format statements*/
	  and f.indfmt='INDL' and f.datafmt='STD' and f.popsrc='D' and f.consol='C';
	quit;
					/*sort and clean up*/
					proc sort data=data nodupkey;
						by gvkey datadate;
						run;
					/*prep for clean-up and using time series of variables*/
					data data;
						set data;
						retain count;
						by gvkey datadate;
						if first.gvkey then count=1;
						else count+1;
						run;
					data data ;
						set data;
						*do some clean up, several of these variables have lots of missing values;
						if not missing(drc) and not missing(drlt) then dr=drc+drlt;
						if not missing(drc) and missing(drlt) then dr=drc;
						if not missing(drlt) and missing(drc) then dr=drlt;

						if missing(dcvt) and not missing(dcpstk) and not missing(pstk) and dcpstk>pstk then dc=dcpstk-pstk;
						if missing(dcvt) and not missing(dcpstk) and missing(pstk) then dc=dcpstk;
						if missing(dc) then dc=dcvt;

						if missing(xint) then xint0=0;
							else xint0=xint;
						if missing(xsga) then xsga0=0;
							else xsga0=0;
						run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.funda; set data; run;
proc export data = data(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/funda.csv' dbms=csv replace; run;

*==============================================================================================================

				Merge comp & crsp
				comp: gvkey date
				crsp: permno date

==============================================================================================================;

/*  lnk & comp */
proc sort data=crsp.ccmxpf_linktable out=lnk;
	where substr(linktype,1,1)='L' and linkprim in ('P','C') and
		(2018 >= year(LINKDT) or LINKDT = .B) and (1950 <= year(LINKENDDT) or LINKENDDT = .E);    /* v5 4 */
	by GVKEY LINKDT; run;

proc sql; create table temp as select a.lpermno as permno, a.linkprim, b.*
	from lnk a, data b where a.gvkey=b.gvkey
	and (LINKDT <= intnx('month',intnx('year',b.datadate,0,'E'),6,'E') or LINKDT = .B)
	and ( intnx('month',intnx('year',b.datadate,0,'E'),6,'E') <= LINKENDDT or LINKENDDT = .E)
	and lpermno ne .
	and not missing(b.gvkey);
quit;
data temp;
	set temp;
	where not missing(permno);
run;

/* filter exchcd & shrcd */
proc sort data=crsp.mseall(keep=cusip ncusip date permno exchcd shrcd siccd) out=mseall nodupkey; /* v2 */
	where exchcd in (1,2,3) or shrcd in (10,11);
	by permno exchcd date; run;
proc sql; create table mseall as
	select *,min(date) as exchstdt,max(date) as exchedt
	from mseall group by permno,exchcd; quit;
proc sort data=mseall nodupkey;
	by permno exchcd; run;
proc sql; create table temp as select *
	from temp as a left join mseall as b
	on a.permno=b.permno
	order by datadate, permno, linkprim desc
	;
	/* and exchstdt<=datadate<= exchedt;    */
quit;

data temp;
	set temp;                                                           /* constraint on exchcd */
   	where exchcd in (1,2,3) and shrcd in (10,11) and not missing(permno);  /* try a small sample  */
	drop shrcd date siccd exchstdt exchedt;
run;

data temp; set temp;
by datadate permno descending linkprim;
if first.permno;
run;

data temp; set temp;
year = year(datadate);
run;

proc sort data=temp nodupkey;
by permno year datadate;
run;

data temp; set temp;
  by permno year datadate;
  if last.year;
run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.temp; set temp; run;
proc export data = temp(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/temp.csv' dbms=csv replace; run;

/* add crsp me retadj */

proc sql;
create table temp1 as
select a.*, b.me/1000 as mve_f
from temp(drop=mve_f) a left join crspm2a b
on a.permno=b.permno and
intnx('month',a.datadate,0,'End')=intnx('month',b.date,0,'End')
;
quit;

libname chars '/scratch/cityuhk/xinchars/';
data chars.temp1; set temp1; run;
proc export data = temp1(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/temp1.csv' dbms=csv replace; run;

*==============================================================================================================

				Annual zt

==============================================================================================================;

data data2;
	set temp1;
/*create simple-just annual Compustat variables*/
	za_be_PS = coalesce(PSTKRV,PSTKL,PSTK,0);
	if missing(TXDITC) then TXDITC = 0;
	za_BE = seq + TXDITC - za_be_PS ;
	if za_BE<=0 then za_BE=.;
	/**/
	za_AC=((ACT-LCT+NP)-(lag(ACT)-lag(LCT)+lag(NP)))/(10*za_BE);
	if missing(NP) then za_AC=((ACT-LCT)-(lag(ACT)-lag(LCT)))/(10*za_BE);
	if missing(ACT) or missing(LCT) then za_AC=(IB-OANCF)/(10*za_BE);
	/**/
	za_INV = -(lag(at)-at)/lag(at);
	if count=1 then za_inv=.;
	/**/
	za_BM = za_BE/mve_f;
	/**/
	za_CFP=(IB+DP)/mve_f;
	if missing(DP) then za_CFP=(IB+0)/mve_f;
	if missing(IB) then za_CFP=.;
	/**/
	za_EP = IB/mve_f;
	/**/
	LAT = lag(AT);
	if count=1 then LAT=.;
	za_INV=-(LAT-AT)/LAT;
	/**/
	za_NI=log(CSHO*AJEX)-log(lag(CSHO)*lag(AJEX));
	if count=1 then za_NI=.;
	/**/
	cogs0 = coalesce(cogs,0);
	xint0 = coalesce(xint,0);
	xsga0 = coalesce(xsga,0);
	za_OP = (revt-cogs0-xsga0-xint0)/za_BE;
	if missing(revt) then za_OP=.;
	if missing(cogs)=1 and missing(xsga)=1 and missing(xint)=1 then za_OP=.;
	if missing(za_BE) then za_OP=.;
	/**/
	za_rsup = (sale-lag(sale))/mve_f;
	if count=1 then za_rsup=.;
	/**/
	za_sue = (ib-lag(ib))/mve_f;
	if count=1 then za_sue=.;
	/**/
	za_cash = che/at;
	/**/

	bm=ceq/mve_f;
	ep=ib/mve_f;
	cashpr=((mve_f+dltt-at)/che);
	dy=dvt/mve_f;
	lev=lt/mve_f;
	za_lev = lev;
	sp=sale/mve_f;
	za_sp = sp;
	roic=(ebit-nopi)/(ceq+lt-che);
	rd_sale=xrd/sale;
	za_rd_sale = rd_sale;
	rd_mve=xrd/mve_f;
	za_rd_mve = rd_mve;
	agr= (at/lag(at)) - 1;
	gma=(revt-cogs)/lag(at);
	za_gma = gma;
	chcsho=(csho/lag(csho))-1;
	za_chcsho=(csho/lag(csho))-1;
	if gvkey ne lag(gvkey) then za_chcsho = .;
	lgr=(lt/lag(lt))-1;
	za_lgr = lgr;
	acc=(ib-oancf) /  ((at+lag(at))/2);
		if missing(oancf) then acc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/  ((at+lag(at))/2);
	pctacc=(ib-oancf)/abs(ib);
		if ib=0 then pctacc=(ib-oancf)/.01;
	if missing(oancf) then pctacc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/abs(ib);
		if missing(oancf) and ib=0 then pctacc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/.01;
	za_pctacc = pctacc;

	cfp=(ib-(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) ))/mve_f;
		if not missing(oancf) then cfp=oancf/mve_f;
	absacc=abs(acc);
	age=count;
	chinv=(invt-lag(invt))/((at+lag(at))/2);
	if spi ne 0 and not missing(spi) then spii=1; else spii=0;
	spi=spi/ ((at+lag(at))/2);
	cf=oancf/((at+lag(at))/2);
			if missing(oancf) then cf=(ib-(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) ))/((at+lag(at))/2);
	hire=(emp-lag(emp))/lag(emp);
		if missing(emp) or missing(lag(emp)) then hire=0;
	sgr=(sale/lag(sale))-1;
	za_sgr = sgr;
	chpm=(ib/sale)-(lag(ib)/lag(sale));
	za_chpm = chpm;
	if gvkey ne lag(gvkey) then za_chpm = .;
	chato=(sale/((at+lag(at))/2)) - (lag(sale)/((lag(at)+lag2(at))/2));
	za_chato = chato;
	if gvkey ne lag(gvkey) then za_chato = .;
	za_chtx = (txt-lag(txt))/lag(at);
	if gvkey ne lag(gvkey) then za_chtx = .;
	/* v5_3*/
	za_ala=che+0.75*(act-che)-0.5*(at-act-gdwl-intan);
	za_alm=za_ala/(at+prcc_f*csho-ceq);
	za_noa=((at-che-coalesce(ivao,0))-(at-coalesce(dlc,0)-coalesce(dltt,0)-coalesce(mib,0)-coalesce(pstk,0)-ceq))/lag(at);
	za_rna=oiadp/lag(za_noa);
	za_pm=oiadp/sale;
	za_ato=sale/lag(za_noa);
	/**/
	pchsale_pchinvt=((sale-lag(sale))/lag(sale))-((invt-lag(invt))/lag(invt));
	pchsale_pchrect=((sale-lag(sale))/lag(sale))-((rect-lag(rect))/lag(rect));
	pchgm_pchsale=(((sale-cogs)-(lag(sale)-lag(cogs)))/(lag(sale)-lag(cogs)))-((sale-lag(sale))/lag(sale));
	pchsale_pchxsga=( (sale-lag(sale))/lag(sale) )-( (xsga-lag(xsga)) /lag(xsga) );
	depr=dp/ppent;
	za_depr = depr;
	pchdepr=((dp/ppent)-(lag(dp)/lag(ppent)))/(lag(dp)/lag(ppent));
	chadv=log(1+xad)-log((1+lag(xad)));	*had error here before, might work better now...;
	invest=( 	(ppegt-lag(ppegt)) +  (invt-lag(invt))	)	/ lag(at);
	if missing(ppegt) then invest=( 	(ppent-lag(ppent)) +  (invt-lag(invt))	)	/ lag(at);
	za_invest = invest;
	egr=( (ceq-lag(ceq))/lag(ceq)  );
	za_egr = egr;
		if missing(capx) and count>=2 then capx=ppent-lag(ppent);
	pchcapx=(capx-lag(capx))/lag(capx);
	grcapx=(capx-lag2(capx))/lag2(capx);
	grGW=(gdwl-lag(gdwl))/lag(gdwl);
		if missing(gdwl) or gdwl=0 then grGW=0;
		if gdwl ne 0 and not missing(gdwl) and missing(grGW) then grGW=1;
	if (not missing(gdwlia) and gdwlia ne 0) or (not missing(gdwlip) and gdwlip ne 0) or (not missing(gwo) and gwo ne 0) then woGW=1;
		else woGW=0;
	tang=(che+rect*0.715+invt*0.547+ppent*0.535)/at;
	if (2100<=sic<=2199) or (2080<=sic<=2085) or (naics in ('7132','71312','713210','71329','713290','72112','721120'))
		then sin=1; else sin=0;
		if missing(act) then act=che+rect+invt;
		if missing(lct) then lct=ap;
	currat=act/lct;
	pchcurrat=((act/lct)-(lag(act)/lag(lct)))/(lag(act)/lag(lct));
	quick=(act-invt)/lct;
	pchquick=(		(act-invt)/lct - (lag(act)-lag(invt))/lag(lct)    )/  (   (  lag(act)-lag(invt)  )/lag(lct)   );
	salecash=sale/che;
	salerec=sale/rect;
	saleinv=sale/invt;
	pchsaleinv=( (sale/invt)-(lag(sale)/lag(invt)) ) / (lag(sale)/lag(invt));
	cashdebt=(ib+dp)/((lt+lag(lt))/2);
	za_cashdebt = cashdebt;
	realestate=(fatb+fatl)/ppegt;
		if missing(ppegt) then realestate=(fatb+fatl)/ppent;
	if (not missing(dvt) and dvt>0) and (lag(dvt)=0 or missing(lag(dvt))) then divi=1; else divi=0;
	if (missing(dvt) or dvt=0) and (lag(dvt)>0 and not missing(lag(dvt))) then divo=1; else divo=0;
	obklg=ob/((at+lag(at))/2);
	chobklg=(ob-lag(ob))/((at+lag(at))/2);
	if not missing(dm) and dm ne 0 then securedind=1; else securedind=0;
	secured=dm/dltt;
	if not missing(dc) and dc ne 0 or (not missing(cshrc) and CSHRC ne 0) then convind=1; else convind=0;
	conv=dc/dltt;
	grltnoa=  ((rect+invt+ppent+aco+intan+ao-ap-lco-lo)-(lag(rect)+lag(invt)+lag(ppent)+lag(aco)+lag(intan)+lag(ao)-lag(ap)-lag(lco)-lag(lo))
			-( rect-lag(rect)+invt-lag(invt)+aco-lag(aco)-(ap-lag(ap)+lco-lag(lco)) -dp ))/((at+lag(at))/2);
	za_grltnoa = grltnoa;
	chdrc=(dr-lag(dr))/((at+lag(at))/2);
	if ((xrd/at)-(lag(xrd/lag(at))))/(lag(xrd/lag(at))) >.05 then rd=1; else rd=0;
	za_rd = rd;
	rdbias=(xrd/lag(xrd))-1 - ib/lag(ceq);
	roe=ib/lag(ceq);
	operprof = (revt-cogs-xsga0-xint0)/lag(ceq);
	ps		= (ni>0)+(oancf>0)+(ni/at > lag(ni)/lag(at))+(oancf>ni)+(dltt/at < lag(dltt)/lag(at))+(act/lct > lag(act)/lag(lct))
			+((sale-cogs)/sale > (lag(sale)-lag(cogs))/lag(sale))+ (sale/at > lag(sale)/lag(at))+ (scstkc=0);
	za_ps = ps;
		*-----Lev and Nissim (2004);
		if fyear<=1978 then tr=.48;
		if 1979<=fyear<=1986 then tr=.46;
		if fyear=1987 then tr=.4;
		if 1988<=fyear<=1992 then tr=.34;
		if 1993<=fyear then tr=.35;
		tb_1=((txfo+txfed)/tr)/ib;
		if missing(txfo) or missing(txfed) then tb_1=((txt-txdi)/tr)/ib;  *they rank within industries;
		if (txfo+txfed>0 or txt>txdi) and ib<=0 then tb_1=1;
		*variables that will be used in subsequent steps to get to final RPS;
		*--prep for for Mohanram (2005) score;
		roa=ni/((at+lag(at))/2);
		za_roa = roa;
		cfroa=oancf/((at+lag(at))/2);
			if missing(oancf) then cfroa=(ib+dp)/((at+lag(at))/2);
		xrdint=xrd/((at+lag(at))/2);
		capxint=capx/((at+lag(at))/2);
		xadint=xad/((at+lag(at))/2);

	/*clean up for observations that do not have lagged observations to create variables*/
	array req{*} chadv agr invest gma chcsho lgr egr chpm chinv hire cf acc pctacc absacc spi sgr
				pchsale_pchinvt pchsale_pchrect pchgm_pchsale pchsale_pchxsga pchcapx ps roa cfroa xrdint capxint xadint divi divo
				obklg chobklg grltnoa chdrc rd pchdepr grGW pchcurrat pchquick pchsaleinv roe
				za_ac
				za_inv
				za_bm
				za_cfp
				za_ep
				za_ni
				za_op
				za_rsup
				za_sue
				za_cash
				za_chcsho
				za_rd
				za_cashdebt
				za_pctacc
				za_gma
				za_lev
				za_rd_mve
				za_sgr
				za_sp
					za_invest
					za_rd_sale
					za_ps
					za_lgr
					za_roa
					za_depr
					za_egr
					za_grltnoa
					za_chpm
					za_chato
					za_chtx
					za_ala
					za_alm
					za_noa
					za_rna
					za_pm
					za_ato

				operprof;
	if count=1 then do;
		do b=1 to dim(req);
		req(b)=.;
		end;
	end;
	if count<3 then do;
		chato=.;
		grcapx=.;
	end;
	run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.data2; set data2; run;
proc export data = data2(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/data2.csv' dbms=csv replace; run;

/* Industry Adjustments */
proc sql;
	create table data2
	as select *,chpm-mean(chpm) as chpmia,chato-mean(chato) as chatoia,
	sum(sale) as indsale,hire-mean(hire) as chempia,bm-mean(bm) as bm_ia,
	pchcapx-mean(pchcapx) as pchcapx_ia,tb_1-mean(tb_1) as tb,
	cfp-mean(cfp) as cfp_ia,mve_f-mean(mve_f) as mve_ia
from data2
group by sic2,fyear;
quit;
proc sql;
create table data2
as select *,sum( (sale/indsale)*(sale/indsale) ) as herf
from data2
group by sic2,fyear;
quit;
*---industry measures for ms----;
proc sort data=data2;
by fyear sic2;
run;
proc univariate data=data2 noprint;
	by fyear sic2;
var roa cfroa  xrdint capxint xadint;
output out=indmd median=md_roa md_cfroa  md_xrdint md_capxint md_xadint;
run;
proc sql;
create table data2
as select *
from data2 a left join indmd b
on a.fyear=b.fyear and a.sic2=b.sic2;
quit;
proc sort data=data2 nodupkey;
	by gvkey datadate;
run;
data data2;
	set data2;
	*more for Mohanram score;
if roa>md_roa then m1=1; else m1=0;
if cfroa>md_cfroa then m2=1; else m2=0;
if oancf>ni then m3=1; else m3=0;
if xrdint>md_xrdint then m4=1; else m4=0;
if capxint>md_capxint then m5=1; else m5=0;
if xadint>md_xadint then m6=1; else m6=0;
	*still need to add another thing for Mohanram (2005) score;
run;
*----add credit rating--------;
proc sql;
create table data2
as select a.*,b.splticrm
from data2 a left join comp.adsprate b
on a.gvkey=b.gvkey
and year(a.datadate)=year(b.datadate);
		quit;
proc sort data=data2 nodupkey;
by gvkey datadate;
run;
*consumer price index to create orgcap measure
from Bureau of Labor Statistics website;
data cpi;
infile datalines;
input yr 4.0 cpi 10.3;
datalines;   /* 2019  254.07 */
2018  251.11
2017  245.12
2016  240.01
2015	237.02
2014	236.74
2013	232.96
2012	229.594
2011	224.939
2010	218.056
2009	214.537
2008	215.303
2007	207.342
2006	201.6
2005	195.3
2004	188.9
2003	183.96
2002	179.88
2001	177.1
2000	172.2
1999	166.6
1998	163.00
1997	160.5
1996	156.9
1995	152.4
1994	148.2
1993	144.5
1992	140.3
1991	136.2
1990	130.7
1989	124.00
1988	118.3
1987	113.6
1986	109.6
1985	107.6
1984	103.9
1983	99.6
1982	96.5
1981	90.9
1980	82.4
1979	72.6
1978	65.2
1977	60.6
1976	56.9
1975	53.8
1974  49.3
1973  44.4
1972  41.82
1971  40.49
1970    38.82
;
run;
proc sql;
create table data2
as select a.*,b.cpi
from data2 a left join cpi b
on a.fyear=b.yr;
quit;
proc sort data=data2 nodupkey;
by gvkey datadate;
run;
data data2;
set data2;			*an attempt to coding credit ratings into numerical format;
by gvkey datadate;
if splticrm='D' then credrat=1;
if splticrm='C' then credrat=2;
if splticrm='CC' then credrat=3;
if splticrm='CCC-' then credrat=4;
if splticrm='CCC' then credrat=5;
if splticrm='CCC+' then credrat=6;
if splticrm='B-' then credrat=7;
if splticrm='B' then credrat=8;
if splticrm='B+' then credrat=9;
if splticrm='BB-' then credrat=10;
if splticrm='BB' then credrat=11;
if splticrm='BB+' then credrat=12;
if splticrm='BBB-' then credrat=13;
if splticrm='BBB' then credrat=14;
if splticrm='BBB+' then credrat=15;
if splticrm='A-' then credrat=16;
if splticrm='A' then credrat=17;
if splticrm='A+' then credrat=18;
if splticrm='AA-' then credrat=19;
if splticrm='AA' then credrat=20;
if splticrm='AA+' then credrat=21;
if splticrm='AAA' then credrat=22;
*if missing(credrat) then credrat=0;
if credrat<lag(credrat) then credrat_dwn=1; else credrat_dwn=0;
if count=1 then credrat_dwn=0;
run;
proc sort data=data2 nodupkey;
	by gvkey datadate;
run;
*finish orgcap measure;
data data;
	set data2;
by gvkey datadate;
retain orgcap_1;
avgat=((at+lag(at))/2);
if first.gvkey then orgcap_1=(xsga/cpi)/(.1+.15);
	else orgcap_1=orgcap_1*(1-.15)+xsga/cpi;
orgcap=orgcap_1/avgat;
if count=1 then orgcap=.;
run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.data2_plus; set data2; run;
proc export data = data2(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/data2_plus.csv' dbms=csv replace; run;

*==========================================================================================================

				Annual Accounting Variables

==========================================================================================================;

data temp;
	set data2;
	keep cusip ncusip cnum gvkey permno exchcd datadate fyear sic2 sic           /* v2 */
	bm cfp ep cashpr dy lev sp roic rd_sale rd_mve chadv agr invest gma
	chcsho lgr egr chpm chato chinv hire cf acc pctacc absacc age spii spi
	sgr pchsale_pchinvt	pchsale_pchrect	pchgm_pchsale	pchsale_pchxsga pchcapx
	ps  divi divo obklg chobklg securedind secured convind conv grltnoa
	chdrc rd rdbias chpmia chatoia chempia bm_ia pchcapx_ia tb cfp_ia mve_ia herf
	credrat credrat_dwn orgcap m1-m6
	grcapx depr pchdepr grGW tang
	woGW sin mve_f currat pchcurrat quick pchquick
	salecash salerec saleinv pchsaleinv cashdebt realestate roe
	za_ac
	za_inv
	za_bm
	za_cfp
	za_ep
	za_ni
	za_op
	za_rsup
	za_sue
	za_cash
	 za_chcsho
	 za_rd
	 za_cashdebt
	 za_pctacc
	 za_gma
	 za_lev
	 za_rd_mve
	 za_sgr
	 za_sp
	 	za_invest
		za_rd_sale
		za_ps
		za_lgr
		za_roa
		za_depr
		za_egr
		za_grltnoa
    za_chpm
    za_chato
    za_chtx
		za_ala
		za_alm
		za_noa
		za_rna
		za_pm
		za_ato
	operprof;
	run;

data temp; set temp;
za_dy=dy;
run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.temp_real; set temp; run;
proc export data = temp(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/temp_real2018.csv' dbms=csv replace; run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.temp_real; set temp; run;
proc export data = temp(where=(year(datadate)=2017))
outfile='/scratch/cityuhk/xintempv6/temp_real2017.csv' dbms=csv replace; run;


*========================================================================================================

		Now align the annual Compustat variables in calendar month with the assumption that
		annual information is available with a lag of 6 months (if we had point-in-time we would use that)

=========================================================================================================;

proc sql;
create table temp2
as select a.*, b.retadj as ret, abs(prc) as prc,
shrout, vol, b.date,
(b.ret-b.retx) as retdy, b.me as mcap_crsp                       /* v4 */
from temp a left join crspm2a b
on a.permno=b.permno and intnx('MONTH',datadate,6,'E')<=intnx('MONTH',b.date,0,'E')<intnx('MONTH',datadate,24,'E');
quit;

proc sort data=temp2;
	by permno date descending datadate;
	run;
proc sort data=temp2 nodupkey;
	by permno date;
run;

data temp2;
set temp2;
by permno date;
lme = lag(mcap_crsp);
if permno ne lag(permno) then lme=.;
/* dy */
mdivpay = retdy * lme;
z_dy = %ttm12(mdivpay)/mcap_crsp;
if permno ne lag11(permno) then z_dy=.;
run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.temp2; set temp2; run;
proc export data = temp2(where=(year(date)=2018))
outfile='/scratch/cityuhk/xintempv6/temp2.csv' dbms=csv replace; run;

*==============================================================================================================

			COMPUSTAT QUARTERLY INFOR
			GET RAW INFOMATION

==============================================================================================================;

proc sql;
create table data
as select substr(compress(cusip),1,6) as cnum,c.gvkey,fyearq,fqtr,datadate,rdq,substr(sic,1,2) as sic2,

	/*income items*/
		ibq,saleq,txtq,revtq,cogsq,xsgaq,revty,cogsy,saley,
	/*balance sheet items*/
		atq,actq,cheq,lctq,dlcq,ppentq, ppegtq,
	/*other*/
		abs(prccq) as prccq,abs(prccq)*cshoq as mveq,ceqq,

	seqq,pstkq,atq,ltq,pstkrq,gdwlq,intanq,mibq,oiadpq,ivaoq

	/* v3 my formula add */
	,ajexq, cshoq, TXDITCq, NPq, xrdy,xrdq,
	DPq, xintq, invtq,scstkcy, niq, oancfy, dlttq

	from comp.company as c, comp.fundq as f
	where f.gvkey = c.gvkey
	and f.indfmt='INDL' and f.datafmt='STD' and f.popsrc='D' and f.consol='C'
	and not missing(ibq) and datadate>='01JAN1950'd;                            /* v5 4 */
quit;

proc sort data=data nodupkey;
	by gvkey datadate;
	run;

proc sort data=data ;
	by gvkey datadate;
run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.data_q; set data; run;
proc export data = data(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/data_q.csv' dbms=csv replace; run;

/* link with funda+crspm, get mveq  */

proc sql; create table data_q as select a.lpermno as permno,b.*
	from lnk a, data b where a.gvkey=b.gvkey
	and (LINKDT <= b.datadate or LINKDT = .B) and (b.datadate <= LINKENDDT or LINKENDDT = .E) and lpermno ne . and not missing(b.gvkey);
quit;
data data_q;
	set data_q;
	where not missing(permno);
run;

proc sql;
create table data_q as
select a.*, b.me/1000 as mveq from
data_q(drop=mveq) a left join crspm2a b on
a.permno=b.permno and
intnx('month',a.datadate,0,'End')=intnx('month',b.date,0,'End')
;
quit;

proc sql;
create table data as
select a.*, b.mveq from
data(drop=mveq) a left join data_q b
on a.gvkey=b.gvkey and
intnx('month',a.datadate,0,'End')=intnx('month',b.datadate,0,'End')
;
quit;

libname chars '/scratch/cityuhk/xinchars/';
data chars.data_q_real; set data; run;
proc export data = data(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/data_q_real.csv' dbms=csv replace; run;

*==============================================================================================================

			COMPUSTAT QUARTERLY INFOR
			Zt

==============================================================================================================;

data data3;
	set data;
	by gvkey datadate;
	retain count;
	if not missing(pstkrq) then pstk=pstkrq;
		else pstk=pstkq;
	scal=seqq;
	if missing(seqq) then scal=ceqq+pstk;
	if missing(seqq) and (missing(ceqq) or missing(pstk)) then scal=atq-ltq;
	chtx=(txtq-lag4(txtq))/lag4(atq);
	z_chtx = chtx;
	roaq=ibq/lag(atq);
	z_roa = roaq;
	roeq=(ibq)/lag(scal);
	rsup=(saleq-lag4(saleq))/mveq;
	z_rsup = rsup;
	sacc=( (actq-lag(actq) - (cheq-lag(cheq))) - (  (lctq-lag(lctq))-(dlcq-lag(dlcq)) ) ) /saleq; ;
	if saleq<=0 then sacc=( (actq-lag(actq) - (cheq-lag(cheq))) - (  (lctq-lag(lctq))-(dlcq-lag(dlcq)) ) ) /.01;
	stdacc=std(sacc,lag(sacc),lag2(sacc),lag3(sacc),lag4(sacc),lag5(sacc),lag6(sacc),lag7(sacc),
		lag8(sacc),lag9(sacc),lag10(sacc),lag11(sacc),lag12(sacc),lag13(sacc),lag14(sacc),lag15(sacc));
	sgrvol=std(rsup,lag(rsup),lag2(rsup),lag3(rsup),lag4(rsup),lag5(rsup),lag6(rsup),lag7(rsup),
		lag8(rsup),lag9(rsup),lag10(rsup),lag11(rsup),lag12(rsup),lag13(rsup),lag14(rsup));
	roavol=std(roaq,lag(roaq),lag2(roaq),lag3(roaq),lag4(roaq),lag5(roaq),lag6(roaq),lag7(roaq),
		lag8(roaq),lag9(roaq),lag10(roaq),lag11(roaq),lag12(roaq),lag13(roaq),lag14(roaq),lag15(roaq));
	scf=(ibq/saleq)-sacc;
	if saleq<=0 then scf=(ibq/.01)-sacc;
	stdcf=std(scf,lag(scf),lag2(scf),lag3(scf),lag4(scf),lag5(scf),lag6(scf),lag7(scf),
		lag8(scf),lag9(scf),lag10(scf),lag11(scf),lag12(scf),lag13(scf),lag14(scf),lag15(scf));
	cash=cheq/atq;
	z_cash = cash;
	cinvest=((ppentq-lag(ppentq))/saleq)-mean(((lag(ppentq)-lag2(ppentq))/lag(saleq)),((lag2(ppentq)-lag3(ppentq))/lag2(saleq)),((lag3(ppentq)-lag4(ppentq))/lag3(saleq)));
		if saleq<=0 then cinvest=((ppentq-lag(ppentq))/.01)-mean(((lag(ppentq)-lag2(ppentq))/(.01)),((lag2(ppentq)-lag3(ppentq))/(.01)),((lag3(ppentq)-lag4(ppentq))/(.01)));


	*for sue later and for nincr;
	che=ibq-lag4(ibq);

	nincr	=(  (ibq>lag(ibq))
+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))
+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))
+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))
+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))*(lag4(ibq)>lag5(ibq))
+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))*(lag4(ibq)>lag5(ibq))*(lag5(ibq)>lag6(ibq))
+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))*(lag4(ibq)>lag5(ibq))*(lag5(ibq)>lag6(ibq))*(lag6(ibq)>lag7(ibq))
+ (ibq>lag(ibq))*(lag(ibq)>lag2(ibq))*(lag2(ibq)>lag3(ibq))*(lag3(ibq)>lag4(ibq))*(lag4(ibq)>lag5(ibq))*(lag5(ibq)>lag6(ibq))*(lag6(ibq)>lag7(ibq))*(lag7(ibq)>lag8(ibq))  );

/* v3: my formula */
/* prepare BE */
if SEQq>0 then BEq = sum(SEQq, TXDITCq, -PSTKq); if BEq<=0 then BEq=.;          /* financial ratio code*/
/* AC */
z_ac=((ACTq-LCTq+NPq)-(lag4(ACTq)-lag4(LCTq)+lag4(NPq)))/(10*BEq);              /* yuhe formula in quarterly freq.*/
if missing(NPq) then z_ac=((ACTq-LCTq)-(lag4(ACTq)-lag4(LCTq)))/(10*BEq);
if missing(ACTq) or missing(LCTq) then z_ac=.;
/* BM */
z_bm = BEq/mveq;                                                            /* the denominator ME is quarterly*/
/* CFP */
z_cfp = (%ttm4(IBq)+%ttm4(DPq))/mveq;                     						                      /* HXZ formula */
if missing(DPq) then z_cfp = (%ttm4(IBq))/mveq;
if lag3(gvkey) ne gvkey then do;
z_cfp =.;
end;
/* EP */
z_ep = %ttm4(IBq)/mveq;                                                              /* yuhe's formula */
if lag3(gvkey) ne gvkey then do;
z_ep =.;
end;
/* INV */
z_inv = -(lag4(atq)-atq)/lag4(atq);                                           /* yuhe's formula */
if lag4(gvkey) ne gvkey then z_inv=.;
/* NI */
z_ni = log(cshoq*ajexq) - log(lag4(cshoq)*lag4(ajexq));
if missing(cshoq) then z_ni=.;
if lag4(gvkey) ne gvkey then z_ni=.;
/* OP */
if missing(xintq) then xintq0=0;                                              /* yuhe's */
else xintq0=xintq;
if missing(xsgaq) then xsgaq0=0; /* typo?*/
else xsgaq0=xsgaq;
z_op = (%ttm4(revtq)-%ttm4(cogsq)-%ttm4(xsgaq0)-%ttm4(xintq0))/lag4(beq);
if lag3(gvkey) ne gvkey then z_op=.;
/* sue */
z_sue = (ibq-lag4(ibq))/abs(mveq);
if gvkey ne lag4(gvkey) then z_sue=.;
/*csho*/
z_chcsho=(cshoq/lag4(cshoq))-1;
if gvkey ne lag4(gvkey) then z_chcsho=.;
/*cashdebt*/
z_cashdebt = (%ttm4(IBq)+%ttm4(DPq))/((ltq+lag4(ltq))/2);
if lag4(gvkey) ne gvkey then z_cashdebt=.;
/*rd*/
xrdq4 = %ttm4(xrdq);
if gvkey ne lag3(gvkey) then xrdq4=xrdy;
atq4 = lag4(atq);
if gvkey ne lag4(gvkey) then atq4=.;
if ((xrdq4/atq)-(lag4(xrdq4/atq4)))/(lag4(xrdq4/atq4)) >.05 then z_rd=1; else z_rd=0;
/*pctacc*/
z_pctacc=((ACTq-LCTq+NPq)-(lag4(ACTq)-lag4(LCTq)+lag4(NPq)))/abs(%ttm4(ibq));
if missing(NPq) then z_pctacc=((ACTq-LCTq)-(lag4(ACTq)-lag4(LCTq)))/abs(%ttm4(ibq));
if missing(ACTq) or missing(LCTq) then z_pctacc=.;
if gvkey ne lag4(gvkey) then z_pctacc=.;
/*gma*/
revtq4 = %ttm4(revtq);
cogsq4 = %ttm4(cogsq);
if gvkey ne lag3(gvkey) then revtq4=revty;
if gvkey ne lag3(gvkey) then cogsq4=cogsy;
/*atq4*/
z_gma = (revtq4 - cogsq4)/atq4;
/*lev*/
z_lev = ltq/mveq;
/*rd_mve*/
/*xrdq4*/
z_rd_mve = xrdq4 / mveq;
/*sgr*/
saleq4 = %ttm4(saleq);
if gvkey ne lag3(gvkey) then saleq4=saley;
z_sgr = (saleq4 / lag4(saleq4))-1;
/*sp*/
z_sp = saleq4/mveq;
/*invest*/
z_invest = ( 	(ppegtq-lag4(ppegtq)) +  (invtq-lag4(invtq))	)	/ atq4;
if missing(ppegtq) then z_invest=( 	(ppentq-lag4(ppentq)) +  (invtq-lag4(invtq))	)	/ atq4;
if gvkey ne lag4(gvkey) then z_invest=.;
/*rd_sale*/
z_rd_sale = xrdq4 / saleq4;
/*ps*/
niq4 = %ttm4(niq);
if gvkey ne lag3(gvkey) then niq4=.;
z_ps = (niq4>0)+(oancfy>0)+(niq4/atq > lag4(niq4)/atq4)+(oancfy>niq4)+(dlttq/atq < lag4(dlttq)/atq4)+(actq/lctq > lag4(actq)/lag4(lctq))
+((saleq4-cogsq4)/saleq4 > (lag4(saleq4)-lag4(cogsq4))/lag4(saleq4))+ (saleq4/atq > lag4(saleq4)/atq4 + (scstkcy=0));
/*lgr*/
z_lgr = (ltq/lag4(ltq))-1;
if gvkey ne lag4(gvkey) then z_lgr=.;
/*depr*/
z_depr = %ttm4(dpq)/ppentq;
if gvkey ne lag4(gvkey) then z_depr=.;
/*egr*/
z_egr = ( (ceqq-lag4(ceqq))/lag4(ceqq)  );
if gvkey ne lag4(gvkey) then z_egr=.;
/*grltnoa*/
z_grltnoa = ((rectq+invtq+ppentq+acoq+intanq+aoq-apq-lcoq-loq)-(lag4(rectq)+lag4(invtq)+lag4(ppentq)+lag4(acoq)+lag4(intanq)+lag4(aoq)-lag4(apq)-lag4(lcoq)-lag4(lcoq))
		-( rectq-lag4(rectq)+invtq-lag4(invtq)+acoq-lag4(acoq)-(apq-lag4(apq)+lcoq-lag4(lcoq)) - %ttm4(dpq) ))/((atq+lag4(atq))/2);
if gvkey ne lag4(gvkey) then z_grltnoa=.;
/* chpm */
z_chpm=(%ttm4(ibq)/%ttm4(saleq))-(lag(%ttm4(ibq))/lag(%ttm4(saleq)));
/* chato */
z_chato = (%ttm4(saleq)/((atq+lag4(atq))/2)) - (lag4(%ttm4(saleq))/((lag4(atq)+lag8(atq))/2));
/* v5_3 */
z_ala=cheq + 0.75*(actq - cheq) + 0.5*(atq - actq - gdwlq - intanq);
z_alm=z_alaq/(atq + mveq - ceqq);
z_noa=(atq-cheq-coalesce(ivaoq,0)) - (atq-coalesce(dlcq,0)-coalesce(dlttq,0)-coalesce(mibq,0)-coalesce(pstkq,0)-ceqq)/lag4(atq);
z_rna= oiadpq/lag4(z_noa);
z_pm=oiadpq/saleq;
z_ato=saleq/lag4(z_noa);
if gvkey ne lag4(gvkey) then z_noa=.;
if gvkey ne lag4(gvkey) then z_rna=.;
if gvkey ne lag4(gvkey) then z_pm=.;
/*  */

*clean up;
if first.gvkey then count=1;
	else count+1;

if first.gvkey then do;
	roaq=.;
	roeq=.;
end;
if count<5 then do;
	chtx=.;
	che=.;
	cinvest=.;
end;
if count<17 then do;
	stdacc=.;
	stdcf=.;
	sgrvol=.;
	roavol=.;
end;

run;
*finally finish Mohanram score components;
proc sort data=data3;
by fyearq fqtr sic2;
run;
proc univariate data=data3 noprint;
by fyearq fqtr sic2;
var roavol sgrvol ;
output out=indmd median=md_roavol md_sgrvol;
run;
proc sql;
create table data3
as select *
from data3 a left join indmd b
on a.fyearq=b.fyearq and a.fqtr=b.fqtr and a.sic2=b.sic2;
quit;
proc sort data=data3 nodupkey;
by gvkey fyearq fqtr;
run;
data data3;
set data3;
if roavol<md_roavol then m7=1; else m7=0;
if sgrvol<md_sgrvol then m8=1; else m8=0;
run;

/*  to get earnings forecasts from I/B/E/S that matches to quarterly Compustat variables  */
proc sql;
create table ibessum
	as select ticker,cusip,fpedats,statpers,ANNDATS_ACT,numest,ANNTIMS_ACT,medest,actual,stdev
from ibes.statsum_epsus
where fpi='6'  /*1 is for annual forecasts, 6 is for quarterly*/
and statpers<ANNDATS_ACT /*only keep summarized forecasts prior to earnings annoucement*/
and measure='EPS'
and not missing(medest) and not missing(fpedats)
and (fpedats-statpers)>=0;
quit;
*forecasts closest prior to fiscal period end date;
proc sort data=ibessum;
by cusip  fpedats descending statpers;
run;
proc sort data=ibessum nodupkey;
by cusip fpedats;
run;
** Prepare Compustat-IBES translation file;
proc sort data=crsp.msenames(where=(ncusip ne '')) out=names nodupkey;
by permno ncusip;
run;
* Add current cusip to IBES (IBES cusip is historical);
proc sql;
create table ibessum2 as select
a.*, substr(compress(b.cusip),1,6) as cusip6
from ibessum a left join names b on
(a.cusip = b.ncusip);
quit;
* Merge IBES, CRSP/Compustat;
proc sql;
create table data4 as select a.*,b.medest,b.actual,b.cusip6           /*v2*/
from data3 a left join ibessum2 b on
(a.cnum = b.cusip6) and a.datadate=b.fpedats;
quit;
proc sort data=data4 nodupkey;
by gvkey datadate;
run;
data data4;
set data4;
* finish SUE variable;
if missing(medest) or missing(actual) then sue=che/mveq;
if not missing(medest) and not missing(actual) then sue=(actual-medest)/abs(prccq);
run;
*get permno for CRSP data;
proc sql; create table data5 as select a.lpermno as permno,b.*
from lnk a,data4 b where a.gvkey=b.gvkey
and (LINKDT <= b.datadate or LINKDT = .B) and (b.datadate <= LINKENDDT or LINKENDDT = .E) and lpermno ne . and not missing(b.gvkey);
quit;
data data5;
set data5;
where not missing(permno);
run;
data data5;
set data5;
where not missing(rdq);  *seems like a reasonable screen at this point to make sure have at least some of this information;
run;


/* Some of the RPS require daily CRSP data in conjunction with Compustat quarterly, */
/* so add daily CRSP info to create these RPS   */

proc sql;
	create table data5
	as select a.*,b.vol
	from data5 a left join crsp.dsf b
	on a.permno=b.permno and
		 intnx('WEEKDAY',rdq,-30)<=b.date<=intnx('WEEKDAY',rdq,-10);
quit;

proc sql;
	create table data5
	as select *,mean(vol) as avgvol
	from data5
group by permno,datadate,rdq;
quit;
proc sort data=data5(drop=vol) nodupkey;
where not missing(rdq);
by permno datadate rdq;
run;
proc sql;
create table data6
as select a.*,b.vol,b.ret
from data5 a left join crsp.dsf b
on a.permno=b.permno and
		intnx('WEEKDAY',rdq,-1)<=b.date<=intnx('WEEKDAY',rdq,1);
quit;
proc sql;
create table data6
as select *,(mean(vol)-avgvol)/avgvol as aeavol,sum(ret) as ear
from data6
group by permno,datadate,rdq;
quit;
proc sort data=data6(drop=vol avgvol ret) nodupkey;
by permno datadate rdq;
run;

data data6;
	set data6;
	keep gvkey permno datadate rdq cusip6                                         /* Xin He add cusip6 */
	chtx roaq rsup stdacc stdcf sgrvol roavol cash cinvest nincr
	sue
	z_sue z_rsup
	aeavol ear	m7 m8 prccq roeq
	z_ac z_bm z_cfp z_ep /* v3 Xin He add variables */
	z_inv z_ni z_op
	z_cash
			 z_chcsho
			 z_rd
			 z_cashdebt
			 z_pctacc
			 z_gma
			 z_lev
			 z_rd_mve
			 z_sgr
			 z_sp
				z_invest
				z_rd_sale
				z_ps
				z_lgr
				z_roa
				z_depr
				z_egr
				z_grltnoa
				z_chato
				z_chpm
				z_chtx
				z_ala
				z_alm
				z_noa
				z_rna
				z_pm
				z_ato
;
run;

libname chars '/scratch/cityuhk/xinchars/';
data chars.data6; set data6; run;
proc export data = data6(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xintempv6/data6.csv' dbms=csv replace; run;


/* add quarterly compustat data to monthly returns and annual compustat data */

proc sql;
	alter table temp2
	drop datadate;

create table temp3
as select *
from temp2 a left join data6 b
on a.permno=b.permno and
	 intnx('MONTH',a.date,-12)<=b.datadate<=intnx('MONTH',a.date,-3,'E');
quit;

proc sort data=temp3;
	by permno date descending datadate;
	run;
proc sort data=temp3 nodupkey;
	by permno date;
	run;
*----------------add eamonth--------------------------;
proc sort data=data6 out=lst(keep=permno rdq) nodupkey;
	where not missing(permno) and not missing(rdq);
	by permno rdq;
	run;
proc sql;
	alter table lst
	add eamonth integer;
	update  lst
	set eamonth=1;

	create table temp3
	as select a.*,b.eamonth
	from temp3 a left join lst b
	on a.permno=b.permno and year(a.date)=year(b.rdq) and month(a.date)=month(b.rdq);

	update  temp3
	set eamonth=0 where eamonth=.;
	quit;
*finally finish Mohanram score;
data temp3;
set temp3;
	ms=m1+m2+m3+m4+m5+m6+m7+m8;
drop m1-m8;
run;

run;


/* 				now add RPS that come straight from IBES data:                 							*/
/*				set these up in monthly intervals where the IBES variables have the monthly statistical summary */

proc sql;
	create table ibessum
		as select ticker,cusip,fpedats,statpers,ANNDATS_ACT,numest,ANNTIMS_ACT,
			medest,meanest,actual,stdev
		from ibes.statsum_epsus
		where fpi='1'  /*1 is for annual forecasts, 6 is for quarterly*/
		and statpers<ANNDATS_ACT /*only keep summarized forecasts prior to earnings annoucement*/
		and measure='EPS'
		and not missing(medest) and not missing(fpedats)
		and (fpedats-statpers)>=0;
	quit;
				proc sort data=ibessum;
				by ticker cusip statpers descending fpedats;   *doing this places all of the missing fpedats at the beginning of the file if not there....;
				run;
				proc sort data=ibessum nodupkey;
				by ticker cusip statpers;
				run;
				data ibessum;
				set ibessum;
				by ticker cusip statpers;
				disp=stdev/abs(meanest);
				if meanest=0 then disp=stdev/.01;
				chfeps=meanest-lag(meanest);
				if first.cusip then chfeps=.;
				run;
				*add long term forecasts;
				proc sql;
				create table ibessum2
				as select ticker,cusip,fpedats,statpers,ANNDATS_ACT,numest,ANNTIMS_ACT,
				medest,meanest,actual,stdev
				from ibes.statsum_epsus
				where fpi='0'  /*1 is for annual forecasts, 6 is for quarterly,0 LTG*/
				/*and statpers<ANNDATS_ACT
				and measure='EPS'
				and not missing(medest)
				and (fpedats-statpers)>=0*/
				and not missing(meanest);
				quit;
				proc sort data=ibessum2 nodupkey;
				by ticker cusip statpers;
				run;
				proc sql;
				create table ibessum2b
				as select a.*,b.meanest as fgr5yr
				from ibessum a left join ibessum2 b
				on a.ticker=b.ticker and a.cusip=b.cusip
				and a.statpers=b.statpers;
				quit;
				data rec;
					set ibes.recdsum;
					where not missing(statpers) and not missing(meanrec);
					run;
				proc sql;
				create table ibessum2b
				as select a.*,b.meanrec
				from ibessum2b a left join rec b
				on a.ticker=b.ticker and a.cusip=b.cusip
				and a.statpers=b.statpers;
				quit;
				proc sort data=ibessum2b;
				by ticker statpers;
				run;
				data ibessum2c;
					set ibessum2b;
					by ticker statpers;
					retain count;
					chrec=meanrec-mean(lag(meanrec),lag2(meanrec))-mean(lag3(meanrec),lag4(meanrec),lag5(meanrec));
					if first.ticker then count=1;
					else count+1;
					if count<6 then chrec=.;
				run;
				*prepare for merge;
				proc sort data=crsp.msenames(where=(ncusip ne '')) out=names nodupkey;
					by permno ncusip;
					run;
				proc sql;
				create table ibessum2b as select
				a.*, b.permno
				from ibessum2c a left join names b on
				(a.cusip = b.ncusip);
quit;

proc sql;
	create table temp4
	as select a.*,b.disp,b.chfeps,b.fgr5yr,b.statpers,b.meanrec,b.chrec,b.numest as nanalyst,b.meanest/abs(a.prccq) as sfe,b.meanest
	from temp3 a left join ibessum2b b
	on a.permno=b.permno and
		 intnx('MONTH',a.date,-4,'beg')<=b.statpers<=intnx('MONTH',a.date,-1,'end');
	quit;
				proc sort data=temp4;
					by permno date descending statpers;
					run;
				proc sort data=temp4(drop=statpers) nodupkey;
					by permno date;
				run;
				*--------a little clean up for IBES variables------------;
				data temp4;
					set temp4;
					if year(date)>=1989 and missing(nanalyst) then nanalyst=0;
					if year(date)>=1989 and missing(fgr5yr) then ltg=0;
					if year(date)>=1989 and not missing(fgr5yr) then ltg=1;
				array f{*} disp chfeps meanest nanalyst sfe ltg fgr5yr ;
				array s{*} meanrec chrec;
				do i=1 to dim(f);
					if year(date)<1989 then f(i)=.;
				end;
				do j=1 to dim(s);
					if year(date)<1994 then s(j)=.;
				end;
run;

data temp4;
	set temp4;
*count to make sure we have enough time series for each firm to create variables;
	where not missing(ret);
	by permno date;
	retain count;
		if first.permno then count=1;
		else count+1;
	run;
proc sql;
	create table temp4
	as select *,mean(ret) as ewret  /*we have used this before, doesn't seem to make a big difference in the variables*/
	from temp4
	group by date;
	quit;

proc sort data=temp4;
	by permno date;
	run;

	data temp5;
		set temp4;
		where not missing(ret);
		by permno date;
		retain count;
			if first.permno then count=1;
			else count+1;
		run;
	data temp6;
		set temp5;
			chnanalyst=nanalyst-lag3(nanalyst);
			mom6m=  (  (1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret)) ) - 1;
			mom12m=  (   (1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret))*
				(1+lag7(ret))*(1+lag8(ret))*(1+lag9(ret))*(1+lag10(ret))*(1+lag11(ret))*(1+lag12(ret))   ) - 1;
			mom36m=(   (1+lag13(ret))*(1+lag14(ret))*(1+lag15(ret))*(1+lag16(ret))*(1+lag17(ret))*(1+lag18(ret))   *
				(1+lag19(ret))*(1+lag20(ret))*(1+lag21(ret))*(1+lag22(ret))*(1+lag23(ret))*(1+lag24(ret))*
				(1+lag25(ret))*(1+lag26(ret))*(1+lag27(ret))*(1+lag28(ret))*(1+lag29(ret))*(1+lag30(ret))     *
				(1+lag31(ret))*(1+lag32(ret))*(1+lag33(ret))*(1+lag34(ret))*(1+lag35(ret))*(1+lag36(ret))  ) - 1;
			mom1m=	lag(ret);
			mom60m=(   (1+lag13(ret))*(1+lag14(ret))*(1+lag15(ret))*(1+lag16(ret))*(1+lag17(ret))*(1+lag18(ret))   *
					(1+lag19(ret))*(1+lag20(ret))*(1+lag21(ret))*(1+lag22(ret))*(1+lag23(ret))*(1+lag24(ret))*
					(1+lag25(ret))*(1+lag26(ret))*(1+lag27(ret))*(1+lag28(ret))*(1+lag29(ret))*(1+lag30(ret))     *
					(1+lag31(ret))*(1+lag32(ret))*(1+lag33(ret))*(1+lag34(ret))*(1+lag35(ret))*(1+lag36(ret))     *
					(1+lag37(ret))*(1+lag38(ret))*(1+lag39(ret))*(1+lag40(ret))     *
					(1+lag41(ret))*(1+lag42(ret))*(1+lag43(ret))*(1+lag44(ret))*(1+lag45(ret))*(1+lag46(ret))     *
					(1+lag47(ret))*(1+lag48(ret))*(1+lag49(ret))*(1+lag50(ret))     *
					(1+lag51(ret))*(1+lag52(ret))*(1+lag53(ret))*(1+lag54(ret))*(1+lag55(ret))*(1+lag56(ret))     *
					(1+lag57(ret))*(1+lag58(ret))*(1+lag59(ret))*(1+lag60(ret))
					) - 1;                                                                  /* v4 */
			dolvol=log(lag2(vol)*lag2(prc));
			chmom =(   (1+lag(ret))*(1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret))   ) - 1
				- ((  (1+lag7(ret))*(1+lag8(ret))*(1+lag9(ret))*(1+lag10(ret))*(1+lag11(ret))*(1+lag12(ret))   ) - 1);
			turn=mean(lag(vol),lag2(vol),lag3(vol))/shrout;

			if lag(ret)>0 and lag2(ret)>0 and lag3(ret)>0 and lag4(ret)>0 and lag5(ret)>0 and lag6(ret)>0 then retcons_pos=1; else retcons_pos=0;
			if lag(ret)<0 and lag2(ret)<0 and lag3(ret)<0 and lag4(ret)<0 and lag5(ret)<0 and lag6(ret)<0 then retcons_neg=1; else retcons_neg=0;

		if count=1 then mom1m=.;
		if count<13 then do;
				mom12m=.;
				chmom=.;
		end;
		if count<60 then mom60m=.;                                                    /* v4 we may drop many firms here*/
		if count<7 then mom6m=.;
		if count<37 then mom36m=.;
		if count<3 then dolvol=.;
		if count<4 then turn=.;
		if count<4 then chnanalyst=.;
		if count<7 then retcons_pos=.;
		if count<7 then retcons_neg=.;

		if count<=12 then IPO=1; else IPO=0;
		run;

	/**************************/
	/* momentum from another table */
		data momcrsp;
		set crsp.msf(keep=permno date ret);
		by permno date;
		run;

		proc sort data=momcrsp nodupkey; by permno date; run;

        data momcrsp;
        set momcrsp;
        z_mom60m = (   (1+lag13(ret))*(1+lag14(ret))*(1+lag15(ret))*(1+lag16(ret))*(1+lag17(ret))*(1+lag18(ret))   *
                        (1+lag19(ret))*(1+lag20(ret))*(1+lag21(ret))*(1+lag22(ret))*(1+lag23(ret))*(1+lag24(ret))*
                        (1+lag25(ret))*(1+lag26(ret))*(1+lag27(ret))*(1+lag28(ret))*(1+lag29(ret))*(1+lag30(ret))     *
                        (1+lag31(ret))*(1+lag32(ret))*(1+lag33(ret))*(1+lag34(ret))*(1+lag35(ret))*(1+lag36(ret))     *
                        (1+lag37(ret))*(1+lag38(ret))*(1+lag39(ret))*(1+lag40(ret))     *
                        (1+lag41(ret))*(1+lag42(ret))*(1+lag43(ret))*(1+lag44(ret))*(1+lag45(ret))*(1+lag46(ret))     *
                        (1+lag47(ret))*(1+lag48(ret))*(1+lag49(ret))*(1+lag50(ret))     *
                        (1+lag51(ret))*(1+lag52(ret))*(1+lag53(ret))*(1+lag54(ret))*(1+lag55(ret))*(1+lag56(ret))     *
                        (1+lag57(ret))*(1+lag58(ret))*(1+lag59(ret))*(1+lag60(ret))
                        ) - 1;                                                                  /* v4 */
        z_mom12m =  (   (1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret))*
                    (1+lag7(ret))*(1+lag8(ret))*(1+lag9(ret))*(1+lag10(ret))*(1+lag11(ret))*(1+lag12(ret))   ) - 1;
        z_mom1m =	lag(ret);
        z_mom6m=  (  (1+lag2(ret))*(1+lag3(ret))*(1+lag4(ret))*(1+lag5(ret))*(1+lag6(ret)) ) - 1;
        z_mom36m=(   (1+lag13(ret))*(1+lag14(ret))*(1+lag15(ret))*(1+lag16(ret))*(1+lag17(ret))*(1+lag18(ret))   *
            (1+lag19(ret))*(1+lag20(ret))*(1+lag21(ret))*(1+lag22(ret))*(1+lag23(ret))*(1+lag24(ret))*
            (1+lag25(ret))*(1+lag26(ret))*(1+lag27(ret))*(1+lag28(ret))*(1+lag29(ret))*(1+lag30(ret))     *
            (1+lag31(ret))*(1+lag32(ret))*(1+lag33(ret))*(1+lag34(ret))*(1+lag35(ret))*(1+lag36(ret))  ) - 1;
        z_moms12m=(lag(ret)+lag2(ret)+lag3(ret)+lag4(ret)+lag5(ret)+lag6(ret)+lag7(ret)+lag8(ret)+lag9(ret)+lag10(ret)+lag11(ret))/11.0;

        if permno ne lag60(permno) then z_mom60m=.;
        if permno ne lag12(permno) then z_mom12m=.;
        if first.permno then z_mom1m=.;
        if permno ne lag36(permno) then z_mom36m=.;
        if permno ne lag6(permno) then z_mom6m=.;
        if permno ne lag12(permno) then z_moms12m=.;
        run;

		proc sql;
		create table mytemp6 as
		select a.*, b.z_mom1m, b.z_mom12m, b.z_mom60m, b.z_mom6m, b.z_mom36m, b.z_moms12m
		from
		temp6 a left join momcrsp b
		on
		a.permno = b.permno
		and
		intnx('month',a.date,0,'E') = intnx('month',b.date,0,'E')
		order by a.permno, a.date;
		run;

		proc sql;
		drop table temp6;
		quit;

		data temp6;
		set mytemp6;
		run;

/**************************/
proc sql;
	create table temp5
	as select *,mean(mom12m) as indmom
	from temp6
	group by sic2,date;
quit;

*=====================================================================================================================

			crsp vars

======================================================================================================================;

proc sql;
	create table dcrsp
	as select permno,year(date) as yr,month(date) as month,max(ret) as maxret,std(ret) as retvol,
			mean((askhi-bidlo)/((askhi+bidlo)/2)) as baspread,
			std(log(abs(prc*vol))) as std_dolvol,std(vol/shrout) as std_turn,
			mean(abs(ret)/(abs(prc)*vol)) as ill,
			sum(vol=0) as countzero,n(permno) as ndays,sum(vol/shrout) as turn
	from crsp.dsf
	group by permno,year(date),month(date)
	having year(date)>=1950;                                                      /* v5 4 */
	quit;
					proc sort data=dcrsp nodupkey;
						by permno yr month;
					run;
					data dcrsp;
						set dcrsp;
						zerotrade=(countzero+((1/turn)/480000))*21/ndays;
						run;
					*match to prior month to use lagged variables to predict returns;
					proc sql;
						create table temp6
						as select a.*,b.maxret,b.retvol,baspread,std_dolvol,std_turn,ill,zerotrade
						from temp5 a left join dcrsp b
						on a.permno=b.permno and year(intnx('MONTH',date,-1))=b.yr
						and month(intnx('MONTH',date,-1))=b.month;
					quit;
					proc sort data=temp6 nodupkey;
						by permno date;
						run;



*=====================================================================================================================

				create beta from weekly returns

======================================================================================================================;
proc sql;
	create table dcrsp
	as select permno,intnx('WEEK',date,0,'end') as wkdt,
	exp(sum(log(1+(ret))))-1 as wkret
from crsp.dsf
group by permno,calculated wkdt;
quit;
proc sort data=dcrsp nodupkey;
where wkdt>='01JAN1950'd;
by permno wkdt;
run;
proc sql;
	create table dcrsp
	as select *,mean(wkret) as ewret
	from dcrsp
	group by wkdt;
quit;
data dcrsp;
	set dcrsp;
	where not missing(wkret) and not missing(ewret);
	run;
proc sort data=temp6 out=lst(keep=permno date) nodupkey;
	where not missing(permno) and not missing(date);
	by permno date;
run;
proc sql;
create table betaest
	as select a.*,b.wkret,b.ewret as ewmkt,b.wkdt
from lst a left join dcrsp b
on a.permno=b.permno and intnx('MONTH',date,-36)<=wkdt<=intnx('MONTH',date,-1);
quit;					*3 years of weekly returns;
proc sql;
create table betaest
as select *
from betaest
group by permno,date
having n(wkret)>=52;
quit;				*require at least 1 year of weekly returns;
proc sort data=betaest;
	by permno date wkdt;
run;
data betaest;
	set betaest;
	by permno date wkdt;
	retain count;
	ewmkt_l1=lag(ewmkt);
	ewmkt_l2=lag2(ewmkt);
	ewmkt_l3=lag3(ewmkt);
	ewmkt_l4=lag4(ewmkt);
	if first.date then count=1;
		else count+1;
	if count<5 then do;
	ewmkt_l1=.;
	ewmkt_l2=.;
	ewmkt_l3=.;
	ewmkt_l4=.;
	end;
	run;
proc reg data=betaest outest=est noprint;
	by permno date;
	model wkret=ewmkt/adjrsq;
output out=idiovolest residual=idioret;
run;			*two different approaches, one typical, the other including lagged market values to use as price delay measure;
proc reg data=betaest outest=est2 noprint;
	by permno date;
	model wkret=ewmkt ewmkt_l1 ewmkt_l2 ewmkt_l3 ewmkt_l4/adjrsq;
output out=idiovolest residual=idioret;
run;
proc sql;
	create table idiovolest
	as select permno,date,std(idioret) as idiovol
	from idiovolest
	group by permno,date;
	quit;
proc sort data=idiovolest nodupkey;
	where not missing(idiovol);
	by permno date;
	run;
data est;
	set est;
	where not missing(permno) and not missing(date);
	beta=ewmkt;
run;

proc sql;                                                             /* merge beta with main table */
create table temp7
as select a.*,b.beta,b.beta*b.beta as betasq,_adjrsq_ as rsq1
from temp6 a left join est b
on a.permno=b.permno and a.date=b.date;
quit;
proc sql;
create table temp7
as select a.*,	1-(	rsq1 / _adjrsq_) as pricedelay
from temp7 a left join est2 b
on a.permno=b.permno and a.date=b.date;
quit;
proc sql;                                                             /* merge idiovol with main table */
create table temp7
as select a.*,b.idiovol
from temp7 a left join idiovolest b
on a.permno=b.permno and a.date=b.date;
quit;


libname chars '/scratch/cityuhk/xinchars/';
data chars.temp7; set temp7; run;
proc export data = temp7(where=(year(date)=2018))
outfile='/scratch/cityuhk/xintempv6/temp7.csv' dbms=csv replace; run;
