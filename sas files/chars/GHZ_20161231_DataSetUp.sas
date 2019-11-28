*<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<


Create signals (RPS) that are aligned in calendar month from firm characteristics pulled from Compustat, CRSP, and I/B/E/S
used to predict monthly cross-sections of stock returns

Creating this data includes every RPS in the paper plus a few more that we didn't use, mainly because there were a lot of missing observations for those RPS
	the ones not in the paper are from the RPS in the earlier paper Green, Hand, and Zhang (2013)


Some sort of liability disclaimer here...
If you use the program, make sure you understand it,
If you spot some errors, let is know,
and if you use it, please cite us :)

This is the data creation program for Green, Hand, and Zhang (2016)

>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
;

********************************

	log on to wrds to get data

********************************;
%let wrds=wrds.wharton.upenn.edu 4016;options comamid=TCP remote=WRDS;
	signon username=_prompt_; 
rsubmit;
*==============================================================================================================


				start with COMPUSTAT ANNUAL information


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
		capx,oancf,dvt,ob,gdwlia,gdwlip,gwo,

						/*assets*/
		rect,act,che,ppegt,invt,at,aco,intan,ao,ppent,gdwl,fatb,fatl,

						/*liabilities*/
		lct,dlc,dltt,lt,dm,dcvt,cshrc,dcpstk,pstk,ap,lco,lo,drc,drlt,txdi,

						/*equity and other*/
		ceq,scstkc,emp,csho,

						/*market*/
		abs(prcc_f) as prcc_f,csho*calculated prcc_f as mve_f
	
		
	  from comp.company as c, comp.funda as f
	  where f.gvkey = c.gvkey

	  				/*require some reasonable amount of information*/
	  and not missing(at)  and not missing(prcc_f) and not missing(ni) and datadate>='01JAN1975'd

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
/*
look at how many missing;
data test;
	set data;
	where datadate>='01JAN1980'd;
	array lst{*} xrd nopi xad dvt ob dm dc aco ap intan ao lco lo rect invt drc drlt dr spi gdwl emp dm dcvt fatb fatl che dp lct act xsga at;
	array dms{*} dxrd dnopi dxad ddvt dob ddm ddc daco dap dintan dao dlco dlo drect dinvt ddrc ddrlt ddr dspi dgdwl demp ddm ddcvt dfatb dfatl dche ddp dlct dact dxsga dat;
	do i=1 to dim(lst);
	if lst(i)=. then dms(i)=1; else dms(i)=0;
	end;
	run;
proc means data=test mean sum ;
	var dxrd dnopi dxad ddvt dob ddm ddc daco dap dintan dao dlco dlo drect dinvt ddrc ddrlt ddr dspi dgdwl demp ddm ddcvt dfatb dfatl dche ddp dlct dact dxsga dat;
	run;
endrsubmit;	*/				*xsga also has a fair amount missing...;
					/*--------------------------------------------------------

						more clean-up and create first pass of variables

					----------------------------------------------------------*/
					data data2;
						set data;
					/*create simple-just annual Compustat variables*/
						bm=ceq/mve_f;
						ep=ib/mve_f;
						cashpr=((mve_f+dltt-at)/che);	
						dy=dvt/mve_f;
						lev=lt/mve_f;
						sp=sale/mve_f;
						roic=(ebit-nopi)/(ceq+lt-che);		
						rd_sale=xrd/sale;
						rd_mve=xrd/mve_f;					
						agr= (at/lag(at)) - 1;
						gma=(revt-cogs)/lag(at); 
						chcsho=(csho/lag(csho))-1;
						lgr=(lt/lag(lt))-1;
						acc=(ib-oancf) /  ((at+lag(at))/2);
							if missing(oancf) then acc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/  ((at+lag(at))/2);
						pctacc=(ib-oancf)/abs(ib);
							if ib=0 then pctacc=(ib-oancf)/.01;
						if missing(oancf) then pctacc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/abs(ib);
							if missing(oancf) and ib=0 then pctacc=(	(act-lag(act) - (che-lag(che))) - (  (lct-lag(lct))-(dlc-lag(dlc))-(txp-lag(txp))-dp ) )/.01;
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
						chpm=(ib/sale)-(lag(ib)/lag(sale));
						chato=(sale/((at+lag(at))/2)) - (lag(sale)/((lag(at)+lag2(at))/2));
						pchsale_pchinvt=((sale-lag(sale))/lag(sale))-((invt-lag(invt))/lag(invt));
						pchsale_pchrect=((sale-lag(sale))/lag(sale))-((rect-lag(rect))/lag(rect));
						pchgm_pchsale=(((sale-cogs)-(lag(sale)-lag(cogs)))/(lag(sale)-lag(cogs)))-((sale-lag(sale))/lag(sale));
						pchsale_pchxsga=( (sale-lag(sale))/lag(sale) )-( (xsga-lag(xsga)) /lag(xsga) );
						depr=dp/ppent;
						pchdepr=((dp/ppent)-(lag(dp)/lag(ppent)))/(lag(dp)/lag(ppent));
						chadv=log(1+xad)-log((1+lag(xad)));	*had error here before, might work better now...;
						invest=( 	(ppegt-lag(ppegt)) +  (invt-lag(invt))	)	/ lag(at);
						if missing(ppegt) then invest=( 	(ppent-lag(ppent)) +  (invt-lag(invt))	)	/ lag(at);
						egr=( (ceq-lag(ceq))/lag(ceq)  );
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
						chdrc=(dr-lag(dr))/((at+lag(at))/2);
						if ((xrd/at)-(lag(xrd/lag(at))))/(lag(xrd/lag(at))) >.05 then rd=1; else rd=0;
						rdbias=(xrd/lag(xrd))-1 - ib/lag(ceq);
						roe=ib/lag(ceq);
						operprof = (revt-cogs-xsga0-xint0)/lag(ceq);
						ps		= (ni>0)+(oancf>0)+(ni/at > lag(ni)/lag(at))+(oancf>ni)+(dltt/at < lag(dltt)/lag(at))+(act/lct > lag(act)/lag(lct))
								+((sale-cogs)/sale > (lag(sale)-lag(cogs))/lag(sale))+ (sale/at > lag(sale)/lag(at))+ (scstkc=0);
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
							cfroa=oancf/((at+lag(at))/2);
								if missing(oancf) then cfroa=(ib+dp)/((at+lag(at))/2);
							xrdint=xrd/((at+lag(at))/2);
							capxint=capx/((at+lag(at))/2);
							xadint=xad/((at+lag(at))/2);

						/*clean up for observations that do not have lagged observations to create variables*/
						array req{*} chadv agr invest gma chcsho lgr egr chpm chinv hire cf acc pctacc absacc spi sgr 
									pchsale_pchinvt pchsale_pchrect pchgm_pchsale pchsale_pchxsga pchcapx ps roa cfroa xrdint capxint xadint divi divo
									obklg chobklg grltnoa chdrc rd pchdepr grGW pchcurrat pchquick pchsaleinv roe operprof;
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
						/*other preparation steps for annual variables: industry adjustments*/
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
   datalines;  
2015	236.53
2014	229.91
2013	229.17 
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
1974    49.3
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
*===========================================================

			Now moving past annual Compustat

============================================================;
*===========================================================
		
			Create merge with CRSP

============================================================;
					*======================GET CRSP IDENTIFIER=============================;
					proc sort data=crsp.ccmxpf_linktable out=lnk;
  					where LINKTYPE in ("LU", "LC", "LD", "LF", "LN", "LO", "LS", "LX") and
       					(2015 >= year(LINKDT) or LINKDT = .B) and (1950 <= year(LINKENDDT) or LINKENDDT = .E);
  					by GVKEY LINKDT; run;	    
					proc sql; create table temp as select a.lpermno as permno,b.*
						from lnk a,data b where a.gvkey=b.gvkey 
						and (LINKDT <= b.datadate or LINKDT = .B) and (b.datadate <= LINKENDDT or LINKENDDT = .E) and lpermno ne . and not missing(b.gvkey);
					quit;  
					data temp;
						set temp;
						where not missing(permno);
					run;  	
					*======================================

						Screen on Stock market information: common stocks and major exchanges

					=======================================;
					*----------------------screen for only NYSE, AMEX, NASDAQ, and common stock-------------;
					proc sort data=crsp.mseall(keep=date permno exchcd shrcd siccd) out=mseall nodupkey;
						where exchcd in (1,2,3) or shrcd in (10,11,12);
						by permno exchcd date; run;
					proc sql; create table mseall as 
						select *,min(date) as exchstdt,max(date) as exchedt
						from mseall group by permno,exchcd; quit;    
					proc sort data=mseall nodupkey;
						by permno exchcd; run;
					proc sql; create table temp as select *
						from temp as a left join mseall as b
						on a.permno=b.permno 
						and exchstdt<=datadate<= exchedt; 
					quit; 
					data temp; 
						set temp;
					   	where exchcd in (1,2,3) and shrcd in /*(10,11,12)*/ (10,11) and not missing(permno);
						drop shrcd date siccd exchstdt exchedt;
					run;  			
					proc sort data=temp nodupkey;
						by gvkey datadate;
					run;

*==========================================================================================================

	
				Finalize first Compustat data set
				This is most of the annual compustat variables plus a couple components that still need additional information


==========================================================================================================;	

data temp;
	set temp;
	keep gvkey permno exchcd datadate fyear sic2
	bm cfp ep cashpr dy lev sp roic rd_sale rd_mve chadv agr invest gma
	chcsho lgr egr chpm chato chinv hire cf acc pctacc absacc age spii spi
	sgr pchsale_pchinvt	pchsale_pchrect	pchgm_pchsale	pchsale_pchxsga pchcapx
	ps  divi divo obklg chobklg securedind secured convind conv grltnoa
	chdrc rd rdbias chpmia chatoia chempia bm_ia pchcapx_ia tb cfp_ia mve_ia herf
	credrat credrat_dwn orgcap m1-m6 
	grcapx depr pchdepr grGW tang 
	woGW sin mve_f currat pchcurrat quick pchquick
	salecash salerec saleinv pchsaleinv cashdebt realestate roe operprof;
	run;	

*========================================================================================================

		Now align the annual Compustat variables in calendar month with the assumption that
		annual information is available with a lag of 6 months (if we had point-in-time we would use that)

=========================================================================================================;
*---------------------------add returns and monthly CRSP data we need later-----------------------------;					
proc sql;
	create table temp2
	as select a.*,b.ret,abs(prc) as prc,shrout,vol,b.date
	from temp a left join crsp.msf b
	on a.permno=b.permno and intnx('MONTH',datadate,7)<=b.date<intnx('MONTH',datadate,20);
	quit;
							*-----------Included delisted returns in the monthly returns--------------------;
							proc sql;
						 	  create table temp2
							      as select a.*,b.dlret,b.dlstcd,b.exchcd
 							     from temp2 a left join crsp.mseall b
							      on a.permno=b.permno and a.date=b.date;
							      quit;	
							data temp2;
								set temp2;
 								if missing(dlret) and (dlstcd=500 or (dlstcd>=520 and dlstcd<=584))
									and exchcd in (1,2) then dlret=-.35;
 								if missing(dlret) and (dlstcd=500 or (dlstcd>=520 and dlstcd<=584))
									and exchcd in (3) then dlret=-.55; *see Johnson and Zhao (2007), Shumway and Warther (1999) etc.;
								if not missing(dlret) and dlret<-1 then dlret=-1;
								if missing(dlret) then dlret=0;
								ret=ret+dlret;
								if missing(ret) and dlret ne 0 then ret=dlret;
								run;
							proc sort data=temp2;
								by permno date descending datadate;
								run;
							proc sort data=temp2 nodupkey;
								by permno date;
							run;	
						*can use monthly market cap and price, but need to lag because it is currently 
						contemporaneous with the returns we want to predict;	
							data temp2;
								set temp2;
								by permno date;
								/*market cap measure*/
								mve_m=abs(lag(prc))*lag(shrout);

								*mve=log(mve_f);
								mve=log(mve_m);

								pps=log(lag(prc));
								if first.permno then delete;
								run;	

*==============================================================================================================


				Now add in COMPUSTAT QUARTERLY and then add to the monthly aligned dataset


==============================================================================================================;
proc sql;
	create table data
	as select substr(compress(cusip),1,6) as cnum,c.gvkey,fyearq,fqtr,datadate,rdq,substr(sic,1,2) as sic2,

		/*income items*/
			ibq,saleq,txtq,revtq,cogsq,xsgaq,
		/*balance sheet items*/
			atq,actq,cheq,lctq,dlcq,ppentq, 
		/*other*/
	  	abs(prccq) as prccq,abs(prccq)*cshoq as mveq,ceqq,

		seqq,pstkq,atq,ltq,pstkrq

		from comp.company as c, comp.fundq as f
	  where f.gvkey = c.gvkey
	  and f.indfmt='INDL' and f.datafmt='STD' and f.popsrc='D' and f.consol='C'
		and not missing(ibq) and datadate>='01JAN1975'd;
	quit;  	
						proc sort data=data nodupkey;
							by gvkey datadate;
							run;
					
						proc sort data=data ;
							by gvkey datadate;	
							run;										
						*create first set of quarterly compustat variables;
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
							roaq=ibq/lag(atq);
							roeq=(ibq)/lag(scal);
							rsup=(saleq-lag4(saleq))/mveq;
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
					create table data4 as select a.*,b.medest,b.actual 
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

*=============================================

		Some of the RPS require daily CRSP data in conjunction with Compustat quarterly,
		so add daily CRSP info to create these RPS

==============================================;
*this is for abnormal trading volume and returns around earings announcements;
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
*================================================================================


		First Compustat quarterly data set

================================================================================;
data data6;
	set data6;
	keep gvkey permno datadate rdq
	chtx roaq rsup stdacc stdcf sgrvol roavol cash cinvest nincr 
	sue aeavol ear	m7 m8 prccq roeq;
	run;
*==============================================================================

	add quarterly compustat data to monthly returns and annual compustat data

===============================================================================;

proc sql;
	alter table temp2
	drop datadate;

	create table temp3
	as select *
	from temp2 a left join data6 b
	on a.permno=b.permno and
     intnx('MONTH',a.date,-10)<=b.datadate<=intnx('MONTH',a.date,-5,'beg');*allow at least four months for quarterly info to become available; *date is the end of the return month;
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
*=================================================================================================================


				now add RPS that come straight from IBES data:
				set these up in monthly intervals where the IBES variables have the monthly statistical summary


=================================================================================================================;   
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
*================================================================

Add IBES data to the rest of the data: note that these are not available at the beginning of sample, and the recommendations come even later
disp, chfeps, meanest, numest, sfe, fgr5yr--ltg --1989
meanrec, chrec --1994
=================================================================;	
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
*======================================================================================================

				There are some other variables that are based on monthly-CRSP information (already in the dataset from monthly CRSP)
				create those variables plus a couple of others

======================================================================================================;  
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
	if count<7 then mom6m=.;
	if count<37 then mom36m=.;
	if count<3 then dolvol=.;
	if count<4 then turn=.;
	if count<4 then chnanalyst=.;
	if count<7 then retcons_pos=.;
	if count<7 then retcons_neg=.;

	if count<=12 then IPO=1; else IPO=0;
	run;
					proc sql;
						create table temp5
						as select *,mean(mom12m) as indmom
						from temp6 
						group by sic2,date;
					quit;				
*=====================================================================================================================


			finally, a few more directly from daily CRSP to create monthly variables


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
	having year(date)>=1970;
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
					*---create beta from weekly returns---;			
					proc sql;
						create table dcrsp
						as select permno,intnx('WEEK',date,0,'end') as wkdt,
						exp(sum(log(1+(ret))))-1 as wkret
					from crsp.dsf
					group by permno,calculated wkdt;
					quit;
					proc sort data=dcrsp nodupkey;
					where wkdt>='01JAN1975'd;
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
					proc sql;
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
					proc sql;
					create table temp7
					as select a.*,b.idiovol
					from temp7 a left join idiovolest b
					on a.permno=b.permno and a.date=b.date;
					quit;
					proc sort data=temp7 nodupkey;
						where  year(date)>=1980;
						by permno date;
						run;
*=============================================================================================


	So there we go, save this monster,
	after this we do a little clean up, but it is all in here now

=============================================================================================;
data temp;
	set temp7;
	where not missing(mve) and not missing(mom1m) and not missing(bm);
	if missing(eamonth) then eamonth=0;
	if missing(IPO) then IPO=0;
	run;												
/*===============================================================================================


	This is to clean up the data a little,
	primarily we want to limit the influence of extreme outliers which are in a lot of these RPS variables

=================================================================================================*/
*this is for all of the continous variables;
%let vars=beta betasq ep mve dy sue chfeps bm fgr5yr lev currat pchcurrat quick pchquick
baspread mom12m depr
pchdepr mom1m mom6m mom36m sgr chempia SP acc turn
pchsale_pchinvt pchsale_pchrect pchcapx_ia pchgm_pchsale pchsale_pchxsga
nincr indmom ps mve_ia cfp_ia bm_ia meanrec dolvol std_dolvol std_turn
sfe  nanalyst disp chinv idiovol
obklg grltnoa cinvest tb cfp roavol lgr egr ill age ms pricedelay
rd_sale rd_mve retvol herf grCAPX zerotrade chmom roic
aeavol chnanalyst agr chcsho chpmia chatoia grGW 
ear  rsup stdcf tang  spi  hire chadv cashpr roaq
invest absacc stdacc chtx maxret pctacc  cash gma roe roeq
orgcap  salecash salerec saleinv pchsaleinv cashdebt realestate  secured credrat 
	operprof;
*this is for those bounded below at zero but may have large positive outliers;   
%let hitrim=betasq mve dy  lev baspread  depr  SP turn  dolvol std_dolvol std_turn 
			disp idiovol obklg roavol ill age rd_sale rd_mve retvol zerotrade  stdcf tang absacc stdacc   
			cash orgcap  salecash salerec saleinv pchsaleinv cashdebt realestate  secured;
*this is for those that may have large positive or negative outliers;
%let hilotrim=beta ep fgr5yr mom12m mom1m mom6m mom36m indmom sue agr maxret chfeps bm currat pchcurrat quick pchquick pchdepr sgr chempia acc  
				pchsale_pchinvt pchsale_pchrect pchcapx_ia pchgm_pchsale pchsale_pchxsga mve_ia cfp_ia bm_ia 
				sfe chinv grltnoa cinvest tb cfp lgr egr pricedelay grCAPX chmom roic aeavol 
				chcsho chpmia chatoia grGW ear  rsup spi  hire chadv cashpr roaq roe roeq invest  chtx pctacc gma operprof;  
*Some of these are not continuous, they are dummy variables so they are excluded 
	from the outlier issue:
	 rd  eamonth IPO  divi divo securedind convind	ltg credrat_dwn woGW sin retcons_pos retcons_neg;
*----winsorize only positive variables-----;
proc sort data=temp;
   by date;
run; 	
proc means data=temp noprint;
	by date;
	var &hitrim;
  output out=stats p99=/autoname;
run;			
proc sql;
	create table temp2
	as select *
	from temp a left join stats b
	on a.date=b.date;
	quit;
data temp2;
	set temp2;
	array base {*} &hitrim;
	array high {*} betasq_p99--secured_p99;
	do i=1 to dim(base);
		if base(i) ne . and base(i)>(high(i)) then base(i)=(high(i));
		if high(i)=. then base(i)=.;
	end;
	drop _type_ _freq_ betasq_p99--secured_p99;
	run;
*winsorize top and bottom of continuous variables;
proc sort data=temp2;
   by date;
run; 	
proc means data=temp2 noprint;
	by date;
	var &hilotrim;
  output out=stats p1= p99=/autoname;
run;			
proc sql;
	create table temp2
	as select *
	from temp2 a left join stats b
	on a.date=b.date;
	quit;
data temp2;
	set temp2;
	array base {*} &hilotrim;
	array low {*} beta_p1--operprof_p1;
	array high {*} beta_p99--operprof_p99;
	do i=1 to dim(base);
		if base(i) ne . and base(i)<(low(i)) then base(i)=(low(i));
		if base(i) ne . and base(i)>(high(i)) then base(i)=(high(i));
		if low(i)=. then base(i)=.;
	end;
	drop _type_ _freq_ beta_p1--operprof_p1 beta_p99--operprof_p99;
	run;
proc sort data=temp2;
   by date;
run; 		
proc download data=temp2 out=rpsdata_RFS;
	run;
endrsubmit;

*==============================================================================

I finally download and save the data here,

	if you are using this program, you need to save to a different location


==============================================================================;  
/*    Save data   */
libname p '\\smeal.psu.edu\data\Users\Faculty\jrg28\My Documents\_ResearchAndTeaching\_research\_papers';
data p.RPSdata_RFS;
	set RPSdata_RFS;
	run;			







