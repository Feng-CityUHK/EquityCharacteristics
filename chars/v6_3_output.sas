libname chars '/scratch/cityuhk/xinchars/';

data temp7; set chars.temp7_rvars; run;

data temp;
	set temp7;
	/* where not missing(mve) and not missing(mom1m) and not missing(bm); */
	/* this filter ?*/
	where not missing(bm);
	if missing(eamonth) then eamonth=0;
	if missing(IPO) then IPO=0;
	run;


  %let vars=beta betasq z_beta ep dy sue z_sue chfeps bm fgr5yr lev currat pchcurrat quick pchquick
  baspread mom12m depr
  pchdepr mom1m mom6m mom36m sgr chempia SP acc turn
  pchsale_pchinvt pchsale_pchrect pchcapx_ia pchgm_pchsale pchsale_pchxsga
  nincr indmom ps mve_ia cfp_ia bm_ia meanrec dolvol std_dolvol std_turn
  sfe  nanalyst disp chinv idiovol
  obklg grltnoa cinvest tb cfp roavol lgr egr ill age ms pricedelay
  rd_sale rd_mve retvol herf grCAPX zerotrade chmom roic
  aeavol chnanalyst agr chcsho chpmia chatoia grGW
  ear  rsup z_rsup stdcf tang  spi  hire chadv cashpr roaq
  invest absacc stdacc chtx maxret pctacc  cash gma roe roeq
  salecash salerec saleinv pchsaleinv cashdebt realestate  secured credrat
  	z_ac z_bm z_cfp /* v3 Xin He add variables */
  	z_inv z_ni z_op
  	mom60m z_dy z_rvar_ff3	z_rvar_capm	z_rvar_mean	 																							        /* v4 add */
    z_ep
  	za_ac
  	za_inv
  	za_bm
  	za_cfp
  	za_ep
  	za_ni
  	za_op
  	z_mom1m
  	z_mom12m
  	z_mom60m
  	z_mom36m
  	z_mom6m
  	z_moms12m
  	za_rsup
  	za_sue
  	za_dy
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
  						maxret
  						z_invest
  						z_rd_sale
  						z_ps
  						z_lgr
  						z_roa
  						z_depr
  						z_egr
  						z_grltnoa
  						z_chpm
  						z_chato
  						z_chtx
  						z_ala
  						z_alm
  						z_noa
  						z_rna
  						z_pm
  						z_ato
  	operprof
  	;
  *this is for those bounded below at zero but may have large positive outliers;
  %let hitrim=betasq dy  lev baspread  depr  SP turn  dolvol std_dolvol std_turn
  			disp idiovol obklg roavol ill age rd_sale rd_mve retvol zerotrade  stdcf tang absacc stdacc
  			cash  salecash salerec saleinv pchsaleinv cashdebt realestate
  			z_rvar_ff3 z_dy	z_rvar_capm	z_rvar_mean																							        /* v4 add */
  			secured
  			;
  *this is for those that may have large positive or negative outliers;
  %let hilotrim=beta z_beta ep fgr5yr mom12m mom1m mom6m mom36m indmom sue z_sue agr maxret chfeps bm currat pchcurrat quick pchquick pchdepr sgr chempia acc
  				pchsale_pchinvt pchsale_pchrect pchcapx_ia pchgm_pchsale pchsale_pchxsga mve_ia cfp_ia bm_ia
  				sfe chinv grltnoa cinvest tb cfp lgr egr pricedelay grCAPX chmom roic aeavol
  				chcsho chpmia chatoia grGW ear  rsup z_rsup spi  hire chadv cashpr roaq roe roeq invest  chtx pctacc gma
  				mom60m z_ac	z_bm z_cfp z_ep  /* v4 add */
  			 z_inv z_ni z_op
  				za_ac
  				za_inv
  				za_bm
  				za_cfp
  				za_ep
  				za_ni
  				za_op
  				z_mom1m
  				z_mom12m
  				z_mom60m
  				z_mom36m
  				z_mom6m
  				z_moms12m
  				za_rsup
  				za_sue
  				za_dy
  					za_invest
  					za_rd_sale
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
  									z_chpm
  									z_chato
  									z_chtx
  									z_ala
  									z_alm
  									z_noa
  									z_rna
  									z_pm
  									z_ato
  				operprof
  				;
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
  /*
  proc download data=temp2 out=rpsdata_RFS;
  	run;
  */
  data rpsdata_RFS;set temp2;run;
  /* endrsubmit;

  /* add FF industry */
  data rpsdata_RFS; set rpsdata_RFS;
  if sic=0 then sic=.;
  if missing(sic)=0 then %FFI5(sic);
  if missing(sic)=0 then %FFI10(sic);
  if missing(sic)=0 then %FFI12(sic);
  if missing(sic)=0 then %FFI17(sic);
  if missing(sic)=0 then %FFI30(sic);
  if missing(sic)=0 then %FFI38(sic);
  if missing(sic)=0 then %FFI48(sic);
  if missing(sic)=0 then %FFI49(sic);

  *ffi&nind._desc=upcase(ffi&nind._desc);
  ffi5_desc=upcase(ffi5_desc);
  ffi10_desc=upcase(ffi10_desc);
  ffi12_desc=upcase(ffi12_desc);
  ffi17_desc=upcase(ffi17_desc);
  ffi30_desc=upcase(ffi30_desc);
  ffi38_desc=upcase(ffi38_desc);
  ffi48_desc=upcase(ffi48_desc);
  ffi49_desc=upcase(ffi49_desc);

  run;

  *==============================================================================

  I finally download and save the data here,

  	if you are using this program, you need to save to a different location


  ==============================================================================;
  /*    Save data   */


  data chars.firmchars_v6_2_raw;
  	set RPSdata_RFS;
  	run;

  	proc export data=RPSdata_RFS
  	outfile="/scratch/cityuhk/xintempv6/raw.csv" dbms=csv replace; run;

  	*==============================================================================;
  	/*    format data   */
  	/* variables by sources */
  	%let info = date permno gvkey cnum ret
  	sic FFI5_desc	FFI5	FFI10_desc	FFI10	FFI12_desc	FFI12	FFI17_desc	FFI17
  	FFI30_desc	FFI30	FFI38_desc	FFI38	FFI48_desc	FFI48	FFI49_desc	FFI49
  	;
  	%let vara =
  	za_ac
  	za_inv
  	za_bm
  	za_cfp
  	za_ep
  	za_ni
  	za_op
  	za_rsup
  	za_sue
  	za_dy
  	za_invest
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
  	za_ps
  	za_lgr
  	realestate
  	za_rd_sale
  		secured
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

  				 ;  /* 19 accounting vars + dy + cinvest */

  	%let varq = z_ac z_inv z_bm z_cfp z_ni z_op z_ep  z_rsup  z_sue
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
  	 z_dy
  				cinvest
  		z_invest
  		z_rd_sale
  		z_ps
  		z_lgr
  		z_roa
  		z_depr
  		z_egr
  		z_grltnoa
  		z_chapm
  		z_chato
  		z_chtx
  		z_ala
  		z_alm
  		z_noa
  		z_rna
  		z_pm
  		z_ato
  		nincr
  	;   /* 19 accounting + dy */
  	%let varm =
  	disp
  	z_rvar_ff3
  	z_rvar_capm
    z_rvar_mean
  	z_beta
  	z_mom1m
  	z_mom12m
  	z_mom60m
  	z_mom36m
  	z_mom6m
  	z_moms12m
  	baspread
  	mcap_crsp
  	retvol
  	ill
  	pricedelay
  	dolvol
  	std_dolvol
  	turn
  	hire
  	maxret
  	zerotrade
  	std_turn
  		bm_ia
  		chatoia
  		chpmia
  		mve_ia
  		herf
  		indmom
  	;

  	/* final output name */
  	%let var  = ill me bm cfp dy ep lev sgr sp acc agr ni gma op mom12m mom1m mom60m
  	rsup sue bas beta rvar_ff3 rvar_capm svar hire rd_mve
  	cash pricedelay chcsho disp	indmom mve_ia rd turn herf dolvol std_dolvol
  	cashdebt pctacc cinvest
  	maxret invest zerotrade realestate std_turn rd_sale bm_ia chatoia chpmia secured ps lgr roa
  	tb depr egr grltnoa chato chpm chtx nincr mom6m mom36m moms12m ala alm noa rna pm ato
  	;

  	/* if quarterly varq is missing, fill in annual vara */
  	/* if z_dy is missing, fill in annual dy */
  	data check_sample; set RPSdata_RFS;
  	keep &info &vara &varq &varm;
  	run;

  	proc export data=check_sample(where=("01JAN2018"d<=date<="31JAN2018"d))
  	outfile="/scratch/cityuhk/xintempv6/raw2018.csv" dbms=csv replace; run;

    data check_sample; set check_sample;
  	/* acc */
  	if missing(z_ac) then f_acc = za_AC; /* ANNUAL */
  	else f_acc = (z_ac+za_ac)/2;                   /* QUARTERLY */
  	/* agr */
  	if missing(z_inv) then f_agr = za_INV;
  	else f_agr = (z_inv+za_inv)/2;
  	/* book to market */
  	if missing(z_bm) then f_bm = za_BM;
  	else f_bm = (z_bm+za_bm)/2;
  	/* cfp */
  	if missing(z_cfp) then f_cfp = za_CFP;
  	else f_cfp = (z_cfp+za_cfp)/2;
  	/* ni */
  	if missing(z_ni) then f_ni = za_NI;  /* GHZ doesn't have Net Equity Issuance */
  	else f_ni = (z_ni+za_ni)/2;
  	/* earnings to price */
  	if missing(z_ep) then f_ep = za_EP;
  	else f_ep = (z_ep+za_ep)/2;
  	/* operating profit*/
  	if missing(z_op) then f_op = za_OP;
  	else f_op = (z_op+za_op)/2;
  	/* dividend yield */
  	if missing(z_dy) then f_dy = .;  /* GHZ annual dy */
  	else f_dy = z_dy;
  	/* sue */
  	if missing(z_sue) then f_sue = za_sue;
  	else f_sue = (z_sue+za_sue)/2;
  	/* rsup */
  	if missing(z_rsup) then f_rsup = za_rsup;
  	else f_rsup = (z_rsup+za_rsup)/2;
  	/*gma*/
  	if missing(z_gma) then f_gma = za_gma;
  	else f_gma = (z_gma+za_gma)/2;
  	/*lev*/
  	if missing(z_lev) then f_lev = za_lev;
  	else f_lev = (z_lev+za_lev)/2;
  	/*rd_mve*/
  	if missing(z_rd_mve) then f_rd_mve = za_rd_mve;
  	else f_rd_mve = (z_rd_mve+za_rd_mve)/2;
  	/*sgr*/
  	if missing(z_sgr) then f_sgr = za_sgr;
  	else f_sgr = (z_sgr+za_sgr)/2;
  	/*sp*/
  	if missing(z_sp) then f_sp = za_sp;
  	else f_sp = (z_sp+za_sp)/2;
  	/*cash*/
  	if missing(z_cash) then f_cash = za_cash;
  	else f_cash = (z_cash+za_cash)/2;
  	/*chcsho*/
  	if missing(z_chcsho) then f_chcsho = za_chcsho;
  	else f_chcsho = (z_chcsho+za_chcsho)/2;
  	/*rd*/
  	if missing(z_rd) then f_rd = za_rd;
  	else f_rd = (z_rd+za_rd)/2;
  	/*cashdebt*/
  	if missing(z_cashdebt) then f_cashdebt = za_cashdebt;
  	else f_cashdebt = (z_cashdebt+za_cashdebt)/2;
  	/*pctacc*/
  	if missing(z_pctacc) then f_pctacc = za_pctacc;
  	else f_pctacc = (z_pctacc+za_pctacc)/2;
  	/*invest*/
  	if missing(z_invest) then f_invest = za_invest;
  	else f_invest = (z_invest+za_invest)/2;
  	/*rd_sale*/
  	if missing(z_rd_sale) then f_rd_sale = za_rd_sale;
  	else f_rd_sale = (z_rd_sale+za_rd_sale)/2;
  	/*ps*/
  	if missing(z_ps) then f_ps = za_ps;
  	else f_ps = (z_ps+za_ps)/2;
  	/*lgr*/
  	if missing(z_lgr) then f_lgr = za_lgr;
  	else f_lgr = (z_lgr+za_lgr)/2;
  	/*roa*/
  	if missing(z_roa) then f_roa = za_roa;
  	else f_roa = (z_roa+za_roa)/2;
  	/*depr*/
  	if missing(z_depr) then f_depr = za_depr;
  	else f_depr = (z_depr+za_depr)/2;
  	/*egr*/
  	if missing(z_egr) then f_egr = za_egr;
  	else f_egr = (z_egr+za_egr)/2;
  	/*grltnoa*/
  	if missing(z_grltnoa) then f_grltnoa = za_grltnoa;
  	else f_grltnoa = (z_grltnoa+za_grltnoa)/2;
  	/**/
  	/*chpm*/
  	if missing(z_chpm) then f_chpm = za_chpm;
  	else f_chpm = (z_chpm+za_chpm)/2;
  	/*chato*/
  	if missing(z_chato) then f_chato = za_chato;
  	else f_chato = (z_chato+za_chato)/2;
  	/*chtx*/
  	if missing(z_chtx) then f_chtx = za_chtx;
  	else f_chtx = (z_chtx+za_chtx)/2;
  	/*ala*/
  	if missing(z_ala) then f_ala = za_ala;
  	else f_ala = (z_ala+za_ala)/2;
  	/*alm*/
  	if missing(z_alm) then f_alm = za_alm;
  	else f_alm = (z_alm+za_alm)/2;
  	/*noa*/
  	if missing(z_noa) then f_noa = za_noa;
  	else f_noa = (z_noa+za_noa)/2;
  	/*rna*/
  	if missing(z_rna) then f_rna = za_rna;
  	else f_rna = (z_rna+za_rna)/2;
  	/*pm*/
  	if missing(z_pm) then f_pm = za_pm;
  	else f_pm = (z_pm+za_pm)/2;
  	/*ato*/
  	if missing(z_ato) then f_ato = za_ato;
  	else f_ato = (z_ato+za_ato)/2;

  	run;





  	data check_sample;set check_sample;
  	drop z_ac za_AC
  			 z_inv za_inv
  			 z_bm za_BM
  			 z_cfp za_cfp
  			 z_ep za_EP
  			 z_ni za_ni
  			 z_op za_OP
  			 z_dy za_dy
  			 z_sue za_sue
  			 z_rsup za_rsup
  			 z_cash za_cash
  			 z_chcsho za_chcsho
  			 z_rd za_rd
  			 z_cashdebt za_cashdebt
  			 z_pctacc za_pctacc
  			 z_gma za_gma
  			 z_lev za_lev
  			 z_rd_mve za_rd_mve
  			 z_sgr za_sgr
  			 z_sp za_sp
  			 z_invest za_invest
  			 z_rd_sale za_rd_sale
  			 z_ps za_ps
  			 z_lgr za_lgr
  			 z_roa za_roa
  			 z_depr za_depr
  			 z_egr za_egr
  			 z_grltnoa za_grltnoa
  			 z_chpm za_chpm
  			 z_chato za_chato
  			 z_chtx za_chtx
  			 z_ala za_ala
  			 z_alm za_alm
  			 z_noa za_noa
  			 z_rna za_rna
  			 z_pm za_pm
  			 z_ato za_ato
  			 ;
  	run;

  	data check_sample;set check_sample;
  	acc = f_acc;
  	agr = f_agr;
  	bm = f_bm;
  	cfp = f_cfp;
  	ep = f_ep;
  	op = f_op;
  	ni = f_ni;
  	dy = f_dy;
  	sue = f_sue;
  	rsup = f_rsup;
  	cash = f_cash;
  	chcsho = f_chcsho;
  	rd = f_rd;
  	cashdebt = f_cashdebt;
  	pctacc = f_pctacc;
  	gma = f_gma;
  	lev = f_lev;
  	rd_mve = f_rd_mve;
  	sgr = f_sgr;
  	sp = f_sp;
  	invest = f_invest;
  	rd_sale = f_rd_sale;
  	ps = f_ps;
  	lgr = f_lgr;
  	roa = f_roa;
  	depr = f_depr;
  	egr = f_egr;
  	grltnoa = f_grltnoa;
    chpm = f_chpm;
    chato = f_chato;
    chtx = f_chtx;
  	ala = f_ala;
  	alm=f_alm;
  	noa=f_noa;
  	rna=f_rna;
  	pm=f_pm;
  	ato=f_ato;

  	/* rename other variables */
  	me = mcap_crsp;
  	bas = baspread;
  	rvar_ff3 = z_rvar_ff3;
  	rvar_capm = z_rvar_capm;
  	svar = z_rvar_mean;
  	beta = z_beta;
  	mom1m = z_mom1m;
  	mom12m = z_mom12m;
  	mom60m = z_mom60m;
  	mom36m = z_mom36m;
  	mom6m = z_mom6m;
  	moms12m = z_moms12m;
  	run;

  	data check_sample;set check_sample;
  	keep &info &var;
  	run;

  	data check_sample; set check_sample;
  	cusip = cnum;
  	public_date = intnx('month',date,0,'e');
  	format public_date yymmdd10.;
  	run;

  	data check_sample;set check_sample;
  	drop date cnum;
  	run;

  	data chars.firmchars_v6_2_final;
  		set check_sample;
  	run;

  	proc export data=check_sample
  	outfile="/scratch/cityuhk/xintempv6/final.csv" dbms=csv replace; run;

  	*==============================================================================;
  	/*    check data   */

  	/* latest data */
  	data check;
  	set check_sample;
  	where "01JAN2018"d<=public_date<="31JAN2018"d;
  	run;

  	proc sort data=check nodupkey; by permno public_date;run;

  	proc export data=check
  	outfile="/scratch/cityuhk/xintempv6/final2018.csv" dbms=csv replace; run;
