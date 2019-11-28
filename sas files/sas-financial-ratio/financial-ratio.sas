/* ********************************************************************************* */
/* ************** W R D S   R E S E A R C H   A P P L I C A T I O N S ************** */
/* ********************************************************************************* */
/* Summary   : The three-step macro code calculates financial ratios at both firm-   */
/*             level and industry-level. Macro "FinRatio" calculates financial ratio */
/*             at firm-level, macro "FinRatio_Firm" outputs the ratios, and macro    */
/*             "FinRatio_Ind" aggregates financial ratios at user-selected industry  */
/*             level.                                                                */
/*                                                                                   */
/* Date      : Feb 2016                                                              */
/* Author    : Denys Glushkov, WRDS                                                  */
/* Input     :                                                                       */
/*   Universal Inputs                                                                */
/*             - UNI_BEGDT  : Begin Date of the Sample (e.g. 01JAN1990)              */
/*             - UNI_ENDDT  : End Date of the Sample (e.g. 31DEC2015)                */
/*             - UNI_SP500  : Sample Selection S&P500 if=1, CRSP Common Stock if=0   */
/*   Macro FinRatio Input                                                            */
/*             - RATIOS_OUT : Output Data                                            */
/*   Macro FinRatio_Firm Input                                                       */
/*             - FIRMRATIOS : Output Data at Firm-Level                              */
/*   Macro FinRatio_Ind Input                                                        */
/*             - INDCODE    : GICS=GICS 10 Sectors, FF=Fama French Industries        */
/*             - NIND       : Num of Industries (FF only) - 10, 12, 17, 30, 48, 49   */
/*             - AVR        : Choice of Averaging - Median or Mean                   */
/*             - INDRATIOS  : Output Data at Industry-Level                          */
/*                                                                                   */
/*                                                                                   */
/* To run the program, a user should have access to CRSP daily and monthly stock,    */
/* Compustat Annual and Quarterly sets, IBES and CRSP/Compustat Merged database      */
/* ********************************************************************************* */

/* Set Universal Input Variables */
%let uni_begdt = 01JAN2000;
%let uni_enddt = 31DEC2015;
%let uni_sp500 = 1;


%MACRO FINRATIO (begdate=, enddate=, sp500=, ratios_out=);

/*Impose filter to obtain unique gvkey-datadate records*/
%let compcond=indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C';
%if &sp500=1 %then %let sp500_where=and sp500=1; %else %let sp500_where=and 1;

/*List of Ratios to be calculated*/
%let vars=
 pe_op_basic pe_op_dil pe_exi pe_inc ps pcf evm bm capei dpr npm opmbd opmad gpm ptpm cfm roa roe roce aftret_eq aftret_invcapx aftret_equity pretret_noa pretret_earnat
 equity_invcap  debt_invcap totdebt_invcap int_debt int_totdebt cash_lt invt_act rect_act debt_at short_debt curr_debt lt_debt fcf_ocf adv_sale
 profit_lct debt_ebitda ocf_lct lt_ppent dltt_be debt_assets debt_capital de_ratio intcov cash_ratio quick_ratio curr_ratio capital_ratio cash_debt
 inv_turn  at_turn rect_turn pay_turn sale_invcap sale_equity sale_nwc RD_SALE Accrual GProf be cash_conversion efftax intcov_ratio staff_sale;
%let allvars=&vars divyield ptb bm PEG_trailing PEG_1yrforward PEG_ltgforward;
/*Compustat variables to extract*/
%let avars=
  SEQ ceq TXDITC  TXDB ITCB PSTKRV PSTKL PSTK prcc_f csho epsfx epsfi oprepsx opeps ajex ebit spi nopi
  sale ibadj dvc dvp ib oibdp dp oiadp gp revt cogs pi ibc dpc at ni ibcom icapt mib ebitda xsga
  xido xint mii ppent act lct dltt dlc che invt lt rect xopr oancf txp txt ap xrd xad xlr capx;
/*Define which accounting variables are Year-To-Date, usually from income/cash flow statements*/
%let vars_ytd=sale dp capx cogs xido xint xopr ni pi oibdp oiadp opeps oepsx epsfi epsfx ibadj ibcom mii ibc dpc xrd txt spi nopi;

proc sql noprint;
 select distinct lowcase(name) into :qvars separated by ' '
 from dictionary.columns
 where libname='COMP' and memname='FUNDQ' and memtype='DATA'
 and findw(lowcase("&avars."),substr(lowcase(name),1,length(name)-1))>0;
quit;

/*Extracting data for Ratios Based on Annual Data and Quarterly Data*/
data __compa1;
 set comp.funda (keep=gvkey datadate fyear fyr datafmt indfmt consol popsrc prcc_f &avars.);
  where &compcond.;
   if at   <=0 then at   =.;
   if sale <=0 then sale =.;
run;
data __compq1;
 set comp.fundq
  (keep=gvkey datadate fyr fyearq fqtr PRCCQ epsf12 dvy epsfi12
        oepsxq oepsxy oepf12 oeps12 ibadj12 &qvars.);
   if atq  <=0 then atq  =.;
   if saleq<=0 then saleq=.;
run;
/*Quarterize the YTD flow accounting variables*/
%QUARTERIZE(INSET=__compq1, OUTSET=__compq1, IDVAR=gvkey fyr, FYEAR=fyearq, FQTR=fqtr);
/*Calculate annual ratios*/
proc sort data=__compa1 nodupkey; by gvkey fyr datadate; run;
data __compa3; set __compa1;
 by gvkey fyr datadate;
   lagfyear=lag(fyear);
   if first.fyr then lagfyear=.;
   gap=fyear-lagfyear; * year gap between consecutive records;
   pstk_new=coalesce(PSTKRV,PSTKL,PSTK);/*preferred stock*/
 /*Shareholder's Equity, Invested Capital and Operating Cash Flow*/
    if SEQ>0 then BE = sum(SEQ, coalesce(TXDITC,sum(TXDB, ITCB)),-pstk_new);
    if BE<=0 then BE=.;
    if prcc_f*csho>0 then bm = BE/(prcc_f*csho);
    icapt=coalesce(icapt,sum(dltt,pstk,mib,ceq));
    ocf=coalesce(oancf,ib-sum(dif(act),-dif(che),-dif(lct),dif(dlc),dif(txp),-dp));
 /*Annual Valuation Ratios*/
    CAPEI=IB;
    evm=sum(dltt,dlc,mib,pstk_new, prcc_f*csho)/coalesce(ebitda,oibdp,sale-cogs-xsga); /*Enterprise Value Multiple*/
    pe_op_basic=opeps/ajex; /*price-to-operating EPS, excl. EI (basic)*/
    pe_op_dil=oprepsx/ajex; /*price-to-operating EPS, excl. EI (diluted)*/
    pe_exi=epsfx/ajex; /*price-to-earnings, excl. EI (diluted)*/
    pe_inc=epsfi/ajex; /*price-to-earnings, incl. EI (diluted)*/
    ps=sale; /*price-to-sales ratio*/
    pcf=ocf; /*price-to-cash flow*/
    if ibadj>0 then dpr=dvc/ibadj; /*dividend payout ratio*/
 /*Profitability Ratios and Rates of Return*/
    npm=ib/sale;  /*net profit margin*/
    opmbd=coalesce(oibdp,sale-xopr,revt-xopr)/sale;  /*operating profit margin before depreciation*/
    opmad=coalesce(oiadp,oibdp-dp,sale-xopr-dp,revt-xopr-dp)/sale;/*operating profit margin after depreciation*/
    gpm=coalesce(gp,revt-cogs,sale-cogs)/sale; /*gross profit margin*/
    ptpm=coalesce(pi,oiadp-xint+spi+nopi)/sale;  /*pretax profit margin*/
    cfm=coalesce(ibc+dpc,ib+dp)/sale;  /*cash flow margin*/
    roa=coalesce(oibdp,sale-xopr,revt-xopr)/((at+lag(at))/2); /*Return on Assets*/
    if ((be+lag(be))/2)>0 then roe=ib/((be+lag(be))/2); /*Return on Equity*/
    roce=coalesce(ebit,sale-cogs-xsga-dp)/((dltt+lag(dltt)+dlc+lag(dlc)+ceq+lag(ceq))/2); /*Return on Capital Employed*/
    if coalesce(pi,oiadp-xint+spi+nopi)>0 then efftax=txt/coalesce(pi,oiadp-xint+spi+nopi); /*effective tax rate*/
    aftret_eq=coalesce(ibcom,ib-dvp)/((ceq+lag(ceq))/2); /*after tax return on average common equity*/
    if sum(icapt,TXDITC,-mib)>0 then aftret_invcapx=sum(ib+xint,mii)/lag(sum(icapt,TXDITC,-mib)); /*after tax return on invested capital*/
    aftret_equity=ib/((seq+lag(seq))/2); /*after tax return on total stock holder's equity*/
    pretret_noa=coalesce(oiadp,oibdp-dp,sale-xopr-dp,revt-xopr-dp)/((lag(ppent+act-lct)+(ppent+act-lct))/2); /*pretax return on net operating assets*/
    pretret_earnat=coalesce(oiadp,oibdp-dp,sale-xopr-dp,revt-xopr-dp)/((lag(ppent+act)+(ppent+act))/2); /*pretax return on total earning assets*/
    GProf=coalesce(gp,revt-cogs,sale-cogs)/at;  /*gross profitability as % of total assets*/
  /*Capitalization Ratios*/
    if icapt>0 then
      do;
       equity_invcap=ceq/icapt;   /*Common Equity as % of invested capital*/
       debt_invcap=dltt/icapt;    /*Long-term debt as % of invested capital*/
       totdebt_invcap=(dltt+dlc)/icapt;  /*Total Debt as % of invested capital*/
      end;
     capital_ratio=dltt/(dltt+sum(ceq,pstk_new)); /*capitalization ratio*/
  /*Financial Soundness Ratios*/
    int_debt=xint/((dltt+lag(dltt))/2); /*interest as % of average long-term debt*/
    int_totdebt=xint/((dltt+lag(dltt)+dlc+lag(dlc))/2); /*interest as % of average total debt*/
    cash_lt=che/lt; /*Cash balance to Total Liabilities*/
    invt_act=invt/act; /*inventory as % of current assets*/
    rect_act=rect/act; /*receivables as % of current assets*/
    debt_at=(dltt+dlc)/at; /*total debt as % of total assets*/
    debt_ebitda=(dltt+dlc)/coalesce(ebitda,oibdp,sale-cogs-xsga); /*gross debt to ebitda*/
    short_debt=dlc/(dltt+dlc); /*short term term as % of total debt*/
    curr_debt=lct/lt; /*current liabilities as % of total liabilities*/
    lt_debt=dltt/lt; /*long-term debt as % of total liabilities*/
    profit_lct=coalesce(OIBDP,sale-xopr)/lct; /*profit before D&A to current liabilities*/
    ocf_lct=ocf/lct; /*operating cash flow to current liabilities*/
    cash_debt=ocf/coalesce(lt,dltt+dlc);/*operating cash flow to total debt*/
    if ocf>0 then fcf_ocf=(ocf-capx)/ocf;  /*Free Cash Flow/Operating Cash Flow*/
    lt_ppent=lt/ppent; /*total liabilities to total tangible assets*/
    if be>0 then dltt_be=dltt/be; /*long-term debt to book equity*/
  /*Solvency Ratios*/
    debt_assets=lt/at; /*Debt-to-assets*/
    debt_capital=(ap+sum(dlc,dltt))/(ap+sum(dlc,dltt)+sum(ceq,pstk_new)); /*debt-to-capital*/
    de_ratio=lt/sum(ceq,pstk_new); /*debt to shareholders' equity ratio*/
    intcov=(xint+ib)/xint; /*after tax interest coverage*/
    intcov_ratio=coalesce(ebit,OIADP,sale-cogs-xsga-dp)/xint; /*interest coverage ratio*/
  /*Liquidity Ratios*/
   if lct>0 then do;
     cash_ratio=che/lct; /*cash ratio*/
     quick_ratio=coalesce(act-invt, che+rect)/lct; /*quick ratio (acid test)*/
     curr_ratio=coalesce(act,che+rect+invt)/LCT; /*current ratio*/
   end;
   cash_conversion=
   ((invt+lag(invt))/2)/(cogs/365)+((rect+lag(rect))/2)/(sale/365)-((ap+lag(ap))/2)/(cogs/365); /*cash conversion cycle*/
    if cash_conversion<0 then cash_conversion=.;
  /*Activity/Efficiency Ratios*/
   if ((invt+lag(invt))/2)>0 then inv_turn=cogs/((invt+lag(invt))/2);  /*inventory turnover*/
   if ((at+lag(at))/2)>0 then at_turn=sale/((at+lag(at))/2);   /*asset turnover*/
   if ((rect+lag(rect))/2)>0 then rect_turn=sale/((rect+lag(rect))/2); /*receivables turnover*/
   if ((ap+lag(ap))/2)>0 then pay_turn=(cogs+dif(invt))/((ap+lag(ap))/2); /*payables turnover*/
 /*Miscallenous Ratios*/
   if icapt>0 then sale_invcap=sale/icapt; /*sale per $ invested capital*/
   if seq>0 then sale_equity=sale/seq; /*sales per $ total stockholders' equity*/
   if act-lct>=0 then sale_nwc=sale/(act-lct);/*sales per $ working capital*/
   rd_sale=sum(xrd,0)/sale; if rd_sale<0 then rd_sale=0; /*R&D as % of sales*/
   adv_sale=sum(xad,0)/sale; /*advertising as % of sales*/
   staff_sale=sum(xlr,0)/sale; /*labor expense as % of sales*/
   accrual = coalesce(oancf-ib,-sum(dif(act),-dif(che),-dif(lct),dif(dlc),dif(txp),-dp))/mean(AT,lag(AT));

  if first.fyr or gap ne 1 then
   do;
    roa=.;roe=.;roce=.;aftret_eq=.;aftret_invcapx=.;aftret_equity=.;pretret_noa=.;
    pretret_earnat=.;int_debt=.;int_totdebt=.;
    inv_turn=.;at_turn=.;rect_turn=.;cash_conversion=.;
    pay_turn=.;Accrual=.;pcf=.;ocf_lct=.;cash_debt=.;fcf_ocf=.;
   end;
  if at>0;
  rename datadate=adate;
  keep &vars fyear fyr gvkey datadate;
run;

proc sort data=__compa3 nodupkey; by gvkey adate fyr; run;
data __compa4; set __compa3; by gvkey adate; if last.adate; drop fyr; run;
/*Compute the average income before EI over the last 5 years for Shiller's P/E Ratio*/
proc printto log=junk;
proc expand data=__compa4 out=__compa4 method=none;
by gvkey; format adate date9.;
id adate;
convert CAPEI=CAPEI / transformout=(MOVAVE 5 trimleft 3);
quit;
proc printto; run;

%populate(inset=__compa4,outset=__compa4,datevar=adate,idvar=gvkey,forward_max=12);

proc sort data=__compq1 nodupkey; by gvkey fyr fyearq fqtr; run;
%macro ttm(var); (&var + lag1(&var) + lag2(&var) + lag3(&var)) %mend;
%macro mean_year(var); mean(&var, lag1(&var), lag2(&var),lag3(&var)) %mend;

/*Prepare quarterly data: if quarterly Compustat variable is missing, replace with quarterized version*/
data __compq2; set __compq1;
 by gvkey fyr fyearq fqtr;
  if SEQq>0 then BEq = sum(SEQq, TXDITCq, -PSTKq); if BEq<=0 then BEq=.;
  if prccq*cshoq>0 then BMq = BEq/(PRCCq*CSHOq);
  at4=%mean_year(atq);ceq4= %mean_year(ceqq); seq4= %mean_year(seqq);
  lctq4=%mean_year(lctq); be4=%mean_year(beq);
  if gvkey ne lag3(gvkey) or fyr ne lag3(fyr) then do;
    at4=atq;ceq4=ceqq;seq4=seqq;lctq4=lctq;be4=beq; end;
  at5 = mean(atq,lag(atq),lag2(atq),lag3(atq),lag4(atq));
  lctq5=mean(lctq,lag(lctq),lag2(lctq),lag3(lctq),lag4(lctq));
  if gvkey ne lag4(gvkey) or fyr ne lag4(fyr) then do; at5=at4;lctq5=lctq4;end;
  icaptq=coalesce(icaptq,sum(dlttq,pstkq,mibq,ceqq));
  if missing(saleq) then saleq=saley_q;
  SALE= %ttm(Saleq); if SALE<=0 then SALE=.;
  if gvkey ne lag3(gvkey) or fyr ne lag3(fyr) then SALE=.;
  %do i=1 %to %nwords(&vars_ytd); %let var_ytd=%scan(&vars_ytd,&i,%str(' '));
     if missing(&var_ytd.q) then &var_ytd.q=&var_ytd.y_q;
     drop &var_ytd.y_q &var_ytd.y;
  %end;
  if missing(revtq) then revtq=revty_q;
  if missing(revtq) then revtq=saleq;
  if saleq<=0 then saleq=.;if revtq<=0 then revtq=.;
  if missing(ibq) then ibq=iby_q;
  if missing(ibq) then ibq=niq - xidoq;
  if missing(dvq) then dvq=dvy_q;
  if missing(dvpq) then dvpq=dvpy_q;
run;

/*Compute ratios using quarterly data by converting them to TTM values when applicable*/
/*Use mean over the previous 4 quarters for stock accounting variables such as assets, PP&E, etc*/
/*Use TTM values from Compustat Quarterly set whenever available, e.g., epsf12, oeps12, etc*/
/*All per share metrics are adjusted to make them comparable/summable over time*/
/*Price for valuation ratios will be brought in later in the program*/
data __compq3;
set __compq2;
by gvkey fyr fyearq fqtr;
 /* Valuation Ratios*/
    CAPEIq=%ttm(IBq); /*Shiller's P/E*/
    evmq=%mean_year(sum(dlttq,dlcq,mibq,pstkq, prccq*cshoq))/coalesce(%ttm(oibdpq),SALE-%ttm(cogsq)-%ttm(xsgaq)); /*Enterprise Value Multiple*/
    pe_op_basicq=coalesce(oeps12,%ttm(opepsq/ajexq)); /*price-to-operating EPS, excl. EI (basic)*/
    pe_op_dilq=coalesce(oepf12,%ttm(oepsxq/ajexq)); /*price-to-operating EPS, excl. EI (diluted)*/
    pe_exiq=coalesce(epsf12,%ttm(epsfxq/ajexq)); /*price-to-earnings, excl. EI (diluted)*/
    pe_incq=coalesce(epsfi12,%ttm(epsfiq/ajexq)); /*price-to-earnings, incl. EI (diluted)*/
    psq=SALE; /*price-to-sales ratio*/
    opcfq=coalesce(%ttm(oancfy_q),%ttm(ibq)-sum(dif4(actq),-dif4(cheq),-dif4(lctq),dif4(dlcq),dif4(txpq),-%ttm(dpq))); /*operating cash flow*/
    pcfq=opcfq; /*price-to-cash flow*/
    if coalesce(ibadj12,%ttm(ibadjq))>0 then
    dprq=%ttm(sum(dvq,dvpq))/coalesce(ibadj12,%ttm(ibadjq)); /*dividend payout ratio, cash dividends+preferred dividends*/
 /*Profitability Ratios and Rates of Return*/
    npmq=%ttm(ibq)/SALE;  /*net profit margin*/
    opmbdq=coalesce(%ttm(oibdpq),SALE-%ttm(xoprq))/SALE;  /*operating profit margin before depreciation*/
    opmadq=coalesce(%ttm(oiadpq),%ttm(oibdpq-dpq),SALE-%ttm(xoprq)-%ttm(dpq))/SALE;/*operating profit margin after depreciation*/
    gpmq=%ttm(revtq-cogsq)/SALE; /*gross profit margin*/
    ptpmq=coalesce(%ttm(piq),%ttm(oiadpq)-%ttm(xintq)+%ttm(spiq)+%ttm(nopiq))/SALE;  /*pretax profit margin*/
    cfmq=coalesce(%ttm(ibcq+dpcq),%ttm(ibq+dpq))/SALE;  /*cash flow margin*/
    roaq=coalesce(%ttm(oibdpq),SALE-%ttm(xoprq))/lag(at4); /*Return on Assets*/
    roceq=coalesce(%ttm(oiadpq),%ttm(oibdpq)-%ttm(dpq),SALE-%ttm(xoprq)-%ttm(dpq),SALE-%ttm(cogsq)-%ttm(xsgaq)-%ttm(dpq))/lag(%mean_year(dlttq+dlcq+ceqq)); /*Return on Capital Employed*/
    if coalesce(%ttm(piq),%ttm(oiadpq)-%ttm(xintq)+%ttm(spiq)+%ttm(nopiq))>0 then
    efftaxq=%ttm(txtq)/coalesce(%ttm(piq),%ttm(oiadpq)-%ttm(xintq)+%ttm(spiq)+%ttm(nopiq)); /*effective tax rate*/

    lagbe4=lag(be4); lagseq4=lag(seq4); lagicapt4=lag(%mean_year(sum(icaptq,TXDITCq,-mibq)));lagppent4=lag(%mean_year(ppentq+actq-lctq));
    lagppent_alt4=lag(%mean_year(ppentq+actq));

    if first.gvkey or first.fyr then do; lagbe4=be4;lagseq4=seq4;lagicapt4=%mean_year(sum(icaptq,TXDITCq,-mibq));
    lagppent4=%mean_year(ppentq+actq-lctq);lagppent_alt4=%mean_year(ppentq+actq);end;
    if lagbe4>=0 then roeq=%ttm(ibq)/lagbe4; /*Return on Equity*/
    aftret_eqq=coalesce(%ttm(ibcomq),%ttm(ibq-dvpq))/lag(ceq4); /*after tax return on average common equity*/
    if lagicapt4>0 then
    aftret_invcapxq=%ttm(sum(ibq+xintq,miiq))/lagicapt4; /*after tax return on invested capital*/
    aftret_equityq=%ttm(ibq)/lag(seq4); /*after tax return on total stock holder's equity*/
    pretret_noaq=coalesce(%ttm(oiadpq),%ttm(oibdpq-dpq),SALE-%ttm(xoprq)-%ttm(dpq))/lagppent4; /*pretax return on net operating assets*/
    pretret_earnatq=coalesce(%ttm(oiadpq),%ttm(oibdpq-dpq),SALE-%ttm(xoprq)-%ttm(dpq))/lagppent_alt4; /*pretax return on total earning assets*/
    GProfq=%ttm(revtq-cogsq)/at4;  /*gross profitability as % of total assets*/
  /*Capitalization Ratios*/
    if %mean_year(icaptq)>0 then do;
     equity_invcapq=%mean_year(ceqq)/%mean_year(icaptq);   /*Common Equity as % of invested capital*/
     debt_invcapq=%mean_year(dlttq)/%mean_year(icaptq);    /*Long-term debt as % of invested capital*/
     totdebt_invcapq=%mean_year(dlttq+dlcq)/%mean_year(icaptq);  /*Total Debt as % of invested capital*/
     end;
    capital_ratioq=%mean_year(dlttq)/%mean_year(dlttq+sum(ceqq,pstkq)); /*capitalization ratio*/
  /*Financial Soundness Ratios*/
    int_debtq=%ttm(xintq)/%mean_year(dlttq); /*interest as % of average long-term debt*/
    int_totdebtq=%ttm(xintq)/%mean_year(dlttq+dlcq); /*interest as % of average total debt*/
    cash_ltq=%mean_year(cheq)/%mean_year(ltq); /*Cash balance to Total Liabilities*/
    invt_actq=%mean_year(invtq)/%mean_year(actq); /*inventory as % of current assets*/
    rect_actq=%mean_year(rectq)/%mean_year(actq); /*receivables as % of current assets*/
    debt_atq=%mean_year(dlttq+dlcq)/%mean_year(atq); /*total debt as % of total assets*/
    debt_ebitdaq=%mean_year(dlttq+dlcq)/coalesce(%ttm(oibdpq),SALE-%ttm(cogsq)-%ttm(xsgaq)); /*gross debt to ebitda*/
    short_debtq=%mean_year(dlcq)/%mean_year(dlttq+dlcq); /*short term term as % of total debt*/
    curr_debtq=%mean_year(lctq)/%mean_year(ltq); /*current liabilities as % of total liabilities*/
    lt_debtq=%mean_year(dlttq)/%mean_year(ltq); /*long-term debt as % of total liabilities*/
    profit_lctq=coalesce(%ttm(OIBDPq),SALE-%ttm(xoprq))/%mean_year(lctq); /*profit before D&A to current liabilities*/
    ocf_lctq=opcfq/%mean_year(lctq); /*operating cash flow to current liabilities*/
    if opcfq>0 then fcf_ocfq=(opcfq-%ttm(capxq))/opcfq;/*free cash flow to operating cash flow*/
    cash_debtq=coalesce(%ttm(oancfy_q),%ttm(ibq)-sum(dif4(actq),-dif4(cheq),-dif4(lctq),dif4(dlcq),dif4(txpq),-%ttm(dpq)))/%mean_year(ltq);/*cash flow to debt*/
    lt_ppentq=%mean_year(ltq)/%mean_year(ppentq); /*total liabilities to total tangible assets*/
    if %mean_year(beq)>0 then dltt_beq=%mean_year(dlttq)/%mean_year(beq); /*long-term debt to book equity*/
 /*Solvency ratios*/
    debt_assetsq=%mean_year(ltq)/%mean_year(atq); /*Debt-to-assets*/
    debt_capitalq=%mean_year(apq+sum(dlcq,dlttq))/%mean_year(apq+sum(dlcq,dlttq)+sum(ceqq,pstkq)); /*debt-to-capital*/
    de_ratioq=%mean_year(ltq)/%mean_year(sum(ceqq,pstkq)); /*debt to equity ratio*/
    intcovq=%ttm(xintq+ibq)/%ttm(xintq); /*after tax interest coverage*/
    intcov_ratioq=coalesce(%ttm(oiadpq),SALE-%ttm(cogsq)-%ttm(xsgaq)-%ttm(dpq))/%ttm(xintq); /*interest coverage ratio*/
 /*Liquidity Ratios*/
   if %mean_year(lctq)>0 then do;
    cash_ratioq=%mean_year(cheq)/%mean_year(lctq); /*cash ratio*/
    quick_ratioq=coalesce(%mean_year(actq-invtq), %mean_year(cheq+rectq))/%mean_year(lctq); /*quick ratio (acid test)*/
    curr_ratioq=coalesce(%mean_year(actq),%mean_year(cheq+rectq+invtq))/%mean_year(lctq); /*current ratio*/
   end;
   cash_conversionq=(%mean_year(invtq)/(%ttm(cogsq)/365))+(%mean_year(rectq)/(SALE/365))-(%mean_year(apq)/(%ttm(cogsq)/365)); /*cash conversion cycle*/
   if cash_conversionq<0 then cash_conversionq=.;
 /*Activity/Efficiency Ratios*/
   if %mean_year(invtq)>0 then inv_turnq=%ttm(cogsq)/%mean_year(invtq);  /*inventory turnover*/
   if at4>0 then at_turnq=SALE/at4;   /*asset turnover*/
   if %mean_year(rectq)>0 then rect_turnq=SALE/%mean_year(rectq); /*receivables turnover*/
   if %mean_year(apq)>0 then pay_turnq=(%ttm(cogsq)+dif4(invtq))/%mean_year(apq); /*payables turnover*/
 /*Miscallenous Ratios*/
   if %mean_year(icaptq)>0 then sale_invcapq=SALE/%mean_year(icaptq); /*sale per $ invested capital*/
   if seq4>0 then sale_equityq=SALE/seq4; /*sales per $ total stockholders' equity*/
   if %mean_year(actq-lctq)>=0 then sale_nwcq=SALE/%mean_year(actq-lctq);/*sales per $ working capital*/
   rd_saleq=%ttm(sum(xrdq,0))/SALE; if rd_saleq<0 then rd_saleq=0; /*R&D as % of sales*/
   Accrualq = coalesce(%ttm(oancfy_q-ibq),-sum(dif4(actq),-dif4(cheq),-dif4(lctq),dif4(dlcq),dif4(txpq),-%ttm(dpq)))/at5;

 if gvkey ne lag3(gvkey) or fyr ne lag3(fyr) or sum(%ttm(fqtr)) ne 10 then
   do;
        pe_op_basicq=.; pe_op_dilq=.;pe_exiq=.;pe_incq=.;psq=.;pcfq=.;evmq=.;dprq=.;npmq=.;opmbdq=.;opmadq=.;gpmq=.;ptpmq=.;cfmq=.;intcov_ratioq=.;
        GProfq=.;equity_invcapq=.; debt_invcapq=.;capital_ratioq=.;totdebt_invcapq=.;int_debtq=.;int_totdebtq=.;cash_ltq=.;invt_actq=.;
        rect_actq=.;debt_atq=.;short_debtq=.;curr_debtq=.;lt_debtq=.;profit_lctq=.;ocf_lctq=.;lt_ppentq=.;dltt_beq=.;efftaxq=.;fcf_ocfq=.;
        debt_assetsq=.;debt_capitalq=.;de_ratioq=.;intcovq=.;cash_ratioq=.;quick_ratioq=.;curr_ratioq=.;inv_turnq=.;cash_debtq=.;
        at_turnq=.;rect_turnq=.;pay_turnq=.;sale_invcapq=.;sale_equityq=.;sale_nwcq=.;RD_SALEq=.;Accrualq=.;cash_conversionq=.;debt_ebitdaq=.;
    end;
/*Return on "smth" ratios always use lagged scalers, therefore, more lags are needed*/
  if gvkey ne lag4(gvkey) or fyr ne lag4(fyr) then
   do;
      roaq=.; roeq=.;roceq=.; aftret_eqq=.;aftret_invcapxq=.;aftret_equityq=.;pretret_noaq=.;pretret_earnatq=.;
   end;

  keep gvkey fyr fyearq fqtr datadate beq bmq CAPEIq evmq pe_op_basicq pe_op_dilq pe_incq pe_exiq psq pcfq dprq npmq opmbdq opmadq gpmq ptpmq intcov_ratioq
        cfmq roaq roeq roceq aftret_eqq aftret_invcapxq aftret_equityq pretret_noaq pretret_earnatq equity_invcapq  debt_invcapq fcf_ocfq
        totdebt_invcapq int_debtq int_totdebtq cash_ltq invt_actq rect_actq debt_atq short_debtq curr_debtq lt_debtq capital_ratioq
        profit_lctq ocf_lctq lt_ppentq dltt_beq debt_assetsq debt_capitalq de_ratioq intcovq cash_ratioq quick_ratioq curr_ratioq debt_ebitdaq
        inv_turnq CAPEIq at_turnq rect_turnq pay_turnq sale_invcapq sale_equityq sale_nwcq RD_SALEq Accrualq GProfq cash_conversionq;
   rename datadate=qdate;
run;

proc sort data=__compq3 nodupkey; by gvkey qdate fyr; run;
data __compq4; set __compq3; by gvkey qdate; if last.qdate; drop fyr fyearq fqtr; run;
proc sort data=__compq4 nodupkey; by gvkey qdate; run;

/*Calculate moving average income before EI over previous 20 quarters (5 years)*/
proc printto log=junk;
proc expand data=__compq4 out=__compq4 method=none;
 by gvkey;id qdate;
 convert CAPEIq=CAPEIq/ transformout=(MOVAVE 20 trimleft 12);
quit;
proc printto; run;

%populate(inset=__compq4,outset=__compq4,datevar=qdate,idvar=gvkey,forward_max=12);

%let aratios=&vars;
%let aratios=%sysfunc(compbl(&aratios.));
%let qratios=%sysfunc(tranwrd(&aratios. %str(),%str( ),%str(q )));
%let nratios=%nwords(&aratios);
/*Merge populated Annual and Quarterly data and always pick the most recently available metric*/
data __comp1;
 merge __compa4 __compq4;
  by gvkey mdate;
  /* Populate Variables */
   array annratio {&nratios} &aratios;
   array qtrratio {&nratios} &qratios;
   do i=1 to &nratios;
     if not missing(qtrratio(i)) and qdate>adate then annratio(i)=qtrratio(i);
   end;
  /*date when the information becomes public*/
  public_date=intnx("month",mdate,2,"e"); format public_date date9.;
  drop i &qratios;
run;

/*Populate Historical SIC codes into monthly frequency*/
%populate(inset=comp.co_industry,outset=sich,datevar=datadate,idvar=gvkey fyr,forward_max=12);
/*Populate Compustat shares outstanding data CSHOQ into monthly frequency, CSHOM is often missing*/
data comp_shares/view=comp_shares; set  comp.co_ifndq;
  where &compcond.;
  keep gvkey datadate cshoq;
run;
%populate(inset=comp_shares,outset=shares_comp,datevar=datadate,idvar=gvkey,forward_max=3);

/*Get pricing for primary US common shares from Security Monthly table*/
proc sql;
  create table prc_shares_comp
   as select distinct a.*, b.prc_comp_unadj, b.prc_comp_adj, b.cshom, b.dvrate
  from shares_comp a inner join
   (select distinct gvkey, iid, datadate, prccm as prc_comp_unadj, (prccm/ajexm) as prc_comp_adj, cshom, dvrate from
   comp.secm where tpci='0' and fic='USA' and primiss='P') b
   on a.gvkey=b.gvkey and a.mdate=intnx('month',b.datadate,0,'e');
quit;
proc sort data=prc_shares_comp nodupkey; by gvkey mdate;run;
/*Grab Historical GICS*/
proc sql;
  create view gics
    as select a.gvkey, a.gsector, a.indfrom, a.indthru, b.gicdesc
  from comp.co_hgic a, comp.r_giccd b
  where a.gsector=b.giccd and b.gictype='GSECTOR';
/*Merge in historical SIC from CRSP&Compustat and use S&P GICS; Link with CRSP Permno using CCM*/
create view __comp2
  as select distinct c.lpermno as permno, a.*, coalesce(b.sich, d.siccd) as sic
 from __comp1 a left join sich (where=(consol='C' and popsrc='D')) b
   on a.gvkey=b.gvkey and a.mdate=b.mdate
 inner join
   crsp.ccmxpf_linktable (where=(usedflag=1 and linkprim in ('P','C'))) c
   on a.gvkey=c.gvkey and (c.linkdt<=a.mdate<=c.linkenddt or (c.linkdt<=a.mdate and missing(c.linkenddt)))
 inner join (select distinct permno, siccd, shrcd, min(namedt) as mindate, max(nameenddt) as maxdate
 from crsp.stocknames where shrcd in (10,11) group by permno, shrcd, siccd) d
   on c.lpermno=d.permno and d.mindate<=a.mdate<=d.maxdate;

/*Calculate market value using CRSP and Compustat separately as of date when finstatements become available*/
/*Merge in labels for GICS sectors and define S&P 500 members*/
create table __comp3
  as select distinct a.*,
   c.dvrate, abs(b.prc)*b.shrout/1000 as mcap_crsp,
   (c.cshoq*c.prc_comp_unadj) as mcap_comp,
   abs(b.prc) as prc_crsp_unadj, c.prc_comp_unadj,
   (abs(prc)/b.CFACPR) as prc_crsp_adj, c.prc_comp_adj,
   d.gsector, d.gicdesc, not missing(e.gvkeyx) as sp500
 from __comp2 a left join crsp.msf b
   on a.permno=b.permno and a.public_date=intnx('month',b.date,0,'e')
 left join prc_shares_comp c
   on a.gvkey=c.gvkey and a.public_date=c.mdate
 left join gics d
   on a.gvkey=d.gvkey and
      (d.indfrom<=a.public_date<=d.indthru or (d.indfrom<=a.public_date and missing(d.indthru)))
 left join comp.idxcst_his (where=(gvkeyx='000003')) e
   on a.gvkey=e.gvkey and
      (e.from<=a.public_date<=e.thru or (e.from<=a.public_date and missing(e.thru)))
 order by a.gvkey, a.public_date;
quit;

proc sort data=__comp3 nodupkey; by gvkey public_date;run;

/*future EPS and annual EPS growth rate from IBES*/
proc sort data=ibes.act_epsus (keep=ticker pends pdicity anndats value) out=actuals nodupkey;
  where PDICITY='ANN' and not missing(value);
  by ticker PENDS anndats;
run;

data actuals; set actuals;
 by ticker pends;
  lagpends=lag(pends); lagvalue=lag(value); laganndats=lag(anndats);
  format lagpends date9. laganndats date9.;
  if first.ticker then do; lagpends=intnx('month',pends,-12,'e'); lagvalue=.; laganndats=.;end;
run;
/*Prepare IBES file for computing forward 1 year and LTG EPS growth*/
proc sql;
  create table eps_growth (where=(not missing(datadate) and not missing(gvkey)))
   as select distinct d.gvkey, a.ticker, b.lagpends as datadate,b.laganndats as current_anndate,
      b.lagvalue as current_actual, a.statpers, a.fpedats as futdate, a.actual as fut_actual,
      a.meanest as fut_eps, a.anndats_act as fut_anndate, c.meanest as ltg_eps
 from ibes.statsum_epsus (where=(fpi='1' and FISCALP='ANN' and CURR_ACT='USD')) a
  left join actuals b
   on a.ticker=b.ticker and a.fpedats=b.pends
  left join ibes.statsum_epsus (where=(fpi='0' and FISCALP='LTG')) c
   on a.ticker=c.ticker and a.statpers=c.statpers
  left join (select distinct gvkey, ibtic from comp.security where not missing(ibtic)) d
   on a.ticker=d.ibtic
 order by a.ticker, datadate, a.statpers;
quit;

data eps_growth; set eps_growth;
  public_date=intnx('month',statpers,0,'e');
  /*This is expected EPS growth as of given month*/
  futepsgrowth=100*(fut_eps-current_actual)/abs(current_actual);
  format statpers date9. public_date date9.;
  if current_anndate<statpers<fut_anndate or nmiss(current_anndate,fut_anndate)>0;
 keep gvkey public_date ltg_eps current_actual futepsgrowth;
run;
proc sort data=eps_growth nodupkey; by gvkey public_date;run;

/*Merge Accounting data with Pricing data and compute valuation ratios*/
/*NB: all prices are as of date t, whereas all accounting data are as of t-2 months*/
/*All P/E ratio variations use adjusted price scaled by adjusted EPS*/
data ratios;
  retain gvkey permno adate qdate public_date;
 merge __comp3 eps_growth;
  by gvkey public_date;
  /*use CRSP data first, if available*/
    mktcap=coalesce(mcap_crsp, mcap_comp);
    price=coalesce(prc_crsp_unadj, prc_comp_unadj);
    capei=(mktcap/capei); /*Shiller's CAPE*/
    if be>0 then ptb=mktcap/be; else ptb=.;/*price-to-book*/
    bm=coalesce(BM,BE/mktcap); if bm<0 then bm=.; /*book-to-market*/
    /*in the definition of trailing PEG ratio in the line below PE_EXI is the adjusted diluted EPS excluding EI, not PE*/
    eps3yr_growth=mean(pe_exi/lag12(pe_exi)-1,lag12(pe_exi)/lag24(pe_exi)-1,lag24(pe_exi)/lag36(pe_exi)-1);/*3-yr past EPS growth*/
    if eps3yr_growth>0 then
    PEG_trailing=(prc_comp_adj/pe_exi)/(100*eps3yr_growth); /*trailing PEG Ratio*/
    if gvkey ne lag36(gvkey) or eps3yr_growth<0 then PEG_trailing=.;
    pe_op_basic=(prc_comp_adj/pe_op_basic); /*price-to-operating EPS, excl. EI (basic)*/
    pe_op_dil=(prc_comp_adj/pe_op_dil); /*price-to-operating EPS, excl. EI (diluted)*/
    /*now PE_EXI becomes the actual Price-to-Earnings ratio after adjusted Compustat price is used in the numerator of the ratio*/
    pe_exi=(prc_comp_adj/pe_exi); /*price-to-earnings, excl. EI (diluted)*/
    pe_inc=(prc_comp_adj/pe_inc); /*price-to-earnings, incl. EI (diluted)*/
    ps=(mktcap/ps); /*price-to-sales ratio*/
    pcf=(mktcap/pcf); /*price-to-cash flow ratio*/
    divyield=DVRATE/price; /*dividend yield*/ if divyield<0 then divyield=.;
    /*forward PEG Ratios*/
    /*Assume PEG ratios are negative whenver expected EPS growth is negative*/
    if sign(pe_exi)=-1 and sign(futepsgrowth)=-1 then PEG_1yrforward=-(pe_exi/futepsgrowth); else PEG_1yrforward=pe_exi/futepsgrowth;
    if sign(pe_exi)=-1 and sign(ltg_eps)=-1 then PEG_ltgforward=-(pe_exi/ltg_eps); else PEG_ltgforward=pe_exi/ltg_eps;
    /*define FF industries*/
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

    /*format all ratios*/
    format %do i=1 %to %nwords(&allvars); %scan(&allvars, &i, %str(' ')) comma7.3 %end;;
    format divyield percent7.4 qdate date9. adate date9.;
   label
    public_date='Date'
    adate='Fiscal year end'
    qdate='Fiscal quarter end'
    sic='Historical SIC code'
    mktcap='Market Capitalization, $mil.'
    ptb='Price/Book'
    bm='Book/Market'
    divyield='Dividend Yield'
    peg_trailing='Trailing P/E to Growth (PEG) ratio'
    PEG_1yrforward='Forward P/E to 1-year Growth (PEG) ratio'
    PEG_ltgforward='Forward P/E to Long-term Growth (PEG) ratio'
    CAPEI='Shillers Cyclically Adjusted P/E Ratio'
    pe_op_basic='Price/Operating Earnings (Basic, Excl. EI)'
    pe_op_dil='Price/Operating Earnings (Diluted, Excl. EI)'
    pe_exi='P/E (Diluted, Excl. EI)'
    pe_inc='P/E (Diluted, Incl. EI)'
    evm='Enterprise Value Multiple'
    ps='Price/Sales'
    pcf='Price/Cash flow'
    dpr='Dividend Payout Ratio'
    npm='Net Profit Margin'
    opmbd='Operating Profit Margin Before Depreciation'
    opmad='Operating Profit Margin After Depreciation'
    gpm='Gross Profit Margin'
    ptpm='Pre-tax Profit Margin'
    cfm='Cash Flow Margin'
    efftax='Effective Tax Rate'
    ROA='Return on Assets'
    ROE='Return on Equity'
    ROCE='Return on Capital Employed'
    aftret_eq='After-tax Return on Average Common Equity'
    aftret_invcapx='After-tax Return on Invested Capital'
    aftret_equity='After-tax Return on Total Stockholders Equity'
    pretret_noa='Pre-tax return on Net Operating Assets'
    pretret_earnat='Pre-tax Return on Total Earning Assets'
    Gprof='Gross Profit/Total Assets'
    equity_invcap='Common Equity/Invested Capital'
    debt_invcap='Long-term Debt/Invested Capital'
    totdebt_invcap='Total Debt/Invested Capital'
    debt_ebitda='Total Debt/EBITDA'
    int_debt='Interest/Average Long-term Debt'
    int_totdebt='Interest/Average Total Debt'
    cash_lt='Cash Balance/Total Liabilities'
    invt_act='Inventory/Current Assets'
    rect_act='Receivables/Current Assets'
    debt_at='Total Debt/Total Assets'
    cash_debt='Cash Flow to Total Liabilities'
    short_debt='Short-Term Debt/Total Debt'
    curr_debt='Current Liabilities/Total Liabilities'
    lt_debt='Long-term Debt/Total Liabilities'
    profit_lct='Profit Before Depreciation/Current Liabilities'
    ocf_lct='Operating CF/Current Liabilities'
    fcf_ocf='Free Cash Flow/Operating Cash Flow'
    capital_ratio='Capitalization Ratio'
    lt_ppent='Total Liabilities/Total Tangible Assets'
    dltt_be='Long-term Debt/Book Equity'
    debt_assets='Total Debt/Total Assets'
    debt_capital='Total Debt/Capital'
    de_ratio='Total Debt/Equity'
    intcov='After-tax Interest Coverage'
    intcov_ratio='Interest Coverage Ratio'
    cash_ratio='Cash Ratio'
    cash_debt='Cash Flow/Total Debt'
    quick_ratio='Quick Ratio (Acid Test)'
    curr_ratio='Current Ratio'
    cash_conversion='Cash Conversion Cycle (Days)'
    inv_turn='Inventory Turnover'
    at_turn='Asset Turnover'
    rect_turn='Receivables Turnover'
    pay_turn='Payables Turnover'
    sale_invcap='Sales/Invested Capital'
    sale_equity='Sales/Stockholders Equity'
    sale_nwc='Sales/Working Capital'
    rd_sale='Research and Development/Sales'
    adv_sale='Avertising Expenses/Sales'
    staff_sale='Labor Expenses/Sales'
    accrual='Accruals/Average Assets';
if mktcap>0;
keep gvkey permno adate qdate public_date &allvars gsector gicdesc sp500
ffi5 ffi5_desc ffi10 ffi10_desc ffi12 ffi12_desc ffi17 ffi17_desc
ffi30 ffi30_desc ffi38 ffi38_desc ffi48 ffi48_desc ffi49 ffi49_desc
;
run;

/*Apply Winsorization (instead of truncation) to firm-level ratio*/
/*And do not take a 12-month moving average to smooth the ratio*/
%WINSORIZE (INSET=ratios,OUTSET=ratios,SORTVAR=public_date,
            VARS=ptb PEG_trailing pe_op_basic pe_op_dil pe_exi pe_inc ps pcf PEG_ltgforward
            PEG_1yrforward,PERC1=1,TRIM=0);

proc sort data=ratios nodupkey; by gvkey public_date;run;

proc sort data=ratios nodupkey out=&ratios_out;
where "&begdate"d<=public_date<="&enddate"d;
by public_date gvkey;run;

proc sql;
  drop table actuals, eps_growth, prc_shares_comp, ratios, shares_comp, sich, __comp1, __comp3,
       __compa1, __compa3, __compa4, __compq1, __compq2, __compq3, __compq4;
  drop view comp_shares, gics, __comp2;
quit;

%mend FINRATIO;


%MACRO FINRATIO_firm (begdate=, enddate=, sp500=, firmratios=);
%if &sp500=1 %then %let sp500_where=and sp500=1; %else %let sp500_where=and 1;
data &firmratios;
set firm_ratio;
where "&begdate"d<=public_date<="&enddate"d &sp500_where;
run;
%mend FINRATIO_firm;


%MACRO FINRATIO_ind (begdate=, enddate=, sp500=, indcode=, nind=, avr=, indratios=);
/*Impose filter to obtain unique gvkey-datadate records*/
%let compcond=indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C';
%if &sp500=1 %then %let sp500_where=and sp500=1; %else %let sp500_where=and 1;
%if %lowcase(&indcode)=gics %then %let indclass=gicdesc;%else %let indclass=ffi&nind._desc;
/*List of Ratios to be calculated*/
%let vars=
 pe_op_basic pe_op_dil pe_exi pe_inc ps pcf evm bm capei dpr npm opmbd opmad gpm ptpm cfm roa roe roce aftret_eq aftret_invcapx aftret_equity pretret_noa pretret_earnat
 equity_invcap  debt_invcap totdebt_invcap int_debt int_totdebt cash_lt invt_act rect_act debt_at short_debt curr_debt lt_debt fcf_ocf adv_sale
 profit_lct debt_ebitda ocf_lct lt_ppent dltt_be debt_assets debt_capital de_ratio intcov cash_ratio quick_ratio curr_ratio capital_ratio cash_debt
 inv_turn  at_turn rect_turn pay_turn sale_invcap sale_equity sale_nwc RD_SALE Accrual GProf be cash_conversion efftax intcov_ratio staff_sale;
%let allvars=&vars divyield ptb bm PEG_trailing PEG_1yrforward PEG_ltgforward;

data ratios;
set firm_ratio;
/*set time frame*/
where "&begdate"d<=public_date<="&enddate"d;
run;

proc sort data = ratios; by public_date &indclass; run;
/*Computing Industry-level average financial ratios in a given month*/
proc means data=ratios noprint;
  where not missing(&indclass) &sp500_where;
    by public_date; class &indclass;
     var &allvars;
    output out=indratios &avr=/autoname;
run;
proc sort data=indratios; by public_date &indclass;run;

data &indratios; set indratios;
where &indclass ne '';
drop _type_;
run;

proc sql; drop table ratios, indratios;
quit;

%mend FINRATIO_ind;


%FINRATIO      (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, SP500=&uni_sp500, RATIOS_OUT=firm_ratio);
%FINRATIO_firm (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, SP500=&uni_sp500, FirmRatios=firm_output);
%FINRATIO_ind  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, SP500=&uni_sp500, INDCODE=ff, NIND=12, AVR=median, IndRatios=ind_output);

/* ********************************************************************************* */
/* *************  Material Copyright Wharton Research Data Services  *************** */
/* ****************************** All Rights Reserved ****************************** */
/* ********************************************************************************* */
