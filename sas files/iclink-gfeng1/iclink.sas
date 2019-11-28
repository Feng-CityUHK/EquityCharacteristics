
libname xinhe '/scratch/uchicago/xinhe/';


/*CRSP-IBES link*/
%iclink (ibesid=ibes.id, crspid=crspq.stocknames, outset=work.iclink);

data xinhe.iclink;
set work.iclink;
run;
