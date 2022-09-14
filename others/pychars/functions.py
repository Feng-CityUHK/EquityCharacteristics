import pandas as pd
import pickle as pkl
import numpy as np
import re

def ffi49(df):
    condlist = [((100 <= df['sic']) & (df['sic'] <= 199)) | ((200 <= df['sic']) & (df['sic'] <= 299)) |
                ((700 <= df['sic']) & (df['sic'] <= 799)) | ((910 <= df['sic']) & (df['sic'] <= 919)) |
                ((2048 <= df['sic']) & (df['sic'] <= 2048)),
                ((2000 <= df['sic']) & (df['sic'] <= 2009)) | ((2010 <= df['sic']) & (df['sic'] <= 2019)) |
                ((2020 <= df['sic']) & (df['sic'] <= 2029)) | ((2030 <= df['sic']) & (df['sic'] <= 2039)) |
                ((2040 <= df['sic']) & (df['sic'] <= 2046)) | ((2050 <= df['sic']) & (df['sic'] <= 2059)) |
                ((2060 <= df['sic']) & (df['sic'] <= 2063)) | ((2070 <= df['sic']) & (df['sic'] <= 2079)) |
                ((2090 <= df['sic']) & (df['sic'] <= 2092)) | ((2095 <= df['sic']) & (df['sic'] <= 2095)) |
                ((2098 <= df['sic']) & (df['sic'] <= 2099)),
                ((2064 <= df['sic']) & (df['sic'] <= 2068)) | ((2086 <= df['sic']) & (df['sic'] <= 2086)) |
                ((2087 <= df['sic']) & (df['sic'] <= 2087)) | ((2096 <= df['sic']) & (df['sic'] <= 2096)) |
                ((2097 <= df['sic']) & (df['sic'] <= 2097)),
                ((2080 <= df['sic']) & (df['sic'] <= 2080)) | ((2082 <= df['sic']) & (df['sic'] <= 2082)) |
                ((2083 <= df['sic']) & (df['sic'] <= 2083)) | ((2084 <= df['sic']) & (df['sic'] <= 2084)) |
                ((2085 <= df['sic']) & (df['sic'] <= 2085)),
                ((2100 <= df['sic']) & (df['sic'] <= 2199)),
                ((920 <= df['sic']) & (df['sic'] <= 999)) | ((3650 <= df['sic']) & (df['sic'] <= 3651)) |
                ((3652 <= df['sic']) & (df['sic'] <= 3652)) | ((3732 <= df['sic']) & (df['sic'] <= 3732)) |
                ((3930 <= df['sic']) & (df['sic'] <= 3931)) | ((3940 <= df['sic']) & (df['sic'] <= 3949)),
                ((7800 <= df['sic']) & (df['sic'] <= 7829)) | ((7830 <= df['sic']) & (df['sic'] <= 7833)) |
                ((7840 <= df['sic']) & (df['sic'] <= 7841)) | ((7900 <= df['sic']) & (df['sic'] <= 7900)) |
                ((7910 <= df['sic']) & (df['sic'] <= 7911)) | ((7920 <= df['sic']) & (df['sic'] <= 7929)) |
                ((7930 <= df['sic']) & (df['sic'] <= 7933)) | ((7940 <= df['sic']) & (df['sic'] <= 7949)) |
                ((7980 <= df['sic']) & (df['sic'] <= 7980)) | ((7990 <= df['sic']) & (df['sic'] <= 7999)),
                ((2700 <= df['sic']) & (df['sic'] <= 2709)) | ((2710 <= df['sic']) & (df['sic'] <= 2719)) |
                ((2720 <= df['sic']) & (df['sic'] <= 2729)) | ((2730 <= df['sic']) & (df['sic'] <= 2739)) |
                ((2740 <= df['sic']) & (df['sic'] <= 2749)) | ((2770 <= df['sic']) & (df['sic'] <= 2771)) |
                ((2780 <= df['sic']) & (df['sic'] <= 2789)) | ((2790 <= df['sic']) & (df['sic'] <= 2799)),
                ((2047 <= df['sic']) & (df['sic'] <= 2047)) | ((2391 <= df['sic']) & (df['sic'] <= 2392)) |
                ((2510 <= df['sic']) & (df['sic'] <= 2519)) | ((2590 <= df['sic']) & (df['sic'] <= 2599)) |
                ((2840 <= df['sic']) & (df['sic'] <= 2843)) | ((2844 <= df['sic']) & (df['sic'] <= 2844)) |
                ((3160 <= df['sic']) & (df['sic'] <= 3161)) | ((3170 <= df['sic']) & (df['sic'] <= 3171)) |
                ((3172 <= df['sic']) & (df['sic'] <= 3172)) | ((3190 <= df['sic']) & (df['sic'] <= 3199)) |
                ((3229 <= df['sic']) & (df['sic'] <= 3229)) | ((3260 <= df['sic']) & (df['sic'] <= 3260)) |
                ((3262 <= df['sic']) & (df['sic'] <= 3263)) | ((3269 <= df['sic']) & (df['sic'] <= 3269)) |
                ((3230 <= df['sic']) & (df['sic'] <= 3231)) | ((3630 <= df['sic']) & (df['sic'] <= 3639)) |
                ((3750 <= df['sic']) & (df['sic'] <= 3751)) | ((3800 <= df['sic']) & (df['sic'] <= 3800)) |
                ((3860 <= df['sic']) & (df['sic'] <= 3861)) | ((3870 <= df['sic']) & (df['sic'] <= 3873)) |
                ((3910 <= df['sic']) & (df['sic'] <= 3911)) | ((3914 <= df['sic']) & (df['sic'] <= 3914)) |
                ((3915 <= df['sic']) & (df['sic'] <= 3915)) | ((3960 <= df['sic']) & (df['sic'] <= 3962)) |
                ((3991 <= df['sic']) & (df['sic'] <= 3991)) | ((3995 <= df['sic']) & (df['sic'] <= 3995)),
                ((2300 <= df['sic']) & (df['sic'] <= 2390)) | ((3020 <= df['sic']) & (df['sic'] <= 3021)) |
                ((3100 <= df['sic']) & (df['sic'] <= 3111)) | ((3130 <= df['sic']) & (df['sic'] <= 3131)) |
                ((3140 <= df['sic']) & (df['sic'] <= 3149)) | ((3150 <= df['sic']) & (df['sic'] <= 3151)) |
                ((3963 <= df['sic']) & (df['sic'] <= 3965)),
                ((8000 <= df['sic']) & (df['sic'] <= 8099)),
                ((3693 <= df['sic']) & (df['sic'] <= 3693)) | ((3840 <= df['sic']) & (df['sic'] <= 3849)) |
                ((3850 <= df['sic']) & (df['sic'] <= 3851)),
                ((2830 <= df['sic']) & (df['sic'] <= 2830)) | ((2831 <= df['sic']) & (df['sic'] <= 2831)) |
                ((2833 <= df['sic']) & (df['sic'] <= 2833)) | ((2834 <= df['sic']) & (df['sic'] <= 2834)) |
                ((2835 <= df['sic']) & (df['sic'] <= 2835)) | ((2836 <= df['sic']) & (df['sic'] <= 2836)),
                ((2800 <= df['sic']) & (df['sic'] <= 2809)) | ((2810 <= df['sic']) & (df['sic'] <= 2819)) |
                ((2820 <= df['sic']) & (df['sic'] <= 2829)) | ((2850 <= df['sic']) & (df['sic'] <= 2859)) |
                ((2860 <= df['sic']) & (df['sic'] <= 2869)) | ((2870 <= df['sic']) & (df['sic'] <= 2879)) |
                ((2890 <= df['sic']) & (df['sic'] <= 2899)),
                ((3031 <= df['sic']) & (df['sic'] <= 3031)) | ((3041 <= df['sic']) & (df['sic'] <= 3041)) |
                ((3050 <= df['sic']) & (df['sic'] <= 3053)) | ((3060 <= df['sic']) & (df['sic'] <= 3069)) |
                ((3070 <= df['sic']) & (df['sic'] <= 3079)) | ((3080 <= df['sic']) & (df['sic'] <= 3089)) |
                ((3090 <= df['sic']) & (df['sic'] <= 3099)),
                ((2200 <= df['sic']) & (df['sic'] <= 2269)) | ((2270 <= df['sic']) & (df['sic'] <= 2279)) |
                ((2280 <= df['sic']) & (df['sic'] <= 2284)) | ((2290 <= df['sic']) & (df['sic'] <= 2295)) |
                ((2297 <= df['sic']) & (df['sic'] <= 2297)) | ((2298 <= df['sic']) & (df['sic'] <= 2298)) |
                ((2299 <= df['sic']) & (df['sic'] <= 2299)) | ((2393 <= df['sic']) & (df['sic'] <= 2395)) |
                ((2397 <= df['sic']) & (df['sic'] <= 2399)),
                ((800 <= df['sic']) & (df['sic'] <= 899)) | ((2400 <= df['sic']) & (df['sic'] <= 2439)) |
                ((2450 <= df['sic']) & (df['sic'] <= 2459)) | ((2490 <= df['sic']) & (df['sic'] <= 2499)) |
                ((2660 <= df['sic']) & (df['sic'] <= 2661)) | ((2950 <= df['sic']) & (df['sic'] <= 2952)) |
                ((3200 <= df['sic']) & (df['sic'] <= 3200)) | ((3210 <= df['sic']) & (df['sic'] <= 3211)) |
                ((3240 <= df['sic']) & (df['sic'] <= 3241)) | ((3250 <= df['sic']) & (df['sic'] <= 3259)) |
                ((3261 <= df['sic']) & (df['sic'] <= 3261)) | ((3264 <= df['sic']) & (df['sic'] <= 3264)) |
                ((3270 <= df['sic']) & (df['sic'] <= 3275)) | ((3280 <= df['sic']) & (df['sic'] <= 3281)) |
                ((3290 <= df['sic']) & (df['sic'] <= 3293)) | ((3295 <= df['sic']) & (df['sic'] <= 3299)) |
                ((3420 <= df['sic']) & (df['sic'] <= 3429)) | ((3430 <= df['sic']) & (df['sic'] <= 3433)) |
                ((3440 <= df['sic']) & (df['sic'] <= 3441)) | ((3442 <= df['sic']) & (df['sic'] <= 3442)) |
                ((3446 <= df['sic']) & (df['sic'] <= 3446)) | ((3448 <= df['sic']) & (df['sic'] <= 3448)) |
                ((3449 <= df['sic']) & (df['sic'] <= 3449)) | ((3450 <= df['sic']) & (df['sic'] <= 3451)) |
                ((3452 <= df['sic']) & (df['sic'] <= 3452)) | ((3490 <= df['sic']) & (df['sic'] <= 3499)) |
                ((3996 <= df['sic']) & (df['sic'] <= 3996)),
                ((1500 <= df['sic']) & (df['sic'] <= 1511)) | ((1520 <= df['sic']) & (df['sic'] <= 1529)) |
                ((1530 <= df['sic']) & (df['sic'] <= 1539)) | ((1540 <= df['sic']) & (df['sic'] <= 1549)) |
                ((1600 <= df['sic']) & (df['sic'] <= 1699)) | ((1700 <= df['sic']) & (df['sic'] <= 1799)),
                ((3300 <= df['sic']) & (df['sic'] <= 3300)) | ((3310 <= df['sic']) & (df['sic'] <= 3317)) |
                ((3320 <= df['sic']) & (df['sic'] <= 3325)) | ((3330 <= df['sic']) & (df['sic'] <= 3339)) |
                ((3340 <= df['sic']) & (df['sic'] <= 3341)) | ((3350 <= df['sic']) & (df['sic'] <= 3357)) |
                ((3360 <= df['sic']) & (df['sic'] <= 3369)) | ((3370 <= df['sic']) & (df['sic'] <= 3379)) |
                ((3390 <= df['sic']) & (df['sic'] <= 3399)),
                ((3400 <= df['sic']) & (df['sic'] <= 3400)) | ((3443 <= df['sic']) & (df['sic'] <= 3443)) |
                ((3444 <= df['sic']) & (df['sic'] <= 3444)) | ((3460 <= df['sic']) & (df['sic'] <= 3469)) |
                ((3470 <= df['sic']) & (df['sic'] <= 3479)),
                ((3510 <= df['sic']) & (df['sic'] <= 3519)) | ((3520 <= df['sic']) & (df['sic'] <= 3529)) |
                ((3530 <= df['sic']) & (df['sic'] <= 3530)) | ((3531 <= df['sic']) & (df['sic'] <= 3531)) |
                ((3532 <= df['sic']) & (df['sic'] <= 3532)) | ((3533 <= df['sic']) & (df['sic'] <= 3533)) |
                ((3534 <= df['sic']) & (df['sic'] <= 3534)) | ((3535 <= df['sic']) & (df['sic'] <= 3535)) |
                ((3536 <= df['sic']) & (df['sic'] <= 3536)) | ((3538 <= df['sic']) & (df['sic'] <= 3538)) |
                ((3540 <= df['sic']) & (df['sic'] <= 3549)) | ((3550 <= df['sic']) & (df['sic'] <= 3559)) |
                ((3560 <= df['sic']) & (df['sic'] <= 3569)) | ((3580 <= df['sic']) & (df['sic'] <= 3580)) |
                ((3581 <= df['sic']) & (df['sic'] <= 3581)) | ((3582 <= df['sic']) & (df['sic'] <= 3582)) |
                ((3585 <= df['sic']) & (df['sic'] <= 3585)) | ((3586 <= df['sic']) & (df['sic'] <= 3586)) |
                ((3589 <= df['sic']) & (df['sic'] <= 3589)) | ((3590 <= df['sic']) & (df['sic'] <= 3599)),
                ((3600 <= df['sic']) & (df['sic'] <= 3600)) | ((3610 <= df['sic']) & (df['sic'] <= 3613)) |
                ((3620 <= df['sic']) & (df['sic'] <= 3621)) | ((3623 <= df['sic']) & (df['sic'] <= 3629)) |
                ((3640 <= df['sic']) & (df['sic'] <= 3644)) | ((3645 <= df['sic']) & (df['sic'] <= 3645)) |
                ((3646 <= df['sic']) & (df['sic'] <= 3646)) | ((3648 <= df['sic']) & (df['sic'] <= 3649)) |
                ((3660 <= df['sic']) & (df['sic'] <= 3660)) | ((3690 <= df['sic']) & (df['sic'] <= 3690)) |
                ((3691 <= df['sic']) & (df['sic'] <= 3692)) | ((3699 <= df['sic']) & (df['sic'] <= 3699)),
                ((2296 <= df['sic']) & (df['sic'] <= 2296)) | ((2396 <= df['sic']) & (df['sic'] <= 2396)) |
                ((3010 <= df['sic']) & (df['sic'] <= 3011)) | ((3537 <= df['sic']) & (df['sic'] <= 3537)) |
                ((3647 <= df['sic']) & (df['sic'] <= 3647)) | ((3694 <= df['sic']) & (df['sic'] <= 3694)) |
                ((3700 <= df['sic']) & (df['sic'] <= 3700)) | ((3710 <= df['sic']) & (df['sic'] <= 3710)) |
                ((3711 <= df['sic']) & (df['sic'] <= 3711)) | ((3713 <= df['sic']) & (df['sic'] <= 3713)) |
                ((3714 <= df['sic']) & (df['sic'] <= 3714)) | ((3715 <= df['sic']) & (df['sic'] <= 3715)) |
                ((3716 <= df['sic']) & (df['sic'] <= 3716)) | ((3792 <= df['sic']) & (df['sic'] <= 3792)) |
                ((3790 <= df['sic']) & (df['sic'] <= 3791)) | ((3799 <= df['sic']) & (df['sic'] <= 3799)),
                ((3720 <= df['sic']) & (df['sic'] <= 3720)) | ((3721 <= df['sic']) & (df['sic'] <= 3721)) |
                ((3723 <= df['sic']) & (df['sic'] <= 3724)) | ((3725 <= df['sic']) & (df['sic'] <= 3725)) |
                ((3728 <= df['sic']) & (df['sic'] <= 3729)),
                ((3730 <= df['sic']) & (df['sic'] <= 3731)) | ((3740 <= df['sic']) & (df['sic'] <= 3743)),
                ((3760 <= df['sic']) & (df['sic'] <= 3769)) | ((3795 <= df['sic']) & (df['sic'] <= 3795)) |
                ((3480 <= df['sic']) & (df['sic'] <= 3489)),
                ((1040 <= df['sic']) & (df['sic'] <= 1049)),
                ((1000 <= df['sic']) & (df['sic'] <= 1009)) | ((1010 <= df['sic']) & (df['sic'] <= 1019)) |
                ((1020 <= df['sic']) & (df['sic'] <= 1029)) | ((1030 <= df['sic']) & (df['sic'] <= 1039)) |
                ((1050 <= df['sic']) & (df['sic'] <= 1059)) | ((1060 <= df['sic']) & (df['sic'] <= 1069)) |
                ((1070 <= df['sic']) & (df['sic'] <= 1079)) | ((1080 <= df['sic']) & (df['sic'] <= 1089)) |
                ((1090 <= df['sic']) & (df['sic'] <= 1099)) | ((1100 <= df['sic']) & (df['sic'] <= 1119)) |
                ((1400 <= df['sic']) & (df['sic'] <= 1499)),
                ((1200 <= df['sic']) & (df['sic'] <= 1299)),
                ((1300 <= df['sic']) & (df['sic'] <= 1300)) | ((1310 <= df['sic']) & (df['sic'] <= 1319)) |
                ((1320 <= df['sic']) & (df['sic'] <= 1329)) | ((1330 <= df['sic']) & (df['sic'] <= 1339)) |
                ((1370 <= df['sic']) & (df['sic'] <= 1379)) | ((1380 <= df['sic']) & (df['sic'] <= 1380)) |
                ((1381 <= df['sic']) & (df['sic'] <= 1381)) | ((1382 <= df['sic']) & (df['sic'] <= 1382)) |
                ((1389 <= df['sic']) & (df['sic'] <= 1389)) | ((2900 <= df['sic']) & (df['sic'] <= 2912)) |
                ((2990 <= df['sic']) & (df['sic'] <= 2999)),
                ((4900 <= df['sic']) & (df['sic'] <= 4900)) | ((4910 <= df['sic']) & (df['sic'] <= 4911)) |
                ((4920 <= df['sic']) & (df['sic'] <= 4922)) | ((4923 <= df['sic']) & (df['sic'] <= 4923)) |
                ((4924 <= df['sic']) & (df['sic'] <= 4925)) | ((4930 <= df['sic']) & (df['sic'] <= 4931)) |
                ((4932 <= df['sic']) & (df['sic'] <= 4932)) | ((4939 <= df['sic']) & (df['sic'] <= 4939)) |
                ((4940 <= df['sic']) & (df['sic'] <= 4942)),
                ((4800 <= df['sic']) & (df['sic'] <= 4800)) | ((4810 <= df['sic']) & (df['sic'] <= 4813)) |
                ((4820 <= df['sic']) & (df['sic'] <= 4822)) | ((4830 <= df['sic']) & (df['sic'] <= 4839)) |
                ((4840 <= df['sic']) & (df['sic'] <= 4841)) | ((4880 <= df['sic']) & (df['sic'] <= 4889)) |
                ((4890 <= df['sic']) & (df['sic'] <= 4890)) | ((4891 <= df['sic']) & (df['sic'] <= 4891)) |
                ((4892 <= df['sic']) & (df['sic'] <= 4892)) | ((4899 <= df['sic']) & (df['sic'] <= 4899)),
                ((7020 <= df['sic']) & (df['sic'] <= 7021)) | ((7030 <= df['sic']) & (df['sic'] <= 7033)) |
                ((7200 <= df['sic']) & (df['sic'] <= 7200)) | ((7210 <= df['sic']) & (df['sic'] <= 7212)) |
                ((7214 <= df['sic']) & (df['sic'] <= 7214)) | ((7215 <= df['sic']) & (df['sic'] <= 7216)) |
                ((7217 <= df['sic']) & (df['sic'] <= 7217)) | ((7219 <= df['sic']) & (df['sic'] <= 7219)) |
                ((7220 <= df['sic']) & (df['sic'] <= 7221)) | ((7230 <= df['sic']) & (df['sic'] <= 7231)) |
                ((7240 <= df['sic']) & (df['sic'] <= 7241)) | ((7250 <= df['sic']) & (df['sic'] <= 7251)) |
                ((7260 <= df['sic']) & (df['sic'] <= 7269)) | ((7270 <= df['sic']) & (df['sic'] <= 7290)) |
                ((7291 <= df['sic']) & (df['sic'] <= 7291)) | ((7292 <= df['sic']) & (df['sic'] <= 7299)) |
                ((7395 <= df['sic']) & (df['sic'] <= 7395)) | ((7500 <= df['sic']) & (df['sic'] <= 7500)) |
                ((7520 <= df['sic']) & (df['sic'] <= 7529)) | ((7530 <= df['sic']) & (df['sic'] <= 7539)) |
                ((7540 <= df['sic']) & (df['sic'] <= 7549)) | ((7600 <= df['sic']) & (df['sic'] <= 7600)) |
                ((7620 <= df['sic']) & (df['sic'] <= 7620)) | ((7622 <= df['sic']) & (df['sic'] <= 7622)) |
                ((7623 <= df['sic']) & (df['sic'] <= 7623)) | ((7629 <= df['sic']) & (df['sic'] <= 7629)) |
                ((7630 <= df['sic']) & (df['sic'] <= 7631)) | ((7640 <= df['sic']) & (df['sic'] <= 7641)) |
                ((7690 <= df['sic']) & (df['sic'] <= 7699)) | ((8100 <= df['sic']) & (df['sic'] <= 8199)) |
                ((8200 <= df['sic']) & (df['sic'] <= 8299)) | ((8300 <= df['sic']) & (df['sic'] <= 8399)) |
                ((8400 <= df['sic']) & (df['sic'] <= 8499)) | ((8600 <= df['sic']) & (df['sic'] <= 8699)) |
                ((8800 <= df['sic']) & (df['sic'] <= 8899)) | ((7510 <= df['sic']) & (df['sic'] <= 7515)),
                ((2750 <= df['sic']) & (df['sic'] <= 2759)) | ((3993 <= df['sic']) & (df['sic'] <= 3993)) |
                ((7218 <= df['sic']) & (df['sic'] <= 7218)) | ((7300 <= df['sic']) & (df['sic'] <= 7300)) |
                ((7310 <= df['sic']) & (df['sic'] <= 7319)) | ((7320 <= df['sic']) & (df['sic'] <= 7329)) |
                ((7330 <= df['sic']) & (df['sic'] <= 7339)) | ((7340 <= df['sic']) & (df['sic'] <= 7342)) |
                ((7349 <= df['sic']) & (df['sic'] <= 7349)) | ((7350 <= df['sic']) & (df['sic'] <= 7351)) |
                ((7352 <= df['sic']) & (df['sic'] <= 7352)) | ((7353 <= df['sic']) & (df['sic'] <= 7353)) |
                ((7359 <= df['sic']) & (df['sic'] <= 7359)) | ((7360 <= df['sic']) & (df['sic'] <= 7369)) |
                ((7374 <= df['sic']) & (df['sic'] <= 7374)) | ((7376 <= df['sic']) & (df['sic'] <= 7376)) |
                ((7377 <= df['sic']) & (df['sic'] <= 7377)) | ((7378 <= df['sic']) & (df['sic'] <= 7378)) |
                ((7379 <= df['sic']) & (df['sic'] <= 7379)) | ((7380 <= df['sic']) & (df['sic'] <= 7380)) |
                ((7381 <= df['sic']) & (df['sic'] <= 7382)) | ((7383 <= df['sic']) & (df['sic'] <= 7383)) |
                ((7384 <= df['sic']) & (df['sic'] <= 7384)) | ((7385 <= df['sic']) & (df['sic'] <= 7385)) |
                ((7389 <= df['sic']) & (df['sic'] <= 7390)) | ((7391 <= df['sic']) & (df['sic'] <= 7391)) |
                ((7392 <= df['sic']) & (df['sic'] <= 7392)) | ((7393 <= df['sic']) & (df['sic'] <= 7393)) |
                ((7394 <= df['sic']) & (df['sic'] <= 7394)) | ((7396 <= df['sic']) & (df['sic'] <= 7396)) |
                ((7397 <= df['sic']) & (df['sic'] <= 7397)) | ((7399 <= df['sic']) & (df['sic'] <= 7399)) |
                ((7519 <= df['sic']) & (df['sic'] <= 7519)) | ((8700 <= df['sic']) & (df['sic'] <= 8700)) |
                ((8710 <= df['sic']) & (df['sic'] <= 8713)) | ((8720 <= df['sic']) & (df['sic'] <= 8721)) |
                ((8730 <= df['sic']) & (df['sic'] <= 8734)) | ((8740 <= df['sic']) & (df['sic'] <= 8748)) |
                ((8900 <= df['sic']) & (df['sic'] <= 8910)) | ((8911 <= df['sic']) & (df['sic'] <= 8911)) |
                ((8920 <= df['sic']) & (df['sic'] <= 8999)) | ((4220 <= df['sic']) & (df['sic'] <= 4229)),
                ((3570 <= df['sic']) & (df['sic'] <= 3579)) | ((3680 <= df['sic']) & (df['sic'] <= 3680)) |
                ((3681 <= df['sic']) & (df['sic'] <= 3681)) | ((3682 <= df['sic']) & (df['sic'] <= 3682)) |
                ((3683 <= df['sic']) & (df['sic'] <= 3683)) | ((3684 <= df['sic']) & (df['sic'] <= 3684)) |
                ((3685 <= df['sic']) & (df['sic'] <= 3685)) | ((3686 <= df['sic']) & (df['sic'] <= 3686)) |
                ((3687 <= df['sic']) & (df['sic'] <= 3687)) | ((3688 <= df['sic']) & (df['sic'] <= 3688)) |
                ((3689 <= df['sic']) & (df['sic'] <= 3689)) | ((3695 <= df['sic']) & (df['sic'] <= 3695)),
                ((7370 <= df['sic']) & (df['sic'] <= 7372)) | ((7375 <= df['sic']) & (df['sic'] <= 7375)) |
                ((7373 <= df['sic']) & (df['sic'] <= 7373)),
                ((3622 <= df['sic']) & (df['sic'] <= 3622)) | ((3661 <= df['sic']) & (df['sic'] <= 3661)) |
                ((3662 <= df['sic']) & (df['sic'] <= 3662)) | ((3663 <= df['sic']) & (df['sic'] <= 3663)) |
                ((3664 <= df['sic']) & (df['sic'] <= 3664)) | ((3665 <= df['sic']) & (df['sic'] <= 3665)) |
                ((3666 <= df['sic']) & (df['sic'] <= 3666)) | ((3669 <= df['sic']) & (df['sic'] <= 3669)) |
                ((3670 <= df['sic']) & (df['sic'] <= 3679)) | ((3810 <= df['sic']) & (df['sic'] <= 3810)) |
                ((3812 <= df['sic']) & (df['sic'] <= 3812)),
                ((3811 <= df['sic']) & (df['sic'] <= 3811)) | ((3820 <= df['sic']) & (df['sic'] <= 3820)) |
                ((3821 <= df['sic']) & (df['sic'] <= 3821)) | ((3822 <= df['sic']) & (df['sic'] <= 3822)) |
                ((3823 <= df['sic']) & (df['sic'] <= 3823)) | ((3824 <= df['sic']) & (df['sic'] <= 3824)) |
                ((3825 <= df['sic']) & (df['sic'] <= 3825)) | ((3826 <= df['sic']) & (df['sic'] <= 3826)) |
                ((3827 <= df['sic']) & (df['sic'] <= 3827)) | ((3829 <= df['sic']) & (df['sic'] <= 3829)) |
                ((3830 <= df['sic']) & (df['sic'] <= 3839)),
                ((2520 <= df['sic']) & (df['sic'] <= 2549)) | ((2600 <= df['sic']) & (df['sic'] <= 2639)) |
                ((2670 <= df['sic']) & (df['sic'] <= 2699)) | ((2760 <= df['sic']) & (df['sic'] <= 2761)) |
                ((3950 <= df['sic']) & (df['sic'] <= 3955)),
                ((2440 <= df['sic']) & (df['sic'] <= 2449)) | ((2640 <= df['sic']) & (df['sic'] <= 2659)) |
                ((3220 <= df['sic']) & (df['sic'] <= 3221)) | ((3410 <= df['sic']) & (df['sic'] <= 3412)),
                ((4000 <= df['sic']) & (df['sic'] <= 4013)) | ((4040 <= df['sic']) & (df['sic'] <= 4049)) |
                ((4100 <= df['sic']) & (df['sic'] <= 4100)) | ((4110 <= df['sic']) & (df['sic'] <= 4119)) |
                ((4120 <= df['sic']) & (df['sic'] <= 4121)) | ((4130 <= df['sic']) & (df['sic'] <= 4131)) |
                ((4140 <= df['sic']) & (df['sic'] <= 4142)) | ((4150 <= df['sic']) & (df['sic'] <= 4151)) |
                ((4170 <= df['sic']) & (df['sic'] <= 4173)) | ((4190 <= df['sic']) & (df['sic'] <= 4199)) |
                ((4200 <= df['sic']) & (df['sic'] <= 4200)) | ((4210 <= df['sic']) & (df['sic'] <= 4219)) |
                ((4230 <= df['sic']) & (df['sic'] <= 4231)) | ((4240 <= df['sic']) & (df['sic'] <= 4249)) |
                ((4400 <= df['sic']) & (df['sic'] <= 4499)) | ((4500 <= df['sic']) & (df['sic'] <= 4599)) |
                ((4600 <= df['sic']) & (df['sic'] <= 4699)) | ((4700 <= df['sic']) & (df['sic'] <= 4700)) |
                ((4710 <= df['sic']) & (df['sic'] <= 4712)) | ((4720 <= df['sic']) & (df['sic'] <= 4729)) |
                ((4730 <= df['sic']) & (df['sic'] <= 4739)) | ((4740 <= df['sic']) & (df['sic'] <= 4749)) |
                ((4780 <= df['sic']) & (df['sic'] <= 4780)) | ((4782 <= df['sic']) & (df['sic'] <= 4782)) |
                ((4783 <= df['sic']) & (df['sic'] <= 4783)) | ((4784 <= df['sic']) & (df['sic'] <= 4784)) |
                ((4785 <= df['sic']) & (df['sic'] <= 4785)) | ((4789 <= df['sic']) & (df['sic'] <= 4789)),
                ((5000 <= df['sic']) & (df['sic'] <= 5000)) | ((5010 <= df['sic']) & (df['sic'] <= 5015)) |
                ((5020 <= df['sic']) & (df['sic'] <= 5023)) | ((5030 <= df['sic']) & (df['sic'] <= 5039)) |
                ((5040 <= df['sic']) & (df['sic'] <= 5042)) | ((5043 <= df['sic']) & (df['sic'] <= 5043)) |
                ((5044 <= df['sic']) & (df['sic'] <= 5044)) | ((5045 <= df['sic']) & (df['sic'] <= 5045)) |
                ((5046 <= df['sic']) & (df['sic'] <= 5046)) | ((5047 <= df['sic']) & (df['sic'] <= 5047)) |
                ((5048 <= df['sic']) & (df['sic'] <= 5048)) | ((5049 <= df['sic']) & (df['sic'] <= 5049)) |
                ((5050 <= df['sic']) & (df['sic'] <= 5059)) | ((5060 <= df['sic']) & (df['sic'] <= 5060)) |
                ((5063 <= df['sic']) & (df['sic'] <= 5063)) | ((5064 <= df['sic']) & (df['sic'] <= 5064)) |
                ((5065 <= df['sic']) & (df['sic'] <= 5065)) | ((5070 <= df['sic']) & (df['sic'] <= 5078)) |
                ((5080 <= df['sic']) & (df['sic'] <= 5080)) | ((5081 <= df['sic']) & (df['sic'] <= 5081)) |
                ((5082 <= df['sic']) & (df['sic'] <= 5082)) | ((5083 <= df['sic']) & (df['sic'] <= 5083)) |
                ((5084 <= df['sic']) & (df['sic'] <= 5084)) | ((5085 <= df['sic']) & (df['sic'] <= 5085)) |
                ((5086 <= df['sic']) & (df['sic'] <= 5087)) | ((5088 <= df['sic']) & (df['sic'] <= 5088)) |
                ((5090 <= df['sic']) & (df['sic'] <= 5090)) | ((5091 <= df['sic']) & (df['sic'] <= 5092)) |
                ((5093 <= df['sic']) & (df['sic'] <= 5093)) | ((5094 <= df['sic']) & (df['sic'] <= 5094)) |
                ((5099 <= df['sic']) & (df['sic'] <= 5099)) | ((5100 <= df['sic']) & (df['sic'] <= 5100)) |
                ((5110 <= df['sic']) & (df['sic'] <= 5113)) | ((5120 <= df['sic']) & (df['sic'] <= 5122)) |
                ((5130 <= df['sic']) & (df['sic'] <= 5139)) | ((5140 <= df['sic']) & (df['sic'] <= 5149)) |
                ((5150 <= df['sic']) & (df['sic'] <= 5159)) | ((5160 <= df['sic']) & (df['sic'] <= 5169)) |
                ((5170 <= df['sic']) & (df['sic'] <= 5172)) | ((5180 <= df['sic']) & (df['sic'] <= 5182)) |
                ((5190 <= df['sic']) & (df['sic'] <= 5199)),
                ((5200 <= df['sic']) & (df['sic'] <= 5200)) | ((5210 <= df['sic']) & (df['sic'] <= 5219)) |
                ((5220 <= df['sic']) & (df['sic'] <= 5229)) | ((5230 <= df['sic']) & (df['sic'] <= 5231)) |
                ((5250 <= df['sic']) & (df['sic'] <= 5251)) | ((5260 <= df['sic']) & (df['sic'] <= 5261)) |
                ((5270 <= df['sic']) & (df['sic'] <= 5271)) | ((5300 <= df['sic']) & (df['sic'] <= 5300)) |
                ((5310 <= df['sic']) & (df['sic'] <= 5311)) | ((5320 <= df['sic']) & (df['sic'] <= 5320)) |
                ((5330 <= df['sic']) & (df['sic'] <= 5331)) | ((5334 <= df['sic']) & (df['sic'] <= 5334)) |
                ((5340 <= df['sic']) & (df['sic'] <= 5349)) | ((5390 <= df['sic']) & (df['sic'] <= 5399)) |
                ((5400 <= df['sic']) & (df['sic'] <= 5400)) | ((5410 <= df['sic']) & (df['sic'] <= 5411)) |
                ((5412 <= df['sic']) & (df['sic'] <= 5412)) | ((5420 <= df['sic']) & (df['sic'] <= 5429)) |
                ((5430 <= df['sic']) & (df['sic'] <= 5439)) | ((5440 <= df['sic']) & (df['sic'] <= 5449)) |
                ((5450 <= df['sic']) & (df['sic'] <= 5459)) | ((5460 <= df['sic']) & (df['sic'] <= 5469)) |
                ((5490 <= df['sic']) & (df['sic'] <= 5499)) | ((5500 <= df['sic']) & (df['sic'] <= 5500)) |
                ((5510 <= df['sic']) & (df['sic'] <= 5529)) | ((5530 <= df['sic']) & (df['sic'] <= 5539)) |
                ((5540 <= df['sic']) & (df['sic'] <= 5549)) | ((5550 <= df['sic']) & (df['sic'] <= 5559)) |
                ((5560 <= df['sic']) & (df['sic'] <= 5569)) | ((5570 <= df['sic']) & (df['sic'] <= 5579)) |
                ((5590 <= df['sic']) & (df['sic'] <= 5599)) | ((5600 <= df['sic']) & (df['sic'] <= 5699)) |
                ((5700 <= df['sic']) & (df['sic'] <= 5700)) | ((5710 <= df['sic']) & (df['sic'] <= 5719)) |
                ((5720 <= df['sic']) & (df['sic'] <= 5722)) | ((5730 <= df['sic']) & (df['sic'] <= 5733)) |
                ((5734 <= df['sic']) & (df['sic'] <= 5734)) | ((5735 <= df['sic']) & (df['sic'] <= 5735)) |
                ((5736 <= df['sic']) & (df['sic'] <= 5736)) | ((5750 <= df['sic']) & (df['sic'] <= 5799)) |
                ((5900 <= df['sic']) & (df['sic'] <= 5900)) | ((5910 <= df['sic']) & (df['sic'] <= 5912)) |
                ((5920 <= df['sic']) & (df['sic'] <= 5929)) | ((5930 <= df['sic']) & (df['sic'] <= 5932)) |
                ((5940 <= df['sic']) & (df['sic'] <= 5940)) | ((5941 <= df['sic']) & (df['sic'] <= 5941)) |
                ((5942 <= df['sic']) & (df['sic'] <= 5942)) | ((5943 <= df['sic']) & (df['sic'] <= 5943)) |
                ((5944 <= df['sic']) & (df['sic'] <= 5944)) | ((5945 <= df['sic']) & (df['sic'] <= 5945)) |
                ((5946 <= df['sic']) & (df['sic'] <= 5946)) | ((5947 <= df['sic']) & (df['sic'] <= 5947)) |
                ((5948 <= df['sic']) & (df['sic'] <= 5948)) | ((5949 <= df['sic']) & (df['sic'] <= 5949)) |
                ((5950 <= df['sic']) & (df['sic'] <= 5959)) | ((5960 <= df['sic']) & (df['sic'] <= 5969)) |
                ((5970 <= df['sic']) & (df['sic'] <= 5979)) | ((5980 <= df['sic']) & (df['sic'] <= 5989)) |
                ((5990 <= df['sic']) & (df['sic'] <= 5990)) | ((5992 <= df['sic']) & (df['sic'] <= 5992)) |
                ((5993 <= df['sic']) & (df['sic'] <= 5993)) | ((5994 <= df['sic']) & (df['sic'] <= 5994)) |
                ((5995 <= df['sic']) & (df['sic'] <= 5995)) | ((5999 <= df['sic']) & (df['sic'] <= 5999)),
                ((5800 <= df['sic']) & (df['sic'] <= 5819)) | ((5820 <= df['sic']) & (df['sic'] <= 5829)) |
                ((5890 <= df['sic']) & (df['sic'] <= 5899)) | ((7000 <= df['sic']) & (df['sic'] <= 7000)) |
                ((7010 <= df['sic']) & (df['sic'] <= 7019)) | ((7040 <= df['sic']) & (df['sic'] <= 7049)) |
                ((7213 <= df['sic']) & (df['sic'] <= 7213)),
                ((6000 <= df['sic']) & (df['sic'] <= 6000)) | ((6010 <= df['sic']) & (df['sic'] <= 6019)) |
                ((6020 <= df['sic']) & (df['sic'] <= 6020)) | ((6021 <= df['sic']) & (df['sic'] <= 6021)) |
                ((6022 <= df['sic']) & (df['sic'] <= 6022)) | ((6023 <= df['sic']) & (df['sic'] <= 6024)) |
                ((6025 <= df['sic']) & (df['sic'] <= 6025)) | ((6026 <= df['sic']) & (df['sic'] <= 6026)) |
                ((6027 <= df['sic']) & (df['sic'] <= 6027)) | ((6028 <= df['sic']) & (df['sic'] <= 6029)) |
                ((6030 <= df['sic']) & (df['sic'] <= 6036)) | ((6040 <= df['sic']) & (df['sic'] <= 6059)) |
                ((6060 <= df['sic']) & (df['sic'] <= 6062)) | ((6080 <= df['sic']) & (df['sic'] <= 6082)) |
                ((6090 <= df['sic']) & (df['sic'] <= 6099)) | ((6100 <= df['sic']) & (df['sic'] <= 6100)) |
                ((6110 <= df['sic']) & (df['sic'] <= 6111)) | ((6112 <= df['sic']) & (df['sic'] <= 6113)) |
                ((6120 <= df['sic']) & (df['sic'] <= 6129)) | ((6130 <= df['sic']) & (df['sic'] <= 6139)) |
                ((6140 <= df['sic']) & (df['sic'] <= 6149)) | ((6150 <= df['sic']) & (df['sic'] <= 6159)) |
                ((6160 <= df['sic']) & (df['sic'] <= 6169)) | ((6170 <= df['sic']) & (df['sic'] <= 6179)) |
                ((6190 <= df['sic']) & (df['sic'] <= 6199)),
                ((6300 <= df['sic']) & (df['sic'] <= 6300)) | ((6310 <= df['sic']) & (df['sic'] <= 6319)) |
                ((6320 <= df['sic']) & (df['sic'] <= 6329)) | ((6330 <= df['sic']) & (df['sic'] <= 6331)) |
                ((6350 <= df['sic']) & (df['sic'] <= 6351)) | ((6360 <= df['sic']) & (df['sic'] <= 6361)) |
                ((6370 <= df['sic']) & (df['sic'] <= 6379)) | ((6390 <= df['sic']) & (df['sic'] <= 6399)) |
                ((6400 <= df['sic']) & (df['sic'] <= 6411)),
                ((6500 <= df['sic']) & (df['sic'] <= 6500)) | ((6510 <= df['sic']) & (df['sic'] <= 6510)) |
                ((6512 <= df['sic']) & (df['sic'] <= 6512)) | ((6513 <= df['sic']) & (df['sic'] <= 6513)) |
                ((6514 <= df['sic']) & (df['sic'] <= 6514)) | ((6515 <= df['sic']) & (df['sic'] <= 6515)) |
                ((6517 <= df['sic']) & (df['sic'] <= 6519)) | ((6520 <= df['sic']) & (df['sic'] <= 6529)) |
                ((6530 <= df['sic']) & (df['sic'] <= 6531)) | ((6532 <= df['sic']) & (df['sic'] <= 6532)) |
                ((6540 <= df['sic']) & (df['sic'] <= 6541)) | ((6550 <= df['sic']) & (df['sic'] <= 6553)) |
                ((6590 <= df['sic']) & (df['sic'] <= 6599)) | ((6610 <= df['sic']) & (df['sic'] <= 6611)),
                ((6200 <= df['sic']) & (df['sic'] <= 6299)) | ((6700 <= df['sic']) & (df['sic'] <= 6700)) |
                ((6710 <= df['sic']) & (df['sic'] <= 6719)) | ((6720 <= df['sic']) & (df['sic'] <= 6722)) |
                ((6723 <= df['sic']) & (df['sic'] <= 6723)) | ((6724 <= df['sic']) & (df['sic'] <= 6724)) |
                ((6725 <= df['sic']) & (df['sic'] <= 6725)) | ((6726 <= df['sic']) & (df['sic'] <= 6726)) |
                ((6730 <= df['sic']) & (df['sic'] <= 6733)) | ((6740 <= df['sic']) & (df['sic'] <= 6779)) |
                ((6790 <= df['sic']) & (df['sic'] <= 6791)) | ((6792 <= df['sic']) & (df['sic'] <= 6792)) |
                ((6793 <= df['sic']) & (df['sic'] <= 6793)) | ((6794 <= df['sic']) & (df['sic'] <= 6794)) |
                ((6795 <= df['sic']) & (df['sic'] <= 6795)) | ((6798 <= df['sic']) & (df['sic'] <= 6798)) |
                ((6799 <= df['sic']) & (df['sic'] <= 6799)),
                ((4950 <= df['sic']) & (df['sic'] <= 4959)) | ((4960 <= df['sic']) & (df['sic'] <= 4961)) |
                ((4970 <= df['sic']) & (df['sic'] <= 4971)) | ((4990 <= df['sic']) & (df['sic'] <= 4991))]
    choicelist = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
                  29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49]
    return np.select(condlist, choicelist, default=np.nan)


def fillna_atq(df_q, df_a):
    # fina columns are na in df_q and exist in df_a
    df_q_na_list = df_q.columns[df_q.isna().any()].tolist()
    df_a_columns_list = df_a.columns.values.tolist()
    list_temp = list(set(df_q_na_list) & set(df_a_columns_list))
    # remove mom columns, mom chars are same in annual and quarterly
    na_columns_list = []
    for i in list_temp:
        if re.match(r'mom.', i) is None:
            na_columns_list.append(i)
    # get annual columns from df_a
    df_temp = df_a[na_columns_list].copy()
    df_temp[['permno', 'jdate']] = df_a[['permno', 'jdate']].copy()
    # rename annual columns in the form of 'chars_a'
    for na_column in na_columns_list:
        df_temp = df_temp.rename(columns={'%s' % na_column: '%s_a' % na_column})
    df_temp = df_temp.reset_index(drop=True)
    # use annual chars to fill quarterly na
    df_q = pd.merge(df_q, df_temp, how='left', on=['permno', 'jdate'])
    for na_column in na_columns_list:
        df_q['%s' % na_column] = np.where(df_q['%s' % na_column].isnull(), df_q['%s_a' % na_column], df_q['%s' % na_column])
        df_q = df_q.drop(['%s_a' % na_column], axis=1)
    return df_q


def fillna_ind(df, method, ffi):
    df_fill = pd.DataFrame()
    na_columns_list = df.columns[df.isna().any()].tolist()
    for na_column in na_columns_list:
        if method == 'mean':
            df_temp = df.groupby(['jdate', 'ffi%s' % ffi])['%s' % na_column].mean()
        elif method == 'median':
            df_temp = df.groupby(['jdate', 'ffi%s' % ffi])['%s' % na_column].median()
        else:
            None
        df_fill = pd.concat([df_fill, df_temp], axis=1)
        if method == 'mean':
            df_fill = df_fill.rename(columns={'%s' % na_column: '%s_mean' % na_column})
        elif method == 'median':
            df_fill = df_fill.rename(columns={'%s' % na_column: '%s_median' % na_column})
        else:
            None
    df_fill = df_fill.reset_index()
    # reset multiple index to jdate and ffi code
    df_fill['index'] = df_fill['index'].astype(str)
    index_temp = df_fill['index'].str.split(',', expand=True)
    index_temp.columns = ['jdate', 'ffi%s' % ffi]
    index_temp['jdate'] = index_temp['jdate'].str.strip('(Timestamp(\' \')')
    index_temp['ffi%s' % ffi] = index_temp['ffi%s' % ffi].str.strip(')')
    df_fill[['jdate', 'ffi%s' % ffi]] = index_temp[['jdate', 'ffi%s' % ffi]]
    df_fill = df_fill.drop(['index'], axis=1)
    df_fill['jdate'] = pd.to_datetime(df_fill['jdate'])
    df_fill['ffi49'] = df_fill['ffi49'].astype(int)
    # fill na
    df = pd.merge(df, df_fill, how='left', on=['jdate', 'ffi%s' % ffi])
    for na_column in na_columns_list:
        if method == 'mean':
            df['%s' % na_column] = df['%s' % na_column].fillna(df['%s_mean' % na_column])
            df = df.drop(['%s_mean' % na_column], axis=1)
        elif method == 'median':
            df['%s' % na_column] = df['%s' % na_column].fillna(df['%s_median' % na_column])
            df = df.drop(['%s_median' % na_column], axis=1)
        else:
            None
    return df


def fillna_all(df, method):
    df_fill = pd.DataFrame()
    na_columns_list = df.columns[df.isna().any()].tolist()
    for na_column in na_columns_list:
        if method == 'mean':
            df_temp = df.groupby(['jdate'])['%s' % na_column].mean()
        elif method == 'median':
            df_temp = df.groupby(['jdate'])['%s' % na_column].median()
        else:
            None
        df_fill = pd.concat([df_fill, df_temp], axis=1)
        if method == 'mean':
            df_fill = df_fill.rename(columns={'%s' % na_column: '%s_mean' % na_column})
        elif method == 'median':
            df_fill = df_fill.rename(columns={'%s' % na_column: '%s_median' % na_column})
        else:
            None
    df_fill = df_fill.reset_index()
    # reset multiple index to jdate and ffi code
    df_fill['index'] = df_fill['index'].astype(str)
    index_temp = df_fill['index'].str.split(',', expand=True)
    index_temp.columns = ['jdate']
    index_temp['jdate'] = index_temp['jdate'].str.strip('(Timestamp(\' \')')
    df_fill[['jdate']] = index_temp[['jdate']]
    df_fill = df_fill.drop(['index'], axis=1)
    df_fill['jdate'] = pd.to_datetime(df_fill['jdate'])
    # fill na
    df = pd.merge(df, df_fill, how='left', on='jdate')
    for na_column in na_columns_list:
        if method == 'mean':
            df['%s' % na_column] = df['%s' % na_column].fillna(df['%s_mean' % na_column])
            df = df.drop(['%s_mean' % na_column], axis=1)
        elif method == 'median':
            df['%s' % na_column] = df['%s' % na_column].fillna(df['%s_median' % na_column])
            df = df.drop(['%s_median' % na_column], axis=1)
        else:
            None
    return df


def standardize(df):
    df_temp = df.groupby(['jdate'], as_index=False)['gvkey'].count()
    df_temp = df_temp.rename(columns={'gvkey': 'count'})
    df = pd.merge(df, df_temp, how='left', on='jdate')
    col_names = df.columns.values.tolist()
    list_to_remove = ['permno', 'date', 'jdate', 'datadate', 'gvkey', 'sic', 'count', 'exchcd', 'shrcd']
    col_names = list(set(col_names).difference(set(list_to_remove)))
    df = df.fillna(0)
    for col_name in col_names:
        df['%s_rank' % col_name] = df.groupby(['jdate'])['%s' % col_name].rank()
        df['rank_%s' % col_name] = (df['%s_rank' % col_name]-1)/(df['count']-1)*2 - 1
        df = df.drop(['%s_rank' % col_name, '%s' % col_name], axis=1)
    return df