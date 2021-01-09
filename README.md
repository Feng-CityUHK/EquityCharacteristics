- All in Python
- The SAS version is here [EquityCharacteristicsSAS](https://feng-cityuhk.github.io/EquityCharacteristicsSAS/)

## Academic Background

For financial researches, we need equity characteristics. This repository is a toolkit to calculate asset characteristics in individual equity level and portfolio level.

## Reference

Many papers contribute a lot to this repository. I am very sorry for only listing the following papers.
-  **Measuring Mutual Fund Performance with Characteristic‐Based Benchmarks** by [DANIEL, GRINBLATT, TITMAN, WERMERS 1997 JF](https://doi.org/10.1111/j.1540-6261.1997.tb02724.x)
  - [Benchmarks on Wermer's website](http://terpconnect.umd.edu/~wermers/ftpsite/Dgtw/coverpage.htm)

- **Dissecting Anomalies with a Five-Factor Model** by [Fama and French 2015 RFS](https://doi.org/10.1093/rfs/hhv043)
  - Define the characteristics of a portfolio as the value-weight averages (market-cap weights) of the variables for the firms in the portfolio
  - [French's Data Library](http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)

- **The Characteristics that Provide Independent Information about Average U.S. Monthly Stock Returns** by [Green Hand Zhang 2017 RFS](https://doi.org/10.1093/rfs/hhx019)
  - [sas code from Green's website](https://drive.google.com/file/d/0BwwEXkCgXEdRQWZreUpKOHBXOUU/view)
- **Replicating Anormalies** by [Hou Xue Zhang 2018 RFS](https://doi.org/10.1093/rfs/hhy131)
  - [Anormaly Portfolios by Zhang's website](http://global-q.org/index.html)

## Prerequisite

- Read the listed papers
- [WRDS](https://wrds-web.wharton.upenn.edu) account with subscription to CRSP, Compustat and IBES.
- SAS (I use SAS on WRDS Cloud)
- Python (I use Pandas to play with data)

## Method

### Equity Characteristics

This topic is summaried by **Green Hand Zhang** and **Hou Xue Zhang**.

### Files

#### Main Files
- accounting.py  -- most annual, quarterly and monthly frequency characteristics
- functions.py -- impute and rank functions
- merge_chars.py -- merge all the characteristics from different pickle file into one pickle file
- inpute_rank_output_bchmk.py -- impute the missing values and standardize raw data

#### Single Characteristic Files
- beta.py -- 3 months rolling CAPM beta
- rvar_capm.py, rvar_ff3.py -- residual variance of CAPM and fama french 3 factors model, rolling window is 3 months
- rvar_mean.py -- variance of return, rolling window is 3 months
- abr.py -- cumulative abnormal returns around earnings announcement dates
- re.py -- revisions in analysts’ earnings forecasts
- sue.py -- unexpected quarterly earnings
- ill.py -- illiquidity, rolling window is 3 months
- maxret_d.py -- maximum daily returns, rolling window is 3 months
- std_dolvol.py -- std of dollar trading volume, rolling window is 3 months
- std_turn.py -- std of Share turnover, rolling window is 3 months
- bid_ask_spread.py -- bid-ask spread, rolling window is 3 months
- zerotrade.py -- number of zero-trading days, rolling window is 3 months

### Portfolio Characteristics

Portfolio charactaristics is the equal-weighted / value-weighted averge of the characteristics for all equities in the portfolio.

The portfolios includes and not limited to:

- Characteristics-sorted Portfolio, see the listed papers and also [Deep Learning in Characteristics-Sorted Factor Models](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3243683)
- DGTW Benchmark, see [DGTW 1997 JF](https://doi.org/10.1111/j.1540-6261.1997.tb02724.x)
- Industry portfolio

## Codes

- Calculate equity characteristics with SAS code, mainly refering to [SAS code by Green Hand Zhang](https://drive.google.com/file/d/0BwwEXkCgXEdRQWZreUpKOHBXOUU/view).
- Portfolio characteristics, mainly refering to [WRDS Financial Ratios Suite](https://wrds-www.wharton.upenn.edu/pages/support/research-wrds/sample-programs/wrds-sample-programs/wrds-financial-ratios-suite/) and [Variable Definition](https://wrds-www.wharton.upenn.edu/documents/793/WRDS_Industry_Financial_Ratio_Manual.pdf)
- DGTW code refers to [this python code](https://wrds-www.wharton.upenn.edu/pages/support/applications/python-replications/characteristic-based-benchmarks-daniel-grinblatt-titman-and-wermers-1997-python-version/) or [this SAS code](https://wrds-www.wharton.upenn.edu/pages/support/applications/portfolio-construction-and-market-anomalies/characteristic-based-benchmarks-daniel-grinblatt-titman-and-wermers-1997/)

**All comments are welcome.**

