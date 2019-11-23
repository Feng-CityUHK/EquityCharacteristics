
## Academic Background

For financial researches, we need equity characteristics. This repository is a toolkit to calculate asset characteristics in individual equity level and portfolio level.

Many papers contribute a lot to this repository. I am very sorry for only listing the following 4 papers.
- **Common risk factors in the returns on stocks and bonds** by [Fama and French 1993 JFE](https://doi.org/10.1016/0304-405X(93)90023-5)
- **A five-factor asset pricing model** by [Fama and French 2015 JFE](https://doi.org/10.1016/j.jfineco.2014.10.010)
  - [French's Data Library](http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)
- **The Characteristics that Provide Independent Information about Average U.S. Monthly Stock Returns** by [Green Han Zhang 2017 RFS](https://doi.org/10.1093/rfs/hhx019)
  - [sas code from Green's website](https://drive.google.com/file/d/0BwwEXkCgXEdRQWZreUpKOHBXOUU/view)
- **Replicating Anormalies** by [Hou Xue Zhang 2018 RFS](https://doi.org/10.1093/rfs/hhy131)
  - [Anormaly Portfolios by Zhang's website](http://global-q.org/index.html)

## Prerequisite

- Read on the listed papers
- [WRDS](https://wrds-web.wharton.upenn.edu) account with subscription to CRSP, Compustat and IBES.
- SAS (I use SAS on WRDS Cloud)
- Python (I use Pandas to play with data)

## Method

### Equity Characteristics

This topic is summaried by **Green Hand Zhang** and **Hou Xue Zhang**.

### Portfolio Characteristics

Portfolio charactaristics is the equal-weighted / value-weighted averge of the characteristics for all equities in the portfolio.

The portfolios includes and not limited to:

- Characteristics-sorted Portfolio, see the listed papers and also [Deep Learning in Characteristics-Sorted Factor Models](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3243683)
- DGTW Benchmark, see [Measuring Mutual Fund Performance with Characteristic‐Based Benchmarks](https://doi.org/10.1111/j.1540-6261.1997.tb02724.x)
- Industry portfolio

## Codes

- Calculate equity characteristics with SAS code, mainly refering to [Green Hand Zhang 2017 RFS and its SAS code](https://drive.google.com/file/d/0BwwEXkCgXEdRQWZreUpKOHBXOUU/view).
- Portfolio characteristics, mainly refering to [WRDS Financial Ratios Suite](https://wrds-www.wharton.upenn.edu/pages/support/research-wrds/sample-programs/wrds-sample-programs/wrds-financial-ratios-suite/).
- DGTW code refers to [this WRDS code](https://wrds-www.wharton.upenn.edu/pages/support/applications/python-replications/characteristic-based-benchmarks-daniel-grinblatt-titman-and-wermers-1997-python-version/)

**All comments are welcome.**