# 発生している問題

## categories "GROK"の株価その他情報が null となる

- scripts/pipeline/fetch*prices.py を実行して yfiance から株価を取得し data/parqeut/prices*{period}\_{interval}.parquet 及び tech_snapshot_1d.parquet を生成する際に 一部銘柄で株価データが取得できていない
- 株価が取得できていない銘柄
  - ["3628.T", "3676.T","4055.T","4395.T","4475.T","6195.T","7072.T"]
- JupterNotebook で株価の取得だけを切り出したところ問題なく株価が取得できてる

## 期待する改善

- 当該株価を取得して prices*{period}*{interval}.parquet 及び tech_snapshot_1d.parquet のデータを補完
- FastAPI: http://localhost:8000/stocks/enriched?tag=GROK この routing でのデータ反映
- 個別銘柄の補完後 pipeline の見直しを図り全ての銘柄で同様の問題が起きないようにする

### 落ち葉拾い

- この修正に合わせてこの auto_adjust の警告を修正してください
- /var/folders/lc/9gc67bcn35g9t21r_pbch1t80000gn/T/ipykernel_73787/4095697110.py:2: FutureWarning: YF.download() has changed argument auto_adjust default to True
  stock_20y = yf.download(ticker, period="4y", interval="1d", progress=False)
  /var/folders/lc/9gc67bcn35g9t21r_pbch1t80000gn/T/ipykernel_73787/4095697110.py:3: FutureWarning: YF.download() has changed argument auto_adjust default to True
  stock_max = yf.download(ticker, period="max", interval="1d", progress=False)
  /var/folders/lc/9gc67bcn35g9t21r_pbch1t80000gn/T/ipykernel_73787/4095697110.py:4: FutureWarning: YF.download() has changed argument auto_adjust default to True
  stock_5m = yf.download(ticker, period="60d", interval="5m", progress=False)
  /var/folders/lc/9gc67bcn35g9t21r_pbch1t80000gn/T/ipykernel_73787/4095697110.py:5: FutureWarning: YF.download() has changed argument auto_adjust default to True
  stock_15m = yf.download(ticker, period="max", interval="15m",progress=False)
  /var/folders/lc/9gc67bcn35g9t21r_pbch1t80000gn/T/ipykernel_73787/4095697110.py:6: FutureWarning: YF.download() has changed argument auto_adjust default to True
  stock_1h = yf.download(ticker, interval="1h",period="max", progress=False)
  Price Close High Low Open Volume
  Ticker 3628.T 3628.T 3628.T 3628.T 3628.T
  Date
  2008-09-19 259.758301 274.447024 247.698094 269.035383 3456000
  2008-09-22 238.111786 248.007360 234.710198 234.710198 592800
  2008-09-24 241.667999 256.202086 235.019419 236.565604 369600
  2008-09-25 226.515427 247.388847 226.515427 238.111765 103800
  2008-09-26 252.027359 255.119729 235.328627 235.328627 302400
  ... ... ... ... ... ...
  2025-10-16 629.000000 653.000000 616.000000 630.000000 81600
  2025-10-17 599.000000 619.000000 585.000000 619.000000 25400
  2025-10-20 612.000000 618.000000 580.000000 590.000000 23300
  2025-10-21 640.000000 640.000000 606.000000 606.000000 31900
  2025-10-22 638.000000 645.000000 627.000000 635.000000 10100

[4198 rows x 5 columns]
/var/folders/lc/9gc67bcn35g9t21r_pbch1t80000gn/T/ipykernel_73787/4095697110.py:7: FutureWarning: YF.download() has changed argument auto_adjust default to True
stock_1d = yf.download(ticker, period="1d", interval="1d", progress=False)
