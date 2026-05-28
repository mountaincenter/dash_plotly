# Market Report Rules

This file is required first-read context for daily market reports. It supplements `.claude/CLAUDE.md`; it does not replace the existing report workflow.

## Core Contract

- Treat the latest good report HTML as the specification.
- Before writing, inspect the reference report's section order, top summary, stat cards, tables, labels, source notes, and conclusion depth.
- Do not omit sections, collapse sections, rename concepts, or change granularity unless the difference is reported before writing.
- The pipeline provides data. Narrative, interpretation, and missing context are completed manually from verified sources.
- Do not leave `データ未取得`, `取得失敗`, `placeholder`, `追記予定`, or equivalent unfinished text in a completed report.

## Data Reconciliation

For every market report, reconcile these sources before writing:

- `data/parquet/market_summary/structured/report_data_<YYYY-MM-DD>.json`
- S3/parquet data for indices, TOPIX, sectors, Grok, calendar, pairs, and stock results where relevant
- J-Quants API for daily OHLCV, trading value rankings, gainers/losers, investor types, margin, short ratio, and breadth
- yfinance/parquet fallback for US indices, VIX, futures, FX, commodities, and US rates
- e-Stat / official statistics when macro data is part of the explanation
- EDINET / TDnet / company IR when individual-company filings or disclosures are part of the explanation
- Internal API when report values must match frontend-visible data
- S3 report object after upload, re-downloaded and checked as the served artifact

## Web Search Sources

Use web search for explanation and source attribution, not as a replacement for internal numeric data.

Prefer these sources:

- JPX, Bank of Japan, Ministry of Finance, e-Stat, Cabinet Office, METI, MHLW
- TDnet, EDINET, company IR pages
- Nikkei, Reuters, Bloomberg, QUICK/Nikkei QUICK, Kabutan market close articles
- OANDA, CME, EIA, ICE, Investing.com only as secondary market-data/context sources

Do not use YouTube, Amazon, SNS, message boards, anonymous blogs, or unsourced AI summaries as report evidence.

## Completion Standard

Before upload, review the generated HTML against the reference report:

- Same core sections and comparable density
- Top summary includes the same level of market signal
- Required tables and stat cards are present
- Numeric cells are right-aligned / tabular where applicable
- Facts and inference are separated with evidence labels or clear wording
- Source URLs used for narrative are listed
- S3 re-download matches the intended final artifact

## Regression From 2026-05-27 Report Incident

The 2026-05-27 market report was repeatedly uploaded with incomplete parity against the 2026-05-25 reference report. The root failure was not one ticker. The root failure was treating "same styling as the reference report" as a partial CSS/header task instead of full visible-output parity.

For market report work, "same styling as the reference report" means the target report must preserve the reference report's visible conventions across all comparable sections and tables:

- table and section order
- table headers and calculation-basis labels
- display-name conventions and short names
- row labels and market abbreviations
- evidence labels, badges, footnotes, and explanatory notes
- row highlight classes and positive/negative styling
- density, granularity, and whether a value is shown as data, interpretation, or source note

The `1570` row was only the visible sentinel that exposed this broader failure. The report kept this row in the trading-value table:

```text
1570  野村アセットマネジメント株式会社　ＮＥＸＴ　ＦＵＮＤＳ日経平均レバレッジ・インデックス連動型上場投信  O  その他  69,560  -3.99%  231億
```

while the reference report used the display name:

```text
1570  NF日経レバETF
```

Also, the `変化率` column in `売買代金TOP10` was misunderstood as previous-close return. In the generator it was actually calculated as intraday open-to-close return:

```python
day_chg = ((C - O) / O * 100)
```

Therefore, for trading-value leaders, the header must make the basis explicit, e.g. `当日始値比`, unless the generator is changed to calculate true previous-close return.

This incident must be treated as a required regression check for future market reports and fixes. The check is not limited to `1570`; `1570` is a mandatory sentinel row when present, but every user-mentioned row/table and every comparable reference table must be checked:

- Do not treat "same styling as the reference report" as CSS-only. It includes visible display conventions: short names, table headers, row labels, evidence labels, badge placement, footnotes, and row highlight behavior.
- Before upload, compare the corresponding reference-report table and target-report table row-by-row for visible differences that are not explained by data.
- For ETF and long official J-Quants names, verify that the generated HTML uses the intended short display name. At minimum, check `1570` appears as `NF日経レバETF` when it appears in market-report ranking tables.
- For every percentage column, verify and report the calculation basis from code or source data before renaming it. Do not label an intraday return as `前日比`.
- Before upload, print or parse the exact target rows and table headers that were involved in the user request. If the user asks to match a reference report, also print/parse the corresponding reference rows or table headers for comparison. For the 2026-05-27 incident, one required pre-upload check would have been:

```text
headers= ['コード', '銘柄', '市場', 'セクター', '終値', '当日始値比', '売買代金']
1570= ['1570', 'NF日経レバETF', 'O', 'その他', '69,560', '-3.99%', '231億']
```

- After upload, verify the S3 object metadata and, when the user is validating served content, re-download or fetch the served artifact and grep/parse the same target row. Do not report completion from upload success alone.
