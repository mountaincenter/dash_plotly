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
