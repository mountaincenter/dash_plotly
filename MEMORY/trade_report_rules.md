# Trade Report Rules

This file defines the quality bar for trade review reports and historical report repair.

## Core Contract

- Treat the specified good report, usually `trade_review_20260507.html`, as the structure and granularity specification.
- Do not remove top summaries, strategy summaries, pair leg tables, per-symbol tables, notes, or conclusion-level observations that exist in the reference.
- Do not add collapsed sections or hide required content unless the reference does so.
- Do not reclassify a trade by guess. Reconcile CSV, parquet, S3, and API data first.
- Trade mistakes are classified separately from strategy performance.

## Data Sources

For same-day reports, reconcile:

- `data/csv/stock_results__today.csv`
- `data/csv/hold_stocks.csv`
- `data/csv/order.csv` when order intent or order type matters
- S3/parquet stock results, pairs, calendar, Grok, and strategy signals
- J-Quants API for master, sectors, daily OHLCV, and trading data
- yfinance only for intraday or fallback market data when needed
- Internal API for frontend/report-list consistency
- web search only for individual news, sector context, or external market explanations

For historical repair:

- Use the already-published S3/local report as the artifact to repair.
- Use the reference report's structure as the target.
- Do not claim exact regeneration from mutable sources when the historical input is not reproducible.
- Clearly distinguish repaired report content from unavailable historical raw data.

## Completion Standard

- Title and H1 carry total P/L, date, trade count, and strategy breakdown at the reference granularity.
- Tables preserve numeric alignment and the reference column set unless a difference is reported first.
- Pair trades include both legs and pair-level interpretation.
- Open holdings are included only if the reference/report requirement calls for them.
- S3 upload is followed by re-download and report API/listing verification.
