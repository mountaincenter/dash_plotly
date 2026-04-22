---
name: today-results
description: stock_results＿today.csv から当日取引レビューHTMLを生成してS3アップロード
disable-model-invocation: true
---

取引結果レビューを作成する。対象日は **CSV 約定日 = 今日 (JST) のみ**（過去日生成は不可）。

## 実行タイミング制約
- **当日中（CSV 約定日と同日 JST）に実行**する
- 過去日生成は禁止：grok_trending.parquet / signals.parquet / eq master が翌朝上書きされる仕様で、過去日の正確な再現は物理的に不可能

## 手順（省略禁止）

### Step 1: 事前確認
- CSV `data/csv/stock_results＿today.csv` の「約定日」列を確認
- 複数日混在 / 約定日 ≠ 今日 (JST) の場合はスクリプトがエラー停止する

### Step 2: HTML生成
```bash
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly
python3 .claude/skills/today-results/gen_trade_review.py
```
- 出力先: `data/reports/trade_review/YYYYMMDD.html`
- 実行ログで各トレードの分類（grok/pair/granville/reversal/other）を確認

### Step 3: 内容確認（必須）
生成された HTML をユーザーに以下を報告して確認を得る:
- 各トレードの分類結果
- ペア検出結果（セクター・peer銘柄）
- 合計損益・セクション数

### Step 4: S3アップロード
確認が通った場合のみ実行する:
```bash
aws s3 cp data/reports/trade_review/YYYYMMDD.html \
  s3://stock-api-data/reports/trade_review_YYYYMMDD.html \
  --content-type "text/html; charset=utf-8" \
  --cache-control "no-cache, no-store, must-revalidate"
```

### Step 5: 反映確認
```bash
curl -s https://muuq3bv2n2.ap-northeast-1.awsapprunner.com/api/dev/reports | \
  jq '.reports[] | select(.filename | startswith("trade_review_"))' | head -20
```

## データソース

| データ | ソース |
|--------|--------|
| 取引結果 | `data/csv/stock_results＿today.csv` |
| grok bucket/prob | `s3://stock-api-data/parquet/grok_trending.parquet` |
| 戦略分類 (granville/pairs/reversal) | `s3://stock-api-data-staging/parquet/signals.parquet` |
| 銘柄マスタ・セクター | `jquants eq master` |
| 日足 | `jquants eq daily` |
| 5分足 | yfinance |

## 前提

- jquants CLI ログイン済み（`jquants login`）
- AWS 認証済み（`aws s3 ls` が通る）
- memo は空で生成。必要なら HTML を手動編集
