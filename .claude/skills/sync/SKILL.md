---
name: sync
description: S3同期 + 取引結果集計を両方実行
---

以下の2つを順番に実行する。

### 1. S3同期
```bash
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly
python3 scripts/sync/download_from_s3.py
```

### 2. 取引結果集計
```bash
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly
python3 scripts/generate_stock_results_html.py
```

両方の実行結果を報告する。エラーがあれば原因を提示。
