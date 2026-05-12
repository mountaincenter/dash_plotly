---
name: pair-results
description: stock_results.csvからV2_PAIRS該当のペアトレード実績を抽出・集計
---

stock_results.csv から同日・V2_PAIRS 該当・逆方向のペアトレードを検出して一覧表示する。

```bash
python3 /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/.claude/skills/pair-results/pair_results.py
```

検出結果を報告する。件数が0の場合は CSV 最新日付と V2_PAIRS 件数も併記。
