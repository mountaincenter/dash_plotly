---
name: report
description: 日次マーケットレポートを作成してS3にアップロード
disable-model-invocation: true
---

日次マーケットレポートを作成する。対象日: $ARGUMENTS（YYYY-MM-DD形式、省略時は今日）

## 手順（省略禁止）

### Step 1: データ同期確認
ユーザーに「S3同期は完了していますか？」と確認する。完了していなければ待つ。

### Step 2: report_data生成
```bash
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly
python3 scripts/pipeline/generate_market_report_data.py --date <対象日>
```
全セクションがOKであることを確認。ERRORがあれば原因を報告して停止。

### Step 3: データ確認
report_data JSONを読み、以下を全て確認して表示する：
- N225: close, change, change_pct, high, low
- TOPIX: close, change_pct
- VI: close, prev_close, change, high, low
- 騰落銘柄数・売買代金（market_breadth）
- セクター: up_count, down_count, 首位, 最下位
- USD/JPY, WTI, Gold
- Grok: total, grade_distribution
- web_search_required の内容

数値を全てユーザーに提示して確認を得る。

### Step 4: マーケットコンテキスト収集
以下の情報をWebSearchで取得する：
- 当日の主要ニュース・イベント（日経新聞等）
- 前日のNY市場の動き
- 地政学リスク（イラン情勢等、進行中のもの）
- 権利確定日・期末等のカレンダーイベント
- CME NKD先物の動き

収集したコンテキストをユーザーに提示する。

### Step 5: テンプレート読み込み
S3から直近の日次レポートをダウンロードしてテンプレートとする：
```bash
aws s3 ls s3://stock-api-data/reports/ | grep "market_analysis_2026" | grep -v weekly | sort | tail -1
```
テンプレートを完全に読み、セクション構成・カード数・テーブルカラムを確認する。

### Step 6: HTML生成
テンプレートと完全に同じ構造でHTMLを生成する。

**絶対ルール：**
- 全ての数値はStep 3のreport_data JSONから取得する。1つも捏造しない。
- マーケットコンテキスト（Step 4）は要因分析・結論に反映する。
- evidence label（事実/推論/未検証）を全セクションに付与する。
- 3月期末要因（権利確定日、権利落ち日、期末ドレッシング等）が該当する場合は必ず言及する。

出力先: /tmp/market_analysis_<YYYYMMDD>.html

### Step 7: 数値検証
report_data JSONの主要数値（N225, TOPIX, VI, 騰落, WTI, セクター首位/最下位, Grok grade）がHTMLに正しく含まれているかをプログラムで検証する。

```python
# 検証スクリプトを実行
# 全項目一致でなければアップロードしない
```

検証結果をユーザーに提示する。不一致があれば修正してから次に進む。

### Step 8: S3アップロード
検証が全項目通過した場合のみアップロードする。
```bash
aws s3 cp /tmp/market_analysis_<YYYYMMDD>.html s3://stock-api-data/reports/market_analysis_<YYYYMMDD>.html --content-type "text/html; charset=utf-8"
```

アップロード完了をユーザーに報告する。
