---
name: monthly-report
description: 月次マーケットレポートを作成してS3にアップロード
disable-model-invocation: true
---

月次マーケットレポートを作成する。対象月: $ARGUMENTS（YYYY-MM形式、省略時は前月）

## 手順（省略禁止）

### Step 1: 対象月の特定
対象月の全営業日リストを算出する。

### Step 2: 日次report_dataの収集
対象月の全営業日のreport_data JSONをS3から取得する：
```bash
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly
aws s3 ls s3://stock-api-data/parquet/market_summary/structured/ | grep "report_data_<YYYY-MM>" | awk '{print $4}'
```
各JSONをダウンロードし、取得できた/できなかった日付を報告する。

### Step 3: parquetからの月間データ集計
S3から主要parquetをダウンロードし、月間集計を算出する：

**月間サマリー:**
- N225: 月初値→月末値、月間変化率、月間高値/安値/レンジ
- TOPIX: 同上 + Prime/Standard/Growth
- VI: 月間レンジ、平均値
- USD/JPY: 月間変化

**週別パフォーマンス:**
- 各週の始値→終値、変化率、騰落日数

**日次騰落:**
- 全営業日のN225終値・前日比（チャート用データ）

**セクター月間動向:**
- 33業種: 月初→月末の変化率
- 上昇/下落セクター数、トップ5/ワースト5

**為替・金利・商品:**
- USD/JPY, WTI, Gold, Copper: 月初→月末
- US10Y, JGB10Y, 内外金利差: 月間変化
- 無担保コールO/N: 月間推移

算出した全数値をユーザーに提示して確認を得る。

### Step 4: マーケットコンテキスト収集
WebSearchで対象月の以下を取得：
- 月内の重大イベント（FOMC、雇用統計、CPI、BOJ、地政学）
- 市場を動かした主要テーマ（時系列で整理）
- 翌月の注目イベント

収集結果をユーザーに提示する。

### Step 5: テンプレート読み込み
S3から直近の月次レポートをダウンロードしてテンプレートとする：
```bash
aws s3 ls s3://stock-api-data/reports/ | grep "market_analysis_monthly" | sort | tail -1
```
テンプレートのセクション構成を確認する。

**必須セクション（9セクション、テンプレートと完全一致）:**
1. 月間サマリー（N225/TOPIX/VI/USD stat-card + 月間ヘッドライン）
2. 月間タイムライン（重要イベント×株価インパクトを時系列で記述）
3. 週別パフォーマンス（テーブル: 各週の始値/終値/変化率/騰落日数）
4. 日次騰落チャート（CSS barチャート: 全営業日の前日比%）
5. 月間テーマ分析（主要テーマ4つのfactor-card）
6. セクター月間動向（トップ10/ワースト10テーブル + 全33業種テーブル）
7. 為替・金利・商品 月間推移（テーブル: 月初→月末→変化）
8. 月間の教訓・統計（トレード成績やリスク管理の振り返り）
9. 結論 + 来月の展望

### Step 6: HTML生成
テンプレートと同じCSSスタイル・セクション構造でHTMLを生成する。

**絶対ルール：**
- 全数値はStep 2-3のデータから取得。1つも捏造しない。
- evidence label（事実/推論/未検証）を全セクションに付与する。
- 出典ラベルは実際のデータソースを正確に表記する。
- 月間タイムラインのイベント×株価影響は、日次データで裏付けが取れるもののみ記載する。

出力先: /tmp/market_analysis_monthly_<YYYYMMDD>.html（月末営業日の日付）

### Step 7: 数値検証
月間変化率（N225, TOPIX）、セクタートップ/ワースト、週別パフォーマンスがHTMLに正しく反映されているか確認する。
検証結果をユーザーに提示。不一致があれば修正。

### Step 8: S3アップロード
検証通過後のみアップロード：
```bash
aws s3 cp /tmp/market_analysis_monthly_<YYYYMMDD>.html s3://stock-api-data/reports/market_analysis_monthly_<YYYYMMDD>.html --content-type "text/html; charset=utf-8"
```
