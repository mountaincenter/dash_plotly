---
name: weekly-report
description: 週次マーケットレポートを作成してS3にアップロード
disable-model-invocation: true
---

週次マーケットレポートを作成する。対象週: $ARGUMENTS（YYYY-MM-DD形式で週末金曜日を指定、省略時は直近金曜日）

## 手順（省略禁止）

### Step 1: 対象週の特定
引数から対象週の月曜〜金曜の日付範囲を算出する。祝日がある場合は営業日のみ対象。

### Step 2: 日次report_dataの収集
対象週の各営業日のreport_data JSONをS3から取得する：
```bash
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly
for d in <月曜> <火曜> <水曜> <木曜> <金曜>; do
  aws s3 cp "s3://stock-api-data/parquet/market_summary/structured/report_data_${d}.json" "/tmp/report_data_${d}.json" --only-show-errors 2>/dev/null
done
```
取得できた日付と取得できなかった日付を報告する。

### Step 3: parquetからの週間データ集計
S3から主要parquetをダウンロードし、週間集計を算出する：
- N225/TOPIX/VI: 週初値→週末値、週間変化率、日別終値
- TOPIXサブ指数（Prime/Standard/Growth）: 週間変化率
- 33業種: 週間変化率（週初→週末）、上昇/下落セクター数
- USD/JPY, WTI, Gold, US10Y: 日別推移と週間変化
- market_breadth: 日別の騰落銘柄数・売買代金
- カレンダーアノマリー: market_anomaly.parquetから来週分を抽出

算出した全数値をユーザーに提示して確認を得る。

### Step 4: マーケットコンテキスト収集
WebSearchで対象週の以下を取得：
- 週内の主要ニュース・イベント（日経新聞等）
- 地政学リスク・政策イベント
- 来週の注目イベント（FOMC、雇用統計、決算等）

収集結果をユーザーに提示する。

### Step 5: テンプレート読み込み
S3から直近の週次レポートをダウンロードしてテンプレートとする：
```bash
aws s3 ls s3://stock-api-data/reports/ | grep "market_analysis_weekly" | sort | tail -1
```
テンプレートのセクション構成を確認する。

**必須セクション（10セクション、テンプレートと完全一致）:**
1. 週間パフォーマンス（N225/TOPIX/VI stat-card + 週間レンジ）
2. 日別パフォーマンス（日次テーブル: N225終値/変化/変化率/騰落/売買代金）
3. 今週のテーマ分析（4大テーマ factor-card）
4. TOPIXサブ指数 週間比較（テーブル + 累計パフォーマンス）
5. セクター動向（上昇トップ10 / 下落セクター テーブル）
6. 為替・金利・商品 週間推移（USD/JPY日次テーブル + 金利・商品変化テーブル）
7. 今週の注目ニュース（日別ニュースカード）
8. 来週の注目イベント（日別イベントリスト）
9. カレンダーアノマリー（来週のweek/月/曜日別傾向テーブル）
10. 結論（来週の見通し）

### Step 6: HTML生成
テンプレートと同じCSSスタイル・セクション構造でHTMLを生成する。

**絶対ルール：**
- 全数値はStep 2-3のデータから取得。1つも捏造しない。
- evidence label（事実/推論/未検証）を全セクションに付与する。
- 出典ラベルは実際のデータソースを正確に表記する。

出力先: /tmp/market_analysis_weekly_<YYYYMMDD>.html（金曜日の日付）

### Step 7: 数値検証
週間変化率（N225, TOPIX, VI）、セクター首位/最下位、為替変化がHTMLに正しく反映されているか確認する。
検証結果をユーザーに提示。不一致があれば修正。

### Step 8: S3アップロード
検証通過後のみアップロード：
```bash
aws s3 cp /tmp/market_analysis_weekly_<YYYYMMDD>.html s3://stock-api-data/reports/market_analysis_weekly_<YYYYMMDD>.html --content-type "text/html; charset=utf-8"
```
