# Deep Analysis Workflow

`deep_analysis_YYYY-MM-DD.json` を毎日同じ粒度で生成するワークフロー

## 概要

このディレクトリは、`deep_analysis_YYYY-MM-DD.json` を生成するための新しいワークフローです。

**重要**: このワークフローは `docs/DATA_SOURCE_MAPPING.md` の仕様に完全準拠しています。

## ファイル構成

```
scripts/deep_search/
├── README.md                          # このファイル
├── generate_deep_analysis_base.py     # ステップ1: 自動データ収集
└── validate_deep_analysis.py          # ステップ3: バリデーション
```

## データ構造

`deep_analysis_YYYY-MM-DD.json` は29個のトップレベルフィールドを持ちます:

### 自動収集フィールド（generate_deep_analysis_base.py）

1. **companyInfo** (7 keys) - J-Quants `/listed/info` + `meta_jquants.parquet`
   - companyName, companyNameEnglish, sector17, sector33, marketCode, marketName, scaleCategory

2. **fundamentals** (15 keys) - J-Quants `/fins/statements` + yfinance
   - disclosedDate, fiscalYear, fiscalPeriod, eps, bps
   - operatingProfit, ordinaryProfit, netIncome, revenue
   - totalAssets, equity, roe, roa
   - revenueGrowthYoY, profitGrowthYoY

3. **priceAnalysis** (5 keys) - `prices_60d_5m.parquet` + yfinance
   - trend, priceMovement, volumeAnalysis, technicalLevels, patternAnalysis

4. **stockPrice** (4 keys) - `prices_max_1d.parquet`
   - current, change_YYYY-MM-DD, volumeChange_YYYY-MM-DD, materialExhaustion

5. **earnings** (7 keys) - yfinance quarterly earnings
   - date, quarter, revenue, revenueGrowth
   - operatingProfit, operatingProfitGrowth, evaluation

### 手動入力フィールド（Claude Code WebSearch）

6. **analyst** (4 keys) - **必須**
   - hasCoverage, targetPrice, upside, rating
   - WebSearch: "{ticker} {stockName} アナリスト 目標株価 コンセンサス 2025"

7. その他の手動フィールド:
   - latestNews, webMaterials, adjustmentReasons, risks, opportunities
   - sectorTrend, marketSentiment, newsHeadline, verdict

## ワークフロー

### ステップ1: 自動データ収集

```bash
python3 scripts/deep_search/generate_deep_analysis_base.py
```

**入力**: `data/parquet/backtest/trading_recommendation.json`

**出力**: `data/parquet/backtest/analysis/deep_analysis_YYYY-MM-DD_base.json`

**処理内容**:
- trading_recommendation.json から銘柄リストと日付を取得
- J-Quants API で企業情報・財務情報を取得
- yfinance で株価・決算情報を取得
- parquet ファイルから価格データを取得
- analyst フィールドは空プレースホルダー

**所要時間**: 約1-2分（10銘柄の場合）

---

### ステップ2: 手動WebSearch（Claude Code UI）

**重要**: この工程は自動化できません。Claude Code UI で手動実行してください。

各銘柄について以下のクエリでWebSearchを実行:

```
{ticker} {stockName} アナリスト 目標株価 コンセンサス 2025
```

**例**:
- `6269.T 三井海洋開発 アナリスト 目標株価 コンセンサス 2025`
- `8061.T 西華産業 アナリスト 目標株価 コンセンサス 2025`

**抽出情報**:
- `analyst.hasCoverage`: カバレッジがある場合 `true`、ない場合 `false`
- `analyst.targetPrice`: 目標株価（円）
- `analyst.upside`: アップサイド（%）
- `analyst.rating`: レーティング（"買い" / "中立" / "売り" / "カバレッジなし"）

**出力**: 手動でJSONファイルを編集

---

### ステップ3: JSON編集

`deep_analysis_YYYY-MM-DD_base.json` を開いて、各銘柄の `analyst` フィールドを編集:

**カバレッジがある場合**:
```json
"analyst": {
  "hasCoverage": true,
  "targetPrice": 3500,
  "upside": 25.5,
  "rating": "買い"
}
```

**カバレッジがない場合**:
```json
"analyst": {
  "hasCoverage": false,
  "targetPrice": null,
  "upside": null,
  "rating": "カバレッジなし"
}
```

編集完了後、ファイル名を変更:
```bash
mv deep_analysis_YYYY-MM-DD_base.json deep_analysis_YYYY-MM-DD.json
```

---

### ステップ4: バリデーション

```bash
python3 scripts/deep_search/validate_deep_analysis.py YYYY-MM-DD
```

**検証内容**:
- 29個のトップレベルフィールドが存在するか
- 各ネストされたオブジェクトの必須キーが存在するか
- null値がないか（optionalフィールドを除く）
- 空文字列がないか（許可されたフィールドを除く）
- analyst情報が入力されているか（必須）

**成功例**:
```
✅ All validation checks passed!

File is valid: deep_analysis_2025-11-19.json
Total stocks: 10
```

**失敗例**:
```
❌ Found 3 validation errors:

  [8061.T] analyst.targetPrice: null値（hasCoverage=Trueの場合は必須）
  [8061.T] analyst.upside: null値（hasCoverage=Trueの場合は必須）
  [8061.T] analyst.rating: レーティングが設定されていない
```

---

### ステップ5: データ統合

バリデーション成功後、既存のパイプラインに統合:

#### 5-1. trading_recommendation.json にマージ

```bash
python3 scripts/merge_trading_recommendation_with_deep_analysis.py
```

#### 5-2. grok_analysis_merged.parquet にマージ

```bash
python3 scripts/pipeline/enrich_grok_analysis_with_deep_analysis.py
```

---

## データソース優先順位

`docs/DATA_SOURCE_MAPPING.md` の定義に従います:

1. **Parquet ファイル** (最優先)
   - `prices_max_1d.parquet`
   - `prices_60d_5m.parquet`
   - `meta_jquants.parquet`

2. **J-Quants API** (第2優先)
   - `/listed/info`
   - `/fins/statements`

3. **yfinance** (第3優先)
   - 株価履歴
   - 財務諸表
   - 決算情報

4. **Claude Code WebSearch** (手動)
   - アナリスト情報
   - 最新ニュース
   - セクター動向

---

## 環境変数

```bash
export JQUANTS_REFRESH_TOKEN="your_jquants_refresh_token"
```

J-Quants API を使用する場合は必須です。設定がない場合は yfinance と parquet のみ使用します。

---

## トラブルシューティング

### Q1. J-Quants認証に失敗する

```
⚠️  JQUANTS_REFRESH_TOKEN not set, J-Quants data will be unavailable
```

**解決策**: 環境変数を設定してください:
```bash
export JQUANTS_REFRESH_TOKEN="your_token"
```

---

### Q2. バリデーションエラー: analyst情報がない

```
❌ [8061.T] analyst.targetPrice: null値（hasCoverage=Trueの場合は必須）
```

**解決策**: ステップ2のWebSearchを実行し、JSONファイルを手動編集してください。

---

### Q3. parquetファイルが見つからない

```
⚠️  prices_max_1d.parquet not found
```

**解決策**: データ収集スクリプトを実行してください:
```bash
python3 scripts/fetch_jquants_data.py
python3 scripts/fetch_yfinance_data.py
```

---

## 既存ファイルとの関係

### 削除推奨（日付固定・重複機能）

以下のファイルは新しいワークフローで置き換えられます:

- `scripts/finalize_deep_analysis.py` (2025-11-17固定)
- `scripts/finalize_deep_analysis_2025-11-18.py` (2025-11-18固定)
- `scripts/update_deep_analysis_comprehensive.py` (2025-11-17固定)
- `scripts/fetch_deep_analysis_data.py` (ティッカー固定、ファイル保存なし)

### 保持（パイプライン統合用）

以下のファイルは引き続き使用します:

- `scripts/pipeline/enrich_grok_analysis_with_deep_analysis.py` - parquetマージ用
- `scripts/merge_trading_recommendation_with_deep_analysis.py` - JSONマージ用

---

## 参考ドキュメント

- `docs/DATA_SOURCE_MAPPING.md` - フィールド定義・データソース仕様
- `data/parquet/backtest/analysis/deep_analysis_2025-11-18.json` - 参照実装（29フィールド）

---

## 更新履歴

- 2025-11-19: 初版作成（`docs/DATA_SOURCE_MAPPING.md` 完全準拠版）
