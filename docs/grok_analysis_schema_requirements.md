# Grok Analysis Merged データ要件定義書

**基準日**: 2025-11-18
**生成日**: 2025-11-19 20:00:00
**総カラム数**: 78
**目的**: trading_recommendation.json と grok_analysis_merged.parquet の粒度統一  

---

## 概要

このドキュメントは、`grok_analysis_merged.parquet` の 2025-11-18 時点でのスキーマ定義です。
今後の `trading_recommendation_YYYY-MM-DD.json` は、このスキーマに完全準拠する必要があります。

**重要**: deep_search で取得すべきデータは、このスキーマの全78カラムをカバーすること。

各カラムの「取得元スクリプト/ファイル」列を参照して、データを正しく取得してください。

---

## 基本情報

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `selection_date` | grok_trending.parquet の `date` カラム | Grok推奨日 ※backtest_dateと同じ日付 |
| `backtest_date` | 自動（バックテスト実行日） | バックテスト実行日 ※selection_dateと同じ日付 |
| `ticker` | grok_trending.parquet | ティッカーシンボル |
| `company_name` | meta_jquants.parquet の `stock_name` カラム | 会社名（ticker に対応） |
| `data_source` | 固定値 "trading_recommendation" |  |
| `prompt_version` | 固定値 "v2_manual" |  |

## Grok推奨情報

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `grok_rank` | grok_trending.parquet | Grokランク (1-N) |
| `selection_score` | grok_trending.parquet | Grok選定スコア |
| `category` | meta_jquants.parquet の `sectors` カラム | 業種分類（東証33業種） |
| `reason` | grok_trending.parquet | 推奨理由 |

## 売買判断

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `recommendation_action` | 自動設定（final_action優先、なければv2_action） | フロントエンド用の売買判断 (buy/sell/hold) |
| `recommendation_score` | 自動設定（final_score優先、なければv2_score） | フロントエンド用の最終スコア |
| `recommendation_confidence` | scoreから自動計算 | 信頼度 (low/medium/high) |
| `recommendation_v2_score` | generate_trading_recommendation_v2.py | v2基礎スコア |
| `recommendation_v2_action` | generate_trading_recommendation_v2.py | v2基礎判断 (buy/sell/hold) |
| `recommendation_final_score` | deep_analysis_YYYY-MM-DD.json (finalScore) | 最終スコア (v2 + deep_search調整) |
| `recommendation_final_action` | deep_analysis_YYYY-MM-DD.json から計算 | final_scoreベースの判断 (buy/sell/hold) |
| `has_deep_analysis` | 自動設定（final_score存在時true） | deep_analysis実施フラグ (boolean) |
| `recommendation_stop_loss_percent` | generate_trading_recommendation_v2.py | 損切り率 (%) |
| `recommendation_stop_loss_calculation` | generate_trading_recommendation_v2.py | 損切り計算式 |
| `recommendation_reasons_json` | generate_trading_recommendation_v2.py | 推奨理由JSON |

## 株価・出来高

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `buy_price` | prices_max_1d.parquet (前日終値) | 寄り付き買値 |
| `sell_price` | prices_max_1d.parquet (当日終値) | 大引け売値 |
| `daily_close` | prices_max_1d.parquet |  |
| `high` | prices_max_1d.parquet |  |
| `low` | prices_max_1d.parquet |  |
| `volume` | prices_max_1d.parquet |  |
| `prev_day_close` | prices_max_1d.parquet |  |
| `prev_day_volume` | prices_max_1d.parquet |  |
| `prev_2day_close` | prices_max_1d.parquet |  |
| `prev_2day_volume` | prices_max_1d.parquet |  |
| `morning_close_price` | prices_max_1d.parquet (11:30時点) |  |
| `day_close_price` | prices_max_1d.parquet (15:30時点) |  |
| `day_high` | prices_max_1d.parquet |  |
| `day_low` | prices_max_1d.parquet |  |
| `morning_volume` | prices_max_1d.parquet (11:30時点) |  |

## リターン・勝率 (Phase1: 11:30終値)

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `phase1_return` | バックテスト計算 (11:30終値 - 買値) | Phase1 リターン (円) |
| `phase1_return_pct` | バックテスト計算 |  |
| `phase1_win` | バックテスト計算 (phase1_return > 0) |  |
| `profit_per_100_shares_phase1` | バックテスト計算 | Phase1 100株あたり利益 |

## リターン・勝率 (Phase2: 15:30終値)

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `phase2_return` | バックテスト計算 (15:30終値 - 買値) | Phase2 リターン (円) |
| `phase2_return_pct` | バックテスト計算 |  |
| `phase2_win` | バックテスト計算 (phase2_return > 0) |  |
| `profit_per_100_shares_phase2` | バックテスト計算 |  |

## リターン・勝率 (Phase3: 損切り戦略)

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `phase3_1pct_return` | バックテスト計算 (損切り1%) |  |
| `phase3_1pct_return_pct` | バックテスト計算 |  |
| `phase3_1pct_win` | バックテスト計算 |  |
| `phase3_1pct_exit_reason` | バックテスト計算 (stop_loss/take_profit/time_limit) |  |
| `profit_per_100_shares_phase3_1pct` | バックテスト計算 |  |
| `phase3_2pct_return` | バックテスト計算 (損切り2%) |  |
| `phase3_2pct_return_pct` | バックテスト計算 |  |
| `phase3_2pct_win` | バックテスト計算 |  |
| `phase3_2pct_exit_reason` | バックテスト計算 |  |
| `profit_per_100_shares_phase3_2pct` | バックテスト計算 |  |
| `phase3_3pct_return` | バックテスト計算 (損切り3%) |  |
| `phase3_3pct_return_pct` | バックテスト計算 |  |
| `phase3_3pct_win` | バックテスト計算 |  |
| `phase3_3pct_exit_reason` | バックテスト計算 |  |
| `profit_per_100_shares_phase3_3pct` | バックテスト計算 |  |

## イントラデイ分析

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `morning_high` | prices_max_1d.parquet (9:00-11:30) |  |
| `morning_low` | prices_max_1d.parquet (9:00-11:30) |  |
| `morning_max_gain_pct` | バックテスト計算 (morning_high/buy_price - 1) |  |
| `morning_max_drawdown_pct` | バックテスト計算 (morning_low/buy_price - 1) |  |
| `daily_max_gain_pct` | バックテスト計算 (high/buy_price - 1) |  |
| `daily_max_drawdown_pct` | バックテスト計算 (low/buy_price - 1) |  |
| `profit_morning` | バックテスト計算 |  |
| `profit_day_close` | バックテスト計算 |  |
| `profit_morning_pct` | バックテスト計算 |  |
| `profit_day_close_pct` | バックテスト計算 |  |
| `better_profit_timing` | バックテスト計算 (morning/day_close) |  |
| `better_loss_timing` | バックテスト計算 |  |
| `is_win_morning` | バックテスト計算 |  |
| `is_win_day_close` | バックテスト計算 |  |

## テクニカル指標

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `prev_day_change_pct` | prices_max_1d.parquet計算 |  |
| `prev_day_volume_ratio` | prices_max_1d.parquet計算 |  |
| `morning_volatility` | prices_max_1d.parquet計算 (ATR) |  |
| `daily_volatility` | prices_max_1d.parquet計算 (ATR) |  |

## その他

| カラム名 | 取得元スクリプト/ファイル | 説明 |
|---------|------------------------|------|
| `market_cap` | J-Quants API `/listed/info` または yfinance | 時価総額 |

---

## データ生成フロー

```
1. generate_trading_recommendation_v2.py 実行
   → trading_recommendation.json 生成（基礎スコア、27フィールド）
   → J-Quants API、prices parquet から取得

2. deep_search 実行（手動/WebSearch）
   → deep_analysis_YYYY-MM-DD.json 生成
   → adjustmentReasons.impact を計算
   → finalScore = v2Score + Σimpact

3. merge_trading_recommendation_with_deep_analysis_generic.py
   → trading_recommendation.json に上書きマージ
   → ★ここで75カラム全てを含める必要がある

4. enrich_grok_analysis_with_recommendations.py
   → grok_analysis_merged.parquet に追加
```

---

## 重要な注意事項

### 1. 78カラム全てを含めること

現状、`trading_recommendation.json` は27フィールドしか含んでいません。
**今後は、上記の78カラム全てを含める必要があります。**

### 2. バックテストデータの取得

phase1/2/3 のリターン・勝率、イントラデイ分析データは、
**実際のバックテストを実行**して取得する必要があります。

対象スクリプト: `scripts/backtest/*.py` （要確認）

### 3. null/デフォルト値は厳禁

全カラムに実データを格納すること。
データが取得できない場合は、その理由を明記してユーザーに確認すること。

### 4. selection_date と backtest_date の関係

**`selection_date` と `backtest_date` は常に同じ日付になります。**

- `selection_date`: grok_trending.parquet の `date` カラムから取得（Grok推奨日）
- `backtest_date`: バックテスト実行日（selection_dateと同じ値を設定）

### 5. 売買判断の階層構造

**v2判断 → deep_search → 最終判断の3層構造**

```
v2実行:
  recommendation_v2_score, recommendation_v2_action 生成
  ↓
  recommendation_score = v2_score
  recommendation_action = v2_action
  has_deep_analysis = false

deep_search実行（オプション）:
  recommendation_final_score, recommendation_final_action 生成
  ↓
  recommendation_score = final_score（上書き）
  recommendation_action = final_action（上書き）
  has_deep_analysis = true
```

**フロントエンドは `recommendation_action` / `recommendation_score` のみ参照すれば良い。**

**バックテストでは `v2_action` vs `final_action` の勝率を比較可能。**

### 6. 2025-11-18 を基準

今後のデータは全てこのスキーマに準拠すること。
