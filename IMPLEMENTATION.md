# Granville戦略 実装仕様書

## ステータス: 検証完了 → 実装フェーズ

本ドキュメントはstagingブランチでの実装に必要な全情報を記載する。
分析・検証はmainブランチで完了済み（commit 3b5f494）。

---

## 1. 確定パラメータ

### エントリー

| 項目 | 値 |
|------|-----|
| 方向 | LONG のみ（ショートはGrok推奨で別管理） |
| シグナル | グランビル B1, B2, B3, B4 |
| 優先順位 | B4 > B1 > B3 > B2 |
| ML優先順位 | 同一ルール内はML予測スコア順（LightGBM walk-forward） |
| ユニバース | TOPIX 1,660銘柄 |
| エントリー | シグナル翌営業日の寄付(Open) |

### グランビル法則 定義（LONG）

| 法則 | 条件 |
|------|------|
| B1 | 前日Close < SMA20, 当日Close > SMA20, SMA20上昇 |
| B2 | SMA20上昇, 乖離-5~0%, Close < SMA20, 陽線 |
| B3 | SMA20上昇, Close > SMA20, 乖離0-3%, 乖離縮小, 陽線 |
| B4 | 乖離 < -8%, 陽線 |

### 出口

| 項目 | 値 |
|------|-----|
| Exit条件 | 20日高値（当日High >= rolling 20日間の最高High） |
| ストップロス | なし |
| MAX_HOLD（ルール別） | B1=7日, B2=30日, B3=5日, B4=13日 |

### 資金管理

| 項目 | 値 |
|------|-----|
| 集中制限 | 15%（1銘柄の証拠金が現在資金の15%を超えない） |
| 証拠金計算 | upper_limit(entry_price) × 100株 |
| 証拠金率 | 30%（楽天証券 制度信用） |
| 手数料 | 0円（楽天証券 ゼロコース） |

### 証拠金テーブル（upper_limit）

```python
_LIMIT_TABLE = [
    (100, 30), (200, 50), (500, 80), (700, 100),
    (1000, 150), (1500, 300), (2000, 400), (3000, 500),
    (5000, 700), (7000, 1000), (10000, 1500), (15000, 3000),
    (20000, 4000), (30000, 5000), (50000, 7000), (70000, 10000),
    (100000, 15000), (150000, 30000), (200000, 40000), (300000, 50000),
    (500000, 70000), (700000, 100000), (1000000, 150000),
]
```

---

## 2. 検証根拠（主要データ）

### B4 暴落期パフォーマンス
- 市場5日変化率 < -5%: WR=48.0%, avgPnL=+4,425（全環境中最高）
- VIX 25-30: WR=46.7%, avgPnL=+3,169（全VIXレンジ中最高）
- 5日以上連続下落: avgPnL=+2,159, avgRet=+3.48%

### MAX_HOLD ルール別最適化（資金制約下ポートフォリオシミュレーション）
| ルール | 最適MAX_HOLD | 累計PnL | vs 60日比 |
|--------|-------------|---------|-----------|
| B4 | 13日 | +2,903万 | +17万改善 |
| B1 | 7日 | +1,669万 | +817万改善(+96%) |
| B3 | 5日 | +781万 | +215万改善(+38%) |
| B2 | 30日 | +2,084万 | ほぼ同等 |

### ML優先順位
- Walk-forward検証（3年学習→1年テスト）でルール優先・RSI順を上回る
- 特徴量: sma20_dist, sma50_dist, atr14_pct, rsi14, vol20, ret5d, vol_ratio, entry_price + rule/regime dummy

---

## 3. TOPIX 1,660銘柄 データ取得・更新フロー

### 3a. 初期データ構築（検証時に実施済み）

```
meta_jquants.parquet (3,769銘柄)
  ↓ classify_segment() でTOPIX 5セグメントに分類
  ↓
improvement/granville/prices/fetch_all_prices.py
  ↓ yfinance batch download (50銘柄ずつ)
  ↓
improvement/granville/prices/
  ├── core30.parquet     (31銘柄)
  ├── large70.parquet    (69銘柄)
  ├── mid400.parquet     (396銘柄)
  ├── small1.parquet     (494銘柄)
  └── small2.parquet     (671銘柄)
  ↓
strategy_verification/scripts/13_expand_universe.py
  ↓ クリーニング (Volume=0除外, NaN除外, 異常リターン検出, JQuants突合検証)
  ↓
strategy_verification/data/processed/prices_cleaned_topix_v3.parquet
  (1,658 tickers, 1999-05-06 ~ 2026-03-06)
```

### 3b. 日次更新の課題（要実装）

現行パイプラインは `data/parquet/prices_max_1d.parquet` (168銘柄) のみ更新。
TOPIX 1,660銘柄は検証用に手動で取得しただけで、日次更新の仕組みがない。

**実装が必要:**
1. **TOPIX価格の日次差分更新スクリプト** — 毎日の終値を追記
   - 元データ: `improvement/granville/prices/*.parquet` (yfinance)
   - または `prices_cleaned_topix_v3.parquet` に直接追記
   - yfinance の `period="5d"` で直近分を取得し既存データにappend
2. **クリーニング適用** — 13_expand_universe.py と同じルール
3. **パイプライン統合** — `data-pipeline.yml` の16:45ジョブに追加

**方針案:**
```python
# scripts/pipeline/update_topix_prices.py (新規)
# 1. prices_cleaned_topix_v3.parquet の最終日付を取得
# 2. yfinance で最終日〜当日を1,660銘柄分取得
# 3. クリーニング適用
# 4. 既存parquetに追記
# 5. S3にアップロード
```

### 3c. パイプライン統合（23:00 / 16:45）

現行パイプライン `.github/workflows/data-pipeline.yml` に統合する。
mainの既存パイプラインを壊さないこと。

**日次フロー:**
```
16:45 (data-pipeline)
  → 株価更新 168銘柄（既存）
  → TOPIX 1,660銘柄 価格更新（新規 ★）
  → Granvilleシグナル生成（新規）
  → ML予測スコア付与（新規）

23:00 (data-pipeline)
  → Grok選定（既存）
  → Granville + Grok統合レコメンド生成（新規）
```

**新規スクリプト（案）:**
- `scripts/pipeline/update_topix_prices.py` — TOPIX 1,660銘柄の日次差分更新
- `scripts/pipeline/generate_granville_signals.py` — シグナル生成
- `scripts/pipeline/predict_granville_ml.py` — ML予測
- `scripts/pipeline/generate_granville_recommendation.py` — 推奨銘柄リスト

**出力:**
- `data/parquet/granville/prices_topix.parquet` — TOPIX日足（日次更新）
- `data/parquet/granville/signals_YYYY-MM-DD.parquet`
- `data/parquet/granville/recommendations_YYYY-MM-DD.parquet`
- S3: `s3://stock-api-data/parquet/granville/`

### 3b. フロントエンド

`stock-frontend` の staging ブランチに実装。
Granville推奨銘柄の表示ページ。

### 3c. サーバーAPI

`server/` にGranville推奨銘柄のエンドポイント追加。

---

## 4. データソース

| データ | パス | 用途 |
|--------|------|------|
| TOPIX株価 | `strategy_verification/data/processed/prices_cleaned_topix_v3.parquet` | シグナル生成元 |
| TOPIX銘柄リスト | `data/parquet/meta_jquants.parquet` | ticker→銘柄名変換 |
| MLモデル | 学習時に生成（walk-forward） | 優先順位スコア |
| 信用余力 | `data/csv/credit_capacity.csv`（手動更新 or API） | 資金制約 |
| 保有銘柄 | `data/csv/hold_stocks.csv`（手動更新 or API） | 重複排除 |

### 価格データ取得
TOPIX 1,660銘柄のデータは `prices_cleaned_topix_v3.parquet` に格納。
日次更新は J-Quants API 経由。更新スクリプトは既存パイプラインに依存。

---

## 5. 既存コード参照

検証で使用したスクリプト（mainブランチ）:

| ファイル | 内容 |
|----------|------|
| `strategy_verification/scripts/16_entry_priority_analysis_topix.py` | 特徴量計算、証拠金計算、cap_eff分析 |
| `strategy_verification/scripts/17_entry_priority_ml_topix.py` | LightGBM walk-forward ML予測 |
| `strategy_verification/scripts/18_portfolio_simulation_topix.py` | ポートフォリオシミュレーション |
| `improvement/granville/SPEC.md` | 元の検証仕様書 |
| `improvement/backtest_granville_8rules.py` | グランビルシグナル生成ロジック |

---

## 6. 運用イメージ

### 毎日の流れ
1. 16:45: 株価更新 → シグナル生成 → ML予測
2. 23:00: Grok SHORT推奨取得 → 統合レコメンド
3. 翌朝: フロントエンドで確認 → 寄付で発注

### ポジション管理
- LONG: Granvilleシグナル（B4>B1>B3>B2）
- SHORT: Grok推奨
- Exit: 日次で20日高値チェック + MAX_HOLDチェック
- 既存ポジションとの重複排除

---

## 7. ブランチ戦略

```
main          ← 本番パイプライン（23:00稼働中）
staging       ← Granville実装（このブランチ）
```

- stagingで実装・検証完了後にmainへマージ
- mainへの直接pushは保守・修正のみ
- frontendもstagingブランチで並行開発

---

## 変更履歴

| 日付 | 内容 |
|------|------|
| 2026-03-08 | 検証完了、実装仕様書作成 |
