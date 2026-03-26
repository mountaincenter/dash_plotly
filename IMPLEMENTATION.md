# Granville戦略 実装仕様書

## ステータス: 検証完了 → 実装フェーズ

本ドキュメントはstagingブランチでの実装に必要な全情報を記載する。
分析・検証はmainブランチで完了済み（commit 3b5f494）。

全パラメータはセッション履歴 `c61d485c` `1945253e` で検証・合意済み。

---

## 1. 確定パラメータ

### エントリー

| 項目 | 値 | 根拠 |
|------|-----|------|
| 方向 | LONG のみ | ショートは全レジームで機能しない（検証済み） |
| シグナル | グランビル B1, B2, B3, B4 | |
| ルール間優先順位 | B4 > B1 > B3 > B2 | B4のcap_effが唯一正（+71.5） |
| 同一ルール内ソート | **RSI lowest（RSI14が低い順）** | ML不使用。RSI lowがcap_effと最強の負相関(-0.365) |
| ユニバース | TOPIX 1,660銘柄 | |
| エントリー | シグナル翌営業日の寄付(Open) | |
| SHORT | Grok推奨で別管理 | Granvilleとは別系統 |

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
| Exit条件 | 直近高値更新（当日High >= エントリー後rolling高値） |
| Exit約定 | **条件発火翌営業日の寄付(Open)** |
| ストップロス | なし |
| MAX_HOLD（ルール別） | B1=13日, B2=15日, B3=14日, B4=19日 |

MAX_HOLDは資金制約あり（¥4,650,000, 集中制限15%）のポートフォリオシミュレーションで最適化:
- B4=19日: 資金効率+16.0%。長く持つほど利益が伸びる。80%がMAX_HOLDまで保有
- B1=13日: 資金効率+21.0%。無制約では7日だったが、資金制約下では回転より保有が有利
- B3=14日: 資金効率+22.3%。無制約では5日だったが同上
- B2=15日: 資金効率+26.1%。無制約では30日だったが、資金拘束が長すぎて機会損失

旧値（無制約最適化: B1=7, B2=30, B3=5, B4=13）は資金制約を考慮していなかったため、
¥4,650,000規模のポートフォリオでは上記が最適。検証期間: 2024-03-17〜2026-03-17（2年）

### 資金管理

| 項目 | 値 | 根拠 |
|------|-----|------|
| 集中制限 | **15%**（1銘柄の証拠金が現在資金の15%を超えない） | 5-20%は大差なし、制限なし(100%)が最悪。15%がPnL最良 |
| 株価上限フィルター | **なし** | 固定値フィルターはパフォーマンス悪化する。エッジは株価に依存しない(PF=1.546-1.549で同一)。集中制限15%が動的フィルターとして機能する |
| 証拠金計算 | upper_limit(entry_price) × 100株 | |
| 証拠金率 | 30%（楽天証券 制度信用） | |
| 手数料 | 0円（楽天証券 ゼロコース） | |
| Max positions | **未確定** | 検証では10/15/16/20をテスト。資金制約が実質的な上限 |

### ML（機械学習）について

**実装では使わない。** 検証はしたが、RSI lowestが実用上十分。
MLは運用の限界が来たら導入する（ユーザー合意済み）。

検証結果の参考値:
- ML cap_eff: +13.52（最良）
- RSI lowest: +7.42
- ただし累積PnL: RSI +730万 > ML +587万
- 複雑さに見合う効果がない

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

集中制限15%の具体例（資金465万の場合）:
- max_margin = 465万 × 15% = 69.75万
- 15,000円株: margin = 180万 → **スキップ**（15%超）
- 5,000円株: margin = 57万 → **エントリー可**（15%以内）

### シグナル量の設計思想

1日のシグナル数は平均約95件、多い日は数百件になる。これは仕様通り。
大量シグナルを以下で自動選別する設計:
1. ルール優先順位（B4 > B1 > B3 > B2）
2. 同一ルール内RSI lowest
3. 集中制限15%（高額銘柄を動的に除外）
4. 資金制約（余力がなくなれば自動停止）

**シグナル側にフィルターを追加してはいけない。機会損失になる。**

---

## 2. 検証根拠（主要データ）

### B4 暴落期パフォーマンス
- 市場5日変化率 < -5%: WR=48.0%, avgPnL=+4,425（全環境中最高）
- VIX 25-30: WR=46.7%, avgPnL=+3,169（全VIXレンジ中最高）
- 5日以上連続下落: avgPnL=+2,159, avgRet=+3.48%
- B4は暴落時こそ最も機能する

### 株価フィルター検証結果
- エッジは株価に依存しない（PF=1.546-1.549で全価格帯同一）
- 固定値フィルター(15,000円)はPnL+1,482万
- 制限なし: PnL+50,866万（ただし集中リスクあり）
- **集中制限15%: PnL+54,021万（最良）**
- 固定値より集中制限が理論的に正しく、パフォーマンスも上

### MAX_HOLD ルール別最適化
| ルール | 最適MAX_HOLD | 累計PnL | vs 60日比 |
|--------|-------------|---------|-----------|
| B4 | 13日 | +2,903万 | +17万改善 |
| B1 | 7日 | +1,669万 | +817万改善(+96%) |
| B3 | 5日 | +781万 | +215万改善(+38%) |
| B2 | 30日 | +2,084万 | ほぼ同等 |

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

23:00 (data-pipeline)
  → Grok選定（既存）
  → Granville + Grok統合レコメンド生成（新規）
```

**新規スクリプト:**
- `scripts/pipeline/update_topix_prices.py` — TOPIX 1,660銘柄の日次差分更新
- `scripts/pipeline/generate_granville_signals.py` — シグナル生成
- `scripts/pipeline/generate_granville_recommendation.py` — 推奨銘柄リスト（優先順位+資金制約で選別）

**出力:**
- `data/parquet/granville/prices_topix.parquet` — TOPIX日足（日次更新）
- `data/parquet/granville/signals_YYYY-MM-DD.parquet`
- `data/parquet/granville/recommendations_YYYY-MM-DD.parquet`
- S3: `s3://stock-api-data/parquet/granville/`

### 3d. フロントエンド

`stock-frontend` の staging ブランチに実装。
Granville推奨銘柄の表示ページ。

### 3e. サーバーAPI

`server/` にGranville推奨銘柄のエンドポイント追加。

---

## 4. データソース

| データ | パス | 用途 |
|--------|------|------|
| TOPIX株価 | `strategy_verification/data/processed/prices_cleaned_topix_v3.parquet` | シグナル生成元 |
| TOPIX銘柄リスト | `data/parquet/meta_jquants.parquet` | ticker→銘柄名変換 |
| 信用余力 | `data/csv/credit_capacity.csv`（手動更新 or API） | 資金制約 |
| 保有銘柄 | `data/csv/hold_stocks.csv`（手動更新 or API） | 重複排除 |

---

## 5. 既存コード参照

検証で使用したスクリプト（mainブランチ）:

| ファイル | 内容 |
|----------|------|
| `strategy_verification/scripts/16_entry_priority_analysis_topix.py` | 特徴量計算、証拠金計算、cap_eff分析、RSI相関分析 |
| `strategy_verification/scripts/17_entry_priority_ml_topix.py` | ML検証（結論: 不使用。参考用） |
| `strategy_verification/scripts/18_portfolio_simulation_topix.py` | ポートフォリオシミュレーション |
| `improvement/granville/SPEC.md` | 元の検証仕様書 |
| `improvement/backtest_granville_8rules.py` | グランビルシグナル生成ロジック |

---

## 6. 運用イメージ

### 毎日の流れ
1. 16:45: 株価更新 → シグナル生成
2. 23:00: Grok SHORT推奨取得 → 統合レコメンド
3. 翌朝: フロントエンドで確認 → 寄付で発注

### シグナル選別フロー（generate_granville_recommendation.py）
```
全シグナル（1日平均~95件）
  ↓ B4 > B1 > B3 > B2 でソート
  ↓ 同一ルール内はRSI14昇順
  ↓ 既存保有銘柄を除外
  ↓ 上から順に:
     - 証拠金 > 現在資金の15% → スキップ（集中制限）
     - 証拠金 > 残余力 → スキップ（資金不足）
     - それ以外 → エントリー
  ↓
推奨リスト（資金制約で自然に5-15件程度に収束）
```

### ポジション管理
- LONG: Granvilleシグナル（B4>B1>B3>B2、RSI lowest）
- SHORT: Grok推奨
- Exit: 日次で20日高値チェック + ルール別MAX_HOLDチェック
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

## 8. 禁止事項（検証で否定されたもの）

| やってはいけないこと | 理由 |
|---------------------|------|
| ML予測スコアでソート | 複雑さに見合う効果なし。RSI lowestで十分 |
| 固定値の株価上限フィルター | パフォーマンス悪化する。エッジは株価非依存 |
| シグナルへのフィルター追加 | 機会損失。資金制約が自然なフィルター |
| ショート（Granville S1-S4） | 全レジームで機能しない |
| MAX_HOLD=60（全ルール統一） | ルール別最適化で大幅改善（B1: +96%） |

---

## 変更履歴

| 日付 | 内容 |
|------|------|
| 2026-03-08 | 検証完了、実装仕様書作成 |
| 2026-03-09 | **全面書き直し**: ML不使用、RSI lowest、固定値フィルター禁止、集中制限15%の設計思想、シグナル選別フロー、禁止事項を正確に反映 |
