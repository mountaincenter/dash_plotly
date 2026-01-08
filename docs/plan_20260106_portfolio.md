# 計画: テーマ銘柄再定義 + 母集団別統計分析

## 実装状況サマリ

| タスク | 状態 |
|--------|------|
| J-Quants v1→v2移行 | ✅ 完了 |
| 制限値幅関数（price_limit.py） | ✅ 完了 |
| Pipeline統合（grok_trending） | ✅ 完了 |
| API表示（dev_day_trade_list） | ✅ 完了 |
| テーマ銘柄再定義 | ❌ 未実装 |
| 母集団別統計分析 | ❌ 未実装 |
| 日次必要資金集計・ポートフォリオ配分 | ❌ 未実装 |

---

# ✅ 完了済み

## 1. J-Quants v1→v2移行

- commit `8b9508d` でpush済み
- 10ファイルのエンドポイント・カラム名変更完了
- 16:45 JST pipeline実行で検証予定

## 2. 制限値幅計算（Part 3 Step 1）

**ファイル:** `scripts/lib/price_limit.py`
- `calc_price_limit(price)` - 制限値幅
- `calc_upper_limit_price(price)` - ストップ高価格
- `calc_max_cost_100(price)` - 100株最大必要資金

## 3. Pipeline統合（Part 3 Step 3）

**ファイル:** `scripts/pipeline/add_market_cap_to_grok_trending.py`
- L33: `from scripts.lib.price_limit import ...`
- L245-256: price_limit, limit_price_upper, max_cost_100 カラム追加

**検証:** `grok_trending.parquet` に3カラム存在確認済み

## 4. API表示更新（Part 3 Step 4）

**ファイル:** `server/routers/dev_day_trade_list.py`
- L244-261: max_cost_100 カラム追加
- L270-290: 必要資金集計（shortable/day_trade別）

---

# ❌ 未実装

## 5. Part 1: テーマ銘柄再定義

### 背景
現在の政策銘柄（34銘柄）は2025年11月に高市政権誕生を想定して選定。
ゼロから再定義し、1ヶ月程度のロングでファンダメンタル中心の投資に活用する。

### 2026年 投資テーマ方針（シンクタンクレポート総括）

| 機関 | 日経平均 | TOPIX |
|------|----------|-------|
| 野村證券 | 55,000円 | 3,600 |
| 大和AM | 56,000円 | 3,750 |

**国家戦略技術6分野:** AI・先端ロボット、量子、半導体・通信、バイオ・ヘルスケア、核融合、宇宙

**回避セクター:** 鉄鋼、小売、インバウンド関連

### 実装内容
- 入力: `data/csv/takaichi_stock_issue.csv`（現行v1、56銘柄）
- 出力: `data/csv/takaichi_stock_issue_v2.csv`
- 新タグ追加: フィジカルAI、核融合、ステーブルコイン、GX・脱炭素、ペロブスカイト、造船、バイオ・ヘルスケア、量子

### 作業ステップ
1. 現行56銘柄の1年リターン確認
2. 現行CSVをベースにv2作成
3. 新タグ列を追加
4. 既存銘柄のタグを再評価
5. 新規銘柄を追加
6. `meta.parquet` への反映

## 6. Part 2: 母集団別統計分析

### 母集団と投資戦略
| 母集団 | 戦略 | 保有期間 |
|--------|------|----------|
| Grok銘柄 | ショート | 当日〜数日 |
| テーマ銘柄 | ロング | 1-2ヶ月 |
| TOPIX Core30 | ベンチマーク | - |

### 分析内容
- Grok銘柄: phase1/2/3勝率、曜日別、grok_rank別、selection_score帯別
- テーマ銘柄: 維持/入れ替え判断

## 7. Part 3 Step 5: 日次必要資金集計・ポートフォリオ配分

### ロジック
```
最大必要資金 = (終値 + 制限値幅) × 100
日次必要資金 = Σ 除0銘柄の最大必要資金
余剰資金 = 総資金 - 平均必要資金 → 政策銘柄へ
```

### 成果物
- `data/parquet/backtest/population_stats.parquet`
- `output/population_analysis_report.md`

---

# 次の優先順位

1. **API表示更新** - 簡単、すぐできる
2. **Part 3 Step 5** - Grokショート必要資金を確定
3. **Part 1** - テーマ銘柄再定義（余剰資金確定後）
4. **Part 2** - 母集団別統計分析
