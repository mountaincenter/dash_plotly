# ORB + VWAP バックテスト仕様書

**作成日**: 2025-11-28
**バージョン**: 1.0.0
**ステータス**: ドラフト（ユーザー承認待ち）

---

## 1. 目的

### 1.1 背景
- 現行の「寄付買い → 大引け売り」戦略では、ギャップアップ/ダウンへの対応が困難
- 損切り遅れ、利確逃し、高値掴みの心理的バイアスを排除したい
- 5分足データを活用し、より流動的なエントリータイミングを検証したい

### 1.2 検証目標
- Opening Range Breakout (ORB) + VWAP 戦略の有効性を過去データで検証
- Grok推奨銘柄に対してこの戦略を適用した場合の勝率・損益を算出
- 現行戦略との比較を行い、改善余地を定量化

---

## 2. データソース

### 2.1 ファイル配置（improvement/data/）

```
improvement/data/
├── grok_trending.parquet          # Grok推奨銘柄リスト
├── prices_max_1d.parquet          # 日足データ（全銘柄）
├── prices_60d_5m.parquet          # 5分足データ（過去60日）
└── backtest/
    ├── grok_analysis_merged.parquet      # バックテスト結果（既存）
    ├── grok_analysis_merged_v2_1.parquet # v2.1バックテスト結果
    └── trading_recommendation.json       # 推奨銘柄JSON
```

### 2.2 データ仕様

#### grok_trending.parquet
| カラム | 型 | 説明 |
|--------|-----|------|
| ticker | str | 銘柄コード（例: 7203.T） |
| stock_name | str | 銘柄名 |
| grok_rank | int | Grokランキング（1-12） |
| selection_date | str | 選定日（YYYY-MM-DD） |

#### prices_60d_5m.parquet
| カラム | 型 | 説明 |
|--------|-----|------|
| ticker | str | 銘柄コード |
| datetime | datetime | タイムスタンプ（JST） |
| Open | float | 始値 |
| High | float | 高値 |
| Low | float | 安値 |
| Close | float | 終値 |
| Volume | int | 出来高 |

---

## 3. ORB + VWAP 戦略仕様

### 3.1 用語定義

| 用語 | 定義 |
|------|------|
| Opening Range (OR) | 寄付き後30分間（9:00-9:30）の高値・安値レンジ |
| OR High | Opening Range の最高値 |
| OR Low | Opening Range の最安値 |
| OR Range | OR High - OR Low |
| VWAP | Volume Weighted Average Price（出来高加重平均価格） |

### 3.2 VWAP計算式

```python
typical_price = (High + Low + Close) / 3
vwap = cumsum(typical_price * Volume) / cumsum(Volume)
```

※ 各日の9:00からリセットして計算

### 3.3 エントリー条件

#### 買いエントリー (LONG)
以下の**全て**を満たす場合：
1. 現在価格 > OR High（レンジ上抜け）
2. 現在価格 > VWAP（VWAPより上）
3. 時刻 >= 9:30（OR確定後）
4. 時刻 <= 14:30（大引け30分前まで）

#### 売りエントリー (SHORT) ※参考値として算出
以下の**全て**を満たす場合：
1. 現在価格 < OR Low（レンジ下抜け）
2. 現在価格 < VWAP（VWAPより下）
3. 時刻 >= 9:30
4. 時刻 <= 14:30

### 3.4 決済条件

| 条件 | 価格 |
|------|------|
| 損切り (Stop Loss) | VWAP（エントリー時点） |
| 利確 (Take Profit) | エントリー価格 + OR Range（Measured Move） |
| 時間切れ | 15:30（大引け）の終値で強制決済 |

### 3.5 ポジションサイズ（参考）

```
リスク金額 = 総資産 × 2%
ポジションサイズ = リスク金額 / (エントリー価格 - 損切り価格)
```

---

## 4. バックテスト仕様

### 4.1 対象期間
- prices_60d_5m.parquet に含まれる全期間（約60日分）
- Grok推奨銘柄の選定日翌営業日を対象

### 4.2 処理フロー

```
1. grok_trending.parquet から銘柄リストを取得
2. 各銘柄・各日について:
   a. 9:00-9:30 の5分足から OR High/Low を算出
   b. 9:30以降の5分足で VWAP を逐次計算
   c. エントリー条件を満たした最初の足でエントリー
   d. 決済条件を満たした時点、または15:25で決済
   e. 結果を記録
3. 全銘柄・全日の結果を集計
```

### 4.3 出力ファイル

#### orb_vwap_backtest_results.parquet
| カラム | 型 | 説明 |
|--------|-----|------|
| ticker | str | 銘柄コード |
| date | str | 取引日 |
| grok_rank | int | Grokランキング |
| signal | str | BUY / SELL / NO_SIGNAL |
| entry_time | datetime | エントリー時刻 |
| entry_price | float | エントリー価格 |
| exit_time | datetime | 決済時刻 |
| exit_price | float | 決済価格 |
| exit_reason | str | STOP_LOSS / TAKE_PROFIT / TIME_EXIT |
| pnl_pct | float | 損益率（%） |
| pnl_amount | float | 損益額（100株あたり） |
| or_high | float | OR High |
| or_low | float | OR Low |
| or_range_pct | float | ORレンジ幅（%） |
| vwap_at_entry | float | エントリー時VWAP |

#### orb_vwap_backtest_summary.json
```json
{
  "period": {
    "start": "YYYY-MM-DD",
    "end": "YYYY-MM-DD",
    "trading_days": 0
  },
  "total_trades": 0,
  "buy_signals": 0,
  "sell_signals": 0,
  "no_signals": 0,
  "win_rate": 0.0,
  "avg_return_pct": 0.0,
  "total_return_pct": 0.0,
  "max_gain_pct": 0.0,
  "max_loss_pct": 0.0,
  "exit_reasons": {
    "STOP_LOSS": 0,
    "TAKE_PROFIT": 0,
    "TIME_EXIT": 0
  },
  "comparison_vs_current": {
    "current_strategy_return": 0.0,
    "orb_vwap_return": 0.0,
    "improvement": 0.0
  }
}
```

---

## 5. 実装ファイル

### 5.1 ファイル構成

```
improvement/
├── docs/
│   └── ORB_VWAP_BACKTEST_SPEC.md  # 本仕様書
├── scripts/
│   ├── orb_vwap_backtest.py       # メインバックテストスクリプト
│   └── orb_vwap_utils.py          # ユーティリティ関数
└── data/
    └── (出力ファイル)
```

### 5.2 実行方法

```bash
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/improvement
python scripts/orb_vwap_backtest.py
```

---

## 6. 成功基準

### 6.1 技術的成功基準
- [ ] 全Grok銘柄に対してバックテストが完走する
- [ ] 結果ファイル（parquet, json）が正常に出力される
- [ ] エラー・警告なく実行完了する

### 6.2 戦略評価基準
- [ ] 勝率が50%以上
- [ ] 平均リターンがプラス
- [ ] 現行戦略（寄付→大引け）と比較してリターン改善

---

## 7. リスク・制約

### 7.1 データ制約
- yfinance の5分足データは過去60日分のみ
- データ欠損・遅延の可能性あり
- 日本株は流動性により約定価格にスリッページが発生しうる

### 7.2 戦略制約
- ORB戦略は勝率約42%（トレンドフォロー型の特性）
- ギャップが大きい日はORレンジが広く、リスクが増大
- 低流動性銘柄では成行約定が困難

---

## 8. 変更履歴

| 日付 | バージョン | 変更内容 |
|------|-----------|----------|
| 2025-11-28 | 1.0.0 | 初版作成 |

---

## 9. 承認

- [ ] ユーザー承認待ち

**承認後、実装を開始します。**
