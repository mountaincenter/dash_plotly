# Trading Recommendation フィールド定義

**生成日**: 2025-11-19 19:34:54  
**ソース**: trading_recommendation_2025-11-18.json  
**バージョン**: 3.0_merged_v2_scoring  

---

## ルートレベル

- `version`: バージョン情報
- `generatedAt`: 生成日時
- `dataSource`: データソース情報
- `summary`: サマリー統計
- `stocks`: 銘柄配列
- `mergedFrom`: マージ元情報

### dataSource (データソース情報)

- `technicalDataDate`: テクニカルデータ日付

### summary (サマリー統計)

- `total`: 総銘柄数
- `buy`: 買い推奨数
- `sell`: 売り推奨数
- `hold`: 静観推奨数

---

## stocks[] (銘柄配列)

各銘柄オブジェクトの構造:

- `ticker`: ティッカーシンボル (例: 5724.T)
- `stockName`: 銘柄名 (例: アサカ理研)
- `grokRank`: Grokランク (1-10)
- `categories`: カテゴリ配列

### technicalData (テクニカル指標)

- `prevClose`: 前日終値
- `prevDayChangePct`: 前日変化率 (%)
- `volume`: 出来高
- `volatilityLevel`: ボラティリティレベル (低ボラ/中ボラ/高ボラ)

#### atr (ATR指標)

- `value`: ATR値 (%)
- `level`: レベル (low/medium/high)

### recommendation (売買推奨)

- `action`: 推奨アクション (buy/sell/hold)
- `score`: 最終スコア (deep_search調整後)
- `v2Score`: v2基礎スコア
- `confidence`: 信頼度 (low/medium/high)

#### stopLoss (損切りライン)

- `percent`: 損切り率 (%)
- `calculation`: 計算式説明

#### reasons[] (推奨理由配列)

- `type`: 理由タイプ (grok_rank/moving_average等)
- `description`: 説明文
- `impact`: スコア影響度

### deepAnalysis (深掘り分析)

- `verdict`: 総合判定 (例: 強い買い推奨（スコア: +58）)
- `marketSentiment`: 市場センチメント (positive/neutral/negative)
- `sectorTrend`: セクタートレンド説明
- `newsHeadline`: ニュース見出し
- `latestNews`: 最新ニュース配列
- `risks`: リスク要因配列
- `opportunities`: 機会要因配列

#### adjustmentReasons[] (スコア調整理由配列)

- `factor`: 調整要因
- `impact`: 影響度（±数値）

#### fundamentals (ファンダメンタルズ)

- `operatingProfitGrowth`: 営業利益成長率 (%)
- `nextEarningsDate`: 次回決算発表日

---

## mergedFrom (マージ元情報)

- `scoringModel`: v2 (2025-11-17統一)
- `deepAnalysis`: 3.0 (補助情報のみ)
- `deepAnalysisDate`: 2025-11-18