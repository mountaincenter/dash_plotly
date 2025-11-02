# Grok Trending Performance Analysis

Grokバックテストの精度向上とパターン分析のためのplaygroundディレクトリ

## 目的

1. **バックテストパフォーマンス分析**
   - Grok選定銘柄の勝率・平均リターン分析
   - マーケット環境との相関分析
   - 選定理由・タグとパフォーマンスの関係

2. **マーケットデータ統合**
   - 指数・ETF（プライム/グロース市場動向）
   - 先物（短期トレンド）
   - 為替レート（為替影響）
   - 個別株価との相関分析

3. **パターン抽出**
   - 勝ちパターンの特定
   - 負けパターンの特定
   - マーケット環境別の成功条件

## 利用可能なデータ

### バックテストデータ
- `data/parquet/backtest/grok_trending_archive.parquet` - 過去の選定結果とパフォーマンス
- `data/parquet/grok_backtest_meta.parquet` - バックテスト統計情報

### 株価データ
- `data/parquet/prices_max_1d.parquet` - 日足データ
- `data/parquet/prices_730d_1h.parquet` - 時間足データ

### マーケットデータ（NEW）
- `data/parquet/index_prices_max_1d.parquet` - 指数・ETF日足
- `data/parquet/futures_prices_max_1d.parquet` - 先物日足
- `data/parquet/currency_prices_max_1d.parquet` - 為替日足

### メタデータ
- `data/parquet/grok_trending.parquet` - 最新のGrok選定結果
- `data/parquet/all_stocks.parquet` - 全銘柄情報

## ディレクトリ構成

```
grok_trending_performance/
├── README.md                    # このファイル
├── notebooks/                   # Jupyter notebooks
│   ├── 01_backtest_analysis.ipynb
│   ├── 02_market_correlation.ipynb
│   └── 03_pattern_extraction.ipynb
├── scripts/                     # 分析スクリプト
│   ├── load_data.py            # データ読み込みユーティリティ
│   ├── analyze_performance.py  # パフォーマンス分析
│   └── market_features.py      # マーケット特徴量生成
└── outputs/                     # 分析結果・図表
    ├── figures/
    └── reports/
```

## 分析アプローチ

### Phase 1: 基礎分析
1. バックテストデータの読み込みと基礎統計
2. 勝率・平均リターンの時系列推移
3. タグ別・理由別のパフォーマンス比較

### Phase 2: マーケット相関分析
1. 日経平均・TOPIX との相関
2. プライム/グロース市場動向との関係
3. 為替レート（USD/JPY）との相関
4. 市場センチメント（レバレッジ/インバース）との関係

### Phase 3: パターン抽出
1. 勝ちパターンの特徴抽出
2. 負けパターンの特徴抽出
3. マーケット環境別の成功条件
4. 選定ロジック改善のための示唆

## 次のステップ

1. データ読み込みユーティリティの作成
2. 基礎的なパフォーマンス分析スクリプトの実装
3. マーケットデータとの相関分析
4. パターン抽出とレポート生成
