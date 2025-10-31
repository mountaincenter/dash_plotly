"""
Grok API精度向上のための推奨マーケット指標セット

yfinanceで取得可能な日本市場の主要指標・ETF・先物・為替を定義
プライム・スタンダード・グロース各市場の動向を把握可能

データファイル構成:
- index_prices_{period}_{interval}.parquet   : 指数・ETF（東証取引時間）
- futures_prices_{period}_{interval}.parquet : 先物（24時間取引）
- currency_prices_{period}_{interval}.parquet: 為替（24時間取引、1h/1d/1moのみ）
"""

# ===========================================================================================
# 推奨設定: Grok APIのコンテキスト用マーケット指標
# ===========================================================================================

RECOMMENDED_MARKET_INDICATORS = {
    # -------------------------
    # 主要指数
    # -------------------------
    "market_indices": {
        "日経平均株価": "^N225",           # 日本の代表的な株価指数
    },

    # -------------------------
    # 先物（24時間取引）
    # -------------------------
    "futures": {
        "日経225先物": "NKD=F",           # 日経平均の先物
    },

    # -------------------------
    # プライム市場（大型株中心）
    # -------------------------
    "prime_market": {
        "TOPIX連動ETF": "1306.T",         # 東証プライム市場全体の動向（最も流動性高い）
        "TOPIX Core30 ETF": "1311.T",     # 超大型株30銘柄
        "JPX日経400 ETF": "1591.T",       # ROE重視の優良企業400社
        "日経高配当株50 ETF": "1489.T",   # 高配当プライム大型株
    },

    # -------------------------
    # グロース市場（成長株中心）
    # -------------------------
    "growth_market": {
        "グロース250指数 ETF": "2516.T",  # 東証グロース市場の主要250銘柄（最重要）
        "マザーズコア指数 ETF": "1563.T",  # グロース市場のコア銘柄
        "JASDAQ-TOP20 ETF": "1554.T",     # JASDAQ上位20銘柄
    },

    # -------------------------
    # 為替（24時間取引、LightGBM特徴量用）
    # -------------------------
    "currency": {
        "USD/JPY": "JPY=X",               # ドル円（輸出企業への影響大）
        "EUR/JPY": "EURJPY=X",            # ユーロ円
    },

    # -------------------------
    # レバレッジ・インバース（市場センチメント）
    # -------------------------
    "sentiment": {
        "日経レバレッジETF": "1570.T",    # 強気相場時に上昇
        "日経ダブルインバース": "1357.T",  # 弱気相場時に上昇
    },
}


# ===========================================================================================
# フラット化したティッカーリスト（APIコール用）
# ===========================================================================================

def get_all_tickers():
    """すべての推奨ティッカーをフラットなリストで取得"""
    tickers = []
    for category in RECOMMENDED_MARKET_INDICATORS.values():
        tickers.extend(category.values())
    return tickers


def get_tickers_by_category(category: str):
    """カテゴリ別にティッカーを取得"""
    return list(RECOMMENDED_MARKET_INDICATORS.get(category, {}).values())


def get_ticker_name_map():
    """ティッカーと名称のマッピングを取得"""
    ticker_map = {}
    for category in RECOMMENDED_MARKET_INDICATORS.values():
        ticker_map.update({v: k for k, v in category.items()})
    return ticker_map


# ===========================================================================================
# 推奨構成の説明
# ===========================================================================================

CONFIGURATION_GUIDE = """
【Grok API用マーケット指標の推奨構成】

■ 最小構成（必須3指標）
  1. ^N225      - 日経平均株価（市場全体）
  2. 1306.T     - TOPIX連動ETF（プライム市場）
  3. 2516.T     - グロース250指数ETF（グロース市場）

■ 標準構成（推奨7指標）
  上記 + 以下4指標
  4. NKD=F      - 日経225先物（短期トレンド）
  5. JPY=X      - USD/JPY（為替影響）
  6. 1591.T     - JPX日経400 ETF（優良企業）
  7. 1563.T     - マザーズコア指数ETF（グロースコア）

■ 完全構成（全14指標）
  全カテゴリの指標を含む

【各カテゴリの役割】

1. market_indices（必須）
   → 日本市場全体の方向性を把握

2. futures（推奨）
   → 24時間取引の先物で短期トレンドを把握
   → データファイル: futures_prices_{period}_{interval}.parquet

3. prime_market（重要）
   → プライム上場企業の分析精度向上
   → TOPIX連動ETFは東証プライム市場全体を代表
   → データファイル: index_prices_{period}_{interval}.parquet

4. growth_market（重要）
   → グロース上場企業の分析精度向上
   → グロース250指数ETFは東証グロース市場を代表
   → データファイル: index_prices_{period}_{interval}.parquet

5. currency（推奨・LightGBM特徴量用）
   → 輸出入企業の業績分析に必須
   → 為替相場が株価に与える影響を把握
   → データファイル: currency_prices_{period}_{interval}.parquet
   → 注意: 5m/15mは除外（1h/1d/1moのみ）

6. sentiment（オプション）
   → 市場のセンチメント（強気/弱気）を把握
   → レバレッジ・インバースETFの動きで投資家心理を分析
   → データファイル: index_prices_{period}_{interval}.parquet

【データファイル構成】

1. index_prices_{period}_{interval}.parquet
   - 対象: 日経平均、TOPIX ETF、グロースETF、センチメントETF
   - 取引時間: 東証時間（9:00-16:00）
   - 間隔: 5m, 15m, 1h, 1d, 1mo

2. futures_prices_{period}_{interval}.parquet
   - 対象: 日経225先物
   - 取引時間: 24時間（ほぼ連続）
   - 間隔: 5m, 15m, 1h, 1d, 1mo

3. currency_prices_{period}_{interval}.parquet
   - 対象: USD/JPY, EUR/JPY
   - 取引時間: 24時間（土日除く）
   - 間隔: 1h, 1d, 1mo（5m/15mは除外）
   - 用途: LightGBM特徴量

【データ取得スクリプト】

# 指数・ETF取得
python3 scripts/pipeline/fetch_index_prices.py

# 先物取得（index_prices.pyに含まれる）
python3 scripts/pipeline/fetch_index_prices.py

# 為替取得
python3 scripts/pipeline/fetch_currency_prices.py
"""

# ===========================================================================================
# 実行例
# ===========================================================================================

if __name__ == "__main__":
    print("=" * 90)
    print("Grok API用推奨マーケット指標一覧")
    print("=" * 90)

    for category, tickers in RECOMMENDED_MARKET_INDICATORS.items():
        print(f"\n【{category}】")
        for name, ticker in tickers.items():
            print(f"  {ticker:15} - {name}")

    print("\n" + "=" * 90)
    print(f"合計: {len(get_all_tickers())} 指標")
    print("=" * 90)

    print("\n" + CONFIGURATION_GUIDE)
