#!/usr/bin/env python3
"""
generate_static_signals.py
Static銘柄（Core30 + 政策銘柄）のSTRONG_BUY/BUYシグナルを生成

実行方法:
    python improvement/prepare/generate_static_signals.py

出力:
    data/parquet/static_signals.parquet

スコアリングルール:
    【加点】
    - RSI < 20:      +25点
    - RSI 20-30:     +15点
    - MA25乖離 < -10%: +20点
    - MA25乖離 < -5%:  +10点
    - ATR 3.5-5%:    +10点
    - 市場 YELLOW:   +10点
    - 前日 -3%下落:  +15点

    【減点】
    - RSI > 70:      -15点
    - MA25乖離 > 5%:  -10点
    - ATR > 7%:      -10点
    - 市場 RED:       -5点
    - 前日 +5%上昇:  -10点

    【シグナル閾値】
    - STRONG_BUY:  >= 50
    - BUY:         >= 20
    - HOLD:        >= -15
    - SELL:        >= -30
    - STRONG_SELL: < -30

    【除外ルール】
    - セクター除外: 電気・ガス業
    - 銘柄除外: 川崎重工、ダイキン、ルネサス
    - 動的スキップ: 同一銘柄の前回STRONG_BUYが-7%以下なら次をスキップ
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import yfinance as yf

# 出力先（本番と同じ）
OUTPUT_DIR = ROOT / "data" / "parquet"
OUTPUT_FILE = OUTPUT_DIR / "static_signals.parquet"

# 価格フィルター（成行買い可能な範囲）
MAX_PRICE = 20000  # 20,000円以上は除外

# RSIフィルター（落ちるナイフ回避）
MIN_RSI = 12  # RSI 12未満は除外（極端な売られすぎは危険）

# 除外セクター
EXCLUDE_SECTORS = ['電気・ガス業']

# 除外銘柄（過去実績から損失が大きい銘柄）
EXCLUDE_TICKERS = [
    '7012.T',  # 川崎重工
    '6367.T',  # ダイキン
    '6723.T',  # ルネサス
]

# 動的スキップ閾値（前回STRONG_BUYのリターンがこれ以下なら次をスキップ）
SKIP_THRESHOLD = -7.0  # -7%以下

# Static銘柄リスト（Core30 + 政策銘柄）
STATIC_STOCKS = {
    '1605.T': ('INPEX', '鉱業'),
    '1766.T': ('東建コーポレーション', '建設業'),
    '1801.T': ('大成建設', '建設業'),
    '1802.T': ('大林組', '建設業'),
    '1803.T': ('清水建設', '建設業'),
    '1812.T': ('鹿島建設', '建設業'),
    '2914.T': ('JT', '食料品'),
    '3382.T': ('セブン&アイ', '小売業'),
    '4063.T': ('信越化学', '化学'),
    '4307.T': ('野村総研', '情報・通信業'),
    '4502.T': ('武田薬品', '医薬品'),
    '4568.T': ('第一三共', '医薬品'),
    '5020.T': ('ENEOS', '石油・石炭製品'),
    '5631.T': ('日本製鋼所', '機械'),
    '6098.T': ('リクルート', 'サービス業'),
    '6367.T': ('ダイキン', '機械'),
    '6501.T': ('日立製作所', '電気機器'),
    '6503.T': ('三菱電機', '電気機器'),
    '6701.T': ('NEC', '電気機器'),
    '6702.T': ('富士通', '電気機器'),
    '6723.T': ('ルネサス', '電気機器'),
    '6758.T': ('ソニーG', '電気機器'),
    '6762.T': ('TDK', '電気機器'),
    '6857.T': ('アドバンテスト', '電気機器'),
    '6861.T': ('キーエンス', '電気機器'),
    '6920.T': ('レーザーテック', '電気機器'),
    '6946.T': ('日本アビオニクス', '電気機器'),
    '6981.T': ('村田製作所', '電気機器'),
    '7011.T': ('三菱重工', '機械'),
    '7012.T': ('川崎重工', '機械'),
    '7013.T': ('IHI', '機械'),
    '7203.T': ('トヨタ', '輸送用機器'),
    '7267.T': ('ホンダ', '輸送用機器'),
    '7735.T': ('SCREEN', '電気機器'),
    '7741.T': ('HOYA', '精密機器'),
    '7974.T': ('任天堂', 'その他製品'),
    '8001.T': ('伊藤忠', '卸売業'),
    '8031.T': ('三井物産', '卸売業'),
    '8035.T': ('東京エレクトロン', '電気機器'),
    '8058.T': ('三菱商事', '卸売業'),
    '8306.T': ('三菱UFJ', '銀行業'),
    '8316.T': ('三井住友FG', '銀行業'),
    '8411.T': ('みずほFG', '銀行業'),
    '8729.T': ('ソニーFG', '保険業'),
    '8766.T': ('東京海上', '保険業'),
    '9005.T': ('東急', '陸運業'),
    '9021.T': ('JR西日本', '陸運業'),
    '9022.T': ('JR東海', '陸運業'),
    '9432.T': ('NTT', '情報･通信業'),
    '9433.T': ('KDDI', '情報･通信業'),
    '9434.T': ('ソフトバンク', '情報･通信業'),
    '9501.T': ('東京電力', '電気・ガス業'),
    '9503.T': ('関西電力', '電気・ガス業'),
    '9513.T': ('電源開発', '電気・ガス業'),
    '9983.T': ('ファーストリテイリング', '小売業'),
    '9984.T': ('ソフトバンクG', '情報･通信業'),
}


def fetch_stock_data(tickers: list[str], days: int = 60) -> pd.DataFrame:
    """
    yfinanceから株価データを取得

    Args:
        tickers: 銘柄コードリスト
        days: 取得日数

    Returns:
        株価データのDataFrame
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    all_data = []
    for ticker in tickers:
        try:
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if len(data) > 0:
                # MultiIndex columns対策: flatten
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)
                data['ticker'] = ticker
                data = data.reset_index()
                all_data.append(data)
        except Exception as e:
            print(f"[WARN] Failed to fetch {ticker}: {e}")

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


def fetch_n225_data(days: int = 60) -> tuple[float, str]:
    """
    日経225のSMA5との乖離率を取得し、市場環境を判定

    Returns:
        (n225_vs_sma5, market_condition)
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    n225 = yf.download('^N225', start=start_date, end=end_date, progress=False)

    if n225.empty:
        return 0.0, 'YELLOW'

    close = n225['Close'].squeeze()
    sma5 = close.rolling(5).mean()

    n225_vs_sma5 = float((close.iloc[-1] - sma5.iloc[-1]) / sma5.iloc[-1] * 100)

    # 市場環境判定
    if n225_vs_sma5 > 0:
        market = 'GREEN'
    elif n225_vs_sma5 > -1:
        market = 'YELLOW'
    else:
        market = 'RED'

    return n225_vs_sma5, market


def calculate_indicators(df: pd.DataFrame, ticker: str) -> dict | None:
    """
    テクニカル指標を計算

    Args:
        df: 株価データ
        ticker: 銘柄コード

    Returns:
        指標の辞書
    """
    ticker_df = df[df['ticker'] == ticker].copy().reset_index(drop=True)

    if len(ticker_df) < 30:
        return None

    ticker_df = ticker_df.sort_values('Date').reset_index(drop=True)

    close = ticker_df['Close']
    high = ticker_df['High']
    low = ticker_df['Low']

    # RSI (14日)
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    # MA25と乖離率
    ma25 = close.rolling(25).mean()
    ma25_deviation = (close - ma25) / ma25 * 100

    # ATR%
    high_low = high - low
    high_close = abs(high - close.shift())
    low_close = abs(low - close.shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    atr_pct = atr / close * 100

    # 前日騰落率
    daily_change = close.pct_change() * 100

    return {
        'date': ticker_df['Date'].iloc[-1],
        'close': float(close.iloc[-1]),
        'rsi_14d': float(rsi.iloc[-1]),
        'ma25_deviation': float(ma25_deviation.iloc[-1]),
        'atr_pct': float(atr_pct.iloc[-1]),
        'daily_change': float(daily_change.iloc[-1]),
    }


def calculate_score(indicators: dict, market: str) -> int:
    """
    スコアを計算

    Args:
        indicators: テクニカル指標
        market: 市場環境 (GREEN/YELLOW/RED)

    Returns:
        スコア
    """
    score = 0

    # RSI
    rsi = indicators['rsi_14d']
    if rsi < 20:
        score += 25
    elif rsi < 30:
        score += 15
    elif rsi > 70:
        score -= 15

    # MA25乖離
    ma_dev = indicators['ma25_deviation']
    if ma_dev < -10:
        score += 20
    elif ma_dev < -5:
        score += 10
    elif ma_dev > 5:
        score -= 10

    # ATR
    atr = indicators['atr_pct']
    if 3.5 <= atr <= 5:
        score += 10
    elif atr > 7:
        score -= 10

    # 市場環境
    if market == 'YELLOW':
        score += 10
    elif market == 'RED':
        score -= 5

    # 前日騰落
    change = indicators['daily_change']
    if change < -3:
        score += 15
    elif change > 5:
        score -= 10

    return score


def get_signal(score: int) -> str:
    """スコアからシグナルを判定"""
    if score >= 50:
        return 'STRONG_BUY'
    elif score >= 20:
        return 'BUY'
    elif score >= -15:
        return 'HOLD'
    elif score >= -30:
        return 'SELL'
    else:
        return 'STRONG_SELL'


def load_previous_results() -> dict[str, float]:
    """
    過去のシグナル結果を読み込み、各銘柄の直近STRONG_BUYリターンを取得

    Returns:
        {ticker: last_return} の辞書
    """
    # バックテスト結果ファイル（存在すれば）
    backtest_file = ROOT / "data" / "parquet" / "static_backtest.parquet"

    if not backtest_file.exists():
        return {}

    try:
        df = pd.read_parquet(backtest_file)

        # STRONG_BUYのみ
        df = df[df['signal'] == 'STRONG_BUY'].copy()

        if 'return_5d' not in df.columns:
            return {}

        # 各銘柄の直近シグナルのリターンを取得
        df = df.sort_values('signal_date')
        last_returns = df.groupby('ticker')['return_5d'].last().to_dict()

        return last_returns
    except Exception as e:
        print(f"[WARN] Failed to load previous results: {e}")
        return {}


def main():
    """メイン処理"""
    print("=" * 60)
    print("Static Signals Generator")
    print("=" * 60)

    # 1. 株価データ取得
    print("\n[1/5] Fetching stock data...")
    tickers = list(STATIC_STOCKS.keys())
    prices_df = fetch_stock_data(tickers)
    print(f"  Downloaded {len(prices_df['ticker'].unique())} tickers")

    latest_date = prices_df['Date'].max()
    print(f"  Latest date: {latest_date.strftime('%Y-%m-%d')}")

    # 2. 日経225データ取得
    print("\n[2/5] Fetching N225 data...")
    n225_vs_sma5, market = fetch_n225_data()
    print(f"  N225 vs SMA5: {n225_vs_sma5:+.2f}%")
    print(f"  Market condition: {market}")

    # 3. 過去シグナル結果を読み込み（動的スキップ用）
    print("\n[3/5] Loading previous results...")
    prev_returns = load_previous_results()
    print(f"  Loaded {len(prev_returns)} ticker results")

    # 4. 各銘柄のシグナル計算
    print("\n[4/5] Calculating signals...")
    results = []
    skipped_reasons = {'sector': [], 'ticker': [], 'price': [], 'rsi': [], 'prev_loss': []}

    for ticker, (name, sector) in STATIC_STOCKS.items():
        # セクター除外
        if sector in EXCLUDE_SECTORS:
            skipped_reasons['sector'].append(f"{ticker} {name} ({sector})")
            continue

        # 銘柄除外
        if ticker in EXCLUDE_TICKERS:
            skipped_reasons['ticker'].append(f"{ticker} {name}")
            continue

        indicators = calculate_indicators(prices_df, ticker)

        if indicators is None:
            continue

        # 価格フィルター: 20,000円以上は除外
        if indicators['close'] >= MAX_PRICE:
            skipped_reasons['price'].append(f"{ticker} {name} ({indicators['close']:.0f}円)")
            continue

        # RSIフィルター: 極端に低いRSIは「落ちるナイフ」リスク
        if indicators['rsi_14d'] < MIN_RSI:
            skipped_reasons['rsi'].append(f"{ticker} {name} (RSI {indicators['rsi_14d']:.1f})")
            continue

        score = calculate_score(indicators, market)
        signal = get_signal(score)

        # 動的スキップ: STRONG_BUYの場合のみ、前回-7%以下ならスキップ
        if signal == 'STRONG_BUY' and ticker in prev_returns:
            prev_ret = prev_returns[ticker]
            if prev_ret <= SKIP_THRESHOLD:
                skipped_reasons['prev_loss'].append(
                    f"{ticker} {name} (前回 {prev_ret:+.1f}%)"
                )
                continue

        results.append({
            'signal_date': latest_date.strftime('%Y-%m-%d'),
            'target_date': (latest_date + timedelta(days=1)).strftime('%Y-%m-%d'),  # 翌営業日
            'ticker': ticker,
            'stock_name': name,
            'sector': sector,
            'score': score,
            'signal': signal,
            'market_condition': market,
            'n225_vs_sma5': n225_vs_sma5,
            'close': indicators['close'],
            'rsi_14d': indicators['rsi_14d'],
            'ma25_deviation': indicators['ma25_deviation'],
            'atr_pct': indicators['atr_pct'],
            'daily_change': indicators['daily_change'],
            'hold_days': 5,  # 推奨保有日数
            'generated_at': datetime.now().isoformat(),
        })

    df = pd.DataFrame(results)

    # 5. 結果表示と保存
    print("\n[5/5] Results:")
    print("-" * 60)

    # 除外理由の表示
    print("\n【除外された銘柄】")
    if skipped_reasons['sector']:
        print(f"  セクター除外 ({len(skipped_reasons['sector'])}件):")
        for s in skipped_reasons['sector']:
            print(f"    - {s}")
    if skipped_reasons['ticker']:
        print(f"  銘柄除外 ({len(skipped_reasons['ticker'])}件):")
        for s in skipped_reasons['ticker']:
            print(f"    - {s}")
    if skipped_reasons['price']:
        print(f"  価格除外 ({len(skipped_reasons['price'])}件):")
        for s in skipped_reasons['price']:
            print(f"    - {s}")
    if skipped_reasons['rsi']:
        print(f"  RSI除外 ({len(skipped_reasons['rsi'])}件):")
        for s in skipped_reasons['rsi']:
            print(f"    - {s}")
    if skipped_reasons['prev_loss']:
        print(f"  前回損失スキップ ({len(skipped_reasons['prev_loss'])}件):")
        for s in skipped_reasons['prev_loss']:
            print(f"    - {s}")

    # シグナル別集計
    print("\n【シグナル一覧】")
    for signal in ['STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL']:
        count = len(df[df['signal'] == signal])
        if count > 0:
            print(f"\n{signal}: {count}件")
            subset = df[df['signal'] == signal].sort_values('score', ascending=False)
            for _, row in subset.iterrows():
                print(f"  {row['ticker']} {row['stock_name']:<12} | "
                      f"スコア: {row['score']:>3} | "
                      f"RSI: {row['rsi_14d']:.1f} | "
                      f"MA乖離: {row['ma25_deviation']:+.1f}%")

    # 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)

    print("\n" + "=" * 60)
    print(f"✅ Saved to: {OUTPUT_FILE}")
    print(f"   Records: {len(df)}")
    if len(df) > 0:
        print(f"   Signal date: {df['signal_date'].iloc[0]}")
        print(f"   Target date: {df['target_date'].iloc[0]}")
    print(f"   Market: {market}")
    print("=" * 60)

    return df


if __name__ == "__main__":
    main()
