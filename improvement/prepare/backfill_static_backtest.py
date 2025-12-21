#!/usr/bin/env python3
"""
backfill_static_backtest.py
過去1年分のStatic銘柄シグナルとバックテスト結果を生成

実行方法:
    python improvement/prepare/backfill_static_backtest.py

出力:
    data/parquet/backtest/static_signals_archive.parquet

処理:
    1. 過去1年分の株価データを取得
    2. 各営業日でシグナルを計算
    3. STRONG_BUY/BUYシグナルの5日後リターンを計算
    4. アーカイブに保存

除外ルール:
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

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import yfinance as yf
from tqdm import tqdm

# 出力先
OUTPUT_DIR = ROOT / "data" / "parquet" / "backtest"
OUTPUT_FILE = OUTPUT_DIR / "static_signals_archive.parquet"

# 価格フィルター（成行買い可能な範囲）
MAX_PRICE = 20000  # 20,000円以上は除外

# RSIフィルター（落ちるナイフ回避）
MIN_RSI = 12  # RSI 12未満は除外

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

# Static銘柄リスト
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


def fetch_all_data(days: int = 400) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    全銘柄と日経225の株価データを取得

    Returns:
        (stocks_df, n225_df)
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    print(f"Fetching data from {start_date.date()} to {end_date.date()}...")

    # 株価データ取得
    all_data = []
    tickers = list(STATIC_STOCKS.keys())

    for ticker in tqdm(tickers, desc="Stocks"):
        try:
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if len(data) > 0:
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)
                data['ticker'] = ticker
                data = data.reset_index()
                all_data.append(data)
        except Exception as e:
            print(f"[WARN] Failed to fetch {ticker}: {e}")

    stocks_df = pd.concat(all_data, ignore_index=True)
    stocks_df['Date'] = pd.to_datetime(stocks_df['Date'])

    # 日経225データ取得
    print("Fetching N225...")
    n225 = yf.download('^N225', start=start_date, end=end_date, progress=False)
    if isinstance(n225.columns, pd.MultiIndex):
        n225.columns = n225.columns.get_level_values(0)
    n225 = n225.reset_index()
    n225['Date'] = pd.to_datetime(n225['Date'])
    n225['sma5'] = n225['Close'].rolling(5).mean()
    n225['n225_vs_sma5'] = (n225['Close'] - n225['sma5']) / n225['sma5'] * 100

    return stocks_df, n225


def calculate_indicators_for_date(
    stocks_df: pd.DataFrame,
    ticker: str,
    target_date: pd.Timestamp,
    lookback: int = 30
) -> dict | None:
    """
    特定日のテクニカル指標を計算
    """
    ticker_df = stocks_df[
        (stocks_df['ticker'] == ticker) &
        (stocks_df['Date'] <= target_date)
    ].copy().tail(lookback + 5)

    if len(ticker_df) < lookback:
        return None

    ticker_df = ticker_df.sort_values('Date').reset_index(drop=True)

    close = ticker_df['Close']
    high = ticker_df['High']
    low = ticker_df['Low']

    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    # MA25
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
        'close': float(close.iloc[-1]),
        'rsi_14d': float(rsi.iloc[-1]),
        'ma25_deviation': float(ma25_deviation.iloc[-1]),
        'atr_pct': float(atr_pct.iloc[-1]),
        'daily_change': float(daily_change.iloc[-1]),
    }


def calculate_score(indicators: dict, market: str) -> int:
    """スコア計算"""
    score = 0

    rsi = indicators['rsi_14d']
    if rsi < 20:
        score += 25
    elif rsi < 30:
        score += 15
    elif rsi > 70:
        score -= 15

    ma_dev = indicators['ma25_deviation']
    if ma_dev < -10:
        score += 20
    elif ma_dev < -5:
        score += 10
    elif ma_dev > 5:
        score -= 10

    atr = indicators['atr_pct']
    if 3.5 <= atr <= 5:
        score += 10
    elif atr > 7:
        score -= 10

    if market == 'YELLOW':
        score += 10
    elif market == 'RED':
        score -= 5

    change = indicators['daily_change']
    if change < -3:
        score += 15
    elif change > 5:
        score -= 10

    return score


def get_signal(score: int) -> str:
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


def get_market_condition(n225_vs_sma5: float) -> str:
    if n225_vs_sma5 > 0:
        return 'GREEN'
    elif n225_vs_sma5 > -1:
        return 'YELLOW'
    else:
        return 'RED'


def calculate_5day_return(
    stocks_df: pd.DataFrame,
    ticker: str,
    entry_date: pd.Timestamp
) -> dict | None:
    """
    5日後リターンを計算（翌日寄付買い → 5営業日後大引売り）
    """
    ticker_df = stocks_df[
        (stocks_df['ticker'] == ticker) &
        (stocks_df['Date'] > entry_date)
    ].sort_values('Date').head(6)  # 翌日 + 5日 = 6日分

    if len(ticker_df) < 6:
        return None

    # 翌日寄付でエントリー
    entry_row = ticker_df.iloc[0]
    entry_price = float(entry_row['Open'])
    entry_date_actual = entry_row['Date']

    # 5営業日後の大引で決済
    exit_row = ticker_df.iloc[5]
    exit_price = float(exit_row['Close'])
    exit_date = exit_row['Date']

    # リターン計算
    return_pct = (exit_price - entry_price) / entry_price * 100
    profit_100 = (exit_price - entry_price) * 100
    win = return_pct > 0

    return {
        'entry_date': entry_date_actual.strftime('%Y-%m-%d'),
        'entry_price': entry_price,
        'exit_date': exit_date.strftime('%Y-%m-%d'),
        'exit_price': exit_price,
        'return_pct': return_pct,
        'profit_100': profit_100,
        'win': win,
        'hold_days': 5,
    }


def main():
    print("=" * 60)
    print("Static Signals Backfill (1 Year)")
    print("=" * 60)

    # 1. データ取得
    stocks_df, n225_df = fetch_all_data(days=400)
    print(f"Stocks data: {len(stocks_df)} rows")
    print(f"N225 data: {len(n225_df)} rows")

    # 2. 処理対象日を取得（過去1年分の営業日）
    all_dates = sorted(stocks_df['Date'].unique())
    # 最初の30日はインジケータ計算用にスキップ、最後の6日はバックテスト用にスキップ
    target_dates = [d for d in all_dates[30:-6]]
    print(f"Target dates: {len(target_dates)} days")
    print(f"  From: {pd.Timestamp(target_dates[0]).strftime('%Y-%m-%d')}")
    print(f"  To: {pd.Timestamp(target_dates[-1]).strftime('%Y-%m-%d')}")

    # 3. 各日のシグナル計算とバックテスト
    results = []
    prev_returns: dict[str, float] = {}  # 各銘柄の直近STRONG_BUYリターンを追跡
    skip_counts = {'sector': 0, 'ticker': 0, 'price': 0, 'rsi': 0, 'prev_loss': 0}

    for target_date in tqdm(target_dates, desc="Processing"):
        target_date = pd.Timestamp(target_date)

        # N225データ取得
        n225_row = n225_df[n225_df['Date'] == target_date]
        if n225_row.empty:
            continue

        n225_vs_sma5 = float(n225_row['n225_vs_sma5'].iloc[0])
        if pd.isna(n225_vs_sma5):
            continue

        market = get_market_condition(n225_vs_sma5)

        # 各銘柄のシグナル計算
        for ticker, (name, sector) in STATIC_STOCKS.items():
            # セクター除外
            if sector in EXCLUDE_SECTORS:
                skip_counts['sector'] += 1
                continue

            # 銘柄除外
            if ticker in EXCLUDE_TICKERS:
                skip_counts['ticker'] += 1
                continue

            indicators = calculate_indicators_for_date(stocks_df, ticker, target_date)
            if indicators is None:
                continue

            # 価格フィルター: 20,000円以上は除外
            if indicators['close'] >= MAX_PRICE:
                skip_counts['price'] += 1
                continue

            # RSIフィルター: 極端に低いRSIは除外
            if indicators['rsi_14d'] < MIN_RSI:
                skip_counts['rsi'] += 1
                continue

            score = calculate_score(indicators, market)
            signal = get_signal(score)

            # STRONG_BUY と BUY のみバックテスト
            if signal not in ['STRONG_BUY', 'BUY']:
                continue

            # 動的スキップ: STRONG_BUYの場合、前回-7%以下ならスキップ
            if signal == 'STRONG_BUY' and ticker in prev_returns:
                if prev_returns[ticker] <= SKIP_THRESHOLD:
                    skip_counts['prev_loss'] += 1
                    continue

            # 5日後リターン計算
            backtest = calculate_5day_return(stocks_df, ticker, target_date)
            if backtest is None:
                continue

            # STRONG_BUYの場合、リターンを記録（次回のスキップ判定用）
            if signal == 'STRONG_BUY':
                prev_returns[ticker] = backtest['return_pct']

            results.append({
                'signal_date': target_date.strftime('%Y-%m-%d'),
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
                **backtest,
            })

    print(f"\nSkipped counts:")
    print(f"  Sector: {skip_counts['sector']}")
    print(f"  Ticker: {skip_counts['ticker']}")
    print(f"  Price: {skip_counts['price']}")
    print(f"  RSI: {skip_counts['rsi']}")
    print(f"  Prev loss: {skip_counts['prev_loss']}")

    # 4. 結果を保存
    df = pd.DataFrame(results)

    print("\n" + "=" * 60)
    print("Results Summary")
    print("=" * 60)

    # シグナル別集計
    for signal in ['STRONG_BUY', 'BUY']:
        subset = df[df['signal'] == signal]
        if len(subset) > 0:
            win_rate = subset['win'].mean() * 100
            avg_return = subset['return_pct'].mean()
            total_profit = subset['profit_100'].sum()
            print(f"\n{signal}:")
            print(f"  Count: {len(subset)}")
            print(f"  Win rate: {win_rate:.1f}%")
            print(f"  Avg return: {avg_return:+.2f}%")
            print(f"  Total profit (100株): ¥{total_profit:,.0f}")

    # 月別集計（STRONG_BUYのみ）
    if len(df[df['signal'] == 'STRONG_BUY']) > 0:
        print("\n\nSTRONG_BUY Monthly:")
        strong_buy = df[df['signal'] == 'STRONG_BUY'].copy()
        strong_buy['month'] = pd.to_datetime(strong_buy['signal_date']).dt.to_period('M')
        monthly = strong_buy.groupby('month').agg({
            'win': ['count', 'sum', 'mean']
        }).round(3)
        monthly.columns = ['n', 'wins', 'win_rate']
        print(monthly.to_string())

    # 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)

    print("\n" + "=" * 60)
    print(f"✅ Saved to: {OUTPUT_FILE}")
    print(f"   Total records: {len(df)}")
    print(f"   Date range: {df['signal_date'].min()} to {df['signal_date'].max()}")
    print("=" * 60)

    return df


if __name__ == "__main__":
    main()
