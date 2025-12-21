#!/usr/bin/env python3
"""
backfill_static_backtest_v2.py
過去1年分のStatic銘柄シグナルとバックテスト結果を生成
1日・5日・10日リターンを計算（データがない場合はNaN）

実行方法:
    python improvement/prepare/backfill_static_backtest_v2.py

出力:
    data/parquet/backtest/static_signals_archive.parquet
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
    """全銘柄と日経225の株価データを取得"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    print(f"Fetching data from {start_date.date()} to {end_date.date()}...")

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
    """特定日のテクニカル指標を計算"""
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

    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    ma25 = close.rolling(25).mean()
    ma25_deviation = (close - ma25) / ma25 * 100

    high_low = high - low
    high_close = abs(high - close.shift())
    low_close = abs(low - close.shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    atr_pct = atr / close * 100

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


def calculate_nday_return(
    stocks_df: pd.DataFrame,
    ticker: str,
    signal_date: pd.Timestamp,
    n_days: int
) -> dict | None:
    """
    N日後リターンを計算（翌日寄付買い → N営業日後大引売り）
    データが不足している場合はNoneを返す
    """
    ticker_df = stocks_df[
        (stocks_df['ticker'] == ticker) &
        (stocks_df['Date'] > signal_date)
    ].sort_values('Date').head(n_days + 1)  # 翌日 + n日

    if len(ticker_df) < 2:  # 最低でもエントリー日が必要
        return None

    # 翌日寄付でエントリー
    entry_row = ticker_df.iloc[0]
    entry_price = float(entry_row['Open'])
    entry_date = entry_row['Date']

    # N日後のデータがあるか確認
    if len(ticker_df) < n_days + 1:
        # データ不足
        return None

    # N営業日後の大引で決済
    exit_row = ticker_df.iloc[n_days]
    exit_price = float(exit_row['Close'])
    exit_date = exit_row['Date']

    return_pct = (exit_price - entry_price) / entry_price * 100
    profit_100 = (exit_price - entry_price) * 100
    win = return_pct > 0

    return {
        'entry_price': entry_price,
        'entry_date': entry_date.strftime('%Y-%m-%d'),
        'exit_price': exit_price,
        'exit_date': exit_date.strftime('%Y-%m-%d'),
        'return_pct': return_pct,
        'profit_100': profit_100,
        'win': win,
    }


def main():
    print("=" * 60)
    print("Static Signals Backfill v2 (1/5/10 day returns)")
    print("=" * 60)

    # 1. データ取得
    stocks_df, n225_df = fetch_all_data(days=400)
    print(f"Stocks data: {len(stocks_df)} rows")
    print(f"N225 data: {len(n225_df)} rows")

    # 2. 処理対象日（最初の30日はスキップ、最後はスキップなし）
    all_dates = sorted(stocks_df['Date'].unique())
    target_dates = [d for d in all_dates[30:]]  # 最後までスキップなし
    print(f"Target dates: {len(target_dates)} days")
    print(f"  From: {pd.Timestamp(target_dates[0]).strftime('%Y-%m-%d')}")
    print(f"  To: {pd.Timestamp(target_dates[-1]).strftime('%Y-%m-%d')}")

    # 3. 各日のシグナル計算とバックテスト
    results = []

    for target_date in tqdm(target_dates, desc="Processing"):
        target_date = pd.Timestamp(target_date)

        n225_row = n225_df[n225_df['Date'] == target_date]
        if n225_row.empty:
            continue

        n225_vs_sma5 = float(n225_row['n225_vs_sma5'].iloc[0])
        if pd.isna(n225_vs_sma5):
            continue

        market = get_market_condition(n225_vs_sma5)

        for ticker, (name, sector) in STATIC_STOCKS.items():
            indicators = calculate_indicators_for_date(stocks_df, ticker, target_date)
            if indicators is None:
                continue

            score = calculate_score(indicators, market)
            signal = get_signal(score)

            # STRONG_BUY と BUY のみ
            if signal not in ['STRONG_BUY', 'BUY']:
                continue

            # 1日・5日・10日リターン計算
            ret_1d = calculate_nday_return(stocks_df, ticker, target_date, 1)
            ret_5d = calculate_nday_return(stocks_df, ticker, target_date, 5)
            ret_10d = calculate_nday_return(stocks_df, ticker, target_date, 10)

            # 少なくとも1日リターンがないとスキップ
            if ret_1d is None:
                continue

            result = {
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
                # 共通エントリー情報
                'entry_date': ret_1d['entry_date'],
                'entry_price': ret_1d['entry_price'],
                # 1日リターン
                'return_1d': ret_1d['return_pct'],
                'profit_100_1d': ret_1d['profit_100'],
                'win_1d': ret_1d['win'],
                'exit_date_1d': ret_1d['exit_date'],
                'exit_price_1d': ret_1d['exit_price'],
                # 5日リターン
                'return_5d': ret_5d['return_pct'] if ret_5d else None,
                'profit_100_5d': ret_5d['profit_100'] if ret_5d else None,
                'win_5d': ret_5d['win'] if ret_5d else None,
                'exit_date_5d': ret_5d['exit_date'] if ret_5d else None,
                'exit_price_5d': ret_5d['exit_price'] if ret_5d else None,
                # 10日リターン
                'return_10d': ret_10d['return_pct'] if ret_10d else None,
                'profit_100_10d': ret_10d['profit_100'] if ret_10d else None,
                'win_10d': ret_10d['win'] if ret_10d else None,
                'exit_date_10d': ret_10d['exit_date'] if ret_10d else None,
                'exit_price_10d': ret_10d['exit_price'] if ret_10d else None,
            }

            results.append(result)

    # 4. 結果を保存
    df = pd.DataFrame(results)

    print("\n" + "=" * 60)
    print("Results Summary")
    print("=" * 60)

    # シグナル別・保有期間別集計
    for signal in ['STRONG_BUY', 'BUY']:
        subset = df[df['signal'] == signal]
        if len(subset) == 0:
            continue

        print(f"\n{signal}:")
        print(f"  Total count: {len(subset)}")

        for period, col_return, col_win, col_profit in [
            ('1日', 'return_1d', 'win_1d', 'profit_100_1d'),
            ('5日', 'return_5d', 'win_5d', 'profit_100_5d'),
            ('10日', 'return_10d', 'win_10d', 'profit_100_10d'),
        ]:
            valid = subset[subset[col_win].notna()]
            if len(valid) > 0:
                win_rate = valid[col_win].mean() * 100
                avg_return = valid[col_return].mean()
                total_profit = valid[col_profit].sum()
                print(f"  {period}: n={len(valid):>4}, 勝率={win_rate:>5.1f}%, "
                      f"平均={avg_return:>+6.2f}%, 合計=¥{total_profit:>12,.0f}")
            else:
                print(f"  {period}: データなし")

    # 直近シグナル表示
    print("\n" + "=" * 60)
    print("Recent STRONG_BUY/BUY Signals (last 10)")
    print("=" * 60)
    recent = df.sort_values('signal_date', ascending=False).head(10)
    for _, row in recent.iterrows():
        ret_1d = f"{row['return_1d']:+.2f}%" if pd.notna(row['return_1d']) else "-"
        ret_5d = f"{row['return_5d']:+.2f}%" if pd.notna(row['return_5d']) else "-"
        ret_10d = f"{row['return_10d']:+.2f}%" if pd.notna(row['return_10d']) else "-"
        print(f"{row['signal_date']} {row['ticker']} {row['stock_name']:<10} "
              f"{row['signal']:<10} | 1d:{ret_1d:>8} 5d:{ret_5d:>8} 10d:{ret_10d:>8}")

    # 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)

    print("\n" + "=" * 60)
    print(f"✅ Saved to: {OUTPUT_FILE}")
    print(f"   Total records: {len(df)}")
    print(f"   Date range: {df['signal_date'].min()} to {df['signal_date'].max()}")
    print(f"   Columns: {len(df.columns)}")
    print("=" * 60)

    return df


if __name__ == "__main__":
    main()
