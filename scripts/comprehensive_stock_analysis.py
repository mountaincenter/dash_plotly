#!/usr/bin/env python3
"""
包括的な株式分析
- prices parquetから詳細な価格分析
- サポート/レジスタンスレベル
- ボリューム分析
- トレンド分析
"""
import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime, timedelta

BASE_DIR = Path(__file__).parent.parent
PRICES_1D = BASE_DIR / "data/parquet/prices_max_1d.parquet"
PRICES_5M = BASE_DIR / "data/parquet/prices_60d_5m.parquet"

def analyze_stock(ticker, stock_name):
    """個別銘柄の包括的分析"""

    # 日次データ読み込み
    df_1d = pd.read_parquet(PRICES_1D)
    df_1d = df_1d[df_1d['ticker'] == ticker].copy()
    df_1d['date'] = pd.to_datetime(df_1d['date'])
    df_1d = df_1d.sort_values('date')

    if len(df_1d) == 0:
        return None

    # 5分足データ読み込み
    try:
        df_5m = pd.read_parquet(PRICES_5M)
        df_5m = df_5m[df_5m['ticker'] == ticker].copy()
        df_5m['date'] = pd.to_datetime(df_5m['date'])
        df_5m = df_5m.sort_values('date')
    except:
        df_5m = pd.DataFrame()

    # 最新データ
    latest = df_1d.iloc[-1]
    latest_date = latest['date']
    latest_close = latest['Close']
    latest_volume = latest['Volume']

    # 期間別分析
    df_5d = df_1d.tail(5)
    df_10d = df_1d.tail(10)
    df_30d = df_1d.tail(30)
    df_60d = df_1d.tail(60)

    # 過去の値動き
    if len(df_5d) >= 5:
        change_5d = ((latest_close - df_5d.iloc[0]['Close']) / df_5d.iloc[0]['Close'] * 100)
        high_5d = df_5d['High'].max()
        low_5d = df_5d['Low'].min()
    else:
        change_5d = 0
        high_5d = latest_close
        low_5d = latest_close

    if len(df_10d) >= 10:
        change_10d = ((latest_close - df_10d.iloc[0]['Close']) / df_10d.iloc[0]['Close'] * 100)
        high_10d = df_10d['High'].max()
        low_10d = df_10d['Low'].min()
    else:
        change_10d = 0
        high_10d = latest_close
        low_10d = latest_close

    if len(df_30d) >= 30:
        change_30d = ((latest_close - df_30d.iloc[0]['Close']) / df_30d.iloc[0]['Close'] * 100)
        high_30d = df_30d['High'].max()
        low_30d = df_30d['Low'].min()
        avg_volume_30d = df_30d['Volume'].mean()
    else:
        change_30d = 0
        high_30d = latest_close
        low_30d = latest_close
        avg_volume_30d = latest_volume

    # ボリューム分析
    volume_ratio_30d = latest_volume / avg_volume_30d if avg_volume_30d > 0 else 1.0

    # サポート/レジスタンスレベル（過去30日）
    support_levels = []
    resistance_levels = []

    if len(df_30d) >= 30:
        # ローカル最安値をサポート、最高値をレジスタンスとする
        for i in range(2, len(df_30d) - 2):
            # サポート（谷）
            if (df_30d.iloc[i]['Low'] < df_30d.iloc[i-1]['Low'] and
                df_30d.iloc[i]['Low'] < df_30d.iloc[i-2]['Low'] and
                df_30d.iloc[i]['Low'] < df_30d.iloc[i+1]['Low'] and
                df_30d.iloc[i]['Low'] < df_30d.iloc[i+2]['Low']):
                support_levels.append(float(df_30d.iloc[i]['Low']))

            # レジスタンス（山）
            if (df_30d.iloc[i]['High'] > df_30d.iloc[i-1]['High'] and
                df_30d.iloc[i]['High'] > df_30d.iloc[i-2]['High'] and
                df_30d.iloc[i]['High'] > df_30d.iloc[i+1]['High'] and
                df_30d.iloc[i]['High'] > df_30d.iloc[i+2]['High']):
                resistance_levels.append(float(df_30d.iloc[i]['High']))

        # 重複を削除し、直近3つのみ抽出
        support_levels = sorted(list(set(support_levels)), reverse=True)[:3]
        resistance_levels = sorted(list(set(resistance_levels)))[:3]

    # トレンド分析（移動平均）
    if len(df_30d) >= 5:
        df_30d['MA5'] = df_30d['Close'].rolling(window=5).mean()
        ma5_current = df_30d['MA5'].iloc[-1]
    else:
        ma5_current = latest_close

    if len(df_30d) >= 10:
        df_30d['MA10'] = df_30d['Close'].rolling(window=10).mean()
        ma10_current = df_30d['MA10'].iloc[-1]
    else:
        ma10_current = latest_close

    if len(df_30d) >= 25:
        df_30d['MA25'] = df_30d['Close'].rolling(window=25).mean()
        ma25_current = df_30d['MA25'].iloc[-1]
    else:
        ma25_current = latest_close

    # トレンド判定
    if latest_close > ma5_current > ma10_current > ma25_current:
        trend = "強い上昇トレンド"
    elif latest_close > ma5_current > ma10_current:
        trend = "上昇トレンド"
    elif latest_close < ma5_current < ma10_current < ma25_current:
        trend = "強い下降トレンド"
    elif latest_close < ma5_current < ma10_current:
        trend = "下降トレンド"
    else:
        trend = "レンジ・トレンドレス"

    # 5分足分析（前営業日）
    intraday_analysis = None
    if len(df_5m) > 0:
        # 最新の営業日の5分足データ
        latest_trade_date = df_5m['date'].dt.date.max()
        df_latest_5m = df_5m[df_5m['date'].dt.date == latest_trade_date]

        if len(df_latest_5m) > 0:
            open_price = df_latest_5m.iloc[0]['Open']
            close_price = df_latest_5m.iloc[-1]['Close']
            high_price = df_latest_5m['High'].max()
            low_price = df_latest_5m['Low'].min()
            total_volume = df_latest_5m['Volume'].sum()

            # 前場・後場の出来高比率
            morning_session = df_latest_5m[df_latest_5m['date'].dt.hour < 12]
            afternoon_session = df_latest_5m[df_latest_5m['date'].dt.hour >= 12]

            morning_volume = morning_session['Volume'].sum()
            afternoon_volume = afternoon_session['Volume'].sum()

            morning_volume_pct = (morning_volume / total_volume * 100) if total_volume > 0 else 0
            afternoon_volume_pct = (afternoon_volume / total_volume * 100) if total_volume > 0 else 0

            intraday_analysis = {
                "date": str(latest_trade_date),
                "open": float(open_price),
                "close": float(close_price),
                "high": float(high_price),
                "low": float(low_price),
                "totalVolume": int(total_volume),
                "morningVolumePct": float(morning_volume_pct),
                "afternoonVolumePct": float(afternoon_volume_pct),
                "intradayReturn": float((close_price - open_price) / open_price * 100),
                "volatility": float((high_price - low_price) / open_price * 100)
            }

    # 類似パターン分析
    pattern_analysis = analyze_similar_patterns(df_60d, volume_ratio_30d)

    return {
        "ticker": ticker,
        "stockName": stock_name,
        "latestDate": str(latest_date.date()),
        "latestClose": float(latest_close),
        "latestVolume": int(latest_volume),
        "priceMovement": {
            "5day": {
                "changePct": float(change_5d),
                "high": float(high_5d),
                "low": float(low_5d),
                "fromHigh": float((latest_close - high_5d) / high_5d * 100),
                "fromLow": float((latest_close - low_5d) / low_5d * 100)
            },
            "10day": {
                "changePct": float(change_10d),
                "high": float(high_10d),
                "low": float(low_10d),
                "fromHigh": float((latest_close - high_10d) / high_10d * 100),
                "fromLow": float((latest_close - low_10d) / low_10d * 100)
            },
            "30day": {
                "changePct": float(change_30d),
                "high": float(high_30d),
                "low": float(low_30d),
                "fromHigh": float((latest_close - high_30d) / high_30d * 100),
                "fromLow": float((latest_close - low_30d) / low_30d * 100)
            }
        },
        "volumeAnalysis": {
            "latest": int(latest_volume),
            "avg30day": int(avg_volume_30d),
            "ratio": float(volume_ratio_30d),
            "level": "異常高" if volume_ratio_30d > 2.0 else "高" if volume_ratio_30d > 1.3 else "通常" if volume_ratio_30d > 0.7 else "低"
        },
        "technicalLevels": {
            "supportLevels": support_levels,
            "resistanceLevels": resistance_levels,
            "ma5": float(ma5_current),
            "ma10": float(ma10_current),
            "ma25": float(ma25_current)
        },
        "trend": trend,
        "intradayAnalysis": intraday_analysis,
        "patternAnalysis": pattern_analysis
    }

def analyze_similar_patterns(df, current_volume_ratio):
    """類似パターン分析"""
    if len(df) < 20:
        return None

    # 出来高比率が類似している過去の日を抽出
    df = df.copy()
    df['volume_ma10'] = df['Volume'].rolling(window=10).mean()
    df['volume_ratio'] = df['Volume'] / df['volume_ma10']

    # 類似パターン：出来高比率が±0.2以内
    similar_days = df[
        (df['volume_ratio'] >= current_volume_ratio - 0.2) &
        (df['volume_ratio'] <= current_volume_ratio + 0.2)
    ]

    if len(similar_days) < 5:
        return None

    # 翌日の値動きを計算
    next_day_returns = []
    for idx in similar_days.index[:-1]:  # 最後の日は翌日データがないのでスキップ
        try:
            current_idx = df.index.get_loc(idx)
            if current_idx < len(df) - 1:
                today_close = df.iloc[current_idx]['Close']
                next_close = df.iloc[current_idx + 1]['Close']
                next_return = (next_close - today_close) / today_close * 100
                next_day_returns.append(next_return)
        except:
            continue

    if len(next_day_returns) == 0:
        return None

    win_count = sum(1 for r in next_day_returns if r > 0)
    win_rate = win_count / len(next_day_returns) * 100
    avg_return = np.mean(next_day_returns)

    return {
        "matchCount": len(next_day_returns),
        "winRate": float(win_rate),
        "avgReturn": float(avg_return),
        "maxReturn": float(max(next_day_returns)),
        "minReturn": float(min(next_day_returns)),
        "description": f"出来高比率{current_volume_ratio:.1f}x類似パターン（過去{len(next_day_returns)}回）"
    }

def main():
    # 対象銘柄
    stocks = [
        ("2586.T", "フルッタフルッタ"),
        ("6594.T", "ニデック"),
        ("5597.T", "ブルーイノベーション"),
        ("5574.T", "ABEJA"),
        ("302A.T", "ビースタイルホールディングス"),
        ("9348.T", "ispace"),
        ("6495.T", "宮入バルブ製作所"),
        ("2462.T", "ライク"),
        ("265A.T", "Hmcomm"),
        ("2492.T", "インフォマート"),
        ("6432.T", "竹内製作所")
    ]

    results = {}
    for ticker, name in stocks:
        print(f"[INFO] Analyzing {name} ({ticker})...", file=__import__('sys').stderr)
        analysis = analyze_stock(ticker, name)
        if analysis:
            results[ticker] = analysis
        else:
            print(f"[WARN] No data for {ticker}", file=__import__('sys').stderr)

    print(json.dumps(results, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
