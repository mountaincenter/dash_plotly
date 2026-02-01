#!/usr/bin/env python3
"""
金曜日/制度信用/1000-3000円 銘柄の日中パターン分析

分析項目:
1. 最安値の時間帯
2. 寄付からの下落幅
3. 反発タイミング
4. 最高値タイミング
5. RSI1（寄付/最安値/最高値/大引け）
"""

import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = ROOT / "data" / "parquet" / "backtest"
OUTPUT_DIR = ROOT / "improvement" / "output"
RSI_PERIOD = 14  # RSI期間

# 分析対象データ（ユーザー提供）
TARGET_DATA = [
    {"date": "2026-01-16", "ticker": "6167.T", "name": "冨士ダイス", "margin": "制度", "open": 1393},
    {"date": "2026-01-16", "ticker": "1434.T", "name": "JESCOホールディングス", "margin": "制度", "open": 1941},
    {"date": "2026-01-16", "ticker": "4443.T", "name": "Sansan", "margin": "制度", "open": 1976},
    {"date": "2026-01-09", "ticker": "7167.T", "name": "めぶきフィナンシャルグループ", "margin": "制度", "open": 1093},
    {"date": "2025-12-19", "ticker": "3079.T", "name": "ディーブイエックス", "margin": "制度", "open": 1250},
    {"date": "2025-12-12", "ticker": "2730.T", "name": "エディオン", "margin": "制度", "open": 2043},
    {"date": "2025-12-12", "ticker": "5351.T", "name": "品川リフラ", "margin": "制度", "open": 1985},
    {"date": "2025-12-05", "ticker": "5016.T", "name": "JX金属", "margin": "制度", "open": 1700},
    {"date": "2025-11-14", "ticker": "5406.T", "name": "神戸製鋼所", "margin": "制度", "open": 1846},
    {"date": "2025-11-14", "ticker": "4911.T", "name": "資生堂", "margin": "制度", "open": 2695},
]


def load_5m_data(ticker: str, target_date: str, include_prev_day: bool = True) -> pd.DataFrame | None:
    """
    5分足データを読み込み（前日データ含む）

    Args:
        ticker: 銘柄コード
        target_date: 対象日
        include_prev_day: 前日データも含めるか（RSI計算用）
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    prev_dt = target_dt - timedelta(days=1)

    # 週末の場合は金曜日を前日とする
    while prev_dt.weekday() >= 5:  # 土日
        prev_dt = prev_dt - timedelta(days=1)

    # まずbacktest parquetを試す
    for parquet_file in sorted(BACKTEST_DIR.glob("grok_5m_60d_*.parquet"), reverse=True):
        try:
            df = pd.read_parquet(parquet_file)
            if "ticker" not in df.columns:
                continue

            ticker_df = df[df["ticker"] == ticker].copy()
            if ticker_df.empty:
                continue

            # datetime or date カラムを統一
            if "datetime" in ticker_df.columns:
                ticker_df["date"] = pd.to_datetime(ticker_df["datetime"])
            elif "date" in ticker_df.columns:
                ticker_df["date"] = pd.to_datetime(ticker_df["date"])
            else:
                continue

            # JST変換（UTCの場合）
            if ticker_df["date"].dt.tz is not None:
                ticker_df["date"] = ticker_df["date"].dt.tz_convert("Asia/Tokyo")

            # 対象日と前日のデータを取得
            if include_prev_day:
                day_df = ticker_df[
                    (ticker_df["date"].dt.date == target_dt.date()) |
                    (ticker_df["date"].dt.date == prev_dt.date())
                ]
            else:
                day_df = ticker_df[ticker_df["date"].dt.date == target_dt.date()]

            if not day_df.empty:
                # カラム名を統一（小文字→大文字）
                rename_map = {"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
                day_df = day_df.rename(columns=rename_map)
                print(f"[OK] Found {ticker} in {parquet_file.name} (rows: {len(day_df)})")
                return day_df.sort_values("date")
        except Exception as e:
            print(f"[WARN] Failed to read {parquet_file}: {e}")
            continue

    # yfinanceから取得（前日+当日）
    print(f"[INFO] Fetching {ticker} for {target_date} from yfinance...")
    try:
        if include_prev_day:
            start = prev_dt
        else:
            start = target_dt
        end = target_dt + timedelta(days=1)
        yf_ticker = yf.Ticker(ticker)
        df = yf_ticker.history(start=start, end=end, interval="5m")

        if df.empty:
            return None

        df = df.reset_index()
        df = df.rename(columns={"Datetime": "date"})
        df["ticker"] = ticker
        print(f"[OK] Fetched from yfinance (rows: {len(df)})")
        return df.sort_values("date")
    except Exception as e:
        print(f"[ERROR] Failed to fetch {ticker}: {e}")
        return None


def get_prev_day_close(ticker: str, target_date: str) -> float | None:
    """前日終値をyfinance日足から取得"""
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    start = target_dt - timedelta(days=10)
    end = target_dt

    try:
        yf_ticker = yf.Ticker(ticker)
        df = yf_ticker.history(start=start, end=end, interval="1d")
        if df.empty:
            return None
        return df["Close"].iloc[-1]
    except Exception as e:
        print(f"[WARN] Failed to get prev close for {ticker}: {e}")
        return None


def get_day_close(ticker: str, target_date: str) -> float | None:
    """
    当日終値を取得
    1. grok_trending_archive.parquetから取得を試みる
    2. なければyfinance日足から取得
    """
    target_dt = datetime.strptime(target_date, "%Y-%m-%d")

    # grok_trending_archive.parquetから取得
    archive_path = ROOT / "data" / "parquet" / "grok_trending_archive.parquet"
    if archive_path.exists():
        try:
            df = pd.read_parquet(archive_path)
            if "backtest_date" in df.columns and "ticker" in df.columns:
                # backtest_dateを文字列として比較
                df["backtest_date_str"] = pd.to_datetime(df["backtest_date"]).dt.strftime("%Y-%m-%d")
                match = df[(df["ticker"] == ticker) & (df["backtest_date_str"] == target_date)]
                if not match.empty:
                    # 終値カラムを探す
                    for col in ["close_price", "Close", "終値", "sell_price"]:
                        if col in match.columns and pd.notna(match[col].iloc[0]):
                            return float(match[col].iloc[0])
        except Exception as e:
            print(f"[WARN] Failed to get close from archive for {ticker}: {e}")

    # yfinance日足から取得
    try:
        start = target_dt
        end = target_dt + timedelta(days=1)
        yf_ticker = yf.Ticker(ticker)
        df = yf_ticker.history(start=start, end=end, interval="1d")
        if df.empty:
            return None
        return df["Close"].iloc[0]
    except Exception as e:
        print(f"[WARN] Failed to get day close for {ticker}: {e}")
        return None


def calc_rsi1(prices: pd.Series, period: int = 14) -> pd.Series:
    """
    RSI1を計算（楽天証券方式）
    RSI = A / (A + B) * 100
    A = 期間内の値上がり幅の平均
    B = 期間内の値下がり幅の平均
    """
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def analyze_intraday(df: pd.DataFrame, open_price: float, prev_close: float = None, day_close: float = None, target_date: str = None) -> dict:
    """
    日中パターンを分析（RSI含む）

    Args:
        df: 5分足データ（前日+当日）
        open_price: 当日始値
        prev_close: 前日終値（15:30の値として追加）
        day_close: 当日終値（15:30の値として追加）
        target_date: 分析対象日（YYYY-MM-DD）
    """
    if df is None or df.empty:
        return None

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # 前日終値を15:30として追加（5分足は15:25までしかないため）
    if prev_close is not None:
        # 前日の日付を特定
        dates_in_df = df["date"].dt.date.unique()
        if len(dates_in_df) >= 1:
            prev_day = min(dates_in_df)
            prev_ts = pd.Timestamp(prev_day).replace(hour=15, minute=30)

            # tz-awareの場合はタイムゾーンを合わせる
            if df["date"].dt.tz is not None:
                prev_ts = prev_ts.tz_localize(df["date"].dt.tz)

            prev_row = pd.DataFrame([{
                "date": prev_ts,
                "Open": prev_close,
                "High": prev_close,
                "Low": prev_close,
                "Close": prev_close,
                "Volume": 0,
            }])
            df = pd.concat([prev_row, df], ignore_index=True)
            df = df.sort_values("date").reset_index(drop=True)

    # 当日終値を15:30として追加（5分足は15:25までしかないため）
    if day_close is not None and target_date is not None:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        day_ts = pd.Timestamp(target_dt).replace(hour=15, minute=30)

        # tz-awareの場合はタイムゾーンを合わせる
        if df["date"].dt.tz is not None:
            day_ts = day_ts.tz_localize(df["date"].dt.tz)

        day_row = pd.DataFrame([{
            "date": day_ts,
            "Open": day_close,
            "High": day_close,
            "Low": day_close,
            "Close": day_close,
            "Volume": 0,
        }])
        df = pd.concat([df, day_row], ignore_index=True)
        df = df.sort_values("date").reset_index(drop=True)

    df["time"] = df["date"].dt.strftime("%H:%M")

    # RSI計算（全データで計算）
    df["rsi"] = calc_rsi1(df["Close"], RSI_PERIOD)

    # 対象日のみをフィルタ（分析用）- 15:30も含める
    if target_date:
        target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
        trading_df = df[
            (df["date"].dt.date == target_dt) &
            (df["date"].dt.hour >= 9) &
            ((df["date"].dt.hour < 15) | ((df["date"].dt.hour == 15) & (df["date"].dt.minute <= 30)))
        ].copy()
    else:
        # target_dateがない場合は最新日の取引時間のみ
        trading_df = df[
            (df["date"].dt.hour >= 9) &
            ((df["date"].dt.hour < 15) | ((df["date"].dt.hour == 15) & (df["date"].dt.minute <= 30)))
        ].copy()

    if trading_df.empty:
        return None

    # 以降はtrading_dfを使用
    df = trading_df

    # 最安値
    low_idx = df["Low"].idxmin()
    low_row = df.loc[low_idx]
    low_time = low_row["time"]
    low_price = low_row["Low"]
    drop_from_open = open_price - low_price
    drop_pct = (drop_from_open / open_price) * 100

    # 最高値
    high_idx = df["High"].idxmax()
    high_row = df.loc[high_idx]
    high_time = high_row["time"]
    high_price = high_row["High"]
    rise_from_open = high_price - open_price
    rise_pct = (rise_from_open / open_price) * 100

    # 反発タイミング（最安値後、3本連続で上昇したポイント）
    rebound_time = None
    rebound_price = None
    low_position = df.index.get_loc(low_idx)

    for i in range(low_position + 1, len(df) - 2):
        current = df.iloc[i]
        next1 = df.iloc[i + 1]
        next2 = df.iloc[i + 2]

        if current["Close"] < next1["Close"] < next2["Close"]:
            rebound_time = current["time"]
            rebound_price = current["Close"]
            break

    # 時間帯分類
    def classify_time(time_str):
        hour = int(time_str.split(":")[0])
        minute = int(time_str.split(":")[1])
        total_min = hour * 60 + minute

        if total_min < 9 * 60 + 30:
            return "9:00-9:30"
        elif total_min < 10 * 60:
            return "9:30-10:00"
        elif total_min < 10 * 60 + 30:
            return "10:00-10:30"
        elif total_min < 11 * 60:
            return "10:30-11:00"
        elif total_min < 11 * 60 + 30:
            return "11:00-11:30"
        elif total_min < 12 * 60 + 30:
            return "前場引け後"
        elif total_min < 13 * 60:
            return "12:30-13:00"
        elif total_min < 13 * 60 + 30:
            return "13:00-13:30"
        elif total_min < 14 * 60:
            return "13:30-14:00"
        elif total_min < 14 * 60 + 30:
            return "14:00-14:30"
        else:
            return "14:30-15:00"

    # RSI取得（寄付、最安値、最高値、大引け）
    rsi_open = df["rsi"].iloc[0] if not pd.isna(df["rsi"].iloc[0]) else None
    rsi_low = df.loc[low_idx, "rsi"] if not pd.isna(df.loc[low_idx, "rsi"]) else None
    rsi_high = df.loc[high_idx, "rsi"] if not pd.isna(df.loc[high_idx, "rsi"]) else None
    rsi_close = df["rsi"].iloc[-1] if not pd.isna(df["rsi"].iloc[-1]) else None

    # 大引け価格
    close_price = df["Close"].iloc[-1]
    close_time = df["time"].iloc[-1]

    return {
        "low_time": low_time,
        "low_time_zone": classify_time(low_time),
        "low_price": low_price,
        "drop_from_open": drop_from_open,
        "drop_pct": drop_pct,
        "high_time": high_time,
        "high_time_zone": classify_time(high_time),
        "high_price": high_price,
        "rise_from_open": rise_from_open,
        "rise_pct": rise_pct,
        "rebound_time": rebound_time,
        "rebound_price": rebound_price,
        "low_before_high": low_time < high_time,
        "rsi_open": round(rsi_open, 1) if rsi_open else None,
        "rsi_low": round(rsi_low, 1) if rsi_low else None,
        "rsi_high": round(rsi_high, 1) if rsi_high else None,
        "rsi_close": round(rsi_close, 1) if rsi_close else None,
        "close_price": close_price,
        "close_time": close_time,
    }


def generate_html(results: list) -> str:
    """HTML生成"""
    html = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>金曜日 制度信用 1000-3000円 日中パターン分析</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }
        h1 { color: #00d4ff; }
        h2 { color: #ff6b6b; margin-top: 30px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #444; padding: 10px; text-align: center; }
        th { background: #2d2d44; color: #00d4ff; }
        tr:nth-child(even) { background: #252538; }
        tr:hover { background: #3d3d5c; }
        .positive { color: #4ade80; }
        .negative { color: #f87171; }
        .summary { background: #2d2d44; padding: 20px; border-radius: 10px; margin: 20px 0; }
        .summary h3 { color: #00d4ff; margin-top: 0; }
        .highlight { background: #3d5a80; }
    </style>
</head>
<body>
    <h1>金曜日 制度信用 1000-3000円 日中パターン分析</h1>
    <p>ショートエントリー時の最安値・最高値タイミング分析</p>

    <h2>個別銘柄分析</h2>
    <table>
        <tr>
            <th>日付</th>
            <th>銘柄</th>
            <th>始値</th>
            <th>RSI(寄)</th>
            <th>最安値時間</th>
            <th>最安値</th>
            <th>RSI(安)</th>
            <th>最高値時間</th>
            <th>最高値</th>
            <th>RSI(高)</th>
            <th>終値</th>
            <th>RSI(引)</th>
            <th>パターン</th>
        </tr>
"""

    low_time_zones = {}
    high_time_zones = {}

    for r in results:
        if r["analysis"] is None:
            html += f"""
        <tr>
            <td>{r['date']}</td>
            <td>{r['name']} ({r['ticker']})</td>
            <td>{r['open']}</td>
            <td colspan="10">データなし</td>
        </tr>
"""
            continue

        a = r["analysis"]
        drop_class = "negative" if a["drop_pct"] > 0 else "positive"
        rise_class = "positive" if a["rise_pct"] > 0 else "negative"

        # 時間帯集計
        low_time_zones[a["low_time_zone"]] = low_time_zones.get(a["low_time_zone"], 0) + 1
        high_time_zones[a["high_time_zone"]] = high_time_zones.get(a["high_time_zone"], 0) + 1

        # RSI色分け
        def rsi_class(rsi):
            if rsi is None:
                return ""
            if rsi >= 70:
                return "negative"  # 過買い
            if rsi <= 30:
                return "positive"  # 過売り
            return ""

        html += f"""
        <tr>
            <td>{r['date']}</td>
            <td>{r['name']}</td>
            <td>{r['open']:,.0f}</td>
            <td class="{rsi_class(a['rsi_open'])}">{a['rsi_open'] or '-'}</td>
            <td class="highlight">{a['low_time']}<br><small>({a['low_time_zone']})</small></td>
            <td>{a['low_price']:,.0f}</td>
            <td class="{rsi_class(a['rsi_low'])}">{a['rsi_low'] or '-'}</td>
            <td class="highlight">{a['high_time']}<br><small>({a['high_time_zone']})</small></td>
            <td>{a['high_price']:,.0f}</td>
            <td class="{rsi_class(a['rsi_high'])}">{a['rsi_high'] or '-'}</td>
            <td>{a['close_price']:,.0f}<br><small>({a['close_time']})</small></td>
            <td class="{rsi_class(a['rsi_close'])}">{a['rsi_close'] or '-'}</td>
            <td>{'先安後高' if a['low_before_high'] else '先高後安'}</td>
        </tr>
"""

    html += """
    </table>

    <div class="summary">
        <h3>最安値時間帯分布</h3>
        <table>
            <tr><th>時間帯</th><th>件数</th><th>割合</th></tr>
"""

    total_low = sum(low_time_zones.values())
    for zone, count in sorted(low_time_zones.items(), key=lambda x: -x[1]):
        pct = count / total_low * 100 if total_low > 0 else 0
        html += f"<tr><td>{zone}</td><td>{count}</td><td>{pct:.1f}%</td></tr>\n"

    html += """
        </table>
    </div>

    <div class="summary">
        <h3>最高値時間帯分布</h3>
        <table>
            <tr><th>時間帯</th><th>件数</th><th>割合</th></tr>
"""

    total_high = sum(high_time_zones.values())
    for zone, count in sorted(high_time_zones.items(), key=lambda x: -x[1]):
        pct = count / total_high * 100 if total_high > 0 else 0
        html += f"<tr><td>{zone}</td><td>{count}</td><td>{pct:.1f}%</td></tr>\n"

    html += """
        </table>
    </div>

    <div class="summary">
        <h3>ショート戦略への示唆</h3>
        <ul>
            <li><strong>最安値（利確ポイント）</strong>: 最も多い時間帯を確認</li>
            <li><strong>最高値（損切りリスク）</strong>: 最も多い時間帯を確認</li>
            <li><strong>先安後高</strong>: ショートに不利（早めに利確すべき）</li>
            <li><strong>先高後安</strong>: ショートに有利（大引けまで持てる）</li>
        </ul>
    </div>

    <p><small>Generated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</small></p>
</body>
</html>
"""
    return html


def main():
    print("=" * 60)
    print("金曜日 制度信用 1000-3000円 日中パターン分析")
    print("=" * 60)

    results = []

    for target in TARGET_DATA:
        print(f"\n[INFO] Analyzing {target['ticker']} ({target['name']}) on {target['date']}...")

        df = load_5m_data(target["ticker"], target["date"], include_prev_day=True)
        prev_close = get_prev_day_close(target["ticker"], target["date"])
        day_close = get_day_close(target["ticker"], target["date"])
        print(f"  前日終値: {prev_close}, 当日終値: {day_close}")
        analysis = analyze_intraday(df, target["open"], prev_close, day_close, target["date"]) if df is not None else None

        results.append({
            "date": target["date"],
            "ticker": target["ticker"],
            "name": target["name"],
            "margin": target["margin"],
            "open": target["open"],
            "analysis": analysis,
        })

        if analysis:
            print(f"  最安値: {analysis['low_time']} ({analysis['low_price']:,.0f}円, {analysis['drop_pct']:+.2f}%)")
            print(f"  最高値: {analysis['high_time']} ({analysis['high_price']:,.0f}円, {analysis['rise_pct']:+.2f}%)")

    # HTML生成
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    html = generate_html(results)
    output_path = OUTPUT_DIR / "friday_seido_1000_3000_intraday.html"
    output_path.write_text(html, encoding="utf-8")
    print(f"\n[OK] Output: {output_path}")

    return str(output_path)


if __name__ == "__main__":
    output_file = main()
    print(f"\nopen {output_file}")
