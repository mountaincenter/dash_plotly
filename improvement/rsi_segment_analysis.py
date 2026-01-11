"""
RSI x 4-segment profit analysis (SHORT basis)

Analyze correlation between 5min RSI (period=9, Rakuten RSI1 method) and 4-segment profit
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional
from datetime import datetime

# Path settings
BACKTEST_DIR = Path(__file__).resolve().parent.parent / "data" / "parquet" / "backtest"
ARCHIVE_PATH = BACKTEST_DIR / "grok_trending_archive.parquet"
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

# 5-min candle files
PRICES_5M_FILES = [
    BACKTEST_DIR / "grok_5m_60d_20251230.parquet",
    BACKTEST_DIR / "grok_5m_60d_20260110.parquet",
]

# RSI measurement times
RSI_TIMES = ["09:05", "09:30", "10:00", "10:25", "11:30", "12:35", "14:45", "15:30"]

# RSI period
RSI_PERIOD = 9


def calc_rsi(prices: pd.Series, period: int = RSI_PERIOD) -> float:
    """
    Calculate RSI using Rakuten RSI1 method

    RSI = avg_gain / (avg_gain + avg_loss) * 100
    Uses available data even if < period (like Rakuten)
    """
    deltas = prices.diff().dropna()
    if len(deltas) == 0:
        return np.nan

    # Use last N deltas, or all if fewer
    use_n = min(period, len(deltas))
    recent_deltas = deltas.iloc[-use_n:]

    gains = recent_deltas.clip(lower=0)
    losses = (-recent_deltas).clip(lower=0)

    total_gain = gains.sum()
    total_loss = losses.sum()

    if total_gain + total_loss == 0:
        return 50.0  # no change

    rsi = (total_gain / (total_gain + total_loss)) * 100
    return rsi


def load_5m_prices() -> pd.DataFrame:
    """Load 5-min candle data (merge multiple files)"""
    dfs = []
    for f in PRICES_5M_FILES:
        if f.exists():
            df = pd.read_parquet(f)
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert("Asia/Tokyo")
            df["date"] = df["datetime"].dt.date
            df["time"] = df["datetime"].dt.strftime("%H:%M")
            df = df.rename(columns={
                "open": "Open", "high": "High", "low": "Low",
                "close": "Close", "volume": "Volume"
            })
            dfs.append(df)
            print(f"  {f.name}: {len(df)}行")

    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    merged = merged.drop_duplicates(subset=["ticker", "datetime"], keep="last")
    merged = merged.sort_values(["ticker", "datetime"])
    return merged


def get_rsi_at_time(
    combined_data: pd.DataFrame,
    target_time: str,
    period: int = RSI_PERIOD
) -> Optional[float]:
    """
    Calculate RSI at specified time

    Calculate RSI using data before target_time
    combined_data should contain prev_day + current_day data
    """
    # Get data before target_time
    data_before = combined_data[combined_data["time"] <= target_time].copy()

    if len(data_before) < period + 1:
        return None

    # RSI based on Close price
    prices = data_before["Close"]
    return calc_rsi(prices, period)


def get_previous_trading_date(prices_5m: pd.DataFrame, ticker: str, current_date) -> Optional[object]:
    """Get previous trading date"""
    ticker_data = prices_5m[prices_5m["ticker"] == ticker]
    dates = sorted(ticker_data["date"].unique())
    if current_date in dates:
        idx = dates.index(current_date)
        if idx > 0:
            return dates[idx - 1]
    return None


def calculate_rsi_for_trades(archive: pd.DataFrame, prices_5m: pd.DataFrame) -> pd.DataFrame:
    """Calculate RSI for each trade"""
    results = []

    total = len(archive)
    for idx, row in archive.iterrows():
        if idx % 100 == 0:
            print(f"  Processing: {idx}/{total}")

        ticker = row["ticker"]
        sel_date = pd.to_datetime(row["selection_date"]).date()
        daily_close = row.get("daily_close")

        # Current day 5-min data
        day_data = prices_5m[
            (prices_5m["ticker"] == ticker) &
            (prices_5m["date"] == sel_date)
        ].copy()

        # Previous day 5-min data (for morning RSI)
        # Add prev day daily close as 15:30 (yfinance only has up to 15:20)
        prev_date = get_previous_trading_date(prices_5m, ticker, sel_date)
        prev_close = row.get("prev_close")  # This is prev day's daily close

        if prev_date is not None:
            prev_data = prices_5m[
                (prices_5m["ticker"] == ticker) &
                (prices_5m["date"] == prev_date)
            ].copy()

            # Add 15:30 candle with prev day daily close
            if pd.notna(prev_close) and len(prev_data) > 0:
                last_row = prev_data.iloc[-1:].copy()
                last_dt = last_row["datetime"].iloc[0]
                last_row["datetime"] = last_dt.replace(hour=15, minute=30)
                last_row["time"] = "15:30"
                last_row["Close"] = prev_close
                prev_data = pd.concat([prev_data, last_row], ignore_index=True)

            # Combine prev + current day (time order)
            combined_data = pd.concat([prev_data, day_data], ignore_index=True)
            combined_data = combined_data.sort_values("datetime")
        else:
            combined_data = day_data.copy()

        # RSI calculation
        rsi_values = {}
        for t in RSI_TIMES:
            col_name = f"rsi_{t.replace(':', '')}"

            if t == "15:30":
                # 15:30: add daily close to current day data
                if pd.notna(daily_close) and len(day_data) > 0:
                    day_for_1530 = day_data.copy()
                    last_row = day_for_1530.iloc[-1:].copy()
                    last_dt = last_row["datetime"].iloc[0]
                    last_row["datetime"] = last_dt.replace(hour=15, minute=30)
                    last_row["time"] = "15:30"
                    last_row["Close"] = daily_close
                    day_for_1530 = pd.concat([day_for_1530, last_row], ignore_index=True)

                    if prev_date is not None:
                        combined_1530 = pd.concat([prev_data, day_for_1530], ignore_index=True)
                        combined_1530 = combined_1530.sort_values("datetime")
                    else:
                        combined_1530 = day_for_1530

                    filtered = combined_1530[
                        (combined_1530["date"] < sel_date) |
                        ((combined_1530["date"] == sel_date) & (combined_1530["time"] <= t))
                    ]
                    rsi_values[col_name] = calc_rsi(filtered["Close"], RSI_PERIOD)
                else:
                    rsi_values[col_name] = None
            else:
                # All times: use prev_day (with 15:30) + current_day data
                if len(combined_data) > 0:
                    filtered_data = combined_data[
                        (combined_data["date"] < sel_date) |
                        ((combined_data["date"] == sel_date) & (combined_data["time"] <= t))
                    ]
                    rsi_values[col_name] = calc_rsi(filtered_data["Close"], RSI_PERIOD)
                else:
                    rsi_values[col_name] = None

        # 元データ + RSI（ショート基準: 符号反転）
        me_long = row.get("profit_per_100_shares_morning_early")
        p1_long = row.get("profit_per_100_shares_phase1")
        ae_long = row.get("profit_per_100_shares_afternoon_early")
        p2_long = row.get("profit_per_100_shares_phase2")

        result = {
            "ticker": ticker,
            "date": sel_date,
            "stock_name": row.get("stock_name"),
            "prev_close": row.get("prev_close"),
            "buy_price": row.get("buy_price"),
            "margin_type": row.get("margin_code_name"),
            # 4区分損益（ショート基準: 符号反転）
            "me": -me_long if pd.notna(me_long) else None,
            "p1": -p1_long if pd.notna(p1_long) else None,
            "ae": -ae_long if pd.notna(ae_long) else None,
            "p2": -p2_long if pd.notna(p2_long) else None,
        }
        result.update(rsi_values)
        results.append(result)

    return pd.DataFrame(results)


def get_rsi_band(rsi: float) -> str:
    """RSI値を帯域に分類"""
    if pd.isna(rsi):
        return "N/A"
    if rsi < 20:
        return "0-20"
    if rsi < 30:
        return "20-30"
    if rsi < 50:
        return "30-50"
    if rsi < 70:
        return "50-70"
    return "70-100"


# Price ranges (matching dev_analysis.py)
PRICE_RANGES = [
    {"label": "~1,000", "min": 0, "max": 1000},
    {"label": "1,000~3,000", "min": 1000, "max": 3000},
    {"label": "3,000~5,000", "min": 3000, "max": 5000},
    {"label": "5,000~10,000", "min": 5000, "max": 10000},
    {"label": "10,000~", "min": 10000, "max": float("inf")},
]

# Weekday names
WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri"]


def get_price_range(buy_price: float) -> str:
    """Classify price range"""
    if pd.isna(buy_price):
        return "N/A"
    for pr in PRICE_RANGES:
        if pr["min"] <= buy_price < pr["max"]:
            return pr["label"]
    return PRICE_RANGES[-1]["label"]


def get_weekday(date_val) -> str:
    """Get weekday name"""
    if pd.isna(date_val):
        return "N/A"
    wd = pd.to_datetime(date_val).weekday()
    return WEEKDAY_NAMES[wd] if wd < 5 else "N/A"


def calc_segment_stats(data: pd.DataFrame) -> dict:
    """Calculate 4-segment stats for given data"""
    if len(data) == 0:
        return {
            "count": 0,
            "me_sum": 0, "me_win": 0,
            "p1_sum": 0, "p1_win": 0,
            "ae_sum": 0, "ae_win": 0,
            "p2_sum": 0, "p2_win": 0,
            "best_segment": "-"
        }

    stats = {
        "count": len(data),
        "me_sum": data["me"].sum(),
        "me_win": (data["me"] > 0).sum() / len(data) * 100,
        "p1_sum": data["p1"].sum(),
        "p1_win": (data["p1"] > 0).sum() / len(data) * 100,
        "ae_sum": data["ae"].sum(),
        "ae_win": (data["ae"] > 0).sum() / len(data) * 100,
        "p2_sum": data["p2"].sum(),
        "p2_win": (data["p2"] > 0).sum() / len(data) * 100,
    }

    profits = {"me": stats["me_sum"], "p1": stats["p1_sum"], "ae": stats["ae_sum"], "p2": stats["p2_sum"]}
    stats["best_segment"] = max(profits, key=profits.get)

    return stats


def analyze_rsi_segments(df: pd.DataFrame) -> dict:
    """RSI band x 4-segment profit analysis"""
    analyses = {}

    for rsi_col in [f"rsi_{t.replace(':', '')}" for t in RSI_TIMES]:
        df["rsi_band"] = df[rsi_col].apply(get_rsi_band)

        band_stats = []
        for band in ["0-20", "20-30", "30-50", "50-70", "70-100", "N/A"]:
            band_data = df[df["rsi_band"] == band]
            if len(band_data) == 0:
                continue

            stats = calc_segment_stats(band_data)
            stats["band"] = band
            band_stats.append(stats)

        analyses[rsi_col] = band_stats

    return analyses


def analyze_by_price_range(df: pd.DataFrame, rsi_col: str) -> dict:
    """RSI band x Price range analysis"""
    df["rsi_band"] = df[rsi_col].apply(get_rsi_band)
    df["price_range"] = df["buy_price"].apply(get_price_range)

    results = {}
    for band in ["0-20", "20-30", "30-50", "50-70", "70-100"]:
        band_data = df[df["rsi_band"] == band]
        price_stats = []
        for pr in PRICE_RANGES:
            pr_data = band_data[band_data["price_range"] == pr["label"]]
            stats = calc_segment_stats(pr_data)
            stats["price_range"] = pr["label"]
            price_stats.append(stats)
        results[band] = price_stats

    return results


def analyze_by_weekday(df: pd.DataFrame, rsi_col: str) -> dict:
    """RSI band x Weekday analysis"""
    df["rsi_band"] = df[rsi_col].apply(get_rsi_band)
    df["weekday"] = df["date"].apply(get_weekday)

    results = {}
    for band in ["0-20", "20-30", "30-50", "50-70", "70-100"]:
        band_data = df[df["rsi_band"] == band]
        weekday_stats = []
        for wd in WEEKDAY_NAMES:
            wd_data = band_data[band_data["weekday"] == wd]
            stats = calc_segment_stats(wd_data)
            stats["weekday"] = wd
            weekday_stats.append(stats)
        results[band] = weekday_stats

    return results


def analyze_exit_timing(df: pd.DataFrame) -> dict:
    """
    Analyze optimal exit timing based on RSI at each checkpoint.

    For each checkpoint (ME/P1/AE), analyze:
    - If RSI is in band X at this point, what's the best exit?
    - Compare: exit now vs hold to next checkpoints

    Group by: weekday x price_range x RSI_band at checkpoint
    """
    df = df.copy()
    df["weekday"] = df["date"].apply(get_weekday)
    df["price_range"] = df["buy_price"].apply(get_price_range)

    # Checkpoints and their RSI columns
    checkpoints = [
        {"name": "ME", "time": "10:25", "rsi_col": "rsi_1025", "profit_col": "me",
         "remaining": ["p1", "ae", "p2"]},
        {"name": "P1", "time": "11:30", "rsi_col": "rsi_1130", "profit_col": "p1",
         "remaining": ["ae", "p2"]},
        {"name": "AE", "time": "14:45", "rsi_col": "rsi_1445", "profit_col": "ae",
         "remaining": ["p2"]},
    ]

    results = {}

    for cp in checkpoints:
        cp_name = cp["name"]
        rsi_col = cp["rsi_col"]
        profit_col = cp["profit_col"]
        remaining = cp["remaining"]

        # Add RSI band for this checkpoint
        df[f"rsi_band_{cp_name}"] = df[rsi_col].apply(get_rsi_band)

        cp_results = []

        # Group by weekday x price_range
        for wd in WEEKDAY_NAMES:
            for pr in PRICE_RANGES:
                pr_label = pr["label"]

                group = df[(df["weekday"] == wd) & (df["price_range"] == pr_label)]
                if len(group) == 0:
                    continue

                # For each RSI band at this checkpoint
                for band in ["0-20", "20-30", "30-50", "50-70", "70-100"]:
                    band_data = group[group[f"rsi_band_{cp_name}"] == band]
                    if len(band_data) == 0:
                        continue

                    n = len(band_data)

                    # Calculate profit if exit at this checkpoint
                    exit_now_profit = band_data[profit_col].sum()
                    exit_now_win = (band_data[profit_col] > 0).sum() / n * 100

                    # Calculate profit if hold to each remaining checkpoint
                    hold_profits = {}
                    for rem in remaining:
                        hold_profits[rem] = {
                            "sum": band_data[rem].sum(),
                            "win": (band_data[rem] > 0).sum() / n * 100
                        }

                    # Determine best exit
                    all_options = {cp_name: exit_now_profit}
                    for rem, data in hold_profits.items():
                        all_options[rem.upper()] = data["sum"]

                    best_exit = max(all_options, key=all_options.get)

                    # Recommendation
                    if best_exit == cp_name:
                        recommendation = f"Exit at {cp_name}"
                    else:
                        recommendation = f"Hold to {best_exit}"

                    cp_results.append({
                        "weekday": wd,
                        "price_range": pr_label,
                        "rsi_band": band,
                        "count": n,
                        "exit_now": {
                            "profit": exit_now_profit,
                            "win_rate": exit_now_win
                        },
                        "hold_profits": hold_profits,
                        "best_exit": best_exit,
                        "recommendation": recommendation,
                    })

        results[cp_name] = cp_results

    return results


def generate_exit_timing_html(df: pd.DataFrame, exit_analysis: dict) -> str:
    """Generate HTML for exit timing analysis"""

    html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RSI Exit Timing Analysis</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-gray-100 p-6">
    <div class="max-w-7xl mx-auto">
        <h1 class="text-2xl font-bold mb-2">RSI Exit Timing Analysis (SHORT basis)</h1>
        <p class="text-gray-400 mb-6">At each checkpoint, analyze optimal exit based on RSI</p>
"""

    for cp_name, cp_results in exit_analysis.items():
        html += f"""
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-xl font-semibold mb-4 text-blue-400">Checkpoint: {cp_name}</h2>
"""

        # Group by weekday
        for wd in WEEKDAY_NAMES:
            wd_results = [r for r in cp_results if r["weekday"] == wd]
            if not wd_results:
                continue

            html += f"""
            <h3 class="text-lg font-medium mt-4 mb-2 text-yellow-300">{wd}</h3>
            <div class="overflow-x-auto">
                <table class="w-full text-sm mb-4">
                    <thead>
                        <tr class="text-gray-400 border-b border-gray-700">
                            <th class="text-left py-2">Price</th>
                            <th class="text-left py-2">RSI</th>
                            <th class="text-right py-2">N</th>
                            <th class="text-right py-2">{cp_name}</th>
                            <th class="text-right py-2">Win%</th>
"""
            # Add remaining columns dynamically
            if cp_name == "ME":
                html += """
                            <th class="text-right py-2">P1</th>
                            <th class="text-right py-2">AE</th>
                            <th class="text-right py-2">P2</th>
"""
            elif cp_name == "P1":
                html += """
                            <th class="text-right py-2">AE</th>
                            <th class="text-right py-2">P2</th>
"""
            elif cp_name == "AE":
                html += """
                            <th class="text-right py-2">P2</th>
"""

            html += """
                            <th class="text-center py-2">Best</th>
                        </tr>
                    </thead>
                    <tbody>
"""

            for r in wd_results:
                exit_profit = r["exit_now"]["profit"]
                exit_win = r["exit_now"]["win_rate"]
                best = r["best_exit"]

                # Color for best exit
                best_color = "text-emerald-400 font-bold"

                html += f"""
                        <tr class="border-b border-gray-700/50">
                            <td class="py-2">{r['price_range']}</td>
                            <td class="py-2">{r['rsi_band']}</td>
                            <td class="text-right py-2">{r['count']}</td>
                            <td class="text-right py-2 {'text-emerald-400' if exit_profit > 0 else 'text-rose-400'} {best_color if best == cp_name else ''}">{exit_profit:+,.0f}</td>
                            <td class="text-right py-2">{exit_win:.0f}%</td>
"""

                # Add remaining profits
                for rem_key in ["p1", "ae", "p2"]:
                    if rem_key in r["hold_profits"]:
                        rem_data = r["hold_profits"][rem_key]
                        rem_profit = rem_data["sum"]
                        is_best = best == rem_key.upper()
                        html += f"""
                            <td class="text-right py-2 {'text-emerald-400' if rem_profit > 0 else 'text-rose-400'} {best_color if is_best else ''}">{rem_profit:+,.0f}</td>
"""

                html += f"""
                            <td class="text-center py-2 text-emerald-400 font-bold">{best}</td>
                        </tr>
"""

            html += """
                    </tbody>
                </table>
            </div>
"""

        html += """
        </div>
"""

    html += """
    </div>
</body>
</html>
"""
    return html


def generate_html(df: pd.DataFrame, analyses: dict, sample_ticker: str = "6227.T") -> str:
    """分析結果をHTML出力"""

    # サンプル銘柄のデータ
    sample_data = df[df["ticker"] == sample_ticker]

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RSI × 4区分損益 分析</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-gray-100 p-6">
    <div class="max-w-7xl mx-auto">
        <h1 class="text-2xl font-bold mb-2">RSI × 4区分損益 分析（ショート基準）</h1>
        <p class="text-gray-400 mb-6">5分足RSI（期間9）と4区分損益の相関分析 ※損益はショート基準</p>

        <!-- サマリー -->
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-2">データサマリー</h2>
            <div class="grid grid-cols-4 gap-4 text-center">
                <div>
                    <div class="text-2xl font-bold text-blue-400">{len(df)}</div>
                    <div class="text-xs text-gray-400">総トレード数</div>
                </div>
                <div>
                    <div class="text-2xl font-bold text-emerald-400">{df['me'].dropna().sum():+,.0f}</div>
                    <div class="text-xs text-gray-400">10:25合計</div>
                </div>
                <div>
                    <div class="text-2xl font-bold text-emerald-400">{df['ae'].dropna().sum():+,.0f}</div>
                    <div class="text-xs text-gray-400">14:45合計</div>
                </div>
                <div>
                    <div class="text-2xl font-bold text-emerald-400">{df['p2'].dropna().sum():+,.0f}</div>
                    <div class="text-xs text-gray-400">大引合計</div>
                </div>
            </div>
        </div>

        <!-- 検証: サンプル銘柄 -->
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-2">検証: {sample_ticker}</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="text-gray-400 border-b border-gray-700">
                            <th class="text-left py-2">日付</th>
                            <th class="text-right py-2">始値</th>
                            {"".join(f'<th class="text-right py-2">{t}</th>' for t in RSI_TIMES)}
                            <th class="text-right py-2">10:25損益</th>
                            <th class="text-right py-2">大引損益</th>
                        </tr>
                    </thead>
                    <tbody>
"""

    for _, row in sample_data.iterrows():
        rsi_cells = ""
        for t in RSI_TIMES:
            col = f"rsi_{t.replace(':', '')}"
            val = row.get(col)
            if pd.notna(val):
                color = "text-rose-400" if val < 30 else "text-emerald-400" if val > 70 else "text-gray-300"
                rsi_cells += f'<td class="text-right py-2 {color}">{val:.1f}</td>'
            else:
                rsi_cells += '<td class="text-right py-2 text-gray-500">-</td>'

        me_val = row.get("me")
        p2_val = row.get("p2")
        me_color = "text-emerald-400" if pd.notna(me_val) and me_val > 0 else "text-rose-400"
        p2_color = "text-emerald-400" if pd.notna(p2_val) and p2_val > 0 else "text-rose-400"
        me_str = f"{me_val:+,.0f}" if pd.notna(me_val) else "-"
        p2_str = f"{p2_val:+,.0f}" if pd.notna(p2_val) else "-"
        buy_str = f"{row['buy_price']:,.0f}" if pd.notna(row['buy_price']) else "-"

        html += f"""
                        <tr class="border-b border-gray-700/50">
                            <td class="py-2">{row['date']}</td>
                            <td class="text-right py-2">{buy_str}</td>
                            {rsi_cells}
                            <td class="text-right py-2 {me_color}">{me_str}</td>
                            <td class="text-right py-2 {p2_color}">{p2_str}</td>
                        </tr>
"""

    html += """
                    </tbody>
                </table>
            </div>
        </div>
"""

    # RSI帯別分析
    for rsi_col, stats in analyses.items():
        time_label = rsi_col.replace("rsi_", "")
        time_formatted = f"{time_label[:2]}:{time_label[2:]}"

        html += f"""
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-2">RSI帯別分析: {time_formatted}時点</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="text-gray-400 border-b border-gray-700">
                            <th class="text-left py-2">RSI帯</th>
                            <th class="text-right py-2">件数</th>
                            <th class="text-right py-2">10:25損益</th>
                            <th class="text-right py-2">勝率</th>
                            <th class="text-right py-2">前場引損益</th>
                            <th class="text-right py-2">勝率</th>
                            <th class="text-right py-2">14:45損益</th>
                            <th class="text-right py-2">勝率</th>
                            <th class="text-right py-2">大引損益</th>
                            <th class="text-right py-2">勝率</th>
                            <th class="text-center py-2">最良区分</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        for s in stats:
            best_color = {
                "me": "text-blue-400",
                "p1": "text-yellow-400",
                "ae": "text-purple-400",
                "p2": "text-emerald-400"
            }.get(s["best_segment"], "text-gray-400")

            best_label = {
                "me": "10:25",
                "p1": "前場引",
                "ae": "14:45",
                "p2": "大引"
            }.get(s["best_segment"], "-")

            html += f"""
                        <tr class="border-b border-gray-700/50">
                            <td class="py-2 font-medium">{s['band']}</td>
                            <td class="text-right py-2">{s['count']}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['me_sum'] > 0 else 'text-rose-400'}">{s['me_sum']:+,.0f}</td>
                            <td class="text-right py-2">{s['me_win']:.0f}%</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['p1_sum'] > 0 else 'text-rose-400'}">{s['p1_sum']:+,.0f}</td>
                            <td class="text-right py-2">{s['p1_win']:.0f}%</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['ae_sum'] > 0 else 'text-rose-400'}">{s['ae_sum']:+,.0f}</td>
                            <td class="text-right py-2">{s['ae_win']:.0f}%</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['p2_sum'] > 0 else 'text-rose-400'}">{s['p2_sum']:+,.0f}</td>
                            <td class="text-right py-2">{s['p2_win']:.0f}%</td>
                            <td class="text-center py-2 {best_color} font-bold">{best_label}</td>
                        </tr>
"""
        html += """
                    </tbody>
                </table>
            </div>
        </div>
"""

    html += """
    </div>
</body>
</html>
"""
    return html


def generate_html_extended(
    df: pd.DataFrame,
    analyses: dict,
    price_analyses: dict,
    weekday_analyses: dict,
    sample_ticker: str = "6227.T"
) -> str:
    """Extended HTML with price range and weekday analysis"""

    sample_data = df[df["ticker"] == sample_ticker]

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RSI x 4-segment Profit Analysis</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-gray-100 p-6">
    <div class="max-w-7xl mx-auto">
        <h1 class="text-2xl font-bold mb-2">RSI x 4-segment Profit Analysis (SHORT basis)</h1>
        <p class="text-gray-400 mb-6">5min RSI (period=9) correlation with 4-segment profit</p>

        <!-- Summary -->
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-2">Summary</h2>
            <div class="grid grid-cols-4 gap-4 text-center">
                <div>
                    <div class="text-2xl font-bold text-blue-400">{len(df)}</div>
                    <div class="text-xs text-gray-400">Total trades</div>
                </div>
                <div>
                    <div class="text-2xl font-bold text-emerald-400">{df['me'].dropna().sum():+,.0f}</div>
                    <div class="text-xs text-gray-400">ME (10:25)</div>
                </div>
                <div>
                    <div class="text-2xl font-bold text-emerald-400">{df['ae'].dropna().sum():+,.0f}</div>
                    <div class="text-xs text-gray-400">AE (14:45)</div>
                </div>
                <div>
                    <div class="text-2xl font-bold text-emerald-400">{df['p2'].dropna().sum():+,.0f}</div>
                    <div class="text-xs text-gray-400">P2 (Close)</div>
                </div>
            </div>
        </div>

        <!-- Sample ticker -->
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-2">Sample: {sample_ticker}</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="text-gray-400 border-b border-gray-700">
                            <th class="text-left py-2">Date</th>
                            <th class="text-right py-2">Open</th>
                            {"".join(f'<th class="text-right py-2">{t}</th>' for t in RSI_TIMES)}
                            <th class="text-right py-2">ME</th>
                            <th class="text-right py-2">P2</th>
                        </tr>
                    </thead>
                    <tbody>
"""

    for _, row in sample_data.iterrows():
        rsi_cells = ""
        for t in RSI_TIMES:
            col = f"rsi_{t.replace(':', '')}"
            val = row.get(col)
            if pd.notna(val):
                color = "text-rose-400" if val < 30 else "text-emerald-400" if val > 70 else "text-gray-300"
                rsi_cells += f'<td class="text-right py-2 {color}">{val:.1f}</td>'
            else:
                rsi_cells += '<td class="text-right py-2 text-gray-500">-</td>'

        me_val = row.get("me")
        p2_val = row.get("p2")
        me_color = "text-emerald-400" if pd.notna(me_val) and me_val > 0 else "text-rose-400"
        p2_color = "text-emerald-400" if pd.notna(p2_val) and p2_val > 0 else "text-rose-400"
        me_str = f"{me_val:+,.0f}" if pd.notna(me_val) else "-"
        p2_str = f"{p2_val:+,.0f}" if pd.notna(p2_val) else "-"
        buy_str = f"{row['buy_price']:,.0f}" if pd.notna(row['buy_price']) else "-"

        html += f"""
                        <tr class="border-b border-gray-700/50">
                            <td class="py-2">{row['date']}</td>
                            <td class="text-right py-2">{buy_str}</td>
                            {rsi_cells}
                            <td class="text-right py-2 {me_color}">{me_str}</td>
                            <td class="text-right py-2 {p2_color}">{p2_str}</td>
                        </tr>
"""

    html += """
                    </tbody>
                </table>
            </div>
        </div>
"""

    # RSI band analysis (same as before)
    for rsi_col, stats in analyses.items():
        time_label = rsi_col.replace("rsi_", "")
        time_formatted = f"{time_label[:2]}:{time_label[2:]}"

        html += f"""
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-2">RSI Band Analysis: {time_formatted}</h2>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="text-gray-400 border-b border-gray-700">
                            <th class="text-left py-2">RSI</th>
                            <th class="text-right py-2">N</th>
                            <th class="text-right py-2">ME</th>
                            <th class="text-right py-2">Win%</th>
                            <th class="text-right py-2">P1</th>
                            <th class="text-right py-2">Win%</th>
                            <th class="text-right py-2">AE</th>
                            <th class="text-right py-2">Win%</th>
                            <th class="text-right py-2">P2</th>
                            <th class="text-right py-2">Win%</th>
                            <th class="text-center py-2">Best</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        for s in stats:
            best_label = {"me": "ME", "p1": "P1", "ae": "AE", "p2": "P2"}.get(s["best_segment"], "-")
            best_color = {"me": "text-blue-400", "p1": "text-yellow-400", "ae": "text-purple-400", "p2": "text-emerald-400"}.get(s["best_segment"], "text-gray-400")

            html += f"""
                        <tr class="border-b border-gray-700/50">
                            <td class="py-2 font-medium">{s['band']}</td>
                            <td class="text-right py-2">{s['count']}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['me_sum'] > 0 else 'text-rose-400'}">{s['me_sum']:+,.0f}</td>
                            <td class="text-right py-2">{s['me_win']:.0f}%</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['p1_sum'] > 0 else 'text-rose-400'}">{s['p1_sum']:+,.0f}</td>
                            <td class="text-right py-2">{s['p1_win']:.0f}%</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['ae_sum'] > 0 else 'text-rose-400'}">{s['ae_sum']:+,.0f}</td>
                            <td class="text-right py-2">{s['ae_win']:.0f}%</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['p2_sum'] > 0 else 'text-rose-400'}">{s['p2_sum']:+,.0f}</td>
                            <td class="text-right py-2">{s['p2_win']:.0f}%</td>
                            <td class="text-center py-2 {best_color} font-bold">{best_label}</td>
                        </tr>
"""
        html += """
                    </tbody>
                </table>
            </div>
        </div>
"""

    # Price range analysis
    for rsi_col, band_data in price_analyses.items():
        time_label = rsi_col.replace("rsi_", "")
        time_formatted = f"{time_label[:2]}:{time_label[2:]}"

        html += f"""
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-2">Price Range Analysis: {time_formatted}</h2>
"""
        for band, price_stats in band_data.items():
            html += f"""
            <h3 class="text-md font-medium mt-4 mb-2 text-blue-300">RSI {band}</h3>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="text-gray-400 border-b border-gray-700">
                            <th class="text-left py-2">Price</th>
                            <th class="text-right py-2">N</th>
                            <th class="text-right py-2">ME</th>
                            <th class="text-right py-2">P1</th>
                            <th class="text-right py-2">AE</th>
                            <th class="text-right py-2">P2</th>
                            <th class="text-center py-2">Best</th>
                        </tr>
                    </thead>
                    <tbody>
"""
            for s in price_stats:
                best_label = {"me": "ME", "p1": "P1", "ae": "AE", "p2": "P2"}.get(s["best_segment"], "-")
                html += f"""
                        <tr class="border-b border-gray-700/50">
                            <td class="py-2">{s['price_range']}</td>
                            <td class="text-right py-2">{s['count']}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['me_sum'] > 0 else 'text-rose-400'}">{s['me_sum']:+,.0f}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['p1_sum'] > 0 else 'text-rose-400'}">{s['p1_sum']:+,.0f}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['ae_sum'] > 0 else 'text-rose-400'}">{s['ae_sum']:+,.0f}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['p2_sum'] > 0 else 'text-rose-400'}">{s['p2_sum']:+,.0f}</td>
                            <td class="text-center py-2 font-bold">{best_label}</td>
                        </tr>
"""
            html += """
                    </tbody>
                </table>
            </div>
"""
        html += """
        </div>
"""

    # Weekday analysis
    for rsi_col, band_data in weekday_analyses.items():
        time_label = rsi_col.replace("rsi_", "")
        time_formatted = f"{time_label[:2]}:{time_label[2:]}"

        html += f"""
        <div class="bg-gray-800 rounded-lg p-4 mb-6">
            <h2 class="text-lg font-semibold mb-2">Weekday Analysis: {time_formatted}</h2>
"""
        for band, wd_stats in band_data.items():
            html += f"""
            <h3 class="text-md font-medium mt-4 mb-2 text-blue-300">RSI {band}</h3>
            <div class="overflow-x-auto">
                <table class="w-full text-sm">
                    <thead>
                        <tr class="text-gray-400 border-b border-gray-700">
                            <th class="text-left py-2">Day</th>
                            <th class="text-right py-2">N</th>
                            <th class="text-right py-2">ME</th>
                            <th class="text-right py-2">P1</th>
                            <th class="text-right py-2">AE</th>
                            <th class="text-right py-2">P2</th>
                            <th class="text-center py-2">Best</th>
                        </tr>
                    </thead>
                    <tbody>
"""
            for s in wd_stats:
                best_label = {"me": "ME", "p1": "P1", "ae": "AE", "p2": "P2"}.get(s["best_segment"], "-")
                html += f"""
                        <tr class="border-b border-gray-700/50">
                            <td class="py-2">{s['weekday']}</td>
                            <td class="text-right py-2">{s['count']}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['me_sum'] > 0 else 'text-rose-400'}">{s['me_sum']:+,.0f}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['p1_sum'] > 0 else 'text-rose-400'}">{s['p1_sum']:+,.0f}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['ae_sum'] > 0 else 'text-rose-400'}">{s['ae_sum']:+,.0f}</td>
                            <td class="text-right py-2 {'text-emerald-400' if s['p2_sum'] > 0 else 'text-rose-400'}">{s['p2_sum']:+,.0f}</td>
                            <td class="text-center py-2 font-bold">{best_label}</td>
                        </tr>
"""
            html += """
                    </tbody>
                </table>
            </div>
"""
        html += """
        </div>
"""

    html += """
    </div>
</body>
</html>
"""
    return html


def main():
    print("=== RSI x 4-segment Profit Analysis ===")
    print(f"RSI period: {RSI_PERIOD}")
    print(f"Measurement times: {', '.join(RSI_TIMES)}")

    # Load 5min data
    print("\nLoading 5min data...")
    prices_5m = load_5m_prices()
    if len(prices_5m) == 0:
        print("5min data not found")
        return
    print(f"  Total: {len(prices_5m)} rows, {prices_5m['ticker'].nunique()} tickers")

    # Load archive
    print("\nLoading archive...")
    archive = pd.read_parquet(ARCHIVE_PATH)
    print(f"  {len(archive)} records")

    # Calculate RSI
    print("\nCalculating RSI...")
    df = calculate_rsi_for_trades(archive, prices_5m)

    # RSI band analysis
    print("\nAnalyzing RSI bands...")
    analyses = analyze_rsi_segments(df)

    # Price range analysis (for key RSI times)
    print("\nAnalyzing by price range...")
    key_times = ["rsi_0930", "rsi_1025", "rsi_1130"]
    price_analyses = {}
    for rsi_col in key_times:
        if rsi_col in [f"rsi_{t.replace(':', '')}" for t in RSI_TIMES]:
            price_analyses[rsi_col] = analyze_by_price_range(df.copy(), rsi_col)

    # Weekday analysis
    print("\nAnalyzing by weekday...")
    weekday_analyses = {}
    for rsi_col in key_times:
        if rsi_col in [f"rsi_{t.replace(':', '')}" for t in RSI_TIMES]:
            weekday_analyses[rsi_col] = analyze_by_weekday(df.copy(), rsi_col)

    # Exit timing analysis
    print("\nAnalyzing exit timing...")
    exit_analysis = analyze_exit_timing(df.copy())

    # Generate HTML
    print("\nGenerating HTML...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Main analysis HTML
    html = generate_html_extended(df, analyses, price_analyses, weekday_analyses)
    output_path = OUTPUT_DIR / "rsi_segment_analysis.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Output: {output_path}")

    # Exit timing HTML
    exit_html = generate_exit_timing_html(df, exit_analysis)
    exit_output_path = OUTPUT_DIR / "rsi_exit_timing.html"
    with open(exit_output_path, "w", encoding="utf-8") as f:
        f.write(exit_html)
    print(f"Output: {exit_output_path}")

    # Summary
    print("\n=== Summary ===")
    for rsi_col, stats in analyses.items():
        time_label = rsi_col.replace("rsi_", "")
        time_formatted = f"{time_label[:2]}:{time_label[2:]}"
        print(f"\n{time_formatted}:")
        for s in stats:
            if s["band"] != "N/A":
                print(f"  RSI {s['band']}: {s['count']} trades, best={s['best_segment']}")


if __name__ == "__main__":
    main()
