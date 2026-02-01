"""
日中分析用共通ユーティリティ

データ読み込み、時間帯分類、HTML出力などの共通関数
"""

from pathlib import Path
from typing import Literal
import pandas as pd
import numpy as np

# パス設定
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "parquet"
OUTPUT_DIR = BASE_DIR / "output" / "intraday_analysis"


def load_prices_5m(tickers: list[str] | None = None) -> pd.DataFrame:
    """5分足価格データを読み込み"""
    df = pd.read_parquet(DATA_DIR / "prices_60d_5m.parquet")
    df["date"] = pd.to_datetime(df["date"])
    if tickers:
        df = df[df["ticker"].isin(tickers)]
    return df


def load_index_prices_5m() -> pd.DataFrame:
    """日経/TOPIX 5分足データを読み込み"""
    df = pd.read_parquet(DATA_DIR / "index_prices_60d_5m.parquet")
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_meta() -> pd.DataFrame:
    """銘柄メタデータを読み込み"""
    return pd.read_parquet(DATA_DIR / "meta.parquet")


def load_meta_jquants() -> pd.DataFrame:
    """J-Quants銘柄マスタを読み込み"""
    return pd.read_parquet(DATA_DIR / "meta_jquants.parquet")


def load_grok_trending() -> pd.DataFrame:
    """grok銘柄データを読み込み"""
    return pd.read_parquet(DATA_DIR / "grok_trending.parquet")


def load_all_stocks() -> pd.DataFrame:
    """全銘柄データを読み込み（銘柄名、業種等含む）"""
    return pd.read_parquet(DATA_DIR / "all_stocks.parquet")


def classify_time_slot(time: pd.Timestamp) -> str:
    """
    時刻を30分刻みのスロットに分類

    Returns:
        "09:00-09:30", "09:30-10:00", ... "15:00-15:30"
    """
    hour = time.hour
    minute = time.minute

    if minute < 30:
        start = f"{hour:02d}:00"
        end = f"{hour:02d}:30"
    else:
        start = f"{hour:02d}:30"
        end = f"{hour+1:02d}:00" if hour < 23 else "00:00"

    return f"{start}-{end}"


def classify_session(time: pd.Timestamp) -> Literal["前場", "後場", "昼休み"]:
    """
    時刻を前場/後場/昼休みに分類
    """
    hour = time.hour
    minute = time.minute
    total_minutes = hour * 60 + minute

    if total_minutes < 11 * 60 + 30:  # 11:30前
        return "前場"
    elif total_minutes < 12 * 60 + 30:  # 12:30前
        return "昼休み"
    else:
        return "後場"


def calc_returns(df: pd.DataFrame, price_col: str = "Close") -> pd.DataFrame:
    """
    5分足リターンを計算

    Args:
        df: 価格データ（ticker, date, Close列必須）
        price_col: 価格列名

    Returns:
        元のDataFrameにreturn列を追加
    """
    df = df.copy()
    df = df.sort_values(["ticker", "date"])
    df["return"] = df.groupby("ticker")[price_col].pct_change()
    return df


def get_daily_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    5分足から日次OHLCVを集計

    Args:
        df: 5分足データ（ticker, date, Open, High, Low, Close, Volume列必須）

    Returns:
        日次OHLCV + 高値時間 + 安値時間
    """
    df = df.copy()
    df["trade_date"] = df["date"].dt.date

    # 高値・安値の時間を取得（NaNを除外）
    idx_high = df.groupby(["ticker", "trade_date"])["High"].idxmax().dropna()
    idx_low = df.groupby(["ticker", "trade_date"])["Low"].idxmin().dropna()

    high_times = df.loc[idx_high, ["ticker", "trade_date", "date"]].rename(columns={"date": "high_time"})
    low_times = df.loc[idx_low, ["ticker", "trade_date", "date"]].rename(columns={"date": "low_time"})

    # 日次OHLCV
    daily = df.groupby(["ticker", "trade_date"]).agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum"
    }).reset_index()

    # 高値・安値時間をマージ
    daily = daily.merge(high_times, on=["ticker", "trade_date"], how="left")
    daily = daily.merge(low_times, on=["ticker", "trade_date"], how="left")

    return daily


def generate_html_report(
    title: str,
    sections: list[dict],
    output_path: Path | str
) -> None:
    """
    分析結果をHTML形式で出力

    Args:
        title: レポートタイトル
        sections: [{"title": str, "content": str (HTML)}, ...]
        output_path: 出力ファイルパス
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sections_html = ""
    for sec in sections:
        sections_html += f"""
        <section class="section">
            <h2>{sec['title']}</h2>
            <div class="content">
                {sec['content']}
            </div>
        </section>
        """

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        :root {{
            --bg-color: #0a0a0a;
            --card-bg: #1a1a1a;
            --text-color: #e0e0e0;
            --text-muted: #888;
            --border-color: #333;
            --accent-color: #3b82f6;
            --positive-color: #22c55e;
            --negative-color: #ef4444;
        }}
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            line-height: 1.6;
            padding: 2rem;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        h1 {{
            font-size: 1.75rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            background: linear-gradient(90deg, var(--text-color), var(--accent-color));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}
        .subtitle {{
            color: var(--text-muted);
            font-size: 0.875rem;
            margin-bottom: 2rem;
        }}
        .section {{
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
        }}
        .section h2 {{
            font-size: 1.125rem;
            font-weight: 600;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid var(--border-color);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.875rem;
        }}
        th, td {{
            padding: 0.75rem;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        th {{
            color: var(--text-muted);
            font-weight: 500;
        }}
        .positive {{ color: var(--positive-color); }}
        .negative {{ color: var(--negative-color); }}
        .chart-container {{
            width: 100%;
            height: 400px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 1rem;
        }}
        .summary-card {{
            background: var(--bg-color);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 1rem;
        }}
        .summary-card .label {{
            font-size: 0.75rem;
            color: var(--text-muted);
            margin-bottom: 0.25rem;
        }}
        .summary-card .value {{
            font-size: 1.5rem;
            font-weight: 700;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <p class="subtitle">Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        {sections_html}
    </div>
</body>
</html>
"""

    output_path.write_text(html, encoding="utf-8")
    print(f"Report generated: {output_path}")


def format_pct(value: float, decimals: int = 2) -> str:
    """パーセント値をフォーマット"""
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{decimals}f}%"


def format_pct_with_class(value: float, decimals: int = 2) -> str:
    """パーセント値をHTMLクラス付きでフォーマット"""
    css_class = "positive" if value > 0 else "negative" if value < 0 else ""
    return f'<span class="{css_class}">{format_pct(value, decimals)}</span>'


if __name__ == "__main__":
    # テスト
    print("=== Utils Test ===")
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"OUTPUT_DIR: {OUTPUT_DIR}")

    # データ読み込みテスト
    prices = load_prices_5m()
    print(f"\nprices_60d_5m: {len(prices)} rows, tickers: {prices['ticker'].nunique()}")

    idx_prices = load_index_prices_5m()
    print(f"index_prices_60d_5m: {len(idx_prices)} rows, tickers: {idx_prices['ticker'].unique().tolist()}")

    meta = load_meta()
    print(f"meta: {len(meta)} rows")

    grok = load_grok_trending()
    print(f"grok_trending: {len(grok)} rows")

    # 時間分類テスト
    test_time = pd.Timestamp("2026-01-08 09:45:00")
    print(f"\nTime slot for {test_time}: {classify_time_slot(test_time)}")
    print(f"Session for {test_time}: {classify_session(test_time)}")
