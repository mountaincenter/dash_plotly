#!/usr/bin/env python3
"""
Chatterjee相関を用いたテクニカル指標の予測力バックテスト

目的: テクニカル指標が翌日リターンを予測できるか検証
方法: ピアソン、スピアマン、Chatterjee相関を比較
"""

import numpy as np
import pandas as pd
from scipy.stats import rankdata, pearsonr, spearmanr
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ========== Chatterjee相関係数 ==========

def chatterjee_correlation(x: np.ndarray, y: np.ndarray) -> float:
    """
    Chatterjee相関係数 ξ を計算

    ξ = 0 → 独立
    ξ = 1 → Y = f(X) の関数関係
    """
    # NaNを除去
    mask = ~(np.isnan(x) | np.isnan(y))
    x, y = x[mask], y[mask]

    n = len(x)
    if n < 10:
        return np.nan

    # Xでソートした時のYのインデックス
    order = np.argsort(x)
    y_sorted = y[order]

    # Yの順位を計算
    ranks = rankdata(y_sorted, method='average')

    # 隣接順位の差の絶対値の総和
    rank_diff_sum = np.sum(np.abs(np.diff(ranks)))

    # Chatterjee相関係数
    xi = 1 - (3 * rank_diff_sum) / (n**2 - 1)

    return xi


def all_correlations(x: np.ndarray, y: np.ndarray) -> dict:
    """3種類の相関係数を計算"""
    mask = ~(np.isnan(x) | np.isnan(y))
    x_clean, y_clean = x[mask], y[mask]

    if len(x_clean) < 30:
        return {"pearson": np.nan, "spearman": np.nan, "chatterjee": np.nan, "n": len(x_clean)}

    try:
        pearson = pearsonr(x_clean, y_clean)[0]
    except:
        pearson = np.nan

    try:
        spearman = spearmanr(x_clean, y_clean)[0]
    except:
        spearman = np.nan

    chatterjee = chatterjee_correlation(x_clean, y_clean)

    return {
        "pearson": pearson,
        "spearman": spearman,
        "chatterjee": chatterjee,
        "n": len(x_clean)
    }


# ========== テクニカル指標 ==========

def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=span).mean()

def sma(s: pd.Series, window: int) -> pd.Series:
    return s.rolling(window=window, min_periods=window).mean()

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """RSI (Relative Strength Index)"""
    diff = close.diff()
    gain = diff.clip(lower=0.0)
    loss = (-diff).clip(lower=0.0)
    avg_gain = ema(gain, period)
    avg_loss = ema(loss, period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))

def macd_histogram(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9) -> pd.Series:
    """MACD Histogram"""
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd = ema_fast - ema_slow
    signal = ema(macd, sig)
    return macd - signal

def bb_percent_b(close: pd.Series, window: int = 20, k: float = 2.0) -> pd.Series:
    """Bollinger Bands %B"""
    ma = close.rolling(window=window, min_periods=window).mean()
    sd = close.rolling(window=window, min_periods=window).std(ddof=0)
    upper = ma + k * sd
    lower = ma - k * sd
    rng = (upper - lower).replace(0, np.nan)
    return (close - lower) / rng

def sma_deviation(close: pd.Series, window: int = 25) -> pd.Series:
    """SMA乖離率 (%)"""
    base = close.rolling(window=window, min_periods=window).mean()
    return (close / base - 1.0) * 100.0

def roc(close: pd.Series, window: int = 12) -> pd.Series:
    """Rate of Change (%)"""
    return (close / close.shift(window) - 1.0) * 100.0

def atr_pct(close: pd.Series, high: pd.Series, low: pd.Series, span: int = 14) -> pd.Series:
    """ATR as percentage of close"""
    prev = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev).abs(),
        (low - prev).abs()
    ], axis=1).max(axis=1)
    atr_val = tr.ewm(span=span, adjust=False).mean()
    return (atr_val / close) * 100.0

def volume_ratio(volume: pd.Series, window: int = 20) -> pd.Series:
    """出来高比率（20日平均比）"""
    avg_vol = volume.rolling(window=window, min_periods=window).mean()
    return volume / avg_vol.replace(0, np.nan)

def momentum(close: pd.Series, window: int = 10) -> pd.Series:
    """モメンタム"""
    return close - close.shift(window)

def stochastic_k(close: pd.Series, high: pd.Series, low: pd.Series, window: int = 14) -> pd.Series:
    """ストキャスティクス %K"""
    lowest = low.rolling(window=window, min_periods=window).min()
    highest = high.rolling(window=window, min_periods=window).max()
    return ((close - lowest) / (highest - lowest).replace(0, np.nan)) * 100

def williams_r(close: pd.Series, high: pd.Series, low: pd.Series, window: int = 14) -> pd.Series:
    """Williams %R"""
    highest = high.rolling(window=window, min_periods=window).max()
    lowest = low.rolling(window=window, min_periods=window).min()
    return ((highest - close) / (highest - lowest).replace(0, np.nan)) * -100


# ========== メイン処理 ==========

def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """全テクニカル指標を計算"""
    close = df['Close']
    high = df['High']
    low = df['Low']
    volume = df['Volume']

    indicators = pd.DataFrame(index=df.index)

    # 基本指標
    indicators['rsi_14'] = rsi(close, 14)
    indicators['rsi_7'] = rsi(close, 7)
    indicators['macd_hist'] = macd_histogram(close)
    indicators['bb_pct_b'] = bb_percent_b(close)
    indicators['sma_dev_25'] = sma_deviation(close, 25)
    indicators['sma_dev_75'] = sma_deviation(close, 75)
    indicators['roc_12'] = roc(close, 12)
    indicators['roc_5'] = roc(close, 5)
    indicators['atr_pct'] = atr_pct(close, high, low)
    indicators['volume_ratio'] = volume_ratio(volume)
    indicators['momentum_10'] = momentum(close, 10)
    indicators['stoch_k'] = stochastic_k(close, high, low)
    indicators['williams_r'] = williams_r(close, high, low)

    # 翌日リターン（予測対象）
    indicators['next_day_return'] = close.pct_change().shift(-1) * 100

    return indicators


def analyze_ticker(ticker: str, prices_df: pd.DataFrame) -> dict:
    """1銘柄の分析"""
    df = prices_df[prices_df['ticker'] == ticker].copy()
    df = df.sort_values('date').reset_index(drop=True)

    if len(df) < 100:
        return None

    # 直近1年のデータを使用
    df = df.tail(252)

    # テクニカル指標計算
    indicators = calculate_indicators(df)

    # 翌日リターンとの相関を計算
    target = indicators['next_day_return'].values

    results = []
    indicator_cols = [c for c in indicators.columns if c != 'next_day_return']

    for col in indicator_cols:
        x = indicators[col].values
        corrs = all_correlations(x, target)

        results.append({
            'ticker': ticker,
            'indicator': col,
            'pearson': corrs['pearson'],
            'spearman': corrs['spearman'],
            'chatterjee': corrs['chatterjee'],
            'n': corrs['n']
        })

    return results


def main():
    print("=" * 60)
    print("Chatterjee相関バックテスト: テクニカル指標の予測力検証")
    print("=" * 60)

    # データ読み込み
    data_path = Path("data/parquet/prices_max_1d.parquet")
    prices = pd.read_parquet(data_path)

    tickers = prices['ticker'].unique().tolist()
    print(f"\n分析対象: {len(tickers)}銘柄")
    print(f"期間: 直近1年（252営業日）")

    # 全銘柄を分析
    all_results = []

    for i, ticker in enumerate(tickers):
        results = analyze_ticker(ticker, prices)
        if results:
            all_results.extend(results)

        if (i + 1) % 20 == 0:
            print(f"  {i + 1}/{len(tickers)} 完了")

    # 結果をDataFrameに
    df_results = pd.DataFrame(all_results)

    # 指標ごとの平均相関を計算
    summary = df_results.groupby('indicator').agg({
        'pearson': ['mean', 'std'],
        'spearman': ['mean', 'std'],
        'chatterjee': ['mean', 'std'],
        'n': 'mean'
    }).round(4)

    summary.columns = ['pearson_mean', 'pearson_std', 'spearman_mean', 'spearman_std',
                       'chatterjee_mean', 'chatterjee_std', 'n_mean']
    summary = summary.sort_values('chatterjee_mean', ascending=False)

    print("\n" + "=" * 60)
    print("結果サマリー: 指標ごとの平均相関（翌日リターンとの相関）")
    print("=" * 60)
    print("\n※ Chatterjee相関が高い = 非線形含む予測力あり")
    print("※ Chatterjee高 & Pearson低 = 非線形関係の発見\n")

    print(summary.to_string())

    # 非線形関係の発見（Chatterjee > Pearson）
    summary['nonlinear_gap'] = summary['chatterjee_mean'] - summary['pearson_mean'].abs()
    nonlinear = summary.sort_values('nonlinear_gap', ascending=False)

    print("\n" + "=" * 60)
    print("非線形関係の発見（Chatterjee - |Pearson|）")
    print("=" * 60)
    print(nonlinear[['chatterjee_mean', 'pearson_mean', 'nonlinear_gap']].head(10).to_string())

    # 結果を保存
    output_path = Path("improvement/data/chatterjee_backtest_results.parquet")
    df_results.to_parquet(output_path)
    print(f"\n詳細結果を保存: {output_path}")

    # サマリーもCSVで保存
    summary_path = Path("improvement/data/chatterjee_indicator_summary.csv")
    summary.to_csv(summary_path)
    print(f"サマリーを保存: {summary_path}")

    # 最も予測力のある指標
    print("\n" + "=" * 60)
    print("結論: 最も予測力がありそうな指標 TOP 5")
    print("=" * 60)
    top5 = summary.nlargest(5, 'chatterjee_mean')
    for idx, (indicator, row) in enumerate(top5.iterrows(), 1):
        print(f"{idx}. {indicator}")
        print(f"   Chatterjee: {row['chatterjee_mean']:.4f} (±{row['chatterjee_std']:.4f})")
        print(f"   Pearson:    {row['pearson_mean']:.4f}")
        print(f"   Spearman:   {row['spearman_mean']:.4f}")
        print()

    return df_results, summary


if __name__ == "__main__":
    df_results, summary = main()
