#!/usr/bin/env python3
"""
複数テクニカル指標の組み合わせによる予測力検証

目的: 単一指標より組み合わせの方が予測力が高いか検証
"""

import numpy as np
import pandas as pd
from scipy.stats import rankdata, pearsonr, spearmanr
from itertools import combinations
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ========== Chatterjee相関係数 ==========

def chatterjee_correlation(x: np.ndarray, y: np.ndarray) -> float:
    mask = ~(np.isnan(x) | np.isnan(y))
    x, y = x[mask], y[mask]
    n = len(x)
    if n < 10:
        return np.nan
    order = np.argsort(x)
    y_sorted = y[order]
    ranks = rankdata(y_sorted, method='average')
    rank_diff_sum = np.sum(np.abs(np.diff(ranks)))
    xi = 1 - (3 * rank_diff_sum) / (n**2 - 1)
    return xi


def all_correlations(x: np.ndarray, y: np.ndarray) -> dict:
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
    return {"pearson": pearson, "spearman": spearman, "chatterjee": chatterjee, "n": len(x_clean)}


# ========== テクニカル指標 ==========

def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=span).mean()

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    diff = close.diff()
    gain = diff.clip(lower=0.0)
    loss = (-diff).clip(lower=0.0)
    avg_gain = ema(gain, period)
    avg_loss = ema(loss, period)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))

def macd_histogram(close: pd.Series, fast: int = 12, slow: int = 26, sig: int = 9) -> pd.Series:
    ema_fast = ema(close, fast)
    ema_slow = ema(close, slow)
    macd = ema_fast - ema_slow
    signal = ema(macd, sig)
    return macd - signal

def bb_percent_b(close: pd.Series, window: int = 20, k: float = 2.0) -> pd.Series:
    ma = close.rolling(window=window, min_periods=window).mean()
    sd = close.rolling(window=window, min_periods=window).std(ddof=0)
    upper = ma + k * sd
    lower = ma - k * sd
    rng = (upper - lower).replace(0, np.nan)
    return (close - lower) / rng

def sma_deviation(close: pd.Series, window: int = 25) -> pd.Series:
    base = close.rolling(window=window, min_periods=window).mean()
    return (close / base - 1.0) * 100.0

def roc(close: pd.Series, window: int = 12) -> pd.Series:
    return (close / close.shift(window) - 1.0) * 100.0

def volume_ratio(volume: pd.Series, window: int = 20) -> pd.Series:
    avg_vol = volume.rolling(window=window, min_periods=window).mean()
    return volume / avg_vol.replace(0, np.nan)

def momentum(close: pd.Series, window: int = 10) -> pd.Series:
    return close - close.shift(window)

def stochastic_k(close: pd.Series, high: pd.Series, low: pd.Series, window: int = 14) -> pd.Series:
    lowest = low.rolling(window=window, min_periods=window).min()
    highest = high.rolling(window=window, min_periods=window).max()
    return ((close - lowest) / (highest - lowest).replace(0, np.nan)) * 100

def atr_pct(close: pd.Series, high: pd.Series, low: pd.Series, span: int = 14) -> pd.Series:
    prev = close.shift(1)
    tr = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    atr_val = tr.ewm(span=span, adjust=False).mean()
    return (atr_val / close) * 100.0


# ========== 複合シグナル生成 ==========

def normalize_indicator(s: pd.Series) -> pd.Series:
    """指標を0-1に正規化（ランク変換）"""
    return s.rank(pct=True)


def create_composite_signals(indicators_df: pd.DataFrame) -> pd.DataFrame:
    """様々な複合シグナルを作成"""
    signals = pd.DataFrame(index=indicators_df.index)

    # 正規化
    norm = indicators_df.apply(normalize_indicator)

    # === 逆張り系複合 ===
    # RSI + BB%B（両方低い = 売られすぎ）
    signals['oversold_combo'] = (1 - norm['rsi_14']) + (1 - norm['bb_pct_b'])

    # RSI + Stochastic（オシレーター組み合わせ）
    signals['oscillator_combo'] = norm['rsi_14'] + norm['stoch_k']

    # === モメンタム系複合 ===
    # モメンタム + ROC（トレンド強度）
    signals['trend_strength'] = norm['momentum_10'] + norm['roc_5']

    # SMA乖離 + モメンタム
    signals['deviation_momentum'] = norm['sma_dev_25'] + norm['momentum_10']

    # === 出来高考慮 ===
    # モメンタム × 出来高比率（出来高を伴うモメンタム）
    signals['volume_momentum'] = norm['momentum_10'] * norm['volume_ratio']

    # RSI × 出来高比率
    signals['volume_rsi'] = norm['rsi_14'] * norm['volume_ratio']

    # === 複合逆張り（3指標）===
    # RSI + BB%B + Stochastic
    signals['triple_oscillator'] = norm['rsi_14'] + norm['bb_pct_b'] + norm['stoch_k']

    # === ボラティリティ考慮 ===
    # モメンタム / ATR（ボラ調整モメンタム）
    signals['vol_adj_momentum'] = norm['momentum_10'] / (norm['atr_pct'] + 0.01)

    # === 条件付きシグナル ===
    # RSI < 30 or RSI > 70 の極端ゾーンでのみ有効
    rsi_val = indicators_df['rsi_14']
    signals['rsi_extreme'] = np.where((rsi_val < 30) | (rsi_val > 70), rsi_val, np.nan)

    # BB%B < 0 or > 1 の極端ゾーン
    bb_val = indicators_df['bb_pct_b']
    signals['bb_extreme'] = np.where((bb_val < 0) | (bb_val > 1), bb_val, np.nan)

    # === スコア系（重み付き）===
    # 逆張りスコア: RSI低い + BB低い + Stoch低い + 出来高高い
    signals['contrarian_score'] = (
        (1 - norm['rsi_14']) * 0.3 +
        (1 - norm['bb_pct_b']) * 0.3 +
        (1 - norm['stoch_k']) * 0.2 +
        norm['volume_ratio'] * 0.2
    )

    # 順張りスコア: モメンタム高い + ROC高い + 出来高高い
    signals['trend_score'] = (
        norm['momentum_10'] * 0.4 +
        norm['roc_5'] * 0.3 +
        norm['volume_ratio'] * 0.3
    )

    return signals


# ========== メイン処理 ==========

def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """全テクニカル指標を計算"""
    close = df['Close']
    high = df['High']
    low = df['Low']
    volume = df['Volume']

    indicators = pd.DataFrame(index=df.index)
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

    # 翌日リターン
    indicators['next_day_return'] = close.pct_change().shift(-1) * 100

    return indicators


def analyze_signals(prices_df: pd.DataFrame) -> pd.DataFrame:
    """全銘柄で複合シグナルを分析"""
    tickers = prices_df['ticker'].unique().tolist()
    all_results = []

    for ticker in tickers:
        df = prices_df[prices_df['ticker'] == ticker].copy()
        df = df.sort_values('date').reset_index(drop=True)

        if len(df) < 100:
            continue

        # 直近1年
        df = df.tail(252)

        # テクニカル指標計算
        indicators = calculate_all_indicators(df)

        # 複合シグナル作成
        signals = create_composite_signals(indicators)

        # 翌日リターンとの相関を計算
        target = indicators['next_day_return'].values

        # 単一指標
        for col in ['rsi_14', 'momentum_10', 'volume_ratio', 'bb_pct_b', 'sma_dev_25']:
            x = indicators[col].values
            corrs = all_correlations(x, target)
            all_results.append({
                'ticker': ticker,
                'signal': col,
                'type': 'single',
                'pearson': corrs['pearson'],
                'spearman': corrs['spearman'],
                'chatterjee': corrs['chatterjee'],
                'n': corrs['n']
            })

        # 複合シグナル
        for col in signals.columns:
            x = signals[col].values
            corrs = all_correlations(x, target)
            all_results.append({
                'ticker': ticker,
                'signal': col,
                'type': 'composite',
                'pearson': corrs['pearson'],
                'spearman': corrs['spearman'],
                'chatterjee': corrs['chatterjee'],
                'n': corrs['n']
            })

    return pd.DataFrame(all_results)


def main():
    print("=" * 70)
    print("複数テクニカル指標の組み合わせによる予測力検証")
    print("=" * 70)

    # データ読み込み
    data_path = Path("data/parquet/prices_max_1d.parquet")
    prices = pd.read_parquet(data_path)

    print(f"\n分析対象: {prices['ticker'].nunique()}銘柄")
    print("複合シグナル: 12種類")
    print()

    # 分析実行
    results = analyze_signals(prices)

    # サマリー
    summary = results.groupby(['signal', 'type']).agg({
        'pearson': 'mean',
        'spearman': 'mean',
        'chatterjee': 'mean',
        'n': 'mean'
    }).round(4)

    summary = summary.sort_values('chatterjee', ascending=False)

    print("=" * 70)
    print("結果サマリー: シグナルごとの平均相関（翌日リターンとの相関）")
    print("=" * 70)
    print(summary.to_string())

    # 単一 vs 複合の比較
    single = summary.xs('single', level='type') if 'single' in summary.index.get_level_values('type') else pd.DataFrame()
    composite = summary.xs('composite', level='type') if 'composite' in summary.index.get_level_values('type') else pd.DataFrame()

    print("\n" + "=" * 70)
    print("単一指標 TOP 5")
    print("=" * 70)
    if len(single) > 0:
        print(single.nlargest(5, 'chatterjee').to_string())

    print("\n" + "=" * 70)
    print("複合シグナル TOP 5")
    print("=" * 70)
    if len(composite) > 0:
        print(composite.nlargest(5, 'chatterjee').to_string())

    # 改善率
    if len(single) > 0 and len(composite) > 0:
        best_single = single['chatterjee'].max()
        best_composite = composite['chatterjee'].max()
        improvement = (best_composite - best_single) / best_single * 100

        print("\n" + "=" * 70)
        print("結論")
        print("=" * 70)
        print(f"単一指標ベスト:    Chatterjee = {best_single:.4f}")
        print(f"複合シグナルベスト: Chatterjee = {best_composite:.4f}")
        print(f"改善率: {improvement:+.1f}%")

        if best_composite > best_single:
            print("\n→ 複合シグナルの方が予測力が高い！")
        else:
            print("\n→ 単一指標の方が良い（複合化の効果なし）")

    # 保存
    results.to_parquet("improvement/data/chatterjee_multi_indicator_results.parquet")
    summary.to_csv("improvement/data/chatterjee_multi_indicator_summary.csv")
    print(f"\n結果を保存しました")

    return results, summary


if __name__ == "__main__":
    results, summary = main()
