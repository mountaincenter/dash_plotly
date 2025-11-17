#!/usr/bin/env python3
"""
売買タイミング最適化分析

利確水準と損切り水準の比較:
1. 利確: 前場不成（11:30） vs 大引不成（15:30）
2. 損切り: 前場損切り（11:30） vs 大引精算（15:30）

5分足データをyfinanceで取得して分析
"""
import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / 'test_output'
OUTPUT_DIR.mkdir(exist_ok=True)

# 入力ファイル
GROK_ANALYSIS_PATH = ROOT / 'data' / 'parquet' / 'backtest' / 'grok_analysis_merged.parquet'

# 出力ファイル
TIMING_ANALYSIS_OUTPUT = OUTPUT_DIR / 'timing_analysis_results.parquet'

# 日本株の取引時間（JST）- 2024年11月～延長後
MORNING_START = "09:00"
MORNING_END = "11:30"
AFTERNOON_START = "12:30"
AFTERNOON_END = "15:30"  # 大引け 15:30


def get_5min_data(ticker: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    yfinanceで5分足データを取得

    Args:
        ticker: ティッカーコード（例: "1234.T"）
        start_date: 開始日
        end_date: 終了日

    Returns:
        5分足データのDataFrame（JSTタイムゾーン）
    """
    try:
        # yfinanceで5分足データを取得（過去60日間のみ取得可能）
        stock = yf.Ticker(ticker)

        # 取得期間を調整（前後1日の余裕を持たせる）
        start_str = (start_date - timedelta(days=1)).strftime('%Y-%m-%d')
        end_str = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')

        df = stock.history(start=start_str, end=end_str, interval="5m")

        if df.empty:
            logger.warning(f"  No data for {ticker}")
            return pd.DataFrame()

        # タイムゾーンをJSTに変換
        if df.index.tz is not None:
            df.index = df.index.tz_convert('Asia/Tokyo')
        else:
            df.index = df.index.tz_localize('UTC').tz_convert('Asia/Tokyo')

        return df

    except Exception as e:
        logger.error(f"  Error fetching data for {ticker}: {e}")
        return pd.DataFrame()


def analyze_timing_for_stock(
    ticker: str,
    backtest_date: datetime,
    buy_price: float,
    intraday_data: pd.DataFrame,
    recommendation_action: str = None,
    recommendation_score: float = None
) -> dict:
    """
    個別銘柄の売買タイミング分析

    Args:
        ticker: ティッカーコード
        backtest_date: バックテスト日
        buy_price: 買値（寄り付き価格）
        intraday_data: 5分足データ
        recommendation_action: 売買推奨（'buy', 'sell', 'hold', None）
        recommendation_score: 推奨スコア（静観時の判断に使用）

    Returns:
        分析結果の辞書
    """
    # 対象日のデータのみ抽出
    target_date = backtest_date.date()
    day_data = intraday_data[intraday_data.index.date == target_date].copy()

    if day_data.empty:
        logger.warning(f"  No intraday data for {ticker} on {target_date}")
        return None

    # 前場のデータ（9:00-11:30）
    morning_data = day_data.between_time(MORNING_START, MORNING_END)

    # 後場を含む全日のデータ（9:00-15:30）
    full_day_data = day_data.between_time(MORNING_START, AFTERNOON_END)

    if morning_data.empty or full_day_data.empty:
        logger.warning(f"  Incomplete trading hours data for {ticker} on {target_date}")
        return None

    # 前場終了時の価格（11:30に最も近い価格）
    morning_close_price = morning_data['Close'].iloc[-1]

    # 大引け価格（15:30に最も近い価格）
    day_close_price = full_day_data['Close'].iloc[-1]

    # 日中の最高値・最安値（9:00-15:30）
    day_high = full_day_data['High'].max()
    day_low = full_day_data['Low'].min()

    # 前場の最高値・最安値
    morning_high = morning_data['High'].max()
    morning_low = morning_data['Low'].min()

    # 売買方向を判断
    # - buy: 買い（上がったら利益）
    # - sell: 空売り（下がったら利益、リターンを反転）
    # - hold: スコアがプラスなら買い、マイナスなら売り
    # - None: 買いとして扱う（デフォルト）
    direction = 1  # 1: 買い、-1: 売り

    if recommendation_action == 'sell':
        direction = -1
    elif recommendation_action == 'hold':
        # スコアで判断（プラスなら買い、マイナスなら売り）
        if recommendation_score is not None and recommendation_score < 0:
            direction = -1
    # recommendation_action == 'buy' or None の場合は direction = 1 のまま

    # 利確分析: 前場不成 vs 大引不成
    # direction = -1 の場合、リターンを反転（下がったら利益）
    profit_morning = (morning_close_price - buy_price) * direction
    profit_day_close = (day_close_price - buy_price) * direction

    profit_morning_pct = (profit_morning / buy_price) * 100 if buy_price > 0 else 0
    profit_day_close_pct = (profit_day_close / buy_price) * 100 if buy_price > 0 else 0

    # どちらが有利か判定
    better_profit_timing = "morning_close" if profit_morning > profit_day_close else "day_close"

    # 損切り分析: 前場損切り vs 大引精算
    # 損切りは「損失がより小さい方が良い」
    loss_morning = morning_close_price - buy_price
    loss_day_close = day_close_price - buy_price

    loss_morning_pct = (loss_morning / buy_price) * 100 if buy_price > 0 else 0
    loss_day_close_pct = (loss_day_close / buy_price) * 100 if buy_price > 0 else 0

    # 損失がより小さい方が有利（損切りの場合）
    better_loss_timing = "morning_close" if loss_morning > loss_day_close else "day_close"

    return {
        'ticker': ticker,
        'backtest_date': backtest_date,
        'buy_price': buy_price,
        'recommendation_action': recommendation_action,
        'recommendation_score': recommendation_score,
        'direction': 'buy' if direction == 1 else 'sell',

        # 前場終了時
        'morning_close_price': morning_close_price,
        'profit_morning': profit_morning,
        'profit_morning_pct': profit_morning_pct,

        # 大引け
        'day_close_price': day_close_price,
        'profit_day_close': profit_day_close,
        'profit_day_close_pct': profit_day_close_pct,

        # 日中の価格レンジ
        'day_high': day_high,
        'day_low': day_low,
        'morning_high': morning_high,
        'morning_low': morning_low,

        # 最高値・最安値からのリターン
        'max_gain': day_high - buy_price,
        'max_gain_pct': ((day_high - buy_price) / buy_price) * 100 if buy_price > 0 else 0,
        'max_loss': day_low - buy_price,
        'max_loss_pct': ((day_low - buy_price) / buy_price) * 100 if buy_price > 0 else 0,

        # 有利なタイミング
        'better_profit_timing': better_profit_timing,
        'better_loss_timing': better_loss_timing,

        # 実際の結果（勝ち・負け）
        'is_win_morning': profit_morning > 0,
        'is_win_day_close': profit_day_close > 0,
    }


def main():
    logger.info("=" * 60)
    logger.info("売買タイミング最適化分析")
    logger.info("=" * 60)

    # Step 1: 既存のバックテストデータを読み込み
    logger.info("\n[Step 1] Loading backtest data...")
    grok_df = pd.read_parquet(GROK_ANALYSIS_PATH)
    grok_df['backtest_date'] = pd.to_datetime(grok_df['backtest_date'])

    logger.info(f"  Loaded {len(grok_df)} records")
    logger.info(f"  Date range: {grok_df['backtest_date'].min().date()} ~ {grok_df['backtest_date'].max().date()}")
    logger.info(f"  Unique tickers: {grok_df['ticker'].nunique()}")

    # Step 2: 各銘柄・日付について5分足データを取得して分析
    logger.info("\n[Step 2] Analyzing timing for each stock...")

    results = []
    total = len(grok_df)

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        backtest_date = row['backtest_date']
        buy_price = row['buy_price']
        recommendation_action = row.get('recommendation_action')
        recommendation_score = row.get('recommendation_score')

        logger.info(f"  [{idx+1}/{total}] {ticker} ({backtest_date.date()}) - {recommendation_action or 'N/A'}")

        # 5分足データを取得
        intraday_data = get_5min_data(ticker, backtest_date, backtest_date)

        if intraday_data.empty:
            logger.warning(f"    No intraday data available, skipping")
            continue

        # 売買タイミング分析
        result = analyze_timing_for_stock(
            ticker, backtest_date, buy_price, intraday_data,
            recommendation_action, recommendation_score
        )

        if result:
            results.append(result)
            logger.info(f"    前場不成: {result['profit_morning_pct']:.2f}% / 大引不成: {result['profit_day_close_pct']:.2f}%")
            logger.info(f"    有利なタイミング（利確）: {result['better_profit_timing']}")

        # レート制限対策（yfinanceのAPI制限を考慮）
        time.sleep(0.5)

    # Step 3: 結果を保存
    logger.info("\n[Step 3] Saving results...")

    if not results:
        logger.warning("  No results to save")
        return

    results_df = pd.DataFrame(results)
    results_df.to_parquet(TIMING_ANALYSIS_OUTPUT, index=False)

    logger.info(f"  Saved {len(results_df)} records to {TIMING_ANALYSIS_OUTPUT}")

    # Step 4: サマリー統計
    logger.info("\n" + "=" * 60)
    logger.info("サマリー統計")
    logger.info("=" * 60)

    # 利確タイミングの比較
    morning_better_count = (results_df['better_profit_timing'] == 'morning_close').sum()
    day_close_better_count = (results_df['better_profit_timing'] == 'day_close').sum()

    logger.info(f"\n利確タイミング（どちらが有利か）:")
    logger.info(f"  前場不成（11:30）: {morning_better_count} 件 ({morning_better_count/len(results_df)*100:.1f}%)")
    logger.info(f"  大引不成（15:30）: {day_close_better_count} 件 ({day_close_better_count/len(results_df)*100:.1f}%)")

    # 平均利益率
    logger.info(f"\n平均利益率:")
    logger.info(f"  前場不成: {results_df['profit_morning_pct'].mean():.2f}%")
    logger.info(f"  大引不成: {results_df['profit_day_close_pct'].mean():.2f}%")

    # 勝率
    morning_win_rate = results_df['is_win_morning'].sum() / len(results_df) * 100
    day_close_win_rate = results_df['is_win_day_close'].sum() / len(results_df) * 100

    logger.info(f"\n勝率:")
    logger.info(f"  前場不成: {morning_win_rate:.1f}%")
    logger.info(f"  大引不成: {day_close_win_rate:.1f}%")

    # 損切りタイミングの比較
    morning_better_loss_count = (results_df['better_loss_timing'] == 'morning_close').sum()
    day_close_better_loss_count = (results_df['better_loss_timing'] == 'day_close').sum()

    logger.info(f"\n損切りタイミング（損失がより小さい方）:")
    logger.info(f"  前場損切り（11:30）: {morning_better_loss_count} 件 ({morning_better_loss_count/len(results_df)*100:.1f}%)")
    logger.info(f"  大引精算（15:30）: {day_close_better_loss_count} 件 ({day_close_better_loss_count/len(results_df)*100:.1f}%)")

    logger.info("\n✅ 分析完了")


if __name__ == '__main__':
    main()
