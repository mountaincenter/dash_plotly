#!/usr/bin/env python3
"""
update_grok_analysis_merged_v2_1.py
grok_analysis_merged_v2_1.parquetを更新（全61カラム充足）

実行方法:
    python3 scripts/pipeline/update_grok_analysis_merged_v2_1.py

機能:
    - grok_trending.parquetから当日選定銘柄を読み込み
    - trading_recommendation.jsonからv2.0.3/v2.1データを取得
    - yfinanceから価格データを取得
    - バックテストを実行
    - 全61カラムを充足してgrok_analysis_merged_v2_1.parquetに追加
    - S3にアップロード
"""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple, Any
import json

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import yfinance as yf
from common_cfg.paths import PARQUET_DIR
from common_cfg.s3io import upload_files
from common_cfg.s3cfg import load_s3_config
from scripts.lib.jquants_fetcher import JQuantsFetcher

# パス定義
BACKTEST_DIR = PARQUET_DIR / "backtest"
GROK_TRENDING_PATH = BACKTEST_DIR / "grok_trending.parquet"
TRADING_REC_PATH = BACKTEST_DIR / "trading_recommendation.json"
MERGED_V2_1_PATH = BACKTEST_DIR / "grok_analysis_merged_v2_1.parquet"


def fetch_market_cap(ticker: str, close_price: float) -> Optional[float]:
    """yfinanceから時価総額を取得"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        market_cap = info.get('marketCap')
        if market_cap:
            return float(market_cap)

        # marketCapがない場合は株数×株価で計算
        shares = info.get('sharesOutstanding')
        if shares and close_price:
            return float(shares) * close_price

        return None
    except Exception as e:
        print(f"[WARN] {ticker}: 時価総額取得失敗: {e}")
        return None


def calculate_morning_metrics(df_5min, open_price):
    """前場（9:00-11:30）のメトリクス計算"""
    if df_5min is None or df_5min.empty or open_price is None or open_price == 0:
        return None, None, None, None

    try:
        morning_data = df_5min.between_time("09:00", "11:30")
        if morning_data.empty:
            return None, None, None, None

        # NaNを除外
        valid_high = morning_data['High'].dropna()
        valid_low = morning_data['Low'].dropna()

        if len(valid_high) == 0 or len(valid_low) == 0:
            return None, None, None, None

        morning_high = valid_high.max()
        morning_low = valid_low.min()
        morning_max_gain_pct = ((morning_high - open_price) / open_price * 100)
        morning_max_drawdown_pct = ((morning_low - open_price) / open_price * 100)

        return morning_high, morning_low, morning_max_gain_pct, morning_max_drawdown_pct
    except Exception as e:
        print(f"[WARN] Failed to calculate morning metrics: {e}")
        return None, None, None, None


def calculate_daily_metrics(df_5min, open_price):
    """全日（9:00-15:30）のメトリクス計算"""
    if df_5min is None or df_5min.empty or open_price is None or open_price == 0:
        return None, None, None, None

    try:
        # NaNを除外
        valid_high = df_5min['High'].dropna()
        valid_low = df_5min['Low'].dropna()

        if len(valid_high) == 0 or len(valid_low) == 0:
            return None, None, None, None

        high = valid_high.max()
        low = valid_low.min()
        daily_max_gain_pct = ((high - open_price) / open_price * 100)
        daily_max_drawdown_pct = ((low - open_price) / open_price * 100)

        return high, low, daily_max_gain_pct, daily_max_drawdown_pct
    except Exception as e:
        print(f"[WARN] Failed to calculate daily metrics: {e}")
        return None, None, None, None


def calculate_phase3_return(df_5min, open_price, profit_threshold, loss_threshold):
    """Phase3（利確損切戦略）のリターン計算"""
    if df_5min is None or df_5min.empty or open_price is None or open_price == 0:
        return None, None, None

    try:
        df_sorted = df_5min.sort_index()

        for idx, row in df_sorted.iterrows():
            high_price = row['High']
            low_price = row['Low']

            # NaNチェック
            if pd.isna(high_price) or pd.isna(low_price):
                continue

            # 利確判定（高値で判定）
            if (high_price - open_price) / open_price >= profit_threshold:
                phase_return = profit_threshold
                win = True
                exit_reason = f"profit_take_{profit_threshold*100}%"
                return phase_return, win, exit_reason

            # 損切判定（安値で判定）
            if (low_price - open_price) / open_price <= loss_threshold:
                phase_return = loss_threshold
                win = False
                exit_reason = f"stop_loss_{loss_threshold*100}%"
                return phase_return, win, exit_reason

        # 閾値に到達せず大引けまで保持
        valid_close = df_sorted['Close'].dropna()
        if len(valid_close) == 0:
            return None, None, None

        close_price = valid_close.iloc[-1]
        phase_return = (close_price - open_price) / open_price
        win = phase_return > 0
        exit_reason = "hold_until_close"
        return phase_return, win, exit_reason

    except Exception as e:
        print(f"[WARN] Failed to calculate Phase3 return: {e}")
        return None, None, None


def fetch_intraday_data(ticker: str, target_date: datetime) -> Optional[pd.DataFrame]:
    """5分足データを取得"""
    try:
        stock = yf.Ticker(ticker)
        start_date = target_date.strftime("%Y-%m-%d")
        end_date = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

        df = stock.history(start=start_date, end=end_date, interval="5m")

        if df.empty:
            return None

        # タイムゾーンを除去してDatetimeIndexに変換
        df.index = pd.to_datetime(df.index).tz_localize(None)

        return df

    except Exception as e:
        print(f"[WARN] {ticker}: 5分足データ取得失敗: {e}")
        return None


def run_backtest(ticker: str, grok_data: dict, trading_rec: dict,
                 target_date: datetime, prices_1d_row: pd.Series) -> dict:
    """バックテスト実行（全61カラム生成）"""

    # 基本価格データ
    open_price = float(prices_1d_row['Open'])
    close_price = float(prices_1d_row['Close'])
    high = float(prices_1d_row['High'])
    low = float(prices_1d_row['Low'])
    volume = int(prices_1d_row['Volume'])

    # 5分足データ取得
    df_5min = fetch_intraday_data(ticker, target_date)

    # Phase1: 前場引け売り（11:30売却）
    sell_price_phase1 = close_price  # デフォルトは終値
    if df_5min is not None and not df_5min.empty:
        try:
            morning_data = df_5min.between_time("09:00", "11:30")
            if not morning_data.empty:
                valid_close = morning_data['Close'].dropna()
                if len(valid_close) > 0:
                    sell_price_phase1 = float(valid_close.iloc[-1])
        except Exception as e:
            print(f"[WARN] {ticker}: Phase1計算失敗: {e}")

    # Phase1結果
    phase1_return = (sell_price_phase1 - open_price) / open_price
    phase1_win = 1.0 if phase1_return > 0 else 0.0
    phase1_return_pct = phase1_return * 100

    # Phase2: 大引け売り
    phase2_return = (close_price - open_price) / open_price
    phase2_win = 1.0 if phase2_return > 0 else 0.0
    profit_per_100_shares_phase2 = (close_price - open_price) * 100
    phase2_return_pct = phase2_return * 100

    # Phase3: 利確損切戦略
    phase3_1_return, phase3_1_win, phase3_1_exit = calculate_phase3_return(
        df_5min, open_price, 0.01, -0.03
    )
    phase3_2_return, phase3_2_win, phase3_2_exit = calculate_phase3_return(
        df_5min, open_price, 0.02, -0.03
    )
    phase3_3_return, phase3_3_win, phase3_3_exit = calculate_phase3_return(
        df_5min, open_price, 0.03, -0.03
    )

    # 日次データから計算（5分足がない場合のフォールバック）
    if df_5min is None or df_5min.empty:
        # 日次データのみの場合
        daily_max_gain_pct = ((high - open_price) / open_price * 100) if open_price > 0 else 0.0
        daily_max_drawdown_pct = ((low - open_price) / open_price * 100) if open_price > 0 else 0.0
        morning_volume = None

        # Phase3も日次データで代用
        if phase3_1_return is None:
            phase3_1_return = phase2_return
            phase3_1_win = float(phase2_win)
            phase3_1_exit = "hold_until_close"
        if phase3_2_return is None:
            phase3_2_return = phase2_return
            phase3_2_win = float(phase2_win)
            phase3_2_exit = "hold_until_close"
        if phase3_3_return is None:
            phase3_3_return = phase2_return
            phase3_3_win = float(phase2_win)
            phase3_3_exit = "hold_until_close"
    else:
        # 5分足から計算
        _, _, daily_max_gain_pct, daily_max_drawdown_pct = calculate_daily_metrics(
            df_5min, open_price
        )

        # 前場出来高
        try:
            morning_data = df_5min.between_time("09:00", "11:30")
            morning_volume = morning_data['Volume'].sum() if not morning_data.empty else None
        except:
            morning_volume = None

    # 時価総額取得
    market_cap = fetch_market_cap(ticker, close_price)

    # 前日・前々日データ（JQuantsから営業日取得）
    fetcher = JQuantsFetcher()
    try:
        response = fetcher.client.request("/markets/trading_calendar", params={
            "from": (target_date - timedelta(days=7)).strftime("%Y-%m-%d"),
            "to": target_date.strftime("%Y-%m-%d")
        })
        calendar = pd.DataFrame(response["trading_calendar"])
        trading_days = calendar[calendar["HolidayDivision"] == "1"]["Date"].tolist()

        target_date_str = target_date.strftime("%Y-%m-%d")
        if target_date_str in trading_days:
            idx = trading_days.index(target_date_str)
            prev_trading_day = trading_days[idx - 1] if idx > 0 else None
            prev_2day = trading_days[idx - 2] if idx > 1 else None
        else:
            prev_trading_day = None
            prev_2day = None
    except Exception as e:
        print(f"[WARN] {ticker}: 営業日カレンダー取得失敗: {e}")
        prev_trading_day = None
        prev_2day = None

    # 前日・前々日価格データ取得
    prev_day_close = None
    prev_day_volume = None
    prev_2day_close = None
    prev_2day_volume = None
    prev_day_change_pct = None
    prev_day_volume_ratio = None

    if prev_trading_day:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=prev_trading_day, end=(target_date + timedelta(days=1)).strftime("%Y-%m-%d"))
            if not hist.empty:
                prev_data = hist[hist.index.date == pd.Timestamp(prev_trading_day).date()]
                if not prev_data.empty:
                    prev_day_close = float(prev_data.iloc[0]['Close'])
                    prev_day_volume = float(prev_data.iloc[0]['Volume'])

                    if prev_day_close > 0:
                        prev_day_change_pct = (close_price - prev_day_close) / prev_day_close * 100
                    if prev_day_volume > 0:
                        prev_day_volume_ratio = volume / prev_day_volume
        except Exception as e:
            print(f"[WARN] {ticker}: 前日データ取得失敗: {e}")

    if prev_2day:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=prev_2day, end=(target_date + timedelta(days=1)).strftime("%Y-%m-%d"))
            if not hist.empty:
                prev2_data = hist[hist.index.date == pd.Timestamp(prev_2day).date()]
                if not prev2_data.empty:
                    prev_2day_close = float(prev2_data.iloc[0]['Close'])
                    prev_2day_volume = float(prev2_data.iloc[0]['Volume'])
        except Exception as e:
            print(f"[WARN] {ticker}: 前々日データ取得失敗: {e}")

    # 全61カラムのレコード作成
    record = {
        # 1-8: メタデータ
        'selection_date': target_date.strftime('%Y-%m-%d'),
        'backtest_date': target_date.strftime('%Y-%m-%d'),
        'ticker': ticker,
        'company_name': grok_data.get('company_name', ''),
        'category': grok_data.get('category', ''),
        'reason': grok_data.get('reason', ''),
        'grok_rank': grok_data.get('grok_rank'),
        'selection_score': grok_data.get('selection_score'),

        # 9-14: 当日価格データ
        'buy_price': open_price,
        'sell_price': sell_price_phase1,
        'daily_close': close_price,
        'high': high,
        'low': low,
        'volume': volume,

        # 15-25: バックテスト結果
        'phase1_win': phase1_win,
        'phase2_win': phase2_win,
        'profit_per_100_shares_phase2': profit_per_100_shares_phase2,
        'phase3_1pct_win': float(phase3_1_win) if phase3_1_win is not None else np.nan,
        'phase3_1pct_exit_reason': phase3_1_exit if phase3_1_exit else np.nan,
        'phase3_2pct_win': float(phase3_2_win) if phase3_2_win is not None else np.nan,
        'phase3_2pct_exit_reason': phase3_2_exit if phase3_2_exit else np.nan,
        'phase3_3pct_win': float(phase3_3_win) if phase3_3_win is not None else np.nan,
        'phase3_3pct_exit_reason': phase3_3_exit if phase3_3_exit else np.nan,
        'daily_max_gain_pct': daily_max_gain_pct if daily_max_gain_pct is not None else np.nan,
        'daily_max_drawdown_pct': daily_max_drawdown_pct if daily_max_drawdown_pct is not None else np.nan,

        # 26-31: データソースと当日詳細
        'data_source': 'yfinance',
        'prompt_version': grok_data.get('prompt_version', np.nan),
        'market_cap': market_cap if market_cap else np.nan,
        'morning_volume': morning_volume if morning_volume else np.nan,
        'day_high': high,
        'day_low': low,

        # 32-39: 前日データ
        'prev_day_close': prev_day_close if prev_day_close else np.nan,
        'prev_day_volume': prev_day_volume if prev_day_volume else np.nan,
        'prev_2day_close': prev_2day_close if prev_2day_close else np.nan,
        'prev_2day_volume': prev_2day_volume if prev_2day_volume else np.nan,
        'morning_volatility': np.nan,  # 5分足から計算が必要な場合のみ
        'daily_volatility': np.nan,    # 5分足から計算が必要な場合のみ
        'prev_day_change_pct': prev_day_change_pct if prev_day_change_pct is not None else np.nan,
        'prev_day_volume_ratio': prev_day_volume_ratio if prev_day_volume_ratio is not None else np.nan,

        # 40-45: バックテスト結果（return_pct）
        'phase1_return_pct': phase1_return_pct,
        'phase2_return_pct': phase2_return_pct,
        'phase3_1pct_return_pct': phase3_1_return * 100 if phase3_1_return is not None else np.nan,
        'phase3_2pct_return_pct': phase3_2_return * 100 if phase3_2_return is not None else np.nan,
        'phase3_3pct_return_pct': phase3_3_return * 100 if phase3_3_return is not None else np.nan,
        'prev_date': prev_trading_day if prev_trading_day else np.nan,

        # 46-49: v2.0.3データ
        'v2_score': trading_rec.get('v2_0_3_score', np.nan),
        'v2_action': trading_rec.get('v2_0_3_action', np.nan),
        'v2_confidence': np.nan,  # trading_recommendation.jsonにはない
        'v2_reasons_json': trading_rec.get('v2_0_3_reasons', np.nan),

        # 50-53: v2.0.3追加データ
        'v2_0_3_action': trading_rec.get('v2_0_3_action', np.nan),
        'volume_change_20d': trading_rec.get('volume_change_20d', np.nan),
        'stop_loss_pct': trading_rec.get('stop_loss_pct', np.nan),
        'price_vs_sma5_pct': trading_rec.get('price_vs_sma5_pct', np.nan),

        # 54-56: v2.1データ
        'v2_1_action': trading_rec.get('v2_1_action', np.nan),
        'v2_1_reasons': ', '.join(trading_rec.get('v2_1_reasons', [])) if trading_rec.get('v2_1_reasons') else np.nan,
        'v2_1_score': trading_rec.get('v2_1_score', np.nan),

        # 57-61: 追加指標
        'atr_pct': trading_rec.get('atr_pct', np.nan),
        'v2_0_3_score': trading_rec.get('v2_0_3_score', np.nan),
        'settlement_timing': trading_rec.get('settlement_timing', np.nan),
        'v2_0_3_reasons': trading_rec.get('v2_0_3_reasons', np.nan),
        'rsi_14d': trading_rec.get('rsi_14d', np.nan),
    }

    return record


def main():
    """メイン処理"""
    print("=== grok_analysis_merged_v2_1.parquet 更新 ===")
    print()

    # 1. grok_trending.parquet読み込み
    if not GROK_TRENDING_PATH.exists():
        print(f"[ERROR] grok_trending.parquet not found: {GROK_TRENDING_PATH}")
        return 1

    grok_df = pd.read_parquet(GROK_TRENDING_PATH)
    print(f"grok_trending.parquet: {len(grok_df)}銘柄")

    # selection_date取得
    if 'date' in grok_df.columns:
        selection_date = pd.to_datetime(grok_df['date'].iloc[0])
    else:
        print("[ERROR] 'date' column not found in grok_trending.parquet")
        return 1

    print(f"selection_date: {selection_date.strftime('%Y-%m-%d')}")
    print()

    # 2. trading_recommendation.json読み込み
    if not TRADING_REC_PATH.exists():
        print(f"[ERROR] trading_recommendation.json not found: {TRADING_REC_PATH}")
        return 1

    with open(TRADING_REC_PATH) as f:
        trading_rec_data = json.load(f)

    # ticker → trading_recのマップ作成
    trading_rec_map = {stock['ticker']: stock for stock in trading_rec_data['stocks']}
    print(f"trading_recommendation.json: {len(trading_rec_map)}銘柄")
    print()

    # 3. 既存データ読み込み
    if MERGED_V2_1_PATH.exists():
        existing_df = pd.read_parquet(MERGED_V2_1_PATH)
        print(f"既存データ: {len(existing_df)}レコード")

        # 重複チェック
        existing_dates = existing_df['selection_date'].unique()
        if selection_date.strftime('%Y-%m-%d') in existing_dates:
            print(f"[WARN] {selection_date.strftime('%Y-%m-%d')}のデータは既に存在します。スキップします。")
            return 0
    else:
        existing_df = None
        print("新規作成")
    print()

    # 4. 価格データ取得とバックテスト実行
    print("=== バックテスト実行 ===")
    new_records = []

    for _, row in grok_df.iterrows():
        ticker = row['ticker']
        print(f"処理中: {ticker}")

        # grok_trendingデータ
        grok_data = {
            'company_name': row.get('company_name', ''),
            'category': row.get('category', ''),
            'reason': row.get('reason', ''),
            'grok_rank': row.get('grok_rank'),
            'selection_score': row.get('selection_score'),
            'prompt_version': row.get('prompt_version'),
        }

        # trading_recommendationデータ
        trading_rec = trading_rec_map.get(ticker, {})

        # 価格データ取得
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=selection_date.strftime("%Y-%m-%d"),
                                end=(selection_date + timedelta(days=1)).strftime("%Y-%m-%d"))

            if hist.empty:
                print(f"  ⚠️ {ticker}: 価格データなし")
                continue

            price_row = hist.iloc[0]

            # バックテスト実行
            record = run_backtest(ticker, grok_data, trading_rec, selection_date, price_row)
            new_records.append(record)
            print(f"  ✓ Phase1勝率: {record['phase1_win']}, Phase2勝率: {record['phase2_win']}")

        except Exception as e:
            print(f"  ⚠️ {ticker}: エラー: {e}")
            import traceback
            traceback.print_exc()
            continue

    print()
    print(f"バックテスト完了: {len(new_records)}銘柄")
    print()

    # 5. DataFrameに変換
    new_df = pd.DataFrame(new_records)

    # データ型修正
    phase3_win_cols = ['phase3_1pct_win', 'phase3_2pct_win', 'phase3_3pct_win']
    for col in phase3_win_cols:
        new_df[col] = pd.to_numeric(new_df[col], errors='coerce')

    # 6. 既存データと結合
    if existing_df is not None:
        # カラム順序を既存と同じにする
        new_df = new_df[existing_df.columns]
        merged_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        merged_df = new_df

    print(f"結合後: {len(merged_df)}レコード")
    print()

    # 7. 保存
    # 日付カラムを文字列に統一（pyarrow型エラー回避）
    for col in ['selection_date', 'backtest_date']:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].astype(str)

    # リスト型を含むカラムをJSON文字列に変換（pyarrow型エラー回避）
    import json
    for col in ['v2_1_reasons', 'v2_0_3_reasons', 'v2_reasons_json']:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else (x if pd.isna(x) else str(x))
            )

    MERGED_V2_1_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_parquet(MERGED_V2_1_PATH, index=False)
    print(f"保存完了: {MERGED_V2_1_PATH}")
    print()

    # 8. S3アップロード
    try:
        cfg = load_s3_config()
        upload_files(cfg, [MERGED_V2_1_PATH], base_dir=PARQUET_DIR)
        print("[OK] S3アップロード完了")
    except Exception as e:
        print(f"[WARN] S3アップロード失敗: {e}")

    print()
    print("=== 完了 ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
