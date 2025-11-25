# server/routers/dev_grok_analysis.py
"""
Grok推奨銘柄の詳細分析データAPI
/api/dev/grok-analysis - JSON APIエンドポイント
"""
from fastapi import APIRouter, HTTPException
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Any
import sys
import os

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

router = APIRouter()

# データファイルのパス（新パイプライン: S3同期対象）
DATA_PATH = ROOT / 'data' / 'parquet' / 'backtest' / 'grok_analysis_merged.parquet'
DATA_PATH_V2_1 = ROOT / 'data' / 'parquet' / 'backtest' / 'grok_analysis_merged_v2_1.parquet'

# S3設定（環境変数から取得）
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def load_analysis_data() -> pd.DataFrame:
    """
    分析データを読み込み
    - S3から読み込み（本番環境、常に最新）
    - S3が失敗したらローカルファイルを使用（開発環境）
    - バックテストデータのみ（recommendation_action がないデータ）を返す

    注意:
    2025-11-13にgenerate_trading_recommendation_v2.pyを無許可実行した結果、
    grok_analysis_merged.parquetに不正なrecommendation_actionデータが混入。
    このAPIはバックテスト分析専用のため、recommendation_actionがないデータのみ返す。
    詳細: data/parquet/backtest/README_DATA_CORRUPTION_20251113.md
    """
    # S3から読み込み
    try:
        s3_key = f"{S3_PREFIX}backtest/grok_analysis_merged.parquet"
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"

        print(f"[INFO] Loading analysis data from S3: {s3_url}")

        # S3から直接読み込み（pandas.read_parquet はs3://をサポート）
        df = pd.read_parquet(s3_url, storage_options={
            "client_kwargs": {"region_name": AWS_REGION}
        })

        if 'backtest_date' in df.columns:
            df['backtest_date'] = pd.to_datetime(df['backtest_date'])
        if 'selection_date' in df.columns:
            df['selection_date'] = pd.to_datetime(df['selection_date'])

        print(f"[INFO] Successfully loaded {len(df)} records from S3")
        return df

    except Exception as e:
        print(f"[WARNING] Could not load analysis data from S3: {type(e).__name__}: {e}")

    # ローカルファイルにフォールバック
    if DATA_PATH.exists():
        print(f"[INFO] Loading analysis data from local file: {DATA_PATH}")
        df = pd.read_parquet(DATA_PATH)
        if 'backtest_date' in df.columns:
            df['backtest_date'] = pd.to_datetime(df['backtest_date'])
        if 'selection_date' in df.columns:
            df['selection_date'] = pd.to_datetime(df['selection_date'])
        return df

    # どちらも失敗
    print(f"[ERROR] Analysis data not found in S3 or local file")
    raise


def load_analysis_data_v2_1() -> pd.DataFrame:
    """
    v2.1分析データを読み込み（grok_analysis_merged_v2_1.parquet）
    - S3から読み込み（本番環境、常に最新）
    - S3が失敗したらローカルファイルを使用（開発環境）
    """
    # S3から読み込み
    try:
        s3_key = f"{S3_PREFIX}backtest/grok_analysis_merged_v2_1.parquet"
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"

        print(f"[INFO] Loading v2.1 analysis data from S3: {s3_url}")

        # S3から直接読み込み（pandas.read_parquet はs3://をサポート）
        df = pd.read_parquet(s3_url, storage_options={
            "client_kwargs": {"region_name": AWS_REGION}
        })

        if 'backtest_date' in df.columns:
            df['backtest_date'] = pd.to_datetime(df['backtest_date'])
        if 'selection_date' in df.columns:
            df['selection_date'] = pd.to_datetime(df['selection_date'])

        print(f"[INFO] Successfully loaded {len(df)} records from S3")
        return df

    except Exception as e:
        print(f"[WARNING] Could not load v2.1 analysis data from S3: {type(e).__name__}: {e}")

    # ローカルファイルにフォールバック
    if DATA_PATH_V2_1.exists():
        print(f"[INFO] Loading v2.1 analysis data from local file: {DATA_PATH_V2_1}")
        df = pd.read_parquet(DATA_PATH_V2_1)
        if 'backtest_date' in df.columns:
            df['backtest_date'] = pd.to_datetime(df['backtest_date'])
        if 'selection_date' in df.columns:
            df['selection_date'] = pd.to_datetime(df['selection_date'])
        return df

    # どちらも失敗
    print(f"[ERROR] v2.1 analysis data not found in S3 or local file")
    raise


def safe_float(value: Any) -> float | None:
    """NaN/Infを安全にNoneに変換"""
    if pd.isna(value) or np.isinf(value):
        return None
    return float(value)


def safe_list(value: Any) -> list | None:
    """numpy.ndarray/文字列をPythonリストに変換"""
    # numpy配列を先にチェック（pd.isna()の前に）
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, list):
        return value
    # スカラー値のNaNチェック
    if pd.isna(value):
        return None
    if isinstance(value, str):
        # " / " で区切られた文字列を分割
        if " / " in value:
            return [s.strip() for s in value.split(" / ")]
        return [value]
    return [str(value)]


def calculate_phase_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Phase別統計を計算"""
    phases = {
        'Phase1 (前場終了時)': ('phase1_return_pct', 'phase1_win'),
        'Phase2 (当日終値)': ('phase2_return_pct', 'phase2_win'),
        'Phase3-1% (損切-1%)': ('phase3_1pct_return_pct', 'phase3_1pct_win'),
        'Phase3-2% (損切-2%)': ('phase3_2pct_return_pct', 'phase3_2pct_win'),
        'Phase3-3% (損切-3%)': ('phase3_3pct_return_pct', 'phase3_3pct_win'),
    }

    results = []
    for phase_name, (return_col, win_col) in phases.items():
        win_count = int(df[win_col].sum())
        total = len(df)
        win_rate = safe_float(win_count / total * 100)
        avg_return = safe_float(df[return_col].mean())
        median_return = safe_float(df[return_col].median())

        results.append({
            'phase': phase_name,
            'winRate': win_rate,
            'avgReturn': avg_return,
            'medianReturn': median_return,
            'winCount': win_count,
            'loseCount': total - win_count,
        })

    return results


def calculate_category_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """カテゴリー別統計を計算"""
    cat_stats = df.groupby('category').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': ['mean', 'median', 'std'],
        'daily_max_gain_pct': 'mean',
        'daily_max_drawdown_pct': 'mean'
    }).round(4)

    cat_stats.columns = ['winCount', 'total', 'winRate', 'avgReturn', 'medianReturn',
                         'stdDev', 'avgMaxGain', 'avgMaxLoss']

    results = []
    for category, row in cat_stats.iterrows():
        results.append({
            'category': category,
            'total': int(row['total']),
            'winCount': int(row['winCount']),
            'winRate': safe_float(row['winRate']),
            'avgReturn': safe_float(row['avgReturn']),
            'medianReturn': safe_float(row['medianReturn']),
            'stdDev': safe_float(row['stdDev']),
            'avgMaxGain': safe_float(row['avgMaxGain']),
            'avgMaxLoss': safe_float(row['avgMaxLoss']),
        })

    return sorted(results, key=lambda x: x['winRate'] or 0, reverse=True)


def calculate_grok_rank_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Grokランク別統計を計算"""
    rank_stats = df.groupby('grok_rank').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': 'mean'
    }).round(4)

    rank_stats.columns = ['winCount', 'total', 'winRate', 'avgReturn']
    rank_stats = rank_stats[rank_stats['total'] >= 2]  # 2件以上

    results = []
    for rank, row in rank_stats.iterrows():
        results.append({
            'rank': int(rank),
            'total': int(row['total']),
            'winCount': int(row['winCount']),
            'winRate': safe_float(row['winRate']),
            'avgReturn': safe_float(row['avgReturn']),
        })

    return sorted(results, key=lambda x: x['rank'])


def calculate_risk_reward_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """リスクリワード統計を計算"""
    winners = df[df['phase2_win'] == True]
    losers = df[df['phase2_win'] == False]

    avg_win = safe_float(winners['phase2_return_pct'].mean()) if len(winners) > 0 else 0.0
    avg_loss = safe_float(losers['phase2_return_pct'].mean()) if len(losers) > 0 else 0.0
    risk_reward_ratio = safe_float(avg_win / abs(avg_loss)) if avg_loss and avg_loss != 0 else 0.0

    returns = df['phase2_return_pct']
    sharpe_ratio = safe_float(returns.mean() / returns.std()) if returns.std() > 0 else 0.0

    return {
        'winCount': int(len(winners)),
        'loseCount': int(len(losers)),
        'winRate': safe_float(len(winners) / len(df) * 100),
        'avgWinReturn': avg_win,
        'avgLossReturn': avg_loss,
        'maxGain': safe_float(df['daily_max_gain_pct'].max()),
        'maxLoss': safe_float(df['daily_max_drawdown_pct'].min()),
        'avgMaxGain': safe_float(df['daily_max_gain_pct'].mean()),
        'avgMaxLoss': safe_float(df['daily_max_drawdown_pct'].mean()),
        'riskRewardRatio': risk_reward_ratio,
        'sharpeRatio': sharpe_ratio,
        'avgReturn': safe_float(returns.mean()),
        'stdDev': safe_float(returns.std()),
    }


def calculate_volatility_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """ボラティリティ統計を計算"""
    df_copy = df.copy()
    df_copy['daily_volatility'] = df_copy['daily_max_gain_pct'] - df_copy['daily_max_drawdown_pct']

    # ボラティリティで3分位に分割
    df_copy['volatility_group'] = pd.qcut(
        df_copy['daily_volatility'],
        q=3,
        labels=['低ボラ', '中ボラ', '高ボラ'],
        duplicates='drop'
    )

    vol_stats = df_copy.groupby('volatility_group').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return_pct': 'mean',
        'daily_volatility': 'mean',
        'daily_max_gain_pct': 'mean',
        'daily_max_drawdown_pct': 'mean'
    }).round(4)

    vol_stats.columns = ['winCount', 'total', 'winRate', 'avgReturn', 'avgVolatility',
                         'avgMaxGain', 'avgMaxLoss']

    results = []
    for group, row in vol_stats.iterrows():
        results.append({
            'group': group,
            'total': int(row['total']),
            'winCount': int(row['winCount']),
            'winRate': safe_float(row['winRate']),
            'avgReturn': safe_float(row['avgReturn']),
            'avgVolatility': safe_float(row['avgVolatility']),
            'avgMaxGain': safe_float(row['avgMaxGain']),
            'avgMaxLoss': safe_float(row['avgMaxLoss']),
        })

    # 相関係数（None/NaNを除外）
    df_valid = df_copy.dropna(subset=['daily_volatility', 'phase2_return_pct', 'phase2_win'])
    if len(df_valid) > 0:
        corr_vol_return = safe_float(df_valid['daily_volatility'].corr(df_valid['phase2_return_pct']))
        corr_vol_win = safe_float(df_valid['daily_volatility'].corr(df_valid['phase2_win'].astype(int)))
    else:
        corr_vol_return = None
        corr_vol_win = None

    return {
        'groups': results,
        'corrVolatilityReturn': corr_vol_return,
        'corrVolatilityWin': corr_vol_win,
    }


def calculate_prev_day_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """前日動向別統計を計算"""
    # prev_day_change_pctカラムが存在しない場合はNoneを返す
    if 'prev_day_change_pct' not in df.columns:
        return None

    df_valid = df[df['prev_day_change_pct'].notna()].copy()

    if len(df_valid) == 0:
        return None

    # 前日プラス/マイナスで分類
    df_valid['prev_direction'] = df_valid['prev_day_change_pct'].apply(
        lambda x: 'plus' if x >= 0 else 'minus'
    )

    summary = df_valid.groupby('prev_direction').agg({
        'ticker': 'count',
        'phase1_win': lambda x: x.sum() / len(x) * 100,
        'phase2_win': lambda x: x.sum() / len(x) * 100,
        'phase1_return_pct': 'mean',
        'phase2_return_pct': 'mean',
    }).round(4)

    summary.columns = ['count', 'phase1WinRate', 'phase2WinRate', 'phase1AvgReturn', 'phase2AvgReturn']

    results = []
    for direction, row in summary.iterrows():
        results.append({
            'direction': 'プラス' if direction == 'plus' else 'マイナス',
            'count': int(row['count']),
            'phase1WinRate': safe_float(row['phase1WinRate']),
            'phase2WinRate': safe_float(row['phase2WinRate']),
            'phase1AvgReturn': safe_float(row['phase1AvgReturn']),
            'phase2AvgReturn': safe_float(row['phase2AvgReturn']),
        })

    return results


def calculate_daily_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """日別統計を計算"""
    daily_stats = df.groupby('backtest_date').agg({
        'phase1_return_pct': 'mean',
        'phase2_return_pct': 'mean',
        'phase3_2pct_return_pct': 'mean',
        'ticker': 'count'
    }).round(4)

    daily_stats.columns = ['phase1AvgReturn', 'phase2AvgReturn', 'phase3AvgReturn', 'count']

    results = []
    for date, row in daily_stats.iterrows():
        results.append({
            'date': date.strftime('%Y-%m-%d'),
            'phase1AvgReturn': safe_float(row['phase1AvgReturn']),
            'phase2AvgReturn': safe_float(row['phase2AvgReturn']),
            'phase3AvgReturn': safe_float(row['phase3AvgReturn']),
            'count': int(row['count']),
        })

    return sorted(results, key=lambda x: x['date'])


def calculate_v2_action_stats(df: pd.DataFrame) -> Dict[str, Any] | None:
    """v2 Action別統計を計算（買い/売り/静観）"""
    # v2_actionカラムが存在しない場合はNoneを返す
    if 'v2_action' not in df.columns:
        return None

    # v2_actionがあるデータのみ抽出
    v2_df = df[df['v2_action'].notna()].copy()

    if len(v2_df) == 0:
        return None

    # 全体サマリー
    summary = {
        'total': len(v2_df),
        'buy': int((v2_df['v2_action'] == '買い').sum()),
        'sell': int((v2_df['v2_action'] == '売り').sum()),
        'hold': int((v2_df['v2_action'] == '静観').sum()),
    }

    # アクション別統計
    action_stats = []
    action_mapping = {'買い': 'buy', '売り': 'sell', '静観': 'hold'}

    for action_jp, action_en in action_mapping.items():
        action_df = v2_df[v2_df['v2_action'] == action_jp]
        if len(action_df) == 0:
            continue

        total = len(action_df)

        # 売り推奨の場合は勝敗を反転（下がったら勝ち）
        if action_jp == '売り':
            # phase2_win == False が勝ち（下がった = 勝ち）
            win_count_all = int((action_df['phase2_win'] == False).sum())
            # 引分除外
            non_draw = action_df[action_df['profit_per_100_shares_phase2'] != 0]
            win_count_excl_draw = int((non_draw['phase2_win'] == False).sum())
            total_excl_draw = len(non_draw)
            # profit_per_100_shares_phase2は負の値が利益（ショートポジション）
            total_profit = safe_float(-action_df['profit_per_100_shares_phase2'].sum())
            avg_profit = safe_float(-action_df['profit_per_100_shares_phase2'].mean())
            avg_return = safe_float(-action_df['phase2_return_pct'].mean())
        else:
            # 買い・静観は通常通り
            win_count_all = int(action_df['phase2_win'].sum())
            # 引分除外
            non_draw = action_df[action_df['profit_per_100_shares_phase2'] != 0]
            win_count_excl_draw = int(non_draw['phase2_win'].sum())
            total_excl_draw = len(non_draw)
            total_profit = safe_float(action_df['profit_per_100_shares_phase2'].sum())
            avg_profit = safe_float(action_df['profit_per_100_shares_phase2'].mean())
            avg_return = safe_float(action_df['phase2_return_pct'].mean())

        win_rate_all = safe_float(win_count_all / total * 100) if total > 0 else None
        win_rate_excl_draw = safe_float(win_count_excl_draw / total_excl_draw * 100) if total_excl_draw > 0 else None

        action_stats.append({
            'action': action_en,
            'actionJp': action_jp,
            'total': total,
            'winCount': win_count_all,
            'loseCount': total - win_count_all,
            'winRate': win_rate_all,
            'winRateExclDraw': win_rate_excl_draw,
            'totalProfit': total_profit,
            'avgProfit': avg_profit,
            'avgReturn': avg_return,
        })

    # 日別データ（最新のものも含む）
    date_stats = []
    for date in sorted(v2_df['backtest_date'].unique(), reverse=True):
        date_df = v2_df[v2_df['backtest_date'] == date]

        date_action_stats = []
        for action_jp, action_en in action_mapping.items():
            action_df = date_df[date_df['v2_action'] == action_jp]
            if len(action_df) == 0:
                continue

            stocks = []
            for _, row in action_df.iterrows():
                # 売りの場合は反転
                if action_jp == '売り':
                    is_win = not row['phase2_win'] if pd.notna(row['phase2_win']) else None
                    profit = -row['profit_per_100_shares_phase2'] if pd.notna(row['profit_per_100_shares_phase2']) else None
                    return_pct = -row['phase2_return_pct'] if pd.notna(row['phase2_return_pct']) else None
                else:
                    is_win = row['phase2_win'] if pd.notna(row['phase2_win']) else None
                    profit = row['profit_per_100_shares_phase2'] if pd.notna(row['profit_per_100_shares_phase2']) else None
                    return_pct = row['phase2_return_pct'] if pd.notna(row['phase2_return_pct']) else None

                stocks.append({
                    'ticker': row['ticker'],
                    'companyName': row['company_name'],
                    'grokRank': int(row['grok_rank']) if pd.notna(row['grok_rank']) else None,
                    'prevDayClose': safe_float(row.get('prev_day_close')),
                    'v2Score': safe_float(row['v2_score']),
                    'v2Action': action_jp,
                    'buyPrice': safe_float(row.get('buy_price')),
                    'sellPrice': safe_float(row.get('sell_price')),
                    'high': safe_float(row.get('high')),
                    'low': safe_float(row.get('low')),
                    'profitPer100': safe_float(profit),
                    'returnPct': safe_float(return_pct),
                    'isWin': bool(is_win) if is_win is not None else None,
                })

            date_action_stats.append({
                'action': action_en,
                'actionJp': action_jp,
                'total': len(action_df),
                'stocks': stocks,
            })

        date_stats.append({
            'date': date.strftime('%Y-%m-%d'),
            'total': len(date_df),
            'actions': date_action_stats,
        })

    # v2_scoreの統計
    v2_score_stats = {
        'mean': safe_float(v2_df['v2_score'].mean()),
        'median': safe_float(v2_df['v2_score'].median()),
        'min': safe_float(v2_df['v2_score'].min()),
        'max': safe_float(v2_df['v2_score'].max()),
    }

    return {
        'summary': summary,
        'actionStats': action_stats,
        'dateStats': date_stats,
        'v2ScoreStats': v2_score_stats,
    }


def calculate_recommendation_stats(df: pd.DataFrame) -> Dict[str, Any] | None:
    """売買判断別統計を計算"""
    # recommendation_actionカラムが存在しない場合はNoneを返す
    if 'recommendation_action' not in df.columns:
        return None

    # recommendationがあるデータのみ抽出
    rec_df = df[df['recommendation_action'].notna()].copy()

    if len(rec_df) == 0:
        return None

    # 全体サマリー
    summary = {
        'total': len(rec_df),
        'buy': int((rec_df['recommendation_action'] == 'buy').sum()),
        'sell': int((rec_df['recommendation_action'] == 'sell').sum()),
        'hold': int((rec_df['recommendation_action'] == 'hold').sum()),
    }

    # アクション別統計
    action_stats = []
    for action in ['buy', 'sell', 'hold']:
        action_df = rec_df[rec_df['recommendation_action'] == action]
        if len(action_df) == 0:
            continue

        total = len(action_df)

        # 売り推奨の場合は勝敗を反転（下がったら勝ち）
        if action == 'sell':
            win_count = int((action_df['phase2_win'] == False).sum())  # phase2_win == False が勝ち
            avg_return = safe_float(-action_df['phase2_return_pct'].mean())  # リターンも反転
            median_return = safe_float(-action_df['phase2_return_pct'].median())  # リターンも反転
        else:
            win_count = int(action_df['phase2_win'].sum())
            avg_return = safe_float(action_df['phase2_return_pct'].mean())
            median_return = safe_float(action_df['phase2_return_pct'].median())

        win_rate = safe_float(win_count / total * 100)

        action_stats.append({
            'action': action,
            'total': total,
            'winCount': win_count,
            'loseCount': total - win_count,
            'winRate': win_rate,
            'avgReturn': avg_return,
            'medianReturn': median_return,
        })

    # 最新日の統計
    latest_date = rec_df['backtest_date'].max()
    latest_df = rec_df[rec_df['backtest_date'] == latest_date]

    latest_stats = None
    if len(latest_df) > 0:
        latest_action_stats = []
        for action in ['buy', 'sell', 'hold']:
            action_df = latest_df[latest_df['recommendation_action'] == action]
            if len(action_df) == 0:
                continue

            total = len(action_df)

            # 売り推奨の場合は勝敗を反転（下がったら勝ち）
            if action == 'sell':
                win_count = int((action_df['phase2_win'] == False).sum())
                avg_return = safe_float(-action_df['phase2_return_pct'].mean())
            else:
                win_count = int(action_df['phase2_win'].sum())
                avg_return = safe_float(action_df['phase2_return_pct'].mean())

            win_rate = safe_float(win_count / total * 100)

            latest_action_stats.append({
                'action': action,
                'total': total,
                'winCount': win_count,
                'winRate': win_rate,
                'avgReturn': avg_return,
            })

        # 銘柄別の詳細データ
        stocks_detail = []
        for _, row in latest_df.iterrows():
            action = row['recommendation_action']
            is_win = row['phase2_win']
            return_pct = row['phase2_return_pct']
            profit_per_100 = row['profit_per_100_shares_phase2']

            # 売り推奨の場合は勝敗とリターンを反転
            if action == 'sell':
                is_win = False if is_win is True else (True if is_win is False else None)
                return_pct = -return_pct if return_pct is not None else None
                profit_per_100 = -profit_per_100 if profit_per_100 is not None else None

            stocks_detail.append({
                'ticker': row['ticker'],
                'companyName': row['company_name'],
                'action': action,
                'grokRank': int(row['grok_rank']),
                'isWin': bool(is_win),
                'returnPct': safe_float(return_pct),
                'profitPer100': safe_float(profit_per_100),
                'buyPrice': safe_float(row['buy_price']),
                'sellPrice': safe_float(row['sell_price']),
            })

        latest_stats = {
            'date': latest_date.strftime('%Y-%m-%d'),
            'total': len(latest_df),
            'actions': latest_action_stats,
            'stocks': stocks_detail,
        }

    return {
        'summary': summary,
        'actionStats': action_stats,
        'latestStats': latest_stats,
    }


def calculate_v2_0_3_vs_v2_1_comparison(df: pd.DataFrame) -> Dict[str, Any]:
    """v2.0.3とv2.1の比較統計を計算"""

    # 全体サマリー
    summary = {
        'total': len(df),
        'dateRange': {
            'start': df['backtest_date'].min().strftime('%Y-%m-%d'),
            'end': df['backtest_date'].max().strftime('%Y-%m-%d'),
        }
    }

    # v2.0.3アクション分布
    v2_0_3_actions = {
        'buy': int((df['v2_0_3_action'] == '買い').sum()),
        'sell': int((df['v2_0_3_action'] == '売り').sum()),
        'hold': int((df['v2_0_3_action'] == '静観').sum()),
    }

    # v2.1アクション分布
    v2_1_actions = {
        'buy': int((df['v2_1_action'] == '買い').sum()),
        'sell': int((df['v2_1_action'] == '売り').sum()),
        'hold': int((df['v2_1_action'] == '静観').sum()),
    }

    # アクション変更マトリックス（v2.0.3 → v2.1）
    action_mapping = {
        '買い': 'buy',
        '売り': 'sell',
        '静観': 'hold'
    }

    action_changes = {}
    for v2_0_3_action_jp, v2_0_3_action_en in action_mapping.items():
        for v2_1_action_jp, v2_1_action_en in action_mapping.items():
            key = f"{v2_0_3_action_en}_to_{v2_1_action_en}"
            count = int(((df['v2_0_3_action'] == v2_0_3_action_jp) &
                         (df['v2_1_action'] == v2_1_action_jp)).sum())
            action_changes[key] = count

    # v2.0.3アクション別統計（Phase2基準）
    v2_0_3_stats = []
    for action_jp, action_en in action_mapping.items():
        action_df = df[df['v2_0_3_action'] == action_jp]
        if len(action_df) == 0:
            continue

        total = len(action_df)

        # 売りの場合は勝敗反転
        if action_jp == '売り':
            win_count = int((action_df['phase2_win'] == False).sum())
            total_profit = safe_float(-action_df['profit_per_100_shares_phase2'].sum())
            avg_profit = safe_float(-action_df['profit_per_100_shares_phase2'].mean())
            avg_return = safe_float(-action_df['phase2_return_pct'].mean())
        else:
            win_count = int(action_df['phase2_win'].sum())
            total_profit = safe_float(action_df['profit_per_100_shares_phase2'].sum())
            avg_profit = safe_float(action_df['profit_per_100_shares_phase2'].mean())
            avg_return = safe_float(action_df['phase2_return_pct'].mean())

        win_rate = safe_float(win_count / total * 100) if total > 0 else None

        v2_0_3_stats.append({
            'action': action_en,
            'actionJp': action_jp,
            'total': total,
            'winCount': win_count,
            'loseCount': total - win_count,
            'winRate': win_rate,
            'totalProfit': total_profit,
            'avgProfit': avg_profit,
            'avgReturn': avg_return,
        })

    # v2.1アクション別統計（Phase2基準）
    v2_1_stats = []
    for action_jp, action_en in action_mapping.items():
        action_df = df[df['v2_1_action'] == action_jp]
        if len(action_df) == 0:
            continue

        total = len(action_df)

        # 売りの場合は勝敗反転
        if action_jp == '売り':
            win_count = int((action_df['phase2_win'] == False).sum())
            total_profit = safe_float(-action_df['profit_per_100_shares_phase2'].sum())
            avg_profit = safe_float(-action_df['profit_per_100_shares_phase2'].mean())
            avg_return = safe_float(-action_df['phase2_return_pct'].mean())
        else:
            win_count = int(action_df['phase2_win'].sum())
            total_profit = safe_float(action_df['profit_per_100_shares_phase2'].sum())
            avg_profit = safe_float(action_df['profit_per_100_shares_phase2'].mean())
            avg_return = safe_float(action_df['phase2_return_pct'].mean())

        win_rate = safe_float(win_count / total * 100) if total > 0 else None

        v2_1_stats.append({
            'action': action_en,
            'actionJp': action_jp,
            'total': total,
            'winCount': win_count,
            'loseCount': total - win_count,
            'winRate': win_rate,
            'totalProfit': total_profit,
            'avgProfit': avg_profit,
            'avgReturn': avg_return,
        })

    return {
        'summary': summary,
        'v2_0_3Actions': v2_0_3_actions,
        'v2_1Actions': v2_1_actions,
        'actionChanges': action_changes,
        'v2_0_3Stats': v2_0_3_stats,
        'v2_1Stats': v2_1_stats,
    }


def calculate_stop_loss_tier_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """損切り水準別統計を計算"""

    # stop_loss_pctでグループ化
    tier_stats = []

    for tier in sorted(df['stop_loss_pct'].unique()):
        tier_df = df[df['stop_loss_pct'] == tier]

        if len(tier_df) == 0:
            continue

        total = len(tier_df)

        # 買いアクションのみを対象（静観・売りは損切り対象外）
        buy_df = tier_df[tier_df['v2_1_action'] == '買い']
        buy_total = len(buy_df)

        if buy_total > 0:
            buy_win_count = int(buy_df['phase2_win'].sum())
            buy_win_rate = safe_float(buy_win_count / buy_total * 100)
            buy_total_profit = safe_float(buy_df['profit_per_100_shares_phase2'].sum())
            buy_avg_profit = safe_float(buy_df['profit_per_100_shares_phase2'].mean())
            buy_avg_return = safe_float(buy_df['phase2_return_pct'].mean())
        else:
            buy_win_count = 0
            buy_win_rate = None
            buy_total_profit = None
            buy_avg_profit = None
            buy_avg_return = None

        # 全アクションの統計
        all_win_count = int(tier_df['phase2_win'].sum())
        all_win_rate = safe_float(all_win_count / total * 100)

        tier_stats.append({
            'stopLossPct': safe_float(tier),
            'total': total,
            'buyTotal': buy_total,
            'buyWinCount': buy_win_count,
            'buyWinRate': buy_win_rate,
            'buyTotalProfit': buy_total_profit,
            'buyAvgProfit': buy_avg_profit,
            'buyAvgReturn': buy_avg_return,
            'allWinCount': all_win_count,
            'allWinRate': all_win_rate,
        })

    return tier_stats


def calculate_action_change_analysis(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """アクション変更分析（v2.0.3 → v2.1 の変化）"""

    action_mapping = {
        '買い': 'buy',
        '売り': 'sell',
        '静観': 'hold'
    }

    change_patterns = []

    for v2_0_3_action_jp, v2_0_3_action_en in action_mapping.items():
        for v2_1_action_jp, v2_1_action_en in action_mapping.items():
            # 同じアクションは除外（変化なし）
            if v2_0_3_action_jp == v2_1_action_jp:
                continue

            # 該当するレコードを抽出
            change_df = df[(df['v2_0_3_action'] == v2_0_3_action_jp) &
                          (df['v2_1_action'] == v2_1_action_jp)]

            if len(change_df) == 0:
                continue

            total = len(change_df)

            # Phase2の結果（v2.1アクションで判定）
            if v2_1_action_jp == '売り':
                win_count = int((change_df['phase2_win'] == False).sum())
                avg_return = safe_float(-change_df['phase2_return_pct'].mean())
            else:
                win_count = int(change_df['phase2_win'].sum())
                avg_return = safe_float(change_df['phase2_return_pct'].mean())

            win_rate = safe_float(win_count / total * 100) if total > 0 else None

            # 銘柄詳細
            stocks = []
            for _, row in change_df.head(10).iterrows():  # 上位10件
                stocks.append({
                    'ticker': row['ticker'],
                    'companyName': row['company_name'],
                    'grokRank': int(row['grok_rank']),
                    'date': row['backtest_date'].strftime('%Y-%m-%d'),
                    'v2_0_3Score': safe_float(row['v2_0_3_score']),
                    'v2_1Score': safe_float(row['v2_1_score']),
                    'v2_0_3Reasons': safe_list(row['v2_0_3_reasons']),
                    'v2_1Reasons': safe_list(row['v2_1_reasons']),
                })

            change_patterns.append({
                'from': v2_0_3_action_en,
                'fromJp': v2_0_3_action_jp,
                'to': v2_1_action_en,
                'toJp': v2_1_action_jp,
                'total': total,
                'winCount': win_count,
                'winRate': win_rate,
                'avgReturn': avg_return,
                'stocks': stocks,
            })

    # 変化件数の降順でソート
    return sorted(change_patterns, key=lambda x: x['total'], reverse=True)


@router.get("/api/dev/grok-analysis")
async def get_grok_analysis():
    """
    Grok推奨銘柄の詳細分析データを取得

    Returns:
        GrokAnalysisResponse: 分析データ

    Raises:
        HTTPException: ファイルが見つからない場合は404/500
    """
    try:
        # データ読み込み（ローカルまたはS3から）
        df = load_analysis_data()

        # 各種統計計算
        phase_stats = calculate_phase_stats(df)
        category_stats = calculate_category_stats(df)
        rank_stats = calculate_grok_rank_stats(df)
        risk_stats = calculate_risk_reward_stats(df)
        volatility_stats = calculate_volatility_stats(df)
        prev_day_stats = calculate_prev_day_stats(df)
        daily_stats = calculate_daily_stats(df)
        v2_action_stats = calculate_v2_action_stats(df)
        recommendation_stats = calculate_recommendation_stats(df)

        # メタデータ
        metadata = {
            'totalStocks': len(df),
            'uniqueStocks': int(df['ticker'].nunique()),
            'dateRange': {
                'start': df['backtest_date'].min().strftime('%Y-%m-%d'),
                'end': df['backtest_date'].max().strftime('%Y-%m-%d'),
            },
            'generatedAt': pd.Timestamp.now().isoformat(),
        }

        return {
            'metadata': metadata,
            'phaseStats': phase_stats,
            'categoryStats': category_stats,
            'rankStats': rank_stats,
            'riskStats': risk_stats,
            'volatilityStats': volatility_stats,
            'prevDayStats': prev_day_stats,
            'dailyStats': daily_stats,
            'v2ActionStats': v2_action_stats,
            'recommendationStats': recommendation_stats,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "データ取得中にエラーが発生しました",
                    "details": str(e)
                }
            }
        )


@router.get("/api/dev/grok-analysis-v2")
async def get_grok_analysis_v2():
    """
    Grok推奨銘柄のv2.0.3 vs v2.1比較分析データを取得

    Returns:
        GrokAnalysisV2Response: v2.0.3とv2.1の比較分析データ

    Raises:
        HTTPException: ファイルが見つからない場合は404/500
    """
    try:
        # v2.1データ読み込み（ローカルまたはS3から）
        df = load_analysis_data_v2_1()

        # v2.0.3 vs v2.1比較統計
        comparison_stats = calculate_v2_0_3_vs_v2_1_comparison(df)

        # 損切り水準別統計
        stop_loss_stats = calculate_stop_loss_tier_stats(df)

        # アクション変更分析
        action_change_stats = calculate_action_change_analysis(df)

        # 既存の統計も提供（Phase別、リスクリワード等）
        phase_stats = calculate_phase_stats(df)
        risk_stats = calculate_risk_reward_stats(df)

        # 日別統計（v2.1アクション別）
        date_stats = []
        for date in sorted(df['backtest_date'].unique(), reverse=True)[:30]:  # 最新30日
            date_df = df[df['backtest_date'] == date]

            date_v2_0_3 = {
                'buy': int((date_df['v2_0_3_action'] == '買い').sum()),
                'sell': int((date_df['v2_0_3_action'] == '売り').sum()),
                'hold': int((date_df['v2_0_3_action'] == '静観').sum()),
            }

            date_v2_1 = {
                'buy': int((date_df['v2_1_action'] == '買い').sum()),
                'sell': int((date_df['v2_1_action'] == '売り').sum()),
                'hold': int((date_df['v2_1_action'] == '静観').sum()),
            }

            # v2.1の買いアクションの統計
            buy_df = date_df[date_df['v2_1_action'] == '買い']
            if len(buy_df) > 0:
                buy_win_rate = safe_float(buy_df['phase2_win'].sum() / len(buy_df) * 100)
                buy_avg_return = safe_float(buy_df['phase2_return_pct'].mean())
            else:
                buy_win_rate = None
                buy_avg_return = None

            date_stats.append({
                'date': date.strftime('%Y-%m-%d'),
                'total': len(date_df),
                'v2_0_3Actions': date_v2_0_3,
                'v2_1Actions': date_v2_1,
                'buyWinRate': buy_win_rate,
                'buyAvgReturn': buy_avg_return,
            })

        # メタデータ
        metadata = {
            'totalStocks': len(df),
            'uniqueStocks': int(df['ticker'].nunique()),
            'dateRange': {
                'start': df['backtest_date'].min().strftime('%Y-%m-%d'),
                'end': df['backtest_date'].max().strftime('%Y-%m-%d'),
            },
            'generatedAt': pd.Timestamp.now().isoformat(),
            'version': 'v2.1',
        }

        return {
            'metadata': metadata,
            'comparisonStats': comparison_stats,
            'stopLossStats': stop_loss_stats,
            'actionChangeStats': action_change_stats,
            'phaseStats': phase_stats,
            'riskStats': risk_stats,
            'dateStats': date_stats,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "v2.1データ取得中にエラーが発生しました",
                    "details": str(e)
                }
            }
        )
