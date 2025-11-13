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


def safe_float(value: Any) -> float | None:
    """NaN/Infを安全にNoneに変換"""
    if pd.isna(value) or np.isinf(value):
        return None
    return float(value)


def calculate_phase_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Phase別統計を計算"""
    phases = {
        'Phase1 (前場終了時)': ('phase1_return', 'phase1_win'),
        'Phase2 (当日終値)': ('phase2_return', 'phase2_win'),
        'Phase3-1% (損切-1%)': ('phase3_1pct_return', 'phase3_1pct_win'),
        'Phase3-2% (損切-2%)': ('phase3_2pct_return', 'phase3_2pct_win'),
        'Phase3-3% (損切-3%)': ('phase3_3pct_return', 'phase3_3pct_win'),
    }

    results = []
    for phase_name, (return_col, win_col) in phases.items():
        win_count = int(df[win_col].sum())
        total = len(df)
        win_rate = safe_float(win_count / total * 100)
        avg_return = safe_float(df[return_col].mean() * 100)  # パーセント表示
        median_return = safe_float(df[return_col].median() * 100)  # パーセント表示

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
        'phase2_return': ['mean', 'median', 'std'],
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
            'avgReturn': safe_float(row['avgReturn'] * 100),  # パーセント表示
            'medianReturn': safe_float(row['medianReturn'] * 100),  # パーセント表示
            'stdDev': safe_float(row['stdDev'] * 100),  # パーセント表示
            'avgMaxGain': safe_float(row['avgMaxGain']),
            'avgMaxLoss': safe_float(row['avgMaxLoss']),
        })

    return sorted(results, key=lambda x: x['winRate'] or 0, reverse=True)


def calculate_grok_rank_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Grokランク別統計を計算"""
    rank_stats = df.groupby('grok_rank').agg({
        'phase2_win': ['sum', 'count', lambda x: x.sum() / len(x) * 100],
        'phase2_return': 'mean'
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
            'avgReturn': safe_float(row['avgReturn'] * 100),  # パーセント表示
        })

    return sorted(results, key=lambda x: x['rank'])


def calculate_risk_reward_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """リスクリワード統計を計算"""
    winners = df[df['phase2_win'] == True]
    losers = df[df['phase2_win'] == False]

    avg_win = safe_float(winners['phase2_return'].mean() * 100) if len(winners) > 0 else 0.0
    avg_loss = safe_float(losers['phase2_return'].mean() * 100) if len(losers) > 0 else 0.0
    risk_reward_ratio = safe_float(avg_win / abs(avg_loss)) if avg_loss and avg_loss != 0 else 0.0

    returns = df['phase2_return']
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
        'avgReturn': safe_float(returns.mean() * 100),
        'stdDev': safe_float(returns.std() * 100),
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
        'phase2_return': 'mean',
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
            'avgReturn': safe_float(row['avgReturn'] * 100),  # パーセント表示
            'avgVolatility': safe_float(row['avgVolatility']),
            'avgMaxGain': safe_float(row['avgMaxGain']),
            'avgMaxLoss': safe_float(row['avgMaxLoss']),
        })

    # 相関係数（None/NaNを除外）
    df_valid = df_copy.dropna(subset=['daily_volatility', 'phase2_return', 'phase2_win'])
    if len(df_valid) > 0:
        corr_vol_return = safe_float(df_valid['daily_volatility'].corr(df_valid['phase2_return']))
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
        'phase1_return': 'mean',
        'phase2_return': 'mean',
    }).round(4)

    summary.columns = ['count', 'phase1WinRate', 'phase2WinRate', 'phase1AvgReturn', 'phase2AvgReturn']

    results = []
    for direction, row in summary.iterrows():
        results.append({
            'direction': 'プラス' if direction == 'plus' else 'マイナス',
            'count': int(row['count']),
            'phase1WinRate': safe_float(row['phase1WinRate']),
            'phase2WinRate': safe_float(row['phase2WinRate']),
            'phase1AvgReturn': safe_float(row['phase1AvgReturn'] * 100),  # パーセント表示
            'phase2AvgReturn': safe_float(row['phase2AvgReturn'] * 100),  # パーセント表示
        })

    return results


def calculate_daily_stats(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """日別統計を計算"""
    daily_stats = df.groupby('backtest_date').agg({
        'phase1_return': 'mean',
        'phase2_return': 'mean',
        'phase3_2pct_return': 'mean',
        'ticker': 'count'
    }).round(4)

    daily_stats.columns = ['phase1AvgReturn', 'phase2AvgReturn', 'phase3AvgReturn', 'count']

    results = []
    for date, row in daily_stats.iterrows():
        results.append({
            'date': date.strftime('%Y-%m-%d'),
            'phase1AvgReturn': safe_float(row['phase1AvgReturn'] * 100),  # パーセント表示
            'phase2AvgReturn': safe_float(row['phase2AvgReturn'] * 100),  # パーセント表示
            'phase3AvgReturn': safe_float(row['phase3AvgReturn'] * 100),  # パーセント表示
            'count': int(row['count']),
        })

    return sorted(results, key=lambda x: x['date'])


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
            avg_return = safe_float(-action_df['phase2_return'].mean() * 100)  # リターンも反転、パーセント表示
            median_return = safe_float(-action_df['phase2_return'].median() * 100)  # リターンも反転、パーセント表示
        else:
            win_count = int(action_df['phase2_win'].sum())
            avg_return = safe_float(action_df['phase2_return'].mean() * 100)  # パーセント表示
            median_return = safe_float(action_df['phase2_return'].median() * 100)  # パーセント表示

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
                avg_return = safe_float(-action_df['phase2_return'].mean() * 100)  # パーセント表示
            else:
                win_count = int(action_df['phase2_win'].sum())
                avg_return = safe_float(action_df['phase2_return'].mean() * 100)  # パーセント表示

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
            return_pct = row['phase2_return']
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
                'returnPct': safe_float(return_pct * 100),  # パーセント表示
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
