# server/routers/dev_analyze.py
"""
開発者向け分析ダッシュボードAPI
/api/dev/analyze/* - JSON APIエンドポイント
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from fastapi_cache.decorator import cache
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
import sys
import yfinance as yf
import json
from functools import lru_cache
import time
import os
import boto3
from botocore.exceptions import ClientError
from io import BytesIO

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

router = APIRouter()

BACKTEST_DIR = PARQUET_DIR / "backtest"
ARCHIVE_WITH_PHASE4_FILE = BACKTEST_DIR / "grok_trending_archive_with_phase4.parquet"
ARCHIVE_WITH_MARKET_FILE = BACKTEST_DIR / "grok_trending_archive_with_market.parquet"

# グローバルキャッシュ
_cache = {
    'df': None,
    'df_timestamp': 0,
    'grok_analysis': None,
    'grok_analysis_timestamp': 0,
    'market_segments': None,
    'robust_stats': None,
    'asymmetric_thresholds': None,
    'summary': None,
}


def get_parquet_file_path() -> Path:
    """使用するParquetファイルのパスを返す"""
    if ARCHIVE_WITH_PHASE4_FILE.exists():
        return ARCHIVE_WITH_PHASE4_FILE
    elif ARCHIVE_WITH_MARKET_FILE.exists():
        return ARCHIVE_WITH_MARKET_FILE
    else:
        raise HTTPException(status_code=404, detail="アーカイブファイルが見つかりません")


def clear_calculated_cache():
    """計算結果のキャッシュをクリア"""
    _cache['market_segments'] = None
    _cache['robust_stats'] = None
    _cache['asymmetric_thresholds'] = None
    _cache['summary'] = None


def load_archive_with_market() -> pd.DataFrame:
    """マーケットデータ付きアーカイブを読み込み（キャッシュ付き）"""
    file_path = get_parquet_file_path()
    file_mtime = file_path.stat().st_mtime

    # キャッシュが有効かチェック
    if _cache['df'] is not None and _cache['df_timestamp'] == file_mtime:
        return _cache['df']

    # ファイルが更新された場合は計算結果のキャッシュもクリア
    clear_calculated_cache()

    # ファイルを読み込み
    df = pd.read_parquet(file_path)

    if 'backtest_date' in df.columns:
        df['backtest_date'] = pd.to_datetime(df['backtest_date'])

    # キャッシュに保存
    _cache['df'] = df
    _cache['df_timestamp'] = file_mtime

    return df


def calculate_segment_stats(df: pd.DataFrame, segment_column: str) -> List[Dict[str, Any]]:
    """セグメント別の統計を計算"""
    results = []

    strategies = [
        ('phase1', 'profit_per_100_shares_phase1', 'phase1_win'),
        ('phase2', 'profit_per_100_shares_phase2', 'phase2_win'),
        ('phase3_1pct', 'profit_per_100_shares_phase3_1pct', 'phase3_1pct_win'),
        ('phase3_2pct', 'profit_per_100_shares_phase3_2pct', 'phase3_2pct_win'),
        ('phase3_3pct', 'profit_per_100_shares_phase3_3pct', 'phase3_3pct_win'),
        ('phase4', 'profit_per_100_shares_phase4', 'phase4_win'),
    ]

    for segment_value in df[segment_column].unique():
        segment_df = df[df[segment_column] == segment_value]

        if len(segment_df) < 3:
            continue

        segment_data = {
            'segment': str(segment_value),
            'count': len(segment_df),
            'strategies': {}
        }

        for strategy_name, profit_col, win_col in strategies:
            cumulative_profit = float(segment_df[profit_col].sum())
            cumulative_investment = float((segment_df['buy_price'] * 100).sum())
            cumulative_return_pct = (cumulative_profit / cumulative_investment * 100) if cumulative_investment > 0 else 0
            win_rate = float((segment_df[win_col].sum() / len(segment_df) * 100))
            avg_profit = float(segment_df[profit_col].mean())

            segment_data['strategies'][strategy_name] = {
                'profit': cumulative_profit,
                'return_pct': cumulative_return_pct,
                'win_rate': win_rate,
                'avg_profit': avg_profit
            }

        results.append(segment_data)

    return results


@router.get("/api/dev/analyze/summary")
@cache(expire=300)  # 5分間キャッシュ
async def get_analysis_summary():
    """分析サマリーを取得（キャッシュ付き）"""
    # キャッシュチェック
    if _cache['summary'] is not None:
        return _cache['summary']

    df = load_archive_with_market()

    result = {
        'period': {
            'start': df['backtest_date'].min().strftime('%Y-%m-%d'),
            'end': df['backtest_date'].max().strftime('%Y-%m-%d'),
        },
        'total_trades': len(df),
        'market_data_availability': {
            'morning_nikkei': int(df['morning_nikkei_return'].notna().sum()),
            'daily_nikkei': int(df['daily_nikkei_return'].notna().sum()),
            'daily_topix': int(df['daily_topix_return'].notna().sum()),
        }
    }

    # キャッシュに保存
    _cache['summary'] = result

    return result


@router.get("/api/dev/analyze/market_segments")
@cache(expire=300)  # 5分間キャッシュ
async def get_market_segments():
    """マーケット要因別のセグメント分析（キャッシュ付き）"""
    # キャッシュチェック
    if _cache['market_segments'] is not None:
        return _cache['market_segments']

    df = load_archive_with_market()

    results = {}

    # 1. 日経平均騰落
    df_nikkei = df[df['daily_nikkei_return'].notna()].copy()
    df_nikkei['nikkei_direction'] = df_nikkei['daily_nikkei_return'].apply(
        lambda x: '上昇' if x > 0 else '下落'
    )
    results['nikkei_direction'] = calculate_segment_stats(df_nikkei, 'nikkei_direction')

    # 2. TOPIX騰落
    df_topix = df[df['daily_topix_return'].notna()].copy()
    df_topix['topix_direction'] = df_topix['daily_topix_return'].apply(
        lambda x: '上昇' if x > 0 else '下落'
    )
    results['topix_direction'] = calculate_segment_stats(df_topix, 'topix_direction')

    # 3. 日経平均変動幅
    df_volatility = df[df['daily_nikkei_return'].notna()].copy()
    df_volatility['nikkei_volatility'] = df_volatility['daily_nikkei_return'].apply(
        lambda x: '大幅上昇' if x >= 0.01
        else '大幅下落' if x <= -0.01
        else '安定'
    )
    results['nikkei_volatility'] = calculate_segment_stats(df_volatility, 'nikkei_volatility')

    # 4. マザーズ騰落
    df_mothers = df[df['daily_mothers_return'].notna()].copy()
    df_mothers['mothers_direction'] = df_mothers['daily_mothers_return'].apply(
        lambda x: '上昇' if x > 0 else '下落'
    )
    results['mothers_direction'] = calculate_segment_stats(df_mothers, 'mothers_direction')

    # キャッシュに保存
    _cache['market_segments'] = results

    return results


@router.get("/api/dev/analyze/heatmap")
@cache(expire=300)  # 5分間キャッシュ
async def get_heatmap_data():
    """ヒートマップ用データ"""
    df = load_archive_with_market()

    # 日経平均騰落 × 戦略のヒートマップ
    df_nikkei = df[df['daily_nikkei_return'].notna()].copy()
    df_nikkei['nikkei_direction'] = df_nikkei['daily_nikkei_return'].apply(
        lambda x: '上昇' if x > 0 else '下落'
    )

    heatmap_data = []

    for direction in ['上昇', '下落']:
        segment_df = df_nikkei[df_nikkei['nikkei_direction'] == direction]

        for phase_name, profit_col in [
            ('Phase1', 'profit_per_100_shares_phase1'),
            ('Phase2', 'profit_per_100_shares_phase2'),
            ('Phase3±1%', 'profit_per_100_shares_phase3_1pct'),
            ('Phase3±2%', 'profit_per_100_shares_phase3_2pct'),
            ('Phase3±3%', 'profit_per_100_shares_phase3_3pct'),
        ]:
            avg_profit = float(segment_df[profit_col].mean())
            heatmap_data.append({
                'market': f'日経{direction}',
                'strategy': phase_name,
                'value': avg_profit
            })

    return heatmap_data


@router.get("/api/dev/analyze/robust_stats")
@cache(expire=300)  # 5分間キャッシュ
async def get_robust_stats():
    """
    堅牢統計: 再現性の高い分析（キャッシュ付き）
    - 中央値（外れ値に強い）
    - 勝率
    - 下位25%の平均（リスク指標）
    - 期待値
    """
    # キャッシュチェック
    if _cache['robust_stats'] is not None:
        return _cache['robust_stats']

    df = load_archive_with_market()

    results = {}

    strategies = [
        ('phase1', 'profit_per_100_shares_phase1', 'phase1_win'),
        ('phase2', 'profit_per_100_shares_phase2', 'phase2_win'),
        ('phase3_1pct', 'profit_per_100_shares_phase3_1pct', 'phase3_1pct_win'),
        ('phase3_2pct', 'profit_per_100_shares_phase3_2pct', 'phase3_2pct_win'),
        ('phase3_3pct', 'profit_per_100_shares_phase3_3pct', 'phase3_3pct_win'),
        ('phase4', 'profit_per_100_shares_phase4', 'phase4_win'),
    ]

    # 1. 日経平均騰落別
    df_nikkei = df[df['daily_nikkei_return'].notna()].copy()
    df_nikkei['nikkei_direction'] = df_nikkei['daily_nikkei_return'].apply(
        lambda x: '上昇' if x > 0 else '下落'
    )

    results['nikkei_direction'] = {}
    for direction in ['上昇', '下落']:
        segment_df = df_nikkei[df_nikkei['nikkei_direction'] == direction]
        results['nikkei_direction'][direction] = calculate_robust_strategy_stats(segment_df, strategies)

    # 2. TOPIX騰落別
    df_topix = df[df['daily_topix_return'].notna()].copy()
    df_topix['topix_direction'] = df_topix['daily_topix_return'].apply(
        lambda x: '上昇' if x > 0 else '下落'
    )

    results['topix_direction'] = {}
    for direction in ['上昇', '下落']:
        segment_df = df_topix[df_topix['topix_direction'] == direction]
        results['topix_direction'][direction] = calculate_robust_strategy_stats(segment_df, strategies)

    # 3. 日経変動幅別
    df_volatility = df[df['daily_nikkei_return'].notna()].copy()
    df_volatility['nikkei_volatility'] = df_volatility['daily_nikkei_return'].apply(
        lambda x: '大幅上昇' if x >= 0.01
        else '大幅下落' if x <= -0.01
        else '安定'
    )

    results['nikkei_volatility'] = {}
    for volatility in ['大幅上昇', '安定', '大幅下落']:
        segment_df = df_volatility[df_volatility['nikkei_volatility'] == volatility]
        if len(segment_df) >= 3:
            results['nikkei_volatility'][volatility] = calculate_robust_strategy_stats(segment_df, strategies)

    # 4. マザーズ騰落別
    df_mothers = df[df['daily_mothers_return'].notna()].copy()
    df_mothers['mothers_direction'] = df_mothers['daily_mothers_return'].apply(
        lambda x: '上昇' if x > 0 else '下落'
    )

    results['mothers_direction'] = {}
    for direction in ['上昇', '下落']:
        segment_df = df_mothers[df_mothers['mothers_direction'] == direction]
        if len(segment_df) >= 3:
            results['mothers_direction'][direction] = calculate_robust_strategy_stats(segment_df, strategies)

    # キャッシュに保存
    _cache['robust_stats'] = results

    return results


def calculate_robust_strategy_stats(segment_df: pd.DataFrame, strategies: List) -> Dict[str, Any]:
    """
    戦略別の堅牢統計を計算
    """
    if len(segment_df) < 3:
        return {}

    stats = {}

    for strategy_name, profit_col, win_col in strategies:
        profits = segment_df[profit_col]
        wins = segment_df[win_col]

        # 勝ちと負けを分離
        winning_trades = profits[wins == True]
        losing_trades = profits[wins == False]

        # 中央値
        median_profit = float(profits.median())

        # 勝率
        win_rate = float((wins.sum() / len(segment_df) * 100))

        # 下位25%の平均（リスク指標）
        q25_profit = float(profits.quantile(0.25))
        lower_25_profits = profits[profits <= q25_profit]
        lower_25_avg = float(lower_25_profits.mean()) if len(lower_25_profits) > 0 else 0

        # 期待値
        avg_win = float(winning_trades.mean()) if len(winning_trades) > 0 else 0
        avg_loss = float(losing_trades.mean()) if len(losing_trades) > 0 else 0
        expected_value = (win_rate / 100 * avg_win) + ((100 - win_rate) / 100 * avg_loss)

        # 最大損失
        max_loss = float(profits.min())

        # 平均利益（参考値）
        mean_profit = float(profits.mean())

        # トリム平均（上下5%除外）
        trimmed_profits = profits[(profits >= profits.quantile(0.05)) & (profits <= profits.quantile(0.95))]
        trimmed_mean = float(trimmed_profits.mean()) if len(trimmed_profits) > 0 else 0

        # 推奨判定
        is_recommended = (
            win_rate > 60 and
            median_profit > 0 and
            lower_25_avg > -5000
        )

        stats[strategy_name] = {
            'count': len(segment_df),
            'win_rate': win_rate,
            'median_profit': median_profit,
            'mean_profit': mean_profit,
            'trimmed_mean': trimmed_mean,
            'expected_value': expected_value,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'lower_25_avg': lower_25_avg,
            'max_loss': max_loss,
            'is_recommended': is_recommended,
        }

    return stats


def calculate_asymmetric_return(
    df_5min: pd.DataFrame,
    open_price: float,
    profit_threshold: float,
    loss_threshold: float,
) -> Tuple[Optional[float], Optional[bool], Optional[str]]:
    """
    非対称な利確・損切戦略のリターンを計算

    Args:
        df_5min: 5分足データ（9:00-15:30）
        open_price: 寄付価格
        profit_threshold: 利確閾値（例: 0.02 = +2%）
        loss_threshold: 損切閾値（例: -0.04 = -4%）

    Returns:
        (return, win, exit_reason)
    """
    if df_5min.empty or open_price == 0:
        return None, None, None

    profit_price = open_price * (1 + profit_threshold)
    loss_price = open_price * (1 + loss_threshold)

    for idx, row in df_5min.iterrows():
        high = row['High']
        low = row['Low']
        timestamp = row['date']

        # 利確条件
        if high >= profit_price:
            exit_return = profit_threshold
            return exit_return, True, f"profit_{timestamp.strftime('%H:%M')}"

        # 損切条件
        if low <= loss_price:
            exit_return = loss_threshold
            return exit_return, False, f"loss_{timestamp.strftime('%H:%M')}"

    # 大引け決済
    if not df_5min.empty:
        close_price = df_5min.iloc[-1]['Close']
        final_return = (close_price - open_price) / open_price
        win = final_return > 0
        return final_return, win, "close"

    return None, None, None


@router.get("/api/dev/analyze/asymmetric_thresholds")
@cache(expire=300)  # 5分間キャッシュ
async def get_asymmetric_thresholds():
    """
    非対称な利確・損切閾値の分析結果（キャッシュ付き）
    ヒートマップ用データを返却
    """
    # キャッシュチェック
    if _cache['asymmetric_thresholds'] is not None:
        return _cache['asymmetric_thresholds']

    df = load_archive_with_market()

    # 閾値の組み合わせ
    threshold_combinations = [
        # 対称
        (0.01, -0.01, "±1%"),
        (0.02, -0.02, "±2%"),
        (0.03, -0.03, "±3%"),
        # 非対称（利確 > 損切）
        (0.02, -0.01, "+2% -1%"),
        (0.03, -0.01, "+3% -1%"),
        (0.03, -0.02, "+3% -2%"),
        (0.04, -0.02, "+4% -2%"),
        (0.05, -0.02, "+5% -2%"),
        (0.04, -0.03, "+4% -3%"),
        # 非対称（利確 < 損切）
        (0.01, -0.02, "+1% -2%"),
        (0.02, -0.03, "+2% -3%"),
        (0.02, -0.04, "+2% -4%"),
    ]

    results = []

    for profit_pct, loss_pct, label in threshold_combinations:
        total_profit = 0
        win_count = 0
        total_count = 0
        exit_reasons = {"profit": 0, "loss": 0, "close": 0}

        for idx, row in df.iterrows():
            ticker = row['ticker']
            backtest_date = pd.to_datetime(row['backtest_date']).date()
            buy_price = row['buy_price']

            # yfinanceで5分足データを取得
            try:
                stock = yf.Ticker(ticker)
                df_5min = stock.history(
                    start=backtest_date,
                    end=backtest_date + timedelta(days=1),
                    interval="5m"
                )
            except Exception:
                continue

            if df_5min.empty:
                continue

            # カラム名を統一
            df_5min = df_5min.reset_index()
            df_5min.rename(columns={'Datetime': 'date'}, inplace=True)
            df_5min['date'] = pd.to_datetime(df_5min['date'])

            # 9:00-15:30のデータのみ
            df_5min = df_5min[
                (df_5min['date'].dt.time >= pd.Timestamp("09:00").time()) &
                (df_5min['date'].dt.time <= pd.Timestamp("15:30").time())
            ].sort_values('date')

            # リターン計算
            phase_return, phase_win, exit_reason = calculate_asymmetric_return(
                df_5min, buy_price, profit_pct, loss_pct
            )

            if phase_return is not None:
                profit_per_100 = phase_return * buy_price * 100
                total_profit += profit_per_100
                if phase_win:
                    win_count += 1
                total_count += 1

                # 決済理由を集計
                if exit_reason:
                    if "profit" in exit_reason:
                        exit_reasons["profit"] += 1
                    elif "loss" in exit_reason:
                        exit_reasons["loss"] += 1
                    elif "close" in exit_reason:
                        exit_reasons["close"] += 1

        if total_count > 0:
            win_rate = (win_count / total_count) * 100
            avg_profit = total_profit / total_count
            cumulative_investment = (df['buy_price'] * 100).sum()
            cumulative_return_pct = (total_profit / cumulative_investment * 100) if cumulative_investment > 0 else 0

            results.append({
                "label": label,
                "profit_pct": profit_pct * 100,
                "loss_pct": loss_pct * 100,
                "total_profit": float(total_profit),
                "cumulative_return_pct": float(cumulative_return_pct),
                "win_rate": float(win_rate),
                "avg_profit": float(avg_profit),
                "count": total_count,
                "exit_profit": exit_reasons["profit"],
                "exit_loss": exit_reasons["loss"],
                "exit_close": exit_reasons["close"],
            })

    result = {"results": results}

    # キャッシュに保存
    _cache['asymmetric_thresholds'] = result

    return result


@router.get("/api/dev/analyze/grok-selection-analysis")
@cache(expire=300)  # 5分間キャッシュ
async def get_grok_selection_analysis():
    """Grok銘柄選定の詳細分析（キャッシュ付き）"""
    analysis_file = BACKTEST_DIR / "grok_analysis_all_days.json"

    if not analysis_file.exists():
        raise HTTPException(status_code=404, detail="分析ファイルが見つかりません")

    # ファイルの最終更新時刻を取得
    file_mtime = analysis_file.stat().st_mtime

    # キャッシュが有効かチェック
    if _cache['grok_analysis'] is not None and _cache['grok_analysis_timestamp'] == file_mtime:
        return _cache['grok_analysis']

    # JSONファイルを読み込み
    with open(analysis_file, 'r', encoding='utf-8') as f:
        analysis_data = json.load(f)

    # 全体統計を計算
    total_profit = sum(day['total_profit'] for day in analysis_data)
    total_count = sum(day['count'] for day in analysis_data)
    avg_win_rate = sum(day['win_rate'] for day in analysis_data) / len(analysis_data)

    # カテゴリ別の統計を集計
    category_summary = {}
    for day in analysis_data:
        for cat_stat in day['category_stats']:
            cat = cat_stat['category']
            if cat not in category_summary:
                category_summary[cat] = {
                    'total_profit': 0,
                    'total_wins': 0,
                    'total_count': 0,
                    'days': []
                }
            category_summary[cat]['total_profit'] += cat_stat['total_profit']
            category_summary[cat]['total_wins'] += cat_stat['wins']
            category_summary[cat]['total_count'] += cat_stat['count']
            category_summary[cat]['days'].append(day['date'])

    # カテゴリ別の勝率を計算
    category_list = []
    for cat, data in category_summary.items():
        if data['total_count'] >= 3:  # 3銘柄以上
            category_list.append({
                'category': cat,
                'total_profit': data['total_profit'],
                'win_rate': (data['total_wins'] / data['total_count'] * 100) if data['total_count'] > 0 else 0,
                'count': data['total_count'],
                'days_count': len(set(data['days']))
            })

    # 利益順でソート
    category_list.sort(key=lambda x: x['total_profit'])

    result = {
        'daily_analysis': analysis_data,
        'overall_stats': {
            'total_profit': total_profit,
            'total_count': total_count,
            'avg_win_rate': avg_win_rate,
            'days_analyzed': len(analysis_data)
        },
        'category_analysis': category_list,
        'insights': generate_grok_insights(analysis_data, category_list)
    }

    # キャッシュに保存
    _cache['grok_analysis'] = result
    _cache['grok_analysis_timestamp'] = file_mtime

    return result


def generate_grok_insights(daily_data: List[Dict], category_data: List[Dict]) -> Dict[str, Any]:
    """Grok選定の洞察を生成"""
    insights = {
        'worst_patterns': [],
        'best_patterns': [],
        'recommendations': []
    }

    # ワーストパターン
    worst_categories = [c for c in category_data if c['total_profit'] < -10000][:3]
    for cat in worst_categories:
        insights['worst_patterns'].append({
            'pattern': cat['category'],
            'impact': f"{cat['total_profit']:,.0f}円損失",
            'win_rate': f"{cat['win_rate']:.1f}%",
            'description': f"{cat['count']}銘柄中、{int(cat['count'] * cat['win_rate'] / 100)}勝"
        })

    # ベストパターン
    best_categories = [c for c in category_data if c['total_profit'] > 5000][-3:]
    best_categories.reverse()
    for cat in best_categories:
        insights['best_patterns'].append({
            'pattern': cat['category'],
            'impact': f"{cat['total_profit']:+,.0f}円",
            'win_rate': f"{cat['win_rate']:.1f}%",
            'description': f"{cat['count']}銘柄中、{int(cat['count'] * cat['win_rate'] / 100)}勝"
        })

    # 推奨事項
    if any('株クラバズ' in c['category'] for c in worst_categories):
        insights['recommendations'].append({
            'type': '除外推奨',
            'pattern': '株クラバズ（Twitterバズ）',
            'reason': 'Twitterで「寄付狙い」「デイトレ」言及が多い銘柄は勝率0%。翌朝の高値掴みリスクが高い。'
        })

    # 前日高騰銘柄のチェック
    high_volatility_losses = 0
    for day in daily_data:
        for stock in day['worst3']:
            if 'reason' in stock and ('+10%' in str(stock.get('reason', '')) or '+15%' in str(stock.get('reason', ''))):
                high_volatility_losses += 1

    if high_volatility_losses >= 3:
        insights['recommendations'].append({
            'type': '除外推奨',
            'pattern': '前日大幅上昇銘柄（+10%以上）',
            'reason': f'{high_volatility_losses}回以上、前日+10-16%上昇した銘柄が翌日大損失。利確売りで下落する傾向。'
        })

    return insights


# ========================================
# 政策銘柄バックテスト API
# ========================================

POLITICAL_ARCHIVE_FILE = BACKTEST_DIR / "political_trending_archive.parquet"


def load_political_archive() -> pd.DataFrame:
    """政策銘柄アーカイブを読み込み"""
    if not POLITICAL_ARCHIVE_FILE.exists():
        raise HTTPException(status_code=404, detail="政策銘柄アーカイブファイルが見つかりません")

    df = pd.read_parquet(POLITICAL_ARCHIVE_FILE)

    if 'backtest_date' in df.columns:
        df['backtest_date'] = pd.to_datetime(df['backtest_date'])
    if 'selection_date' in df.columns:
        df['selection_date'] = pd.to_datetime(df['selection_date'])

    return df


@router.get("/political-analysis")
@cache(expire=300)  # 5分間キャッシュ
async def political_analysis():
    """政策銘柄バックテスト分析"""
    try:
        df = load_political_archive()

        # 全体統計
        overall_stats = {
            'total_count': len(df),
            'total_profit_phase2': float(df['profit_per_100_shares_phase2'].sum()),
            'avg_win_rate_phase2': float((df['phase2_win'].sum() / len(df) * 100) if len(df) > 0 else 0),
            'days_analyzed': int(df['selection_date'].nunique()),
            'unique_tickers': int(df['ticker'].nunique()),
        }

        # tags別分析
        tags_expanded = df[df['tags'] != ''].copy()
        tags_expanded['tags_list'] = tags_expanded['tags'].str.split(', ')
        tags_expanded = tags_expanded.explode('tags_list')

        tag_analysis = []
        for tag in tags_expanded['tags_list'].unique():
            if pd.isna(tag) or tag == '':
                continue

            tag_df = tags_expanded[tags_expanded['tags_list'] == tag]

            tag_stats = {
                'tag': tag,
                'count': int(len(tag_df)),
                'total_profit': float(tag_df['profit_per_100_shares_phase2'].sum()),
                'win_rate': float((tag_df['phase2_win'].sum() / len(tag_df) * 100) if len(tag_df) > 0 else 0),
                'avg_profit': float(tag_df['profit_per_100_shares_phase2'].mean()) if len(tag_df) > 0 else 0,
                'days_count': int(tag_df['selection_date'].nunique()),
            }
            tag_analysis.append(tag_stats)

        # 利益順にソート
        tag_analysis = sorted(tag_analysis, key=lambda x: x['total_profit'], reverse=True)

        # 日別分析
        daily_analysis = []
        for date in sorted(df['selection_date'].unique()):
            day_df = df[df['selection_date'] == date]

            day_stats = {
                'date': pd.to_datetime(date).strftime('%Y-%m-%d'),
                'total_profit': float(day_df['profit_per_100_shares_phase2'].sum()),
                'win_rate': float((day_df['phase2_win'].sum() / len(day_df) * 100) if len(day_df) > 0 else 0),
                'avg_return': float((day_df['phase2_return'] * 100).mean()),
                'count': int(len(day_df)),
            }

            # ワースト3
            worst_df = day_df.nsmallest(3, 'profit_per_100_shares_phase2')
            day_stats['worst3'] = [
                {
                    'ticker': row['ticker'],
                    'company_name': row['company_name'],
                    'profit_per_100_shares_phase2': float(row['profit_per_100_shares_phase2']),
                    'return_pct': float(row['phase2_return'] * 100),
                    'tags': row['tags'],
                }
                for _, row in worst_df.iterrows()
            ]

            # ベスト3
            best_df = day_df.nlargest(3, 'profit_per_100_shares_phase2')
            day_stats['best3'] = [
                {
                    'ticker': row['ticker'],
                    'company_name': row['company_name'],
                    'profit_per_100_shares_phase2': float(row['profit_per_100_shares_phase2']),
                    'return_pct': float(row['phase2_return'] * 100),
                    'tags': row['tags'],
                }
                for _, row in best_df.iterrows()
            ]

            daily_analysis.append(day_stats)

        # 推奨事項生成
        insights = {
            'best_tags': [],
            'worst_tags': [],
            'recommendations': [],
        }

        # ベスト・ワーストタグ
        if len(tag_analysis) > 0:
            best_tags = [t for t in tag_analysis if t['total_profit'] > 0][:3]
            worst_tags = [t for t in tag_analysis if t['total_profit'] < 0][-3:]

            for tag in best_tags:
                insights['best_tags'].append({
                    'pattern': tag['tag'],
                    'impact': f"+{tag['total_profit']:,.0f}円",
                    'win_rate': f"{tag['win_rate']:.1f}%",
                    'description': f"{tag['count']}銘柄で累積利益プラス"
                })

            for tag in worst_tags:
                insights['worst_tags'].append({
                    'pattern': tag['tag'],
                    'impact': f"{tag['total_profit']:,.0f}円",
                    'win_rate': f"{tag['win_rate']:.1f}%",
                    'description': f"{tag['count']}銘柄で累積損失"
                })

        # 推奨事項
        if len(worst_tags) > 0 and worst_tags[0]['win_rate'] < 40:
            insights['recommendations'].append({
                'type': '除外推奨',
                'pattern': worst_tags[0]['tag'],
                'reason': f"勝率{worst_tags[0]['win_rate']:.1f}%と低く、累積{worst_tags[0]['total_profit']:,.0f}円の損失。"
            })

        if len(best_tags) > 0 and best_tags[0]['win_rate'] > 50:
            insights['recommendations'].append({
                'type': '重点投資',
                'pattern': best_tags[0]['tag'],
                'reason': f"勝率{best_tags[0]['win_rate']:.1f}%で、累積+{best_tags[0]['total_profit']:,.0f}円の利益。"
            })

        return {
            'daily_analysis': daily_analysis,
            'overall_stats': overall_stats,
            'tag_analysis': tag_analysis,
            'insights': insights,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"政策銘柄分析エラー: {str(e)}")


# ========================================
# Grok Analysis API
# ========================================

GROK_ANALYSIS_FILE = BACKTEST_DIR / "grok_analysis_merged.parquet"

# S3設定
S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
S3_PREFIX = os.getenv("S3_PREFIX", "parquet/")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def load_grok_analysis_from_s3() -> pd.DataFrame:
    """S3からGrok分析データを読み込み（ローカルファイルにフォールバック）"""
    # S3から読み込み
    try:
        s3_key = f"{S3_PREFIX}backtest/grok_analysis_merged.parquet"
        s3_client = boto3.client('s3', region_name=AWS_REGION)

        print(f"[INFO] Loading grok analysis from S3: s3://{S3_BUCKET}/{s3_key}")

        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        # StreamingBodyをBytesIOに変換（parquetのseek操作のため）
        bytes_buffer = BytesIO(response['Body'].read())
        df = pd.read_parquet(bytes_buffer)

        print(f"[INFO] Successfully loaded {len(df)} records from S3")
        return df

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == 'NoSuchKey':
            print(f"[WARNING] Grok analysis not found in S3: {s3_key}")
        else:
            print(f"[WARNING] S3 error: {error_code}: {e}")
    except Exception as e:
        print(f"[WARNING] Could not load from S3: {type(e).__name__}: {e}")

    # ローカルファイルにフォールバック
    if GROK_ANALYSIS_FILE.exists():
        print(f"[INFO] Loading grok analysis from local file: {GROK_ANALYSIS_FILE}")
        return pd.read_parquet(GROK_ANALYSIS_FILE)

    # どちらも失敗
    raise HTTPException(
        status_code=404,
        detail="Grok分析データが見つかりません（S3・ローカル共に存在しません）"
    )


@router.get("/api/dev/grok-analysis")
@cache(expire=300)  # 5分間キャッシュ
async def get_grok_analysis():
    """Grok分析データを取得"""
    try:
        # S3またはローカルから読み込み
        df = load_grok_analysis_from_s3()

        # 日付カラムを文字列に変換（NaT/NaNを先に処理）
        if 'selection_date' in df.columns:
            df['selection_date'] = df['selection_date'].apply(
                lambda x: pd.to_datetime(x).strftime('%Y-%m-%d') if pd.notnull(x) else None
            )
        if 'backtest_date' in df.columns:
            df['backtest_date'] = df['backtest_date'].apply(
                lambda x: pd.to_datetime(x).strftime('%Y-%m-%d') if pd.notnull(x) else None
            )

        # 辞書のリストに変換（NaN/NaTをNoneに変換）
        records = df.where(pd.notnull(df), None).to_dict('records')

        return {
            'success': True,
            'data': records,
            'count': len(records)
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Grok分析データ取得エラー: {str(e)}")
