# server/routers/dev_v3.py
"""
v3.0 スイング分析 API
- 価格帯最適化戦略のサマリー・詳細データを提供
"""

from fastapi import APIRouter, Query
from typing import Optional, List, Dict, Any
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

router = APIRouter()

# データパス
DATA_DIR = Path(__file__).resolve().parents[2] / "improvement" / "data"
BACKTEST_DIR = Path(__file__).resolve().parents[2] / "data" / "parquet" / "backtest"

# キャッシュ
_cache: Dict[str, Any] = {
    "results_df": None,
    "mtime": None,
}


def get_cached_results() -> pd.DataFrame:
    """キャッシュされた計算結果を取得（ファイル更新時は再計算）"""
    grok_path = DATA_DIR / "grok_analysis_merged_v2_1.parquet"
    if not grok_path.exists():
        grok_path = BACKTEST_DIR / "grok_analysis_merged_v2_1.parquet"

    current_mtime = grok_path.stat().st_mtime if grok_path.exists() else None

    if _cache["results_df"] is None or _cache["mtime"] != current_mtime:
        grok, prices = load_data()
        if grok is not None and prices is not None:
            _cache["results_df"] = calculate_v3_results(grok, prices)
            _cache["mtime"] = current_mtime

    return _cache["results_df"]


def apply_v2_1_0_1_strategy(row) -> str:
    """V2.1.0.1 ハイブリッド戦略"""
    v2_0_3_action = row.get('v2_0_3_action', '静観')
    v2_1_action = row.get('v2_1_action', '静観')

    if v2_0_3_action == '買い' and v2_1_action == '静観':
        return '静観'
    elif v2_0_3_action == '静観' and v2_1_action == '売り':
        return '売り'
    return v2_0_3_action


def apply_v3_strategy(row) -> tuple:
    """
    v3.0 戦略: シグナル + 価格帯 → アクション + 保有期間
    Returns: (action, holding_days)
    """
    base_action = row.get('v2_1_0_1_action', row.get('v2_0_3_action', '静観'))
    price = row.get('prev_day_close', row.get('buy_price', 0))

    if pd.isna(price) or price <= 0:
        return base_action, 0

    if base_action == '買い':
        if 7500 <= price < 10000:
            return '買い', 5
        elif 5000 <= price < 7500:
            return '買い', 0
        else:
            return '買い', 0
    elif base_action == '静観':
        if 1500 <= price < 3000:
            return '買い', 5
        else:
            return '静観', 0
    elif base_action == '売り':
        if 2000 <= price < 10000:
            return '売り', 5
        else:
            return '売り', 0

    return base_action, 0


def get_future_prices(prices_df, ticker, base_date, days=5) -> Dict[int, float]:
    """指定日以降のN日分の終値を取得"""
    if isinstance(base_date, str):
        base_date = datetime.strptime(base_date, '%Y-%m-%d').date()
    elif hasattr(base_date, 'date'):
        base_date = base_date.date() if callable(base_date.date) else base_date

    ticker_prices = prices_df[prices_df['ticker'] == ticker].copy()
    if ticker_prices.empty:
        return {}

    future = ticker_prices[ticker_prices['date'] > base_date].sort_values('date').head(days)
    result = {}
    for i, (_, row) in enumerate(future.iterrows(), 1):
        result[i] = row['Close']
    return result


def load_data():
    """データ読み込み"""
    # grok_analysis_merged_v2_1.parquet を優先的に読み込み
    grok_path = DATA_DIR / "grok_analysis_merged_v2_1.parquet"
    if not grok_path.exists():
        grok_path = BACKTEST_DIR / "grok_analysis_merged_v2_1.parquet"

    prices_path = DATA_DIR / "prices_max_1d.parquet"
    archive_path = BACKTEST_DIR / "grok_trending_archive.parquet"

    if not grok_path.exists() or not prices_path.exists():
        return None, None

    grok = pd.read_parquet(grok_path)
    prices = pd.read_parquet(prices_path)
    prices['date'] = pd.to_datetime(prices['date']).dt.date

    # 取引制限情報をgrok_trending_archiveからマージ
    if archive_path.exists() and 'margin_code' not in grok.columns:
        archive = pd.read_parquet(archive_path)
        # ticker + backtest_date でマージ
        margin_cols = ['ticker', 'backtest_date', 'margin_code', 'margin_code_name', 'jsf_restricted', 'is_shortable']
        available_cols = [c for c in margin_cols if c in archive.columns]
        if len(available_cols) > 2:  # ticker, backtest_date + 少なくとも1つの制限情報
            margin_df = archive[available_cols].drop_duplicates(subset=['ticker', 'backtest_date'])
            grok['backtest_date'] = pd.to_datetime(grok['backtest_date']).dt.strftime('%Y-%m-%d')
            margin_df['backtest_date'] = pd.to_datetime(margin_df['backtest_date']).dt.strftime('%Y-%m-%d')
            grok = grok.merge(margin_df, on=['ticker', 'backtest_date'], how='left')

    # v2_1_0_1_action がなければ計算
    if 'v2_1_0_1_action' not in grok.columns:
        grok['v2_1_0_1_action'] = grok.apply(apply_v2_1_0_1_strategy, axis=1)

    return grok, prices


def calculate_v3_results(grok_df, prices_df) -> pd.DataFrame:
    """v3.0戦略の損益計算"""
    results = []

    for _, row in grok_df.iterrows():
        ticker = row['ticker']
        backtest_date = row['backtest_date']
        buy_price = row.get('buy_price', 0)

        if pd.isna(buy_price) or buy_price <= 0:
            continue

        stock_name = row.get('stock_name') or row.get('company_name') or ticker
        if pd.isna(stock_name):
            stock_name = ticker

        v3_action, holding_days = apply_v3_strategy(row)
        v3_label = f"{v3_action}5" if holding_days == 5 else v3_action

        # 信用取引制限
        margin_code = row.get('margin_code', 2)
        jsf_restricted = row.get('jsf_restricted', False)
        can_trade = True
        if margin_code in ['3', 3]:
            can_trade = False
        elif margin_code in ['1', 1] and v3_action == '売り':
            can_trade = False
        elif v3_action == '売り' and jsf_restricted:
            can_trade = False

        # 価格情報
        daily_close = row.get('daily_close')
        sell_price = row.get('sell_price')

        record = {
            'backtest_date': str(backtest_date)[:10],
            'ticker': ticker,
            'stock_name': str(stock_name),
            'v3_action': v3_action,
            'v3_label': v3_label,
            'holding_days': holding_days,
            'buy_price': float(buy_price),
            'sell_price': float(sell_price) if sell_price and not pd.isna(sell_price) else None,
            'daily_close': float(daily_close) if daily_close and not pd.isna(daily_close) else None,
            'can_trade': can_trade,
            # 取引制限情報
            'margin_code': int(margin_code) if margin_code and not pd.isna(margin_code) else 2,
            'margin_code_name': str(row.get('margin_code_name', '')) or '',
            'jsf_restricted': bool(jsf_restricted) if jsf_restricted is not None else False,
            'is_shortable': bool(row.get('is_shortable', False)),
        }

        if can_trade:
            future_closes = get_future_prices(prices_df, ticker, backtest_date)
            daily_close = row.get('daily_close')

            # 当日損益
            if daily_close and not pd.isna(daily_close):
                if v3_action == '売り':
                    record['day0_profit'] = float((buy_price - daily_close) * 100)
                else:
                    record['day0_profit'] = float((daily_close - buy_price) * 100)

            # 1-5日後
            for days in [1, 2, 3, 4, 5]:
                close_price = future_closes.get(days)
                if close_price and close_price > 0:
                    if v3_action == '売り':
                        record[f'day{days}_profit'] = float((buy_price - close_price) * 100)
                    else:
                        record[f'day{days}_profit'] = float((close_price - buy_price) * 100)

            # 推奨保有期間の損益
            if holding_days == 0:
                record['profit'] = record.get('day0_profit')
            else:
                record['profit'] = record.get('day5_profit')

        results.append(record)

    return pd.DataFrame(results)


def generate_summary(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """アクション別サマリー生成"""
    summary = []

    for label in ['買い', '買い5', '静観', '売り', '売り5', '全体']:
        if label == '全体':
            subset = df[df['can_trade'] == True]
        else:
            subset = df[(df['v3_label'] == label) & (df['can_trade'] == True)]

        total_signals = len(df[df['v3_label'] == label]) if label != '全体' else len(df)
        tradeable = len(subset)

        if tradeable == 0:
            summary.append({
                'label': label,
                'signals': total_signals,
                'trades': 0,
                'avg_profit': 0,
                'total_profit': 0,
                'win_rate': 0,
            })
            continue

        profits = subset['profit'].dropna()
        if len(profits) == 0:
            summary.append({
                'label': label,
                'signals': total_signals,
                'trades': tradeable,
                'avg_profit': 0,
                'total_profit': 0,
                'win_rate': 0,
            })
            continue

        summary.append({
            'label': label,
            'signals': total_signals,
            'trades': len(profits),
            'avg_profit': round(profits.mean(), 0),
            'total_profit': round(profits.sum(), 0),
            'win_rate': round((profits > 0).mean() * 100, 1),
        })

    return summary


def generate_price_range_summary(df: pd.DataFrame) -> Dict[str, List[Dict]]:
    """価格帯別サマリー"""
    price_ranges = [
        (0, 500, '〜500'),
        (500, 1000, '500〜1,000'),
        (1000, 1500, '1,000〜1,500'),
        (1500, 2000, '1,500〜2,000'),
        (2000, 3000, '2,000〜3,000'),
        (3000, 5000, '3,000〜5,000'),
        (5000, 7500, '5,000〜7,500'),
        (7500, 10000, '7,500〜10,000'),
        (10000, float('inf'), '10,000〜'),
    ]

    result = {}

    for label in ['買い', '買い5', '静観', '売り', '売り5']:
        subset = df[(df['v3_label'] == label) & (df['can_trade'] == True)]
        ranges_data = []

        for low, high, range_label in price_ranges:
            range_df = subset[(subset['buy_price'] >= low) & (subset['buy_price'] < high)]
            profits = range_df['profit'].dropna()

            if len(profits) == 0:
                continue

            ranges_data.append({
                'range': range_label,
                'count': len(profits),
                'avg_profit': round(profits.mean(), 0),
                'total_profit': round(profits.sum(), 0),
                'win_rate': round((profits > 0).mean() * 100, 1),
            })

        result[label] = ranges_data

    return result


@router.get("/api/dev/v3/summary", summary="v3.0 スイング分析サマリー")
def get_v3_summary():
    """v3.0 スイング分析のサマリーデータを取得"""
    results_df = get_cached_results()
    if results_df is None:
        return {"error": "Data not found"}

    summary = generate_summary(results_df)
    price_range = generate_price_range_summary(results_df)

    return {
        "summary": summary,
        "price_range": price_range,
        "generated_at": datetime.now().isoformat(),
        "total_records": len(results_df),
    }


@router.get("/api/dev/v3/stocks", summary="v3.0 銘柄別詳細")
def get_v3_stocks(
    action: Optional[str] = Query(default=None, description="フィルター: 買い, 買い5, 静観, 売り, 売り5"),
    date: Optional[str] = Query(default=None, description="日付フィルター (YYYY-MM-DD)"),
    limit: int = Query(default=100, description="取得件数上限"),
):
    """v3.0 銘柄別詳細データを取得"""
    results_df = get_cached_results()
    if results_df is None:
        return {"error": "Data not found"}

    results_df = results_df.copy()

    # 取引可能銘柄のみ（実売買で利益が出るか分析するため）
    results_df = results_df[results_df['can_trade'] == True]

    if action:
        results_df = results_df[results_df['v3_label'] == action]

    if date:
        results_df = results_df[results_df['backtest_date'] == date]

    # 日付でソート（新しい順）
    results_df = results_df.sort_values('backtest_date', ascending=False).head(limit)

    # NaN を None に変換（JSON互換性のため）
    import numpy as np
    def clean_value(v):
        if v is None:
            return None
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
        return v

    records = [
        {k: clean_value(v) for k, v in row.items()}
        for row in results_df.to_dict('records')
    ]

    return {
        "stocks": records,
        "total": len(records),
    }


@router.get("/api/dev/v3/daily", summary="v3.0 日別サマリー")
def get_v3_daily():
    """v3.0 日別のパフォーマンスサマリー"""
    results_df = get_cached_results()
    if results_df is None:
        return {"error": "Data not found"}

    tradeable = results_df[results_df['can_trade'] == True]

    daily = []
    for date, group in tradeable.groupby('backtest_date'):
        profits = group['profit'].dropna()
        if len(profits) == 0:
            continue

        daily.append({
            'date': date,
            'count': len(profits),
            'avg_profit': round(profits.mean(), 0),
            'total_profit': round(profits.sum(), 0),
            'win_rate': round((profits > 0).mean() * 100, 1),
        })

    # 日付でソート（新しい順）
    daily.sort(key=lambda x: x['date'], reverse=True)

    return {"daily": daily}
