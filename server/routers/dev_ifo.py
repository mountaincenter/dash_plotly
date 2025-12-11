# server/routers/dev_ifo.py
"""
ショートIFO戦略 API
- 前場（9:00-10:00）と後場（12:30-15:00）のショートIFOバックテスト
- 空売り可能銘柄のみ対象
"""

from fastapi import APIRouter, Query
from typing import Optional, List, Dict, Any
import pandas as pd
from pathlib import Path
from datetime import datetime, time

router = APIRouter()

# パス設定
DATA_DIR = Path(__file__).resolve().parents[2] / "improvement" / "data"
YFINANCE_DIR = Path(__file__).resolve().parents[2] / "improvement" / "yfinance" / "data"
BACKTEST_DIR = Path(__file__).resolve().parents[2] / "data" / "parquet" / "backtest"

# 戦略パラメータ
STOP_LOSS_YEN = 1
TAKE_PROFIT_PCTS = [0.5, 1.0, 1.5, 2.0]
SHARES = 100

# キャッシュ
_ifo_cache: Dict[str, Any] = {
    "all_df": None,
    "morning_df": None,
    "afternoon_df": None,
    "grok_count": None,
    "mtime": None,
}


def get_cached_ifo_results():
    """キャッシュされたIFOバックテスト結果を取得"""
    grok_path = BACKTEST_DIR / "grok_trending_archive.parquet"
    current_mtime = grok_path.stat().st_mtime if grok_path.exists() else None

    if _ifo_cache["all_df"] is None or _ifo_cache["mtime"] != current_mtime:
        grok, prices_5m = load_data()
        if grok is not None and prices_5m is not None:
            morning_df = run_short_ifo_backtest(grok, prices_5m, 'morning')
            afternoon_df = run_short_ifo_backtest(grok, prices_5m, 'afternoon')
            all_df = pd.concat([morning_df, afternoon_df], ignore_index=True)
            _ifo_cache["morning_df"] = morning_df
            _ifo_cache["afternoon_df"] = afternoon_df
            _ifo_cache["all_df"] = all_df
            _ifo_cache["grok_count"] = len(grok)
            _ifo_cache["mtime"] = current_mtime

    return _ifo_cache["all_df"], _ifo_cache["morning_df"], _ifo_cache["afternoon_df"], _ifo_cache["grok_count"]


def load_data():
    """データ読み込み"""
    grok_path = BACKTEST_DIR / "grok_trending_archive.parquet"
    prices_path = YFINANCE_DIR / "prices_60d_5m.parquet"

    if not grok_path.exists() or not prices_path.exists():
        return None, None

    grok = pd.read_parquet(grok_path)
    prices_5m = pd.read_parquet(prices_path)

    # 空売り可能銘柄のみ
    grok = grok[grok['is_shortable'] == True].copy()

    return grok, prices_5m


def run_short_ifo_backtest(grok, prices_5m, session='morning') -> pd.DataFrame:
    """ショートIFOバックテスト実行"""
    results = []

    if session == 'morning':
        entry_time = time(9, 0)
        exit_time = time(10, 0)
    else:
        entry_time = time(12, 30)
        exit_time = time(15, 0)

    # 日付・時刻列の前処理
    prices_5m = prices_5m.copy()
    if 'Datetime' in prices_5m.columns:
        prices_5m['datetime'] = pd.to_datetime(prices_5m['Datetime'])
        prices_5m['date'] = prices_5m['datetime'].dt.date
        prices_5m['time'] = prices_5m['datetime'].dt.time

    for _, row in grok.iterrows():
        ticker = row['ticker']
        backtest_date = pd.to_datetime(row['backtest_date']).date()
        stock_name = row.get('stock_name', ticker)

        ticker_data = prices_5m[prices_5m['ticker'] == ticker]
        if ticker_data.empty:
            continue

        day_data = ticker_data[ticker_data['date'] == backtest_date].copy()
        if day_data.empty:
            continue

        day_data = day_data.sort_values('datetime')

        entry_data = day_data[day_data['time'] == entry_time]
        if entry_data.empty:
            entry_data = day_data[day_data['time'] >= entry_time].head(1)
            if entry_data.empty:
                continue

        entry_price = entry_data.iloc[0]['Open']
        entry_datetime = entry_data.iloc[0]['datetime']

        post_entry = day_data[day_data['datetime'] > entry_datetime]
        post_entry = post_entry[post_entry['time'] <= exit_time]

        if post_entry.empty:
            continue

        for tp_pct in TAKE_PROFIT_PCTS:
            stop_loss_price = entry_price + STOP_LOSS_YEN
            take_profit_price = entry_price * (1 - tp_pct / 100)

            exit_price = None
            exit_reason = 'timeout'

            for _, bar in post_entry.iterrows():
                if bar['High'] >= stop_loss_price:
                    exit_price = stop_loss_price
                    exit_reason = 'stop_loss'
                    break
                if bar['Low'] <= take_profit_price:
                    exit_price = take_profit_price
                    exit_reason = 'take_profit'
                    break

            if exit_price is None:
                exit_price = post_entry.iloc[-1]['Close']

            pnl_amount = (entry_price - exit_price) * SHARES

            results.append({
                'date': str(backtest_date),
                'ticker': ticker,
                'stock_name': str(stock_name) if stock_name else ticker,
                'session': session,
                'take_profit_pct': tp_pct,
                'entry_price': float(entry_price),
                'exit_price': float(exit_price),
                'exit_reason': exit_reason,
                'pnl_amount': float(pnl_amount),
            })

    return pd.DataFrame(results)


def generate_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """セッション・利確%別サマリー生成"""
    summary = {
        'morning': {},
        'afternoon': {},
    }

    for session in ['morning', 'afternoon']:
        session_key = session
        session_df = df[df['session'] == session]

        for tp_pct in TAKE_PROFIT_PCTS:
            tp_df = session_df[session_df['take_profit_pct'] == tp_pct]
            if len(tp_df) == 0:
                summary[session_key][str(tp_pct)] = {
                    'count': 0,
                    'total_profit': 0,
                    'avg_profit': 0,
                    'win_rate': 0,
                    'take_profit_count': 0,
                    'stop_loss_count': 0,
                    'timeout_count': 0,
                }
                continue

            wins = tp_df[tp_df['pnl_amount'] > 0]
            tp_count = len(tp_df[tp_df['exit_reason'] == 'take_profit'])
            sl_count = len(tp_df[tp_df['exit_reason'] == 'stop_loss'])
            to_count = len(tp_df[tp_df['exit_reason'] == 'timeout'])

            summary[session_key][str(tp_pct)] = {
                'count': len(tp_df),
                'total_profit': round(tp_df['pnl_amount'].sum(), 0),
                'avg_profit': round(tp_df['pnl_amount'].mean(), 0),
                'win_rate': round(len(wins) / len(tp_df) * 100, 1) if len(tp_df) > 0 else 0,
                'take_profit_count': tp_count,
                'stop_loss_count': sl_count,
                'timeout_count': to_count,
            }

    return summary


def generate_daily_summary(df: pd.DataFrame, tp_pct: float = 1.5) -> List[Dict]:
    """日別サマリー（指定利確%）"""
    filtered = df[df['take_profit_pct'] == tp_pct]

    daily = []
    for date, group in filtered.groupby('date'):
        morning = group[group['session'] == 'morning']
        afternoon = group[group['session'] == 'afternoon']

        daily.append({
            'date': date,
            'morning_count': len(morning),
            'morning_profit': round(morning['pnl_amount'].sum(), 0) if len(morning) > 0 else 0,
            'morning_win_rate': round((morning['pnl_amount'] > 0).mean() * 100, 1) if len(morning) > 0 else 0,
            'afternoon_count': len(afternoon),
            'afternoon_profit': round(afternoon['pnl_amount'].sum(), 0) if len(afternoon) > 0 else 0,
            'afternoon_win_rate': round((afternoon['pnl_amount'] > 0).mean() * 100, 1) if len(afternoon) > 0 else 0,
            'total_profit': round(group['pnl_amount'].sum(), 0),
        })

    daily.sort(key=lambda x: x['date'], reverse=True)
    return daily


@router.get("/api/dev/ifo/summary", summary="ショートIFO戦略サマリー")
def get_ifo_summary():
    """ショートIFO戦略のサマリーデータを取得"""
    all_df, morning_df, afternoon_df, grok_count = get_cached_ifo_results()
    if all_df is None:
        return {"error": "Data not found"}

    summary = generate_summary(all_df)

    # 合計（利確2.0%）
    tp_20 = all_df[all_df['take_profit_pct'] == 2.0]
    total_profit = round(tp_20['pnl_amount'].sum(), 0) if len(tp_20) > 0 else 0
    morning_20 = morning_df[morning_df['take_profit_pct'] == 2.0]
    afternoon_20 = afternoon_df[afternoon_df['take_profit_pct'] == 2.0]

    return {
        "summary": summary,
        "total": {
            "profit": total_profit,
            "morning_profit": round(morning_20['pnl_amount'].sum(), 0) if len(morning_20) > 0 else 0,
            "afternoon_profit": round(afternoon_20['pnl_amount'].sum(), 0) if len(afternoon_20) > 0 else 0,
        },
        "shortable_count": grok_count or 0,
        "generated_at": datetime.now().isoformat(),
    }


@router.get("/api/dev/ifo/daily", summary="ショートIFO日別詳細")
def get_ifo_daily(
    take_profit_pct: float = Query(default=2.0, description="利確%"),
):
    """ショートIFO戦略の日別詳細を取得"""
    all_df, _, _, _ = get_cached_ifo_results()
    if all_df is None:
        return {"error": "Data not found"}

    daily = generate_daily_summary(all_df, take_profit_pct)

    return {
        "daily": daily,
        "take_profit_pct": take_profit_pct,
    }


@router.get("/api/dev/ifo/trades", summary="ショートIFO取引詳細")
def get_ifo_trades(
    date: Optional[str] = Query(default=None, description="日付フィルター (YYYY-MM-DD)"),
    session: Optional[str] = Query(default=None, description="セッション (morning/afternoon)"),
    take_profit_pct: float = Query(default=2.0, description="利確%"),
    limit: int = Query(default=100, description="取得件数上限"),
):
    """ショートIFO戦略の取引詳細を取得"""
    all_df, _, _, _ = get_cached_ifo_results()
    if all_df is None:
        return {"error": "Data not found"}

    # フィルター
    filtered = all_df[all_df['take_profit_pct'] == take_profit_pct].copy()
    if date:
        filtered = filtered[filtered['date'] == date]
    if session:
        filtered = filtered[filtered['session'] == session]

    # ソート・制限
    filtered = filtered.sort_values('date', ascending=False).head(limit)

    return {
        "trades": filtered.to_dict('records'),
        "total": len(filtered),
    }
