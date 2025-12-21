#!/usr/bin/env python3
"""
backfill_static_backtest_v3.py
テクニカル + 財務 + 空売りデータを統合したStatic銘柄分析

3要素スコアリング:
    【テクニカル】(従来通り)
    - RSI < 20:      +25点
    - RSI 20-30:     +15点
    - MA25乖離 < -10%: +20点
    - MA25乖離 < -5%:  +10点
    - ATR 3.5-5%:    +10点
    - 前日 -3%下落:  +15点

    【財務】(JQuants)
    - 直近決算 増収:  +10点
    - 直近決算 増益:  +15点
    - ROE > 10%:     +10点
    - 自己資本比率 > 40%: +5点
    - 決算発表3日以内: -15点 (リスク回避)

    【空売り】(JQuants)
    - 信用倍率 < 2:   +15点 (ショートスクイーズ期待)
    - 信用倍率 < 3:   +10点
    - 空売り残 週次+10%以上増: +10点 (悲観の行き過ぎ)
    - 信用倍率 > 5:   -10点 (楽観の行き過ぎ)

実行方法:
    python improvement/prepare/backfill_static_backtest_v3.py

出力:
    improvement/data/static_v3_analysis.html
    data/parquet/backtest/static_signals_v3_archive.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import numpy as np
import yfinance as yf
from tqdm import tqdm

# JQuants client
from scripts.lib.jquants_client import JQuantsClient

# 出力先
OUTPUT_DIR = ROOT / "improvement" / "data"
OUTPUT_HTML = OUTPUT_DIR / "static_v3_analysis.html"
OUTPUT_PARQUET_DIR = ROOT / "data" / "parquet" / "backtest"
OUTPUT_PARQUET = OUTPUT_PARQUET_DIR / "static_signals_v3_archive.parquet"

# セクター空売りデータ（10年分）
SECTOR_SHORT_FILE = ROOT / "improvement" / "data" / "short_selling_10y.csv"

# Sector33Codeマッピング（JPX標準33業種）
SECTOR33_MAP = {
    50: '水産・農林業',
    1050: '鉱業',
    2050: '建設業',
    3050: '食料品',
    3100: '繊維製品',
    3150: 'パルプ・紙',
    3200: '化学',
    3250: '医薬品',
    3300: '石油・石炭製品',
    3350: 'ゴム製品',
    3400: 'ガラス・土石製品',
    3450: '鉄鋼',
    3500: '非鉄金属',
    3550: '金属製品',
    3600: '機械',
    3650: '電気機器',
    3700: '輸送用機器',
    3750: '精密機器',
    3800: 'その他製品',
    4050: '電気・ガス業',
    5050: '陸運業',
    5100: '海運業',
    5150: '空運業',
    5200: '倉庫・運輸関連業',
    5250: '情報･通信業',
    6050: '卸売業',
    6100: '小売業',
    7050: '銀行業',
    7100: '証券、商品先物取引業',
    7150: '保険業',
    7200: 'その他金融業',
    8050: '不動産業',
    9050: 'サービス業',
    9999: 'その他',
}

# 逆引きマップ（セクター名→コード）表記揺れ対応含む
SECTOR_NAME_TO_CODE = {v: k for k, v in SECTOR33_MAP.items()}
SECTOR_NAME_TO_CODE['電気･ガス業'] = 4050
SECTOR_NAME_TO_CODE['情報・通信業'] = 5250
SECTOR_NAME_TO_CODE['情報･通信業'] = 5250

# Static銘柄リスト
STATIC_STOCKS = {
    '1605.T': ('INPEX', '鉱業'),
    '1766.T': ('東建コーポレーション', '建設業'),
    '1801.T': ('大成建設', '建設業'),
    '1802.T': ('大林組', '建設業'),
    '1803.T': ('清水建設', '建設業'),
    '1812.T': ('鹿島建設', '建設業'),
    '2914.T': ('JT', '食料品'),
    '3382.T': ('セブン&アイ', '小売業'),
    '4063.T': ('信越化学', '化学'),
    '4307.T': ('野村総研', '情報・通信業'),
    '4502.T': ('武田薬品', '医薬品'),
    '4568.T': ('第一三共', '医薬品'),
    '5020.T': ('ENEOS', '石油・石炭製品'),
    '5631.T': ('日本製鋼所', '機械'),
    '6098.T': ('リクルート', 'サービス業'),
    '6367.T': ('ダイキン', '機械'),
    '6501.T': ('日立製作所', '電気機器'),
    '6503.T': ('三菱電機', '電気機器'),
    '6701.T': ('NEC', '電気機器'),
    '6702.T': ('富士通', '電気機器'),
    '6723.T': ('ルネサス', '電気機器'),
    '6758.T': ('ソニーG', '電気機器'),
    '6762.T': ('TDK', '電気機器'),
    '6857.T': ('アドバンテスト', '電気機器'),
    # '6861.T': ('キーエンス', '電気機器'),  # 除外: 株価が高すぎる
    '6920.T': ('レーザーテック', '電気機器'),
    '6946.T': ('日本アビオニクス', '電気機器'),
    '6981.T': ('村田製作所', '電気機器'),
    '7011.T': ('三菱重工', '機械'),
    '7012.T': ('川崎重工', '機械'),
    '7013.T': ('IHI', '機械'),
    '7203.T': ('トヨタ', '輸送用機器'),
    '7267.T': ('ホンダ', '輸送用機器'),
    '7735.T': ('SCREEN', '電気機器'),
    '7741.T': ('HOYA', '精密機器'),
    '7974.T': ('任天堂', 'その他製品'),
    '8001.T': ('伊藤忠', '卸売業'),
    '8031.T': ('三井物産', '卸売業'),
    '8035.T': ('東京エレクトロン', '電気機器'),
    '8058.T': ('三菱商事', '卸売業'),
    '8306.T': ('三菱UFJ', '銀行業'),
    '8316.T': ('三井住友FG', '銀行業'),
    '8411.T': ('みずほFG', '銀行業'),
    '8729.T': ('ソニーFG', '保険業'),
    '8766.T': ('東京海上', '保険業'),
    '9005.T': ('東急', '陸運業'),
    '9021.T': ('JR西日本', '陸運業'),
    '9022.T': ('JR東海', '陸運業'),
    '9432.T': ('NTT', '情報･通信業'),
    '9433.T': ('KDDI', '情報･通信業'),
    '9434.T': ('ソフトバンク', '情報･通信業'),
    '9501.T': ('東京電力', '電気・ガス業'),
    '9503.T': ('関西電力', '電気・ガス業'),
    '9513.T': ('電源開発', '電気・ガス業'),
    '9983.T': ('ファーストリテイリング', '小売業'),
    '9984.T': ('ソフトバンクG', '情報･通信業'),
}


def fetch_stock_data(days: int = 400) -> tuple[pd.DataFrame, pd.DataFrame]:
    """株価データ取得 (yfinance)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    print(f"Fetching stock data from {start_date.date()} to {end_date.date()}...")

    all_data = []
    tickers = list(STATIC_STOCKS.keys())

    for ticker in tqdm(tickers, desc="Stocks"):
        try:
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if len(data) > 0:
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)
                data['ticker'] = ticker
                data = data.reset_index()
                all_data.append(data)
        except Exception as e:
            print(f"[WARN] Failed to fetch {ticker}: {e}")

    stocks_df = pd.concat(all_data, ignore_index=True)
    stocks_df['Date'] = pd.to_datetime(stocks_df['Date'])

    # N225
    print("Fetching N225...")
    n225 = yf.download('^N225', start=start_date, end=end_date, progress=False)
    if isinstance(n225.columns, pd.MultiIndex):
        n225.columns = n225.columns.get_level_values(0)
    n225 = n225.reset_index()
    n225['Date'] = pd.to_datetime(n225['Date'])
    n225['sma5'] = n225['Close'].rolling(5).mean()
    n225['n225_vs_sma5'] = (n225['Close'] - n225['sma5']) / n225['sma5'] * 100

    return stocks_df, n225


def fetch_financial_data(client: JQuantsClient) -> dict:
    """JQuantsから財務データ取得"""
    print("Fetching financial data from JQuants...")
    financials = {}

    for ticker in tqdm(STATIC_STOCKS.keys(), desc="Financials"):
        code = ticker.replace('.T', '').ljust(5, '0')
        try:
            result = client.request("/fins/statements", params={"code": code})
            statements = result.get("statements", [])
            if statements:
                # 最新2期分を取得（増収増益判定用）
                sorted_stmts = sorted(statements, key=lambda x: x.get("DisclosedDate", ""), reverse=True)
                latest = sorted_stmts[0] if len(sorted_stmts) > 0 else None
                previous = sorted_stmts[1] if len(sorted_stmts) > 1 else None

                financials[ticker] = {
                    'latest': latest,
                    'previous': previous,
                }
        except Exception as e:
            print(f"[WARN] Failed to fetch financials for {ticker}: {e}")

    return financials


def fetch_sector_short_data() -> pd.DataFrame:
    """セクター別空売り比率データを読み込み（10年分）"""
    if SECTOR_SHORT_FILE.exists():
        print(f"Loading sector short selling data from {SECTOR_SHORT_FILE}...")
        df = pd.read_csv(SECTOR_SHORT_FILE)
        df['Date'] = pd.to_datetime(df['Date'])
        return df
    else:
        print("[ERROR] Sector short selling data not found!")
        return pd.DataFrame()


def fetch_margin_from_jquants(client: JQuantsClient = None) -> pd.DataFrame:
    """JQuantsから信用残データ取得（過去1年分）"""
    if client is None:
        client = JQuantsClient()

    all_data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=400)

    for ticker in tqdm(STATIC_STOCKS.keys(), desc="Margin"):
        code = ticker.replace('.T', '').ljust(5, '0')
        try:
            result = client.request(
                "/markets/weekly_margin_interest",
                params={
                    "code": code,
                    "from": start_date.strftime('%Y-%m-%d'),
                    "to": end_date.strftime('%Y-%m-%d'),
                }
            )
            data = result.get("weekly_margin_interest", [])
            for row in data:
                row['original_code'] = int(ticker.replace('.T', ''))
                all_data.append(row)
        except Exception as e:
            print(f"[WARN] Failed to fetch margin for {ticker}: {e}")

    if all_data:
        df = pd.DataFrame(all_data)
        df['Date'] = pd.to_datetime(df['Date'])

        # 信用倍率と変化率を計算
        df = df.sort_values(['original_code', 'Date'])

        # 信用倍率 = 買残 / 売残
        df['margin_ratio'] = df['LongMarginTradeVolume'] / df['ShortMarginTradeVolume']
        df['margin_ratio'] = df['margin_ratio'].replace([np.inf, -np.inf], np.nan)

        # 週次変化率
        df['short_change_pct'] = df.groupby('original_code')['ShortMarginTradeVolume'].pct_change() * 100
        df['long_change_pct'] = df.groupby('original_code')['LongMarginTradeVolume'].pct_change() * 100

        return df

    return pd.DataFrame()


def calculate_technical_indicators(
    stocks_df: pd.DataFrame,
    ticker: str,
    target_date: pd.Timestamp,
    lookback: int = 30
) -> dict | None:
    """テクニカル指標計算"""
    ticker_df = stocks_df[
        (stocks_df['ticker'] == ticker) &
        (stocks_df['Date'] <= target_date)
    ].copy().tail(lookback + 5)

    if len(ticker_df) < lookback:
        return None

    ticker_df = ticker_df.sort_values('Date').reset_index(drop=True)

    close = ticker_df['Close']
    high = ticker_df['High']
    low = ticker_df['Low']

    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    # MA25
    ma25 = close.rolling(25).mean()
    ma25_deviation = (close - ma25) / ma25 * 100

    # ATR%
    high_low = high - low
    high_close = abs(high - close.shift())
    low_close = abs(low - close.shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    atr_pct = atr / close * 100

    # 前日騰落率
    daily_change = close.pct_change() * 100

    return {
        'close': float(close.iloc[-1]),
        'rsi_14d': float(rsi.iloc[-1]),
        'ma25_deviation': float(ma25_deviation.iloc[-1]),
        'atr_pct': float(atr_pct.iloc[-1]),
        'daily_change': float(daily_change.iloc[-1]),
    }


def calculate_technical_score(indicators: dict, market: str) -> tuple[int, list]:
    """テクニカルスコア計算"""
    score = 0
    details = []

    rsi = indicators['rsi_14d']
    if rsi < 20:
        score += 25
        details.append(f"RSI<20: +25")
    elif rsi < 30:
        score += 15
        details.append(f"RSI<30: +15")
    elif rsi > 70:
        score -= 15
        details.append(f"RSI>70: -15")

    ma_dev = indicators['ma25_deviation']
    if ma_dev < -10:
        score += 20
        details.append(f"MA乖離<-10%: +20")
    elif ma_dev < -5:
        score += 10
        details.append(f"MA乖離<-5%: +10")
    elif ma_dev > 5:
        score -= 10
        details.append(f"MA乖離>5%: -10")

    atr = indicators['atr_pct']
    if 3.5 <= atr <= 5:
        score += 10
        details.append(f"ATR3.5-5%: +10")
    elif atr > 7:
        score -= 10
        details.append(f"ATR>7%: -10")

    if market == 'YELLOW':
        score += 10
        details.append(f"市場YELLOW: +10")
    elif market == 'RED':
        score -= 5
        details.append(f"市場RED: -5")

    change = indicators['daily_change']
    if change < -3:
        score += 15
        details.append(f"前日-3%: +15")
    elif change > 5:
        score -= 10
        details.append(f"前日+5%: -10")

    return score, details


def calculate_financial_score(
    financials: dict,
    ticker: str,
    target_date: pd.Timestamp
) -> tuple[int, list, dict]:
    """財務スコア計算"""
    score = 0
    details = []
    fin_data = {}

    if ticker not in financials:
        return score, details, fin_data

    fin = financials[ticker]
    latest = fin.get('latest')
    previous = fin.get('previous')

    if not latest:
        return score, details, fin_data

    # 決算発表日との距離
    disclosed_date = latest.get('DisclosedDate', '')
    if disclosed_date:
        disc_dt = pd.to_datetime(disclosed_date)
        days_since = (target_date - disc_dt).days
        fin_data['days_since_earnings'] = days_since

        # 決算発表3日以内はリスク回避
        if 0 <= days_since <= 3:
            score -= 15
            details.append(f"決算直後: -15")
        # 決算発表前3日以内もリスク
        elif -3 <= days_since < 0:
            score -= 15
            details.append(f"決算直前: -15")

    # 増収増益判定
    if latest and previous:
        try:
            latest_sales = float(latest.get('NetSales', 0) or 0)
            prev_sales = float(previous.get('NetSales', 0) or 0)
            latest_profit = float(latest.get('OperatingProfit', 0) or 0)
            prev_profit = float(previous.get('OperatingProfit', 0) or 0)

            if latest_sales > 0 and prev_sales > 0 and latest_sales > prev_sales:
                score += 10
                details.append(f"増収: +10")
                fin_data['revenue_growth'] = True
            else:
                fin_data['revenue_growth'] = False

            if latest_profit > 0 and prev_profit > 0 and latest_profit > prev_profit:
                score += 15
                details.append(f"増益: +15")
                fin_data['profit_growth'] = True
            else:
                fin_data['profit_growth'] = False
        except (ValueError, TypeError):
            fin_data['revenue_growth'] = None
            fin_data['profit_growth'] = None

    # ROE
    roe = latest.get('ROE')
    if roe:
        try:
            roe_val = float(roe)
            fin_data['roe'] = roe_val
            if roe_val > 10:
                score += 10
                details.append(f"ROE>{roe_val:.1f}%: +10")
        except (ValueError, TypeError):
            pass

    # 自己資本比率
    equity_ratio = latest.get('EquityToAssetRatio')
    if equity_ratio:
        try:
            eq_val = float(equity_ratio)
            fin_data['equity_ratio'] = eq_val
            if eq_val > 40:
                score += 5
                details.append(f"自己資本>{eq_val:.0f}%: +5")
        except (ValueError, TypeError):
            pass

    return score, details, fin_data


def calculate_margin_score(
    sector_short_df: pd.DataFrame,
    sector_name: str,
    target_date: pd.Timestamp
) -> tuple[int, list, dict]:
    """セクター空売り比率スコア計算"""
    score = 0
    details = []
    margin_data = {}

    if sector_short_df.empty:
        return score, details, margin_data

    # セクター名→Sector33Code
    sector_code = SECTOR_NAME_TO_CODE.get(sector_name)
    if sector_code is None:
        return score, details, margin_data

    # target_date以前の最新データを取得
    sector_data = sector_short_df[
        (sector_short_df['Sector33Code'] == sector_code) &
        (sector_short_df['Date'] <= target_date)
    ].sort_values('Date', ascending=False)

    if sector_data.empty:
        return score, details, margin_data

    latest = sector_data.iloc[0]
    short_ratio = latest.get('short_ratio')

    if pd.isna(short_ratio):
        return score, details, margin_data

    margin_data['sector_code'] = sector_code
    margin_data['short_ratio'] = float(short_ratio)
    margin_data['short_data_date'] = latest['Date'].strftime('%Y-%m-%d')

    # 空売り比率スコアリング
    # 平均約42%、高いほど悲観的→反発期待
    if short_ratio > 50:
        score += 15
        details.append(f"高空売り比率({short_ratio:.0f}%): +15")
    elif short_ratio > 45:
        score += 10
        details.append(f"やや高空売り({short_ratio:.0f}%): +10")
    elif short_ratio < 35:
        score -= 10
        details.append(f"低空売り比率({short_ratio:.0f}%): -10")

    return score, details, margin_data


def get_market_condition(n225_vs_sma5: float) -> str:
    if n225_vs_sma5 > 0:
        return 'GREEN'
    elif n225_vs_sma5 > -1:
        return 'YELLOW'
    else:
        return 'RED'


def get_signal(score: int) -> str:
    if score >= 60:
        return 'STRONG_BUY'
    elif score >= 30:
        return 'BUY'
    elif score >= -10:
        return 'HOLD'
    elif score >= -30:
        return 'SELL'
    else:
        return 'STRONG_SELL'


def calculate_nday_return(
    stocks_df: pd.DataFrame,
    ticker: str,
    signal_date: pd.Timestamp,
    n_days: int
) -> dict | None:
    """N日後リターン計算"""
    ticker_df = stocks_df[
        (stocks_df['ticker'] == ticker) &
        (stocks_df['Date'] > signal_date)
    ].sort_values('Date').head(n_days + 1)

    if len(ticker_df) < 2:
        return None

    entry_row = ticker_df.iloc[0]
    entry_price = float(entry_row['Open'])

    if len(ticker_df) < n_days + 1:
        return None

    exit_row = ticker_df.iloc[n_days]
    exit_price = float(exit_row['Close'])

    return_pct = (exit_price - entry_price) / entry_price * 100
    profit_100 = (exit_price - entry_price) * 100
    win = return_pct > 0

    return {
        'entry_price': entry_price,
        'exit_price': exit_price,
        'return_pct': return_pct,
        'profit_100': profit_100,
        'win': win,
    }


def generate_html_report(df: pd.DataFrame, output_path: Path):
    """HTML形式のレポート生成"""
    # 集計
    summary_data = []
    for signal in ['STRONG_BUY', 'BUY', 'HOLD']:
        subset = df[df['signal'] == signal]
        if len(subset) == 0:
            continue

        for period, col_return, col_win in [
            ('1日', 'return_1d', 'win_1d'),
            ('5日', 'return_5d', 'win_5d'),
        ]:
            valid = subset[subset[col_win].notna()]
            if len(valid) > 0:
                summary_data.append({
                    'signal': signal,
                    'period': period,
                    'count': len(valid),
                    'win_rate': valid[col_win].mean() * 100,
                    'avg_return': valid[col_return].mean(),
                    'total_profit': valid[f'profit_100_{period[0]}d'].sum() if f'profit_100_{period[0]}d' in valid.columns else 0,
                })

    # スコア分解別集計
    score_breakdown = df.groupby('signal').agg({
        'tech_score': 'mean',
        'fin_score': 'mean',
        'margin_score': 'mean',
        'total_score': 'mean',
    }).round(1)

    # 直近シグナル（STRONG_BUYのみ）はHTML生成時に取得

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Static銘柄 v3分析 (テクニカル+財務+空売り)</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }}
        h1 {{ color: #00d4ff; }}
        h2 {{ color: #ff6b6b; border-bottom: 1px solid #444; padding-bottom: 8px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #444; padding: 8px 12px; text-align: left; }}
        th {{ background: #2d2d44; color: #00d4ff; }}
        tr:nth-child(even) {{ background: #242438; }}
        tr:hover {{ background: #3d3d5c; }}
        .positive {{ color: #00ff88; }}
        .negative {{ color: #ff4444; }}
        .strong-buy {{ background: #1a4d1a !important; }}
        .buy {{ background: #2d4d2d !important; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
        .badge-tech {{ background: #4a90d9; }}
        .badge-fin {{ background: #d94a90; }}
        .badge-margin {{ background: #90d94a; }}
        .score-detail {{ font-size: 11px; color: #aaa; }}
        .summary-card {{ background: #2d2d44; padding: 20px; border-radius: 8px; margin: 10px 0; display: inline-block; min-width: 200px; }}
        .summary-value {{ font-size: 24px; font-weight: bold; color: #00d4ff; }}
        .r {{ text-align: right; }}
        .c {{ text-align: center; }}
    </style>
</head>
<body>
    <h1>Static銘柄 v3分析レポート</h1>
    <p>生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
    <p>期間: {df['signal_date'].min()} 〜 {df['signal_date'].max()}</p>

    <h2>サマリー</h2>
    <div>
        <div class="summary-card">
            <div>総シグナル数</div>
            <div class="summary-value">{len(df):,}</div>
        </div>
        <div class="summary-card">
            <div>STRONG_BUY数</div>
            <div class="summary-value">{len(df[df['signal'] == 'STRONG_BUY']):,}</div>
        </div>
        <div class="summary-card">
            <div>BUY数</div>
            <div class="summary-value">{len(df[df['signal'] == 'BUY']):,}</div>
        </div>
    </div>

    <h2>シグナル別パフォーマンス</h2>
    <table>
        <tr>
            <th>シグナル</th>
            <th>保有期間</th>
            <th class="r">件数</th>
            <th class="r">勝率</th>
            <th class="r">平均リターン</th>
            <th class="r">合計利益(100株)</th>
        </tr>
"""
    for row in summary_data:
        win_class = 'positive r' if row['win_rate'] >= 60 else ('negative r' if row['win_rate'] < 50 else 'r')
        ret_class = 'positive r' if row['avg_return'] > 0 else 'negative r'
        html += f"""        <tr>
            <td>{row['signal']}</td>
            <td>{row['period']}</td>
            <td class="r">{row['count']}</td>
            <td class="{win_class}">{row['win_rate']:.1f}%</td>
            <td class="{ret_class}">{row['avg_return']:+.2f}%</td>
            <td class="r">{row['total_profit']:,.0f}円</td>
        </tr>
"""

    html += """    </table>

    <h2>スコア分解（平均）</h2>
    <table>
        <tr>
            <th>シグナル</th>
            <th class="r"><span class="badge badge-tech">Tech</span></th>
            <th class="r"><span class="badge badge-fin">Fin</span></th>
            <th class="r"><span class="badge badge-margin">Margin</span></th>
            <th class="r">Total</th>
        </tr>
"""
    for signal, row in score_breakdown.iterrows():
        html += f"""        <tr>
            <td>{signal}</td>
            <td class="r">{row['tech_score']:.1f}</td>
            <td class="r">{row['fin_score']:.1f}</td>
            <td class="r">{row['margin_score']:.1f}</td>
            <td class="r"><strong>{row['total_score']:.1f}</strong></td>
        </tr>
"""

    html += """    </table>

    <h2>直近STRONG_BUYシグナル (30件)</h2>
    <table>
        <tr>
            <th>日付</th>
            <th>銘柄</th>
            <th class="r">Total</th>
            <th class="r">始値</th>
            <th class="r">終値(1日)</th>
            <th class="r">1日利益</th>
            <th class="c">1日</th>
            <th class="r">終値(5日)</th>
            <th class="r">5日利益</th>
            <th class="c">5日</th>
        </tr>
"""
    # STRONG_BUYのみ30件
    recent_strong = df[df['signal'] == 'STRONG_BUY'].sort_values('signal_date', ascending=False).head(30)

    for _, row in recent_strong.iterrows():
        # 始値（エントリー価格）
        entry_price = row.get('entry_price', 0)
        entry_str = f"{entry_price:,.0f}円" if pd.notna(entry_price) and entry_price > 0 else '-'

        # 終値(1日後) と 100株利益
        profit_1d = row.get('profit_100_1d', 0)
        if pd.notna(profit_1d) and pd.notna(entry_price) and entry_price > 0:
            exit_price_1d = entry_price + profit_1d / 100
            exit_1d_str = f"{exit_price_1d:,.0f}円"
            profit_1d_str = f"{profit_1d:+,.0f}円"
            profit_1d_class = 'r positive' if profit_1d > 0 else 'r negative'
        else:
            exit_1d_str = '-'
            profit_1d_str = '-'
            profit_1d_class = 'r'

        # 1日勝敗
        win_1d = row.get('win_1d')
        if pd.notna(win_1d):
            win_1d_str = '○' if win_1d else '×'
            win_1d_class = 'c positive' if win_1d else 'c negative'
        else:
            win_1d_str = '-'
            win_1d_class = 'c'

        # 終値(5日後) と 100株利益
        profit_5d = row.get('profit_100_5d', 0)
        if pd.notna(profit_5d) and pd.notna(entry_price) and entry_price > 0:
            exit_price_5d = entry_price + profit_5d / 100
            exit_5d_str = f"{exit_price_5d:,.0f}円"
            profit_5d_str = f"{profit_5d:+,.0f}円"
            profit_5d_class = 'r positive' if profit_5d > 0 else 'r negative'
        else:
            exit_5d_str = '-'
            profit_5d_str = '-'
            profit_5d_class = 'r'

        # 5日勝敗
        win_5d = row.get('win_5d')
        if pd.notna(win_5d):
            win_5d_str = '○' if win_5d else '×'
            win_5d_class = 'c positive' if win_5d else 'c negative'
        else:
            win_5d_str = '-'
            win_5d_class = 'c'

        html += f"""        <tr class="strong-buy">
            <td>{row['signal_date']}</td>
            <td>{row['ticker']} {row['stock_name']}</td>
            <td class="r"><strong>{row['total_score']}</strong></td>
            <td class="r">{entry_str}</td>
            <td class="r">{exit_1d_str}</td>
            <td class="{profit_1d_class}">{profit_1d_str}</td>
            <td class="{win_1d_class}">{win_1d_str}</td>
            <td class="r">{exit_5d_str}</td>
            <td class="{profit_5d_class}">{profit_5d_str}</td>
            <td class="{win_5d_class}">{win_5d_str}</td>
        </tr>
"""

    html += """    </table>
</body>
</html>
"""

    output_path.write_text(html, encoding='utf-8')
    print(f"HTML report saved to: {output_path}")


def main():
    print("=" * 60)
    print("Static Signals v3 (Technical + Financial + Margin)")
    print("=" * 60)

    # 1. 株価データ取得
    stocks_df, n225_df = fetch_stock_data(days=400)
    print(f"Stocks data: {len(stocks_df)} rows")

    # 2. JQuants財務データ取得
    try:
        client = JQuantsClient()
        financials = fetch_financial_data(client)
        print(f"Financial data: {len(financials)} tickers")
    except Exception as e:
        print(f"[WARN] JQuants financial fetch failed: {e}")
        financials = {}

    # 3. セクター空売りデータ取得（10年分）
    sector_short_df = fetch_sector_short_data()
    print(f"Sector short data: {len(sector_short_df)} rows")

    # 4. シグナル計算
    all_dates = sorted(stocks_df['Date'].unique())
    target_dates = [d for d in all_dates[30:]]
    print(f"Target dates: {len(target_dates)} days")

    results = []

    for target_date in tqdm(target_dates, desc="Processing"):
        target_date = pd.Timestamp(target_date)

        n225_row = n225_df[n225_df['Date'] == target_date]
        if n225_row.empty:
            continue

        n225_vs_sma5 = float(n225_row['n225_vs_sma5'].iloc[0])
        if pd.isna(n225_vs_sma5):
            continue

        market = get_market_condition(n225_vs_sma5)

        for ticker, (name, sector) in STATIC_STOCKS.items():
            # テクニカル
            indicators = calculate_technical_indicators(stocks_df, ticker, target_date)
            if indicators is None:
                continue

            # 株価20,000円以上は除外
            if indicators['close'] >= 20000:
                continue

            tech_score, tech_details = calculate_technical_score(indicators, market)

            # 財務
            fin_score, fin_details, fin_data = calculate_financial_score(financials, ticker, target_date)

            # セクター空売り
            margin_score, margin_details, margin_data = calculate_margin_score(sector_short_df, sector, target_date)

            # 合計スコア
            total_score = tech_score + fin_score + margin_score
            signal = get_signal(total_score)

            # STRONG_BUY, BUY のみ保存
            if signal not in ['STRONG_BUY', 'BUY']:
                continue

            # リターン計算
            ret_1d = calculate_nday_return(stocks_df, ticker, target_date, 1)
            ret_5d = calculate_nday_return(stocks_df, ticker, target_date, 5)

            if ret_1d is None:
                continue

            result = {
                'signal_date': target_date.strftime('%Y-%m-%d'),
                'ticker': ticker,
                'stock_name': name,
                'sector': sector,
                'signal': signal,
                'total_score': total_score,
                'tech_score': tech_score,
                'fin_score': fin_score,
                'margin_score': margin_score,
                'market_condition': market,
                'n225_vs_sma5': n225_vs_sma5,
                # テクニカル指標
                'close': indicators['close'],
                'rsi_14d': indicators['rsi_14d'],
                'ma25_deviation': indicators['ma25_deviation'],
                'atr_pct': indicators['atr_pct'],
                'daily_change': indicators['daily_change'],
                # 財務指標
                'roe': fin_data.get('roe'),
                'equity_ratio': fin_data.get('equity_ratio'),
                'revenue_growth': fin_data.get('revenue_growth'),
                'profit_growth': fin_data.get('profit_growth'),
                # 空売り指標
                'short_ratio': margin_data.get('short_ratio'),
                'short_change_pct': margin_data.get('short_change_pct'),
                # リターン
                'entry_price': ret_1d['entry_price'],
                'return_1d': ret_1d['return_pct'],
                'profit_100_1d': ret_1d['profit_100'],
                'win_1d': ret_1d['win'],
                'return_5d': ret_5d['return_pct'] if ret_5d else None,
                'profit_100_5d': ret_5d['profit_100'] if ret_5d else None,
                'win_5d': ret_5d['win'] if ret_5d else None,
                # 詳細
                'tech_details': ', '.join(tech_details),
                'fin_details': ', '.join(fin_details),
                'margin_details': ', '.join(margin_details),
            }

            results.append(result)

    df = pd.DataFrame(results)

    # 5. サマリー表示
    print("\n" + "=" * 60)
    print("Results Summary")
    print("=" * 60)

    for signal in ['STRONG_BUY', 'BUY']:
        subset = df[df['signal'] == signal]
        if len(subset) == 0:
            continue

        print(f"\n{signal}: {len(subset)}件")
        print(f"  平均スコア: Tech={subset['tech_score'].mean():.1f}, "
              f"Fin={subset['fin_score'].mean():.1f}, "
              f"Margin={subset['margin_score'].mean():.1f}")

        for period, col_win in [('1日', 'win_1d'), ('5日', 'win_5d')]:
            valid = subset[subset[col_win].notna()]
            if len(valid) > 0:
                win_rate = valid[col_win].mean() * 100
                avg_ret = valid[f'return_{period[0]}d'].mean()
                print(f"  {period}: 勝率={win_rate:.1f}%, 平均={avg_ret:+.2f}%")

    # 6. 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    generate_html_report(df, OUTPUT_HTML)
    df.to_parquet(OUTPUT_PARQUET, index=False)

    print("\n" + "=" * 60)
    print(f"HTML: {OUTPUT_HTML}")
    print(f"Parquet: {OUTPUT_PARQUET}")
    print(f"Total records: {len(df)}")
    print("=" * 60)

    return df


if __name__ == "__main__":
    main()
