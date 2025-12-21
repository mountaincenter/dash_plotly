#!/usr/bin/env python3
"""
generate_swing_analysis_html.py
GROK銘柄のデイスイング分析HTMLレポート生成（ライトテーマ）

スタイル: improvement/archive/ の紫グラデーションテーマ準拠
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# 最適価格帯の定義
OPTIMAL_PRICE_RANGES = {
    '買い': {
        'day': [(5000, 7500)],      # 当日決済向け
        'swing': [(300, 500)],       # スイング向け
        'avoid': [(1000, 2000), (3000, 5000)],  # 避けるべき
    },
    '静観': {
        'day': [(300, 500)],         # 当日決済向け（当日のみ）
        'swing': [(0, 300), (1500, 3000)],  # スイング向け
        'avoid': [(3000, 5000)],
    },
    '売り': {
        'day': [(2000, 3000), (15000, 999999)],  # 当日決済向け
        'swing': [(2000, 3000)],     # スイング向け
        'avoid': [],
    },
}

# 価格帯区分（サマリー表示用）
PRICE_BRACKETS = [
    (0, 300, '0-300円'),
    (300, 500, '300-500円'),
    (500, 1000, '500-1,000円'),
    (1000, 1500, '1,000-1,500円'),
    (1500, 2000, '1,500-2,000円'),
    (2000, 3000, '2,000-3,000円'),
    (3000, 5000, '3,000-5,000円'),
    (5000, 7500, '5,000-7,500円'),
    (7500, 10000, '7,500-10,000円'),
    (10000, 15000, '10,000-15,000円'),
    (15000, 999999, '15,000円以上'),
]


def get_price_bracket(price):
    """価格帯ラベルを取得"""
    if pd.isna(price):
        return None
    for low, high, label in PRICE_BRACKETS:
        if low <= price < high:
            return label
    return None


def get_price_range_status(action, price):
    """価格帯の推奨ステータスを返す"""
    if pd.isna(price):
        return 'unknown'

    ranges = OPTIMAL_PRICE_RANGES.get(action, {})

    # 当日向けチェック
    for low, high in ranges.get('day', []):
        if low <= price < high:
            return 'optimal_day'

    # スイング向けチェック
    for low, high in ranges.get('swing', []):
        if low <= price < high:
            return 'optimal_swing'

    # 避けるべきチェック
    for low, high in ranges.get('avoid', []):
        if low <= price < high:
            return 'avoid'

    return 'neutral'


def apply_v2_1_0_1_strategy(row):
    """
    V2.1.0.1 ハイブリッド戦略を適用
    - v2.0.3が買い & v2.1が静観 → 静観
    - v2.0.3が静観 & v2.1が売り → 売り
    - それ以外 → v2.0.3のアクション
    """
    v2_0_3_action = row['v2_0_3_action']
    v2_1_action = row['v2_1_action']

    if v2_0_3_action == '買い' and v2_1_action == '静観':
        return '静観'
    elif v2_0_3_action == '静観' and v2_1_action == '売り':
        return '売り'
    else:
        return v2_0_3_action


def apply_v3_strategy(row):
    """
    v3.0 戦略: シグナル + 価格帯 → アクション + 保有期間

    Returns:
        tuple: (action, holding_days)
        - action: '買い', '売り', '静観'
        - holding_days: 0 (当日), 5 (5日保有)
    """
    # v2.1のシグナルをベースに使用（trading_recommendation_v2_1.py と同じロジック）
    base_action = row.get('v2_1_action', row.get('v2_0_3_action', '静観'))
    price = row.get('prev_day_close', row.get('buy_price', 0))

    if pd.isna(price) or price <= 0:
        return base_action, 0

    # 買いシグナル
    if base_action == '買い':
        if 7500 <= price < 10000:
            return '買い', 5  # スイング推奨
        elif 5000 <= price < 7500:
            return '買い', 0  # 当日決済
        else:
            return '買い', 0  # デフォルト当日

    # 静観シグナル
    elif base_action == '静観':
        if 1500 <= price < 3000:
            return '買い', 5  # 静観だけど買い5に変更
        else:
            return '静観', 0  # そのまま静観

    # 売りシグナル
    elif base_action == '売り':
        if 2000 <= price < 10000:
            return '売り', 5  # スイング推奨（中価格帯）
        else:
            return '売り', 0  # 当日決済（低価格・高価格帯）

    return base_action, 0


def load_data():
    """データ読み込み"""
    grok = pd.read_parquet(DATA_DIR / "grok_analysis_merged_v2_1.parquet")
    prices = pd.read_parquet(DATA_DIR / "prices_max_1d.parquet")
    prices['date'] = pd.to_datetime(prices['date']).dt.date

    # 異常値銘柄を除外
    EXCLUDE_TICKERS = ['4570.T']  # 免疫生物研究所（HIV特許で10倍急騰）
    excluded = grok[grok['ticker'].isin(EXCLUDE_TICKERS)]
    if len(excluded) > 0:
        print(f"⚠️ 異常値除外: {EXCLUDE_TICKERS} ({len(excluded)}件)")
    grok = grok[~grok['ticker'].isin(EXCLUDE_TICKERS)]

    # V2.1.0.1 ハイブリッド戦略カラム追加
    grok['v2_1_0_1_action'] = grok.apply(apply_v2_1_0_1_strategy, axis=1)

    # MarginCodeマスター読み込み（信用取引制限チェック用）
    margin_path = DATA_DIR / "margin_code_master.parquet"
    if margin_path.exists():
        margin_df = pd.read_parquet(margin_path)
        # margin_codeを文字列で保持
        margin_map = margin_df.set_index('ticker')['margin_code'].to_dict()
        grok['margin_code'] = grok['ticker'].map(margin_map).fillna('2')
        print(f"📊 MarginCode: 貸借={len(grok[grok['margin_code']=='2'])}件, 信用={len(grok[grok['margin_code']=='1'])}件, その他={len(grok[grok['margin_code']=='3'])}件")
    else:
        grok['margin_code'] = '2'  # デフォルトは貸借（全取引可）
        print("⚠️ MarginCodeマスターなし（全銘柄取引可として処理）")

    # 日証金制限データ読み込み（申込停止銘柄）
    jsf_path = BASE_DIR.parent / "data" / "parquet" / "jsf_seigenichiran.csv"
    jsf_stop_codes = set()
    if jsf_path.exists():
        try:
            jsf = pd.read_csv(jsf_path, skiprows=4)
            jsf_stop_codes = set(jsf[jsf['実施措置'] == '申込停止']['銘柄コード'].astype(str))
            grok['jsf_restricted'] = grok['ticker'].str.replace('.T', '').isin(jsf_stop_codes)
            print(f"📊 日証金申込停止: {len(jsf_stop_codes)}銘柄（うちGROK対象: {grok['jsf_restricted'].sum()}件）")
        except Exception as e:
            print(f"⚠️ 日証金CSV読み込みエラー: {e}")
            grok['jsf_restricted'] = False
    else:
        grok['jsf_restricted'] = False
        print("⚠️ 日証金CSVなし（制限なしとして処理）")

    # 5分足分析データ読み込み（9時利確用）
    m5_path = OUTPUT_DIR / "grok_5min_analysis.csv"
    if m5_path.exists():
        m5 = pd.read_csv(m5_path)
        m5['date'] = pd.to_datetime(m5['date'])
        grok['backtest_date'] = pd.to_datetime(grok['backtest_date'])
        # 9時利確データをマージ
        grok = grok.merge(
            m5[['ticker', 'date', 'high_9_pct', 'low_9_pct', 'is_yoriten', 'is_yorisoko']],
            left_on=['ticker', 'backtest_date'],
            right_on=['ticker', 'date'],
            how='left'
        )
        grok = grok.drop(columns=['date'], errors='ignore')
        print(f"📊 9時利確データ: {grok['high_9_pct'].notna().sum()}件マージ")
    else:
        grok['high_9_pct'] = None
        grok['low_9_pct'] = None
        grok['is_yoriten'] = None
        grok['is_yorisoko'] = None
        print("⚠️ 5分足分析データなし")

    return grok, prices, jsf_stop_codes


def get_future_prices(prices_df, ticker, start_date, days_list=[1, 2, 3, 4, 5]):
    """指定日から n 営業日後の終値を取得"""
    ticker_prices = prices_df[prices_df['ticker'] == ticker].copy()
    ticker_prices = ticker_prices.sort_values('date')

    # 日付を正規化（文字列比較で統一）
    if isinstance(start_date, str):
        start_date_str = start_date[:10]
    else:
        start_date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')

    # 日付を文字列に変換してリスト化
    dates = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10] for d in ticker_prices['date'].tolist()]
    try:
        start_idx = dates.index(start_date_str)
    except ValueError:
        return {d: None for d in days_list}

    result = {}
    closes = ticker_prices['Close'].tolist()
    for days in days_list:
        target_idx = start_idx + days
        if target_idx < len(closes):
            result[days] = closes[target_idx]
        else:
            result[days] = None
    return result


def calculate_swing_returns(grok_df, prices_df, action_col='v2_1_action'):
    """スイングトレード損益計算（信用取引制限を考慮）"""
    results = []

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        backtest_date = row['backtest_date']
        buy_price = row['buy_price']
        sell_price = row.get('sell_price', row.get('daily_close'))
        action = row.get(action_col, '静観')
        margin_code = row.get('margin_code', 2)  # デフォルトは貸借（全取引可）

        if pd.isna(buy_price) or buy_price <= 0:
            continue

        stock_name = row.get('stock_name') or row.get('company_name') or ticker
        if pd.isna(stock_name) or stock_name == '':
            stock_name = ticker

        future_closes = get_future_prices(prices_df, ticker, backtest_date)

        # 信用取引制限チェック
        # margin_code: '1'=信用（空売りNG）, '2'=貸借（全OK）, '3'=その他（信用取引NG）
        # jsf_restricted: 日証金申込停止銘柄
        jsf_restricted = row.get('jsf_restricted', False)
        can_trade = True
        margin_restricted = False
        if margin_code == '3' or margin_code == 3:
            # その他 → 信用取引不可
            can_trade = False
            margin_restricted = True
        elif (margin_code == '1' or margin_code == 1) and action == '売り':
            # 信用のみ銘柄で売りシグナル → 空売り不可
            can_trade = False
            margin_restricted = True
        elif action == '売り' and jsf_restricted:
            # 日証金申込停止銘柄で売りシグナル → 空売り不可
            can_trade = False
            margin_restricted = True

        # 前日終値（価格帯判定用）
        price_level = row.get('prev_day_close', buy_price)
        price_range_status = get_price_range_status(action, price_level)

        record = {
            'backtest_date': row['backtest_date'],
            'ticker': ticker,
            'stock_name': stock_name,
            'action': action,
            'buy_price': buy_price,
            'price_level': price_level,
            'price_range_status': price_range_status,
            'margin_code': margin_code,
            'margin_restricted': margin_restricted,
        }

        # 損益計算（取引可能な場合のみ）
        if can_trade:
            # 当日損益（100株あたり円）- Phase2: 寄付→大引け なので daily_close を使用
            daily_close = row.get('daily_close')
            if daily_close and not pd.isna(daily_close):
                if action == '売り':
                    day0_profit = (buy_price - daily_close) * 100
                else:
                    day0_profit = (daily_close - buy_price) * 100
                record['day0_profit'] = day0_profit
            else:
                record['day0_profit'] = None

            # 1-5日後の損益
            for days in [1, 2, 3, 4, 5]:
                close_price = future_closes.get(days)
                if close_price is not None and close_price > 0:
                    if action == '売り':
                        profit = (buy_price - close_price) * 100
                    else:
                        profit = (close_price - buy_price) * 100
                    record[f'day{days}_profit'] = profit
                else:
                    record[f'day{days}_profit'] = None
        else:
            # 取引不可 → 損益データなし（シグナルは残す）
            record['day0_profit'] = None
            for days in [1, 2, 3, 4, 5]:
                record[f'day{days}_profit'] = None

        results.append(record)

    return pd.DataFrame(results)


def calculate_v3_returns(grok_df, prices_df):
    """
    v3.0戦略の損益計算
    - 保有期間は戦略が決定（当日 or 5日）
    - 全日（0-5日）の損益を計算
    """
    results = []

    for idx, row in grok_df.iterrows():
        ticker = row['ticker']
        backtest_date = row['backtest_date']
        buy_price = row['buy_price']
        margin_code = row.get('margin_code', 2)

        if pd.isna(buy_price) or buy_price <= 0:
            continue

        stock_name = row.get('stock_name') or row.get('company_name') or ticker
        if pd.isna(stock_name) or stock_name == '':
            stock_name = ticker

        # v3戦略適用
        v3_action, holding_days = apply_v3_strategy(row)

        # 信用取引制限チェック
        # margin_code: '1'=信用（空売りNG）, '2'=貸借（全OK）, '3'=その他（信用取引NG）
        # jsf_restricted: 日証金申込停止銘柄
        jsf_restricted = row.get('jsf_restricted', False)
        can_trade = True
        margin_restricted = False
        if margin_code == '3' or margin_code == 3:
            can_trade = False
            margin_restricted = True
        elif (margin_code == '1' or margin_code == 1) and v3_action == '売り':
            can_trade = False
            margin_restricted = True
        elif v3_action == '売り' and jsf_restricted:
            # 日証金申込停止銘柄で売りシグナル → 空売り不可
            can_trade = False
            margin_restricted = True

        # 前日終値
        price_level = row.get('prev_day_close', buy_price)

        # v3アクションラベル生成
        if holding_days == 5:
            v3_label = f"{v3_action}5"
        else:
            v3_label = v3_action

        record = {
            'backtest_date': row['backtest_date'],
            'ticker': ticker,
            'stock_name': stock_name,
            'base_action': row.get('v2_1_0_1_action', '静観'),
            'v3_action': v3_action,
            'v3_label': v3_label,
            'holding_days': holding_days,
            'buy_price': buy_price,
            'price_level': price_level,
            'margin_code': margin_code,
            'margin_restricted': margin_restricted,
            'is_yoriten': row.get('is_yoriten'),
            'is_yorisoko': row.get('is_yorisoko'),
        }

        # 損益計算（取引可能な場合のみ）
        if can_trade:
            future_closes = get_future_prices(prices_df, ticker, backtest_date)
            daily_close = row.get('daily_close')

            # 9時利確損益（寄付き→9時高値/安値）
            high_9_pct = row.get('high_9_pct')
            low_9_pct = row.get('low_9_pct')
            if v3_action == '売り':
                # 売りの場合: 寄付きで空売り→9時安値で買い戻し
                if low_9_pct is not None and not pd.isna(low_9_pct):
                    record['day9am_profit'] = buy_price * (-low_9_pct) / 100 * 100
                else:
                    record['day9am_profit'] = None
            else:
                # 買いの場合: 寄付きで買い→9時高値で売り
                if high_9_pct is not None and not pd.isna(high_9_pct):
                    record['day9am_profit'] = buy_price * high_9_pct / 100 * 100
                else:
                    record['day9am_profit'] = None

            # 当日損益
            if daily_close and not pd.isna(daily_close):
                if v3_action == '売り':
                    record['day0_profit'] = (buy_price - daily_close) * 100
                else:
                    record['day0_profit'] = (daily_close - buy_price) * 100
            else:
                record['day0_profit'] = None

            # 1-5日後の損益
            for days in [1, 2, 3, 4, 5]:
                close_price = future_closes.get(days)
                if close_price is not None and close_price > 0:
                    if v3_action == '売り':
                        record[f'day{days}_profit'] = (buy_price - close_price) * 100
                    else:
                        record[f'day{days}_profit'] = (close_price - buy_price) * 100
                else:
                    record[f'day{days}_profit'] = None

            # v3推奨の損益（holding_daysに基づく）
            if holding_days == 0:
                record['profit'] = record['day0_profit']
            else:
                record['profit'] = record.get('day5_profit')
        else:
            record['profit'] = None
            record['day9am_profit'] = None
            record['day0_profit'] = None
            for days in [1, 2, 3, 4, 5]:
                record[f'day{days}_profit'] = None

        results.append(record)

    return pd.DataFrame(results)


def generate_v3_summary(df):
    """v3.0アクション別サマリー生成（全日0-5日）"""
    summary_rows = []

    for label in ['買い', '買い5', '静観', '売り', '売り5', '全体']:
        if label == '全体':
            subset = df
        else:
            subset = df[df['v3_label'] == label]

        if len(subset) == 0:
            continue

        profits = subset['profit'].dropna()
        row = {
            'label': label,
            'count': len(subset),
            'trade_count': len(profits),
            'avg_profit': profits.mean() if len(profits) > 0 else None,
            'total_profit': profits.sum() if len(profits) > 0 else None,
            'win_rate': (profits > 0).mean() * 100 if len(profits) > 0 else None,
        }

        # 9時利確の損益を追加
        day9am_profits = subset['day9am_profit'].dropna()
        if len(day9am_profits) > 0:
            row['day9am_avg'] = day9am_profits.mean()
            row['day9am_win'] = (day9am_profits > 0).mean() * 100
            row['day9am_total'] = day9am_profits.sum()
        else:
            row['day9am_avg'] = None
            row['day9am_win'] = None
            row['day9am_total'] = None

        # 各日の損益を追加（0-5日）
        for d in [0, 1, 2, 3, 4, 5]:
            col = f'day{d}_profit'
            day_profits = subset[col].dropna()
            if len(day_profits) > 0:
                row[f'day{d}_avg'] = day_profits.mean()
                row[f'day{d}_win'] = (day_profits > 0).mean() * 100
                row[f'day{d}_total'] = day_profits.sum()
            else:
                row[f'day{d}_avg'] = None
                row[f'day{d}_win'] = None
                row[f'day{d}_total'] = None

        summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def generate_v3_price_range_summary(df):
    """v3.0価格帯別サマリー生成（全日0-5日）"""
    df = df.copy()
    df['price_bracket'] = df['price_level'].apply(get_price_bracket)

    summary_rows = []

    for label in ['買い', '買い5', '静観', '売り', '売り5']:
        label_df = df[df['v3_label'] == label]

        for low, high, bracket_label in PRICE_BRACKETS:
            subset = label_df[label_df['price_bracket'] == bracket_label]

            if len(subset) == 0:
                continue

            row = {
                'label': label,
                'bracket': bracket_label,
                'count': len(subset),
            }

            # 全日の損益（0-5日）
            for d in [0, 1, 2, 3, 4, 5]:
                col = f'day{d}_profit'
                day_profits = subset[col].dropna()
                if len(day_profits) > 0:
                    row[f'day{d}_avg'] = day_profits.mean()
                    row[f'day{d}_win'] = (day_profits > 0).mean() * 100
                else:
                    row[f'day{d}_avg'] = None
                    row[f'day{d}_win'] = None

            # 9時利確の損益
            day9am_profits = subset['day9am_profit'].dropna()
            if len(day9am_profits) > 0:
                row['day9am_avg'] = day9am_profits.mean()
                row['day9am_win'] = (day9am_profits > 0).mean() * 100
            else:
                row['day9am_avg'] = None
                row['day9am_win'] = None

            summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def generate_summary(df):
    """アクション別サマリー生成"""
    summary_rows = []

    for action in ['買い', '静観', '売り', '全体']:
        if action == '全体':
            subset = df
        else:
            subset = df[df['action'] == action]

        if len(subset) == 0:
            continue

        row = {'action': action, 'count': len(subset)}

        for d in [0, 1, 2, 3, 4, 5]:
            col = f'day{d}_profit'
            profits = subset[col].dropna()
            if len(profits) > 0:
                row[f'day{d}_avg'] = profits.mean()
                row[f'day{d}_win'] = (profits > 0).mean() * 100
                row[f'day{d}_total'] = profits.sum()
            else:
                row[f'day{d}_avg'] = None
                row[f'day{d}_win'] = None
                row[f'day{d}_total'] = None

        summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def generate_price_range_summary(df):
    """価格帯別サマリー生成"""
    # price_levelで価格帯を判定
    df = df.copy()
    df['price_bracket'] = df['price_level'].apply(get_price_bracket)

    summary_rows = []

    for action in ['買い', '静観', '売り']:
        action_df = df[df['action'] == action]

        for low, high, bracket_label in PRICE_BRACKETS:
            subset = action_df[action_df['price_bracket'] == bracket_label]

            if len(subset) == 0:
                continue

            row = {
                'action': action,
                'bracket': bracket_label,
                'count': len(subset),
            }

            # 当日と5日後の損益
            for d in [0, 5]:
                col = f'day{d}_profit'
                profits = subset[col].dropna()
                if len(profits) > 0:
                    row[f'day{d}_avg'] = profits.mean()
                    row[f'day{d}_win'] = (profits > 0).mean() * 100
                else:
                    row[f'day{d}_avg'] = None
                    row[f'day{d}_win'] = None

            # 推奨ステータス判定
            status = get_price_range_status(action, (low + high) / 2)
            row['status'] = status

            summary_rows.append(row)

    return pd.DataFrame(summary_rows)


def fmt_yen(val):
    """円表示フォーマット（カンマ区切り、円あり）"""
    if val is None or pd.isna(val):
        return "-"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:,.0f}円"


def fmt_pct(val):
    """パーセント表示"""
    if val is None or pd.isna(val):
        return "-"
    return f"{val:.1f}%"


def make_v3_summary_table(summary_df):
    """v3.0サマリーテーブル生成（全日0-5日表示）- 行ごとの最高値強調"""
    rows = ""
    for _, row in summary_df.iterrows():
        label = row['label']
        if label == '全体':
            continue

        # バッジスタイル
        if '買い' in label:
            badge_cls = 'action-買い-badge'
            row_cls = 'action-買い'
        elif '売り' in label:
            badge_cls = 'action-売り-badge'
            row_cls = 'action-売り'
        else:
            badge_cls = 'action-静観-badge'
            row_cls = 'action-静観'

        # この行の全日の値を集めて最高値を取得
        day_values = {}
        avg9 = row.get('day9am_avg')
        if avg9 is not None and not pd.isna(avg9):
            day_values['9am'] = avg9
        for d in [0, 1, 2, 3, 4, 5]:
            avg = row.get(f'day{d}_avg')
            if avg is not None and not pd.isna(avg):
                day_values[f'day{d}'] = avg

        # 行内の最高値
        best_val = max(day_values.values()) if day_values else None

        cols = f'<td><span class="action-badge {badge_cls}">{label}</span></td>'
        cols += f'<td class="number">{int(row["count"])}</td>'

        # 9時利確の損益表示
        win9 = row.get('day9am_win')
        val_cls9 = 'positive' if avg9 and avg9 > 0 else 'negative' if avg9 and avg9 < 0 else ''
        is_best9 = avg9 is not None and not pd.isna(avg9) and best_val == avg9
        best_cls9 = ' best-cell' if is_best9 else ''
        cols += f'<td class="number {val_cls9}{best_cls9}">{fmt_yen(avg9)}<br><small style="color:var(--text-secondary);">{fmt_pct(win9)}</small></td>'

        # 各日の損益表示（0-5日）
        for d in [0, 1, 2, 3, 4, 5]:
            avg = row.get(f'day{d}_avg')
            win = row.get(f'day{d}_win')
            val_cls = 'positive' if avg and avg > 0 else 'negative' if avg and avg < 0 else ''
            is_best = avg is not None and not pd.isna(avg) and best_val == avg
            best_cls = ' best-cell' if is_best else ''
            cols += f'<td class="number {val_cls}{best_cls}">{fmt_yen(avg)}<br><small style="color:var(--text-secondary);">{fmt_pct(win)}</small></td>'

        rows += f'<tr class="{row_cls}">{cols}</tr>'

    # 全体行
    total_row = summary_df[summary_df['label'] == '全体']
    if len(total_row) > 0:
        row = total_row.iloc[0]

        # 全体行の最高値も計算
        day_values = {}
        avg9 = row.get('day9am_avg')
        if avg9 is not None and not pd.isna(avg9):
            day_values['9am'] = avg9
        for d in [0, 1, 2, 3, 4, 5]:
            avg = row.get(f'day{d}_avg')
            if avg is not None and not pd.isna(avg):
                day_values[f'day{d}'] = avg
        best_val = max(day_values.values()) if day_values else None

        cols = f'<td><strong>全体</strong></td>'
        cols += f'<td class="number"><strong>{int(row["count"])}</strong></td>'

        # 9時利確
        win9 = row.get('day9am_win')
        val_cls9 = 'positive' if avg9 and avg9 > 0 else 'negative' if avg9 and avg9 < 0 else ''
        is_best9 = avg9 is not None and not pd.isna(avg9) and best_val == avg9
        best_cls9 = ' best-cell' if is_best9 else ''
        cols += f'<td class="number {val_cls9}{best_cls9}"><strong>{fmt_yen(avg9)}</strong><br><small style="color:var(--text-secondary);">{fmt_pct(win9)}</small></td>'

        for d in [0, 1, 2, 3, 4, 5]:
            avg = row.get(f'day{d}_avg')
            win = row.get(f'day{d}_win')
            val_cls = 'positive' if avg and avg > 0 else 'negative' if avg and avg < 0 else ''
            is_best = avg is not None and not pd.isna(avg) and best_val == avg
            best_cls = ' best-cell' if is_best else ''
            cols += f'<td class="number {val_cls}{best_cls}"><strong>{fmt_yen(avg)}</strong><br><small style="color:var(--text-secondary);">{fmt_pct(win)}</small></td>'

        rows += f'<tr style="background:var(--bg-tertiary);">{cols}</tr>'

    return f"""
    <table class="swing-table">
        <thead>
            <tr>
                <th>アクション</th>
                <th class="number">件数</th>
                <th class="number">9時</th>
                <th class="number">1日目</th>
                <th class="number">2日目</th>
                <th class="number">3日目</th>
                <th class="number">4日目</th>
                <th class="number">5日目</th>
                <th class="number">6日目</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """


def make_v3_detail_table(df):
    """v3.0銘柄別詳細テーブル（全日0-5日表示）"""
    df_sorted = df.copy()
    df_sorted['action_order'] = df_sorted['v3_label'].map({
        '買い': 0, '買い5': 1, '静観': 2, '売り': 3, '売り5': 4
    })
    df_sorted = df_sorted.sort_values(['backtest_date', 'action_order'], ascending=[False, True])

    rows = ""
    current_date = None

    for _, row in df_sorted.iterrows():
        # 日付セパレーター
        if current_date != row['backtest_date']:
            current_date = row['backtest_date']
            rows += f'<tr class="date-separator"><td colspan="13">{current_date}</td></tr>'

        v3_label = row['v3_label']
        base_action = row['base_action']
        holding_days = row['holding_days']

        # バッジスタイル
        if '買い' in v3_label:
            badge_cls = 'action-買い-badge'
            row_cls = 'action-買い'
        elif '売り' in v3_label:
            badge_cls = 'action-売り-badge'
            row_cls = 'action-売り'
        else:
            badge_cls = 'action-静観-badge'
            row_cls = 'action-静観'

        # 信用制限マーク
        margin_mark = ""
        if row.get('margin_restricted'):
            margin_mark = "<span style='color:var(--negative); font-size:0.8em;' title='信用取引制限'>🚫</span>"

        # 寄り天/寄り底マーク
        yoriten_mark = ""
        if row.get('is_yoriten'):
            yoriten_mark += "<span style='color:var(--negative); font-size:0.7em;' title='寄り天'>天</span>"
        if row.get('is_yorisoko'):
            yoriten_mark += "<span style='color:var(--accent-blue); font-size:0.7em;' title='寄り底'>底</span>"

        # 前日終値
        price_level = row.get('price_level', row['buy_price'])
        price_level_str = f"{price_level:,.0f}" if price_level and not pd.isna(price_level) else "-"

        cols = f"<td>{row['ticker']}{margin_mark}</td>"
        cols += f"<td>{row['stock_name'][:10]}{yoriten_mark}</td>"
        cols += f"<td><small style='color:var(--text-secondary);'>{base_action}</small></td>"
        cols += f"<td><span class='action-badge {badge_cls}'>{v3_label}</span></td>"
        cols += f"<td class='number'>{price_level_str}円</td>"

        # 6日目まで全データがあるか確認（best-cell表示の条件）
        day_values = {}
        day9am_profit = row.get('day9am_profit')
        if day9am_profit is not None and not pd.isna(day9am_profit):
            day_values['9am'] = day9am_profit
        for d in [0, 1, 2, 3, 4, 5]:
            dp = row.get(f'day{d}_profit')
            if dp is not None and not pd.isna(dp):
                day_values[f'day{d}'] = dp

        # 全7日分（9時 + 0-5日）揃っている場合のみbest-cell適用
        has_all_days = len(day_values) == 7
        best_val = max(day_values.values()) if has_all_days and day_values else None

        # 9時利確の損益表示
        if day9am_profit is not None and not pd.isna(day9am_profit):
            val_cls = ' positive' if day9am_profit > 0 else ' negative'
            is_best = has_all_days and best_val == day9am_profit
            best_cls = ' best-cell' if is_best else ''
            day9am_str = fmt_yen(day9am_profit)
        elif row.get('margin_restricted'):
            val_cls = ''
            best_cls = ''
            day9am_str = "<span style='color:var(--text-muted);'>制限</span>"
        else:
            val_cls = ''
            best_cls = ''
            day9am_str = "-"
        cols += f"<td class='number{val_cls}{best_cls}'>{day9am_str}</td>"

        # 各日の損益表示（0-5日）
        for d in [0, 1, 2, 3, 4, 5]:
            day_profit = row.get(f'day{d}_profit')
            if day_profit is not None and not pd.isna(day_profit):
                val_cls = ' positive' if day_profit > 0 else ' negative'
                is_best = has_all_days and best_val == day_profit
                best_cls = ' best-cell' if is_best else ''
                # 推奨日なら強調
                if (holding_days == 0 and d == 0) or (holding_days == 5 and d == 5):
                    day_str = f"<strong>{fmt_yen(day_profit)}</strong>"
                else:
                    day_str = fmt_yen(day_profit)
            elif row.get('margin_restricted'):
                val_cls = ''
                best_cls = ''
                day_str = "<span style='color:var(--text-muted);'>制限</span>"
            else:
                val_cls = ''
                best_cls = ''
                day_str = "-"
            cols += f"<td class='number{val_cls}{best_cls}'>{day_str}</td>"

        rows += f"<tr class='{row_cls}'>{cols}</tr>"

    return f"""
    <table>
        <thead>
            <tr>
                <th>コード</th>
                <th>銘柄名</th>
                <th>元シグナル</th>
                <th>v3アクション</th>
                <th class="number">前日終値</th>
                <th class="number">9時</th>
                <th class="number">1日目</th>
                <th class="number">2日目</th>
                <th class="number">3日目</th>
                <th class="number">4日目</th>
                <th class="number">5日目</th>
                <th class="number">6日目</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """


def make_v3_price_range_section(price_summary_df):
    """v3.0価格帯別サマリーセクションHTML生成（全日0-5日）- 行ごとの最高値強調"""
    if len(price_summary_df) == 0:
        return ""

    cards_html = ""

    for label in ['買い', '買い5', '静観', '売り', '売り5']:
        label_data = price_summary_df[price_summary_df['label'] == label]
        if len(label_data) == 0:
            continue

        # カードスタイル
        if '買い' in label:
            card_cls = 'buy'
        elif '売り' in label:
            card_cls = 'sell'
        else:
            card_cls = 'hold'

        rows_html = ""
        for _, row in label_data.iterrows():
            bracket = row['bracket']
            count = int(row['count'])

            # この行の全日の値を集めて最高値を取得
            day_values = {}
            day9am_avg = row.get('day9am_avg')
            if day9am_avg is not None and not pd.isna(day9am_avg):
                day_values['9am'] = day9am_avg
            for d in [0, 1, 2, 3, 4, 5]:
                day_avg = row.get(f'day{d}_avg')
                if day_avg is not None and not pd.isna(day_avg):
                    day_values[f'day{d}'] = day_avg

            # 行内の最高値
            best_val = max(day_values.values()) if day_values else None

            cols_html = f"<td>{bracket}</td><td class='number'>{count}</td>"

            # 9時利確
            day9am_win = row.get('day9am_win')
            day9am_cls = 'positive' if day9am_avg and day9am_avg > 0 else 'negative' if day9am_avg and day9am_avg < 0 else ''
            is_best9 = day9am_avg is not None and not pd.isna(day9am_avg) and best_val == day9am_avg
            best_cls9 = ' best-cell' if is_best9 else ''
            cols_html += f"<td class='number {day9am_cls}{best_cls9}'>{fmt_yen(day9am_avg)}<br><small>{fmt_pct(day9am_win)}</small></td>"

            # 全日の損益（0-5日）
            for d in [0, 1, 2, 3, 4, 5]:
                day_avg = row.get(f'day{d}_avg')
                day_win = row.get(f'day{d}_win')
                day_cls = 'positive' if day_avg and day_avg > 0 else 'negative' if day_avg and day_avg < 0 else ''
                is_best = day_avg is not None and not pd.isna(day_avg) and best_val == day_avg
                best_cls = ' best-cell' if is_best else ''
                cols_html += f"<td class='number {day_cls}{best_cls}'>{fmt_yen(day_avg)}<br><small>{fmt_pct(day_win)}</small></td>"

            rows_html += f"<tr>{cols_html}</tr>"

        cards_html += f"""
        <div class="price-range-card {card_cls}">
            <h3>{label} 価格帯別</h3>
            <div style="overflow-x:auto;">
            <table>
                <thead>
                    <tr>
                        <th>価格帯</th>
                        <th class="number">件数</th>
                        <th class="number">9時</th>
                        <th class="number">1日目</th>
                        <th class="number">2日目</th>
                        <th class="number">3日目</th>
                        <th class="number">4日目</th>
                        <th class="number">5日目</th>
                        <th class="number">6日目</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
            </div>
        </div>
        """

    return f"""
    <div class="price-range-section">
        <h3 class="section-title">価格帯別パフォーマンス</h3>
        {cards_html}
    </div>
    """


def make_9am_detail_table():
    """9時詳細テーブル（寄付き・高値・安値・時刻）- 取引制限銘柄表示"""
    try:
        df = pd.read_parquet(DATA_DIR / "grok_9am_detail.parquet")
    except FileNotFoundError:
        return "<p style='color:var(--text-muted);'>9時詳細データがありません</p>"

    if len(df) == 0:
        return "<p style='color:var(--text-muted);'>9時詳細データがありません</p>"

    # 日付でソート（新しい順）
    df = df.sort_values('date', ascending=False)

    # 制限銘柄フラグ
    if 'is_restricted' not in df.columns:
        df['is_restricted'] = False

    rows = ""
    current_date = None

    for _, row in df.iterrows():
        # 日付セパレーター
        row_date = str(row['date'])[:10]
        if current_date != row_date:
            current_date = row_date
            rows += f'<tr class="date-separator"><td colspan="10">{current_date}</td></tr>'

        is_restricted = row.get('is_restricted', False)

        # 騰落率の色
        high_cls = 'positive' if row['high_pct'] > 0 else 'negative' if row['high_pct'] < 0 else ''
        low_cls = 'positive' if row['low_pct'] > 0 else 'negative' if row['low_pct'] < 0 else ''

        # 順番のバッジ
        order = row['order']
        if order == '高値先':
            order_badge = "<span style='color:var(--negative);'>高値先↓</span>"
        elif order == '寄天':
            order_badge = "<span style='color:var(--negative);'>寄天↓</span>"
        elif order == '寄底':
            order_badge = "<span style='color:var(--positive);'>寄底↑</span>"
        elif order == '安値先':
            order_badge = "<span style='color:var(--accent-blue);'>安値先↑</span>"
        else:
            order_badge = "<span style='color:var(--text-muted);'>同時</span>"

        # 銘柄名（10文字まで）+ 制限マーク
        stock_name = row.get('stock_name', '')
        if pd.isna(stock_name) or stock_name == '':
            stock_name = row['ticker']
        stock_name = str(stock_name)[:10]
        restrict_mark = "<span style='color:var(--negative); font-size:0.8em;' title='取引制限'>🚫</span>" if is_restricted else ""

        # 高値先で寄付き < 高値の場合、高値セルを強調（緑=利益）
        if order == '高値先' and row['high'] > row['open']:
            high_style = "background:rgba(0,212,170,0.15);"
        else:
            high_style = ""

        # 寄天の場合、安値セルを強調（赤=損失）
        if order == '寄天' and row['low'] < row['open']:
            low_style = "background:rgba(255,82,82,0.15);"
        else:
            low_style = ""
        open_style = ""

        # 制限銘柄は薄く表示
        row_style = "opacity:0.5;" if is_restricted else ""

        rows += f"""
        <tr style="{row_style}">
            <td>{row['ticker']}{restrict_mark}</td>
            <td>{stock_name}</td>
            <td class="number" style="{open_style}">{row['open']:,.0f}円</td>
            <td class="number {high_cls}" style="{high_style}">{row['high']:,.0f}円</td>
            <td class="number {high_cls}">{row['high_pct']:+.2f}%</td>
            <td class="number">{row['high_time']}</td>
            <td class="number {low_cls}" style="{low_style}">{row['low']:,.0f}円</td>
            <td class="number {low_cls}">{row['low_pct']:+.2f}%</td>
            <td class="number">{row['low_time']}</td>
            <td>{order_badge}</td>
        </tr>
        """

    # 統計サマリー（制限除外ベース）
    df_valid = df[~df['is_restricted']]
    high_first = len(df_valid[df_valid['order'] == '高値先'])
    yoriten = len(df_valid[df_valid['order'] == '寄天'])
    yorisoko = len(df_valid[df_valid['order'] == '寄底'])
    low_first = len(df_valid[df_valid['order'] == '安値先'])
    same_time = len(df_valid[df_valid['order'] == '同時'])
    total = len(df_valid)
    restricted_count = len(df[df['is_restricted']])

    summary = f"""
    <div style="display:flex; gap:24px; margin-bottom:16px; font-size:0.9em; flex-wrap:wrap;">
        <div><span style="color:var(--negative);">高値先↓</span> {high_first}件 ({high_first/total*100:.0f}%)</div>
        <div><span style="color:var(--negative);">寄天↓</span> {yoriten}件 ({yoriten/total*100:.0f}%)</div>
        <div><span style="color:var(--positive);">寄底↑</span> {yorisoko}件 ({yorisoko/total*100:.0f}%)</div>
        <div><span style="color:var(--accent-blue);">安値先↑</span> {low_first}件 ({low_first/total*100:.0f}%)</div>
        <div><span style="color:var(--text-muted);">同時</span> {same_time}件 ({same_time/total*100:.0f}%)</div>
        <div style="margin-left:auto;"><span style="color:var(--text-muted);">🚫制限除外</span> {restricted_count}件</div>
    </div>
    """

    return f"""
    {summary}
    <div style="max-height:400px; overflow-y:auto;">
    <table>
        <thead>
            <tr>
                <th>コード</th>
                <th>銘柄名</th>
                <th class="number">寄付</th>
                <th class="number">高値</th>
                <th class="number">騰落</th>
                <th class="number">時刻</th>
                <th class="number">安値</th>
                <th class="number">騰落</th>
                <th class="number">時刻</th>
                <th>順番</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    </div>
    """


def make_v3_strategy_card():
    """v3.0戦略説明カード"""
    return """
    <div class="strategy-card">
        <h3>▸ v3.0 戦略ルール</h3>
        <div class="strategy-grid">
            <div class="strategy-item buy">
                <h4>買いシグナル</h4>
                <ul>
                    <li>7,500-10,000円 → <strong>買い5</strong>（5日保有）</li>
                    <li>5,000-7,500円 → <strong>買い</strong>（当日決済）</li>
                    <li>その他 → <strong>買い</strong>（当日決済）</li>
                </ul>
            </div>
            <div class="strategy-item hold">
                <h4>静観シグナル</h4>
                <ul>
                    <li>1,500-3,000円 → <strong style="color:var(--buy-color);">買い5</strong>（5日保有）</li>
                    <li>その他 → <strong>静観</strong>（取引なし）</li>
                </ul>
            </div>
            <div class="strategy-item sell">
                <h4>売りシグナル</h4>
                <ul>
                    <li>2,000-10,000円 → <strong>売り5</strong>（5日保有）</li>
                    <li>10,000円以上 → <strong>売り</strong>（当日決済）</li>
                    <li>2,000円未満 → <strong>売り</strong>（当日決済）</li>
                </ul>
            </div>
        </div>
    </div>
    """


def make_price_range_section(price_summary_df):
    """価格帯別サマリーセクションHTML生成"""
    if len(price_summary_df) == 0:
        return ""

    cards_html = ""

    for action in ['買い', '静観', '売り']:
        action_data = price_summary_df[price_summary_df['action'] == action]
        if len(action_data) == 0:
            continue

        # カードスタイル
        if action == '買い':
            card_cls = 'buy'
        elif action == '売り':
            card_cls = 'sell'
        else:
            card_cls = 'hold'

        rows_html = ""
        for _, row in action_data.iterrows():
            bracket = row['bracket']
            count = int(row['count'])
            day0_avg = row.get('day0_avg')
            day0_win = row.get('day0_win')
            day5_avg = row.get('day5_avg')
            day5_win = row.get('day5_win')
            status = row.get('status', 'neutral')

            # ステータスによる背景色
            if status == 'optimal_day':
                bg_style = "background: rgba(0,212,170,0.1);"
                status_mark = "★"
            elif status == 'optimal_swing':
                bg_style = "background: rgba(84,160,255,0.1);"
                status_mark = "◎"
            elif status == 'avoid':
                bg_style = "background: rgba(255,107,157,0.1);"
                status_mark = "✗"
            else:
                bg_style = ""
                status_mark = ""

            # 当日損益色
            day0_cls = 'positive' if day0_avg and day0_avg > 0 else 'negative' if day0_avg and day0_avg < 0 else ''
            day5_cls = 'positive' if day5_avg and day5_avg > 0 else 'negative' if day5_avg and day5_avg < 0 else ''

            rows_html += f"""
            <tr style="{bg_style}">
                <td>{bracket} {status_mark}</td>
                <td class="number">{count}</td>
                <td class="number {day0_cls}">{fmt_yen(day0_avg)}</td>
                <td class="number">{fmt_pct(day0_win)}</td>
                <td class="number {day5_cls}">{fmt_yen(day5_avg)}</td>
                <td class="number">{fmt_pct(day5_win)}</td>
            </tr>
            """

        cards_html += f"""
        <div class="price-range-card {card_cls}">
            <h3>{action}シグナル 価格帯別</h3>
            <table>
                <thead>
                    <tr>
                        <th>価格帯</th>
                        <th class="number">件数</th>
                        <th class="number">1日目</th>
                        <th class="number">勝率</th>
                        <th class="number">6日目</th>
                        <th class="number">勝率</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """

    return f"""
    <div class="price-range-section">
        <h3 class="section-title">価格帯別パフォーマンス</h3>
        <p style="color:var(--text-secondary); margin-bottom:16px; font-size:0.85em;">★=当日推奨 | ◎=スイング推奨 | ✗=避けるべき</p>
        <div class="summary-grid">
            {cards_html}
        </div>
    </div>
    """


def generate_html_report(results_v203, results_v21, results_v2101, summary_v203, summary_v21, summary_v2101, price_summary_v203, price_summary_v21, price_summary_v2101, results_v3, summary_v3, price_summary_v3, output_path):
    """Bloomberg Terminal風ダークテーマHTML生成"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    css = """
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Outfit:wght@300;400;500;600;700&display=swap');

        :root {
            --bg-primary: #0a0f1a;
            --bg-secondary: #111827;
            --bg-tertiary: #1a2332;
            --bg-card: #0d1421;
            --border-color: #1e3a5f;
            --border-glow: #00d4aa33;
            --text-primary: #e8f4f8;
            --text-secondary: #8899a6;
            --text-muted: #4a5568;
            --accent-cyan: #00d4aa;
            --accent-magenta: #ff6b9d;
            --accent-orange: #ff9f43;
            --accent-blue: #54a0ff;
            --accent-purple: #a855f7;
            --positive: #00d4aa;
            --negative: #ff6b9d;
            --buy-color: #ff9f43;
            --sell-color: #54a0ff;
            --hold-color: #6c757d;
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: 'JetBrains Mono', 'SF Mono', Monaco, Consolas, monospace;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            min-height: 100vh;
        }

        /* Scanline effect overlay */
        body::before {
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            background: repeating-linear-gradient(
                0deg,
                rgba(0, 212, 170, 0.01) 0px,
                rgba(0, 212, 170, 0.01) 1px,
                transparent 1px,
                transparent 3px
            );
            z-index: 9999;
        }

        /* Grid pattern background */
        body::after {
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            background-image:
                linear-gradient(rgba(0, 212, 170, 0.03) 1px, transparent 1px),
                linear-gradient(90deg, rgba(0, 212, 170, 0.03) 1px, transparent 1px);
            background-size: 50px 50px;
            z-index: -1;
        }

        .container {
            max-width: 1800px;
            margin: 0 auto;
            padding: 20px;
        }

        /* Header */
        .header {
            background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-tertiary) 100%);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 40px;
            margin-bottom: 24px;
            position: relative;
            overflow: hidden;
        }

        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, var(--accent-cyan), var(--accent-purple), var(--accent-magenta));
        }

        .header h1 {
            font-family: 'Outfit', sans-serif;
            font-size: 2.8em;
            font-weight: 700;
            letter-spacing: -0.02em;
            margin-bottom: 8px;
            background: linear-gradient(135deg, var(--accent-cyan), var(--accent-purple));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        .header .subtitle {
            font-size: 0.95em;
            color: var(--text-secondary);
            font-weight: 400;
        }

        .header .meta-info {
            display: flex;
            gap: 24px;
            margin-top: 20px;
            padding-top: 20px;
            border-top: 1px solid var(--border-color);
        }

        .header .meta-item {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 0.85em;
            color: var(--text-muted);
        }

        .header .meta-item span {
            color: var(--accent-cyan);
        }

        /* Version Header */
        .version-header {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px 8px 0 0;
            padding: 24px 32px;
            margin-top: 40px;
            position: relative;
        }

        .version-header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: var(--accent-cyan);
        }

        .version-header.v3::before { background: var(--accent-purple); }
        .version-header.v2101::before { background: var(--accent-orange); }

        .version-header h2 {
            font-family: 'Outfit', sans-serif;
            font-size: 1.5em;
            font-weight: 600;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .version-header h2::before {
            content: '▸';
            color: var(--accent-cyan);
        }

        .version-header.v3 h2::before { color: var(--accent-purple); }
        .version-header.v2101 h2::before { color: var(--accent-orange); }

        .version-header .subtitle {
            font-size: 0.85em;
            color: var(--text-secondary);
            margin-top: 6px;
            padding-left: 24px;
        }

        /* Summary Section */
        .summary-section {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-top: none;
            padding: 32px;
        }

        .section-title {
            font-family: 'Outfit', sans-serif;
            font-size: 1.2em;
            font-weight: 600;
            color: var(--accent-cyan);
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .section-title::before {
            content: '◆';
            font-size: 0.7em;
        }

        /* Summary Grid */
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            gap: 20px;
            margin-bottom: 32px;
        }

        /* Summary Card */
        .summary-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 24px;
            position: relative;
            transition: all 0.3s ease;
        }

        .summary-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 4px;
            height: 100%;
            background: var(--accent-cyan);
            border-radius: 8px 0 0 8px;
        }

        .summary-card.buy::before { background: var(--buy-color); }
        .summary-card.sell::before { background: var(--sell-color); }
        .summary-card.hold::before { background: var(--hold-color); }

        .summary-card:hover {
            border-color: var(--accent-cyan);
            box-shadow: 0 0 20px rgba(0, 212, 170, 0.1);
        }

        .summary-card.buy:hover { border-color: var(--buy-color); box-shadow: 0 0 20px rgba(255, 159, 67, 0.1); }
        .summary-card.sell:hover { border-color: var(--sell-color); box-shadow: 0 0 20px rgba(84, 160, 255, 0.1); }

        .summary-card h3 {
            font-family: 'Outfit', sans-serif;
            font-size: 1.1em;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 16px;
        }

        .summary-card.buy h3 { color: var(--buy-color); }
        .summary-card.sell h3 { color: var(--sell-color); }
        .summary-card.hold h3 { color: var(--hold-color); }

        .stat-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid var(--border-color);
        }

        .stat-row:last-child { border-bottom: none; }

        .stat-label {
            font-size: 0.85em;
            color: var(--text-secondary);
        }

        .stat-value {
            font-size: 1.1em;
            font-weight: 600;
            color: var(--text-primary);
        }

        /* Table Section */
        .table-section {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-top: none;
            border-radius: 0 0 8px 8px;
            padding: 32px;
            overflow-x: auto;
        }

        /* Tables */
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85em;
            margin-top: 16px;
        }

        thead {
            background: var(--bg-tertiary);
            position: sticky;
            top: 0;
            z-index: 10;
        }

        th {
            padding: 14px 12px;
            text-align: left;
            font-weight: 500;
            font-size: 0.8em;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.1em;
            border-bottom: 2px solid var(--accent-cyan);
        }

        th.number { text-align: center; }

        td {
            padding: 12px;
            border-bottom: 1px solid var(--border-color);
            color: var(--text-primary);
        }

        td.number {
            text-align: center;
            font-variant-numeric: tabular-nums;
        }

        td.number.positive {
            color: var(--positive);
            text-shadow: 0 0 10px rgba(0, 212, 170, 0.3);
        }

        td.number.negative {
            color: var(--negative);
            text-shadow: 0 0 10px rgba(255, 107, 157, 0.3);
        }

        tbody tr {
            transition: background 0.2s ease;
        }

        tbody tr:hover:not(.date-separator) {
            background: rgba(0, 212, 170, 0.05) !important;
        }

        tr.date-separator {
            background: var(--bg-tertiary);
        }

        tr.date-separator td {
            padding: 12px;
            font-weight: 600;
            color: var(--accent-cyan);
            border: none;
            font-size: 0.9em;
            letter-spacing: 0.05em;
        }

        tr.action-買い {
            background: rgba(255, 159, 67, 0.05);
        }

        tr.action-売り {
            background: rgba(84, 160, 255, 0.05);
        }

        tr.action-静観 {
            background: rgba(108, 117, 125, 0.05);
        }

        /* Action Badges */
        .action-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 4px;
            font-weight: 600;
            font-size: 0.8em;
            letter-spacing: 0.05em;
        }

        .action-買い-badge {
            background: rgba(255, 159, 67, 0.2);
            color: var(--buy-color);
            border: 1px solid var(--buy-color);
        }

        .action-売り-badge {
            background: rgba(84, 160, 255, 0.2);
            color: var(--sell-color);
            border: 1px solid var(--sell-color);
        }

        .action-静観-badge {
            background: rgba(108, 117, 125, 0.2);
            color: var(--hold-color);
            border: 1px solid var(--hold-color);
        }

        /* Swing Table */
        .swing-table {
            margin: 20px 0;
        }

        .swing-table th, .swing-table td {
            text-align: center;
            padding: 14px 10px;
        }

        .swing-table th:first-child, .swing-table td:first-child {
            text-align: left;
        }

        .swing-table thead th {
            background: var(--bg-tertiary);
            border-bottom: 2px solid var(--accent-cyan);
        }

        /* Profit Cards */
        .profit-card-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
            gap: 12px;
            margin: 20px 0;
        }

        .profit-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            padding: 16px 12px;
            text-align: center;
            transition: all 0.3s ease;
        }

        .profit-card:hover {
            border-color: var(--accent-cyan);
        }

        .profit-card .day-label {
            font-size: 0.75em;
            color: var(--text-muted);
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.1em;
        }

        .profit-card .profit-value {
            font-size: 1.1em;
            font-weight: 600;
        }

        .profit-card .profit-value.positive { color: var(--positive); }
        .profit-card .profit-value.negative { color: var(--negative); }

        .profit-card .win-rate {
            font-size: 0.7em;
            color: var(--text-secondary);
            margin-top: 6px;
        }

        /* Footer */
        .footer {
            padding: 24px;
            text-align: center;
            color: var(--text-muted);
            font-size: 0.8em;
            border-top: 1px solid var(--border-color);
            margin-top: 40px;
        }

        .footer span {
            color: var(--accent-cyan);
        }

        /* Animations */
        @keyframes fadeInUp {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        @keyframes glow {
            0%, 100% { box-shadow: 0 0 5px var(--accent-cyan); }
            50% { box-shadow: 0 0 20px var(--accent-cyan); }
        }

        .summary-card, .version-header, .table-section {
            animation: fadeInUp 0.5s ease-out forwards;
        }

        /* Responsive */
        @media (max-width: 768px) {
            .container { padding: 12px; }
            .header { padding: 24px; }
            .header h1 { font-size: 1.8em; }
            .summary-section, .table-section { padding: 20px; }
            table { font-size: 0.75em; }
            th, td { padding: 8px 6px; }
        }

        /* Strategy Card */
        .strategy-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 24px;
            margin-bottom: 24px;
        }

        .strategy-card h3 {
            font-family: 'Outfit', sans-serif;
            color: var(--accent-purple);
            margin-bottom: 20px;
            font-size: 1.1em;
        }

        .strategy-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }

        .strategy-item h4 {
            font-size: 0.9em;
            margin-bottom: 10px;
        }

        .strategy-item.buy h4 { color: var(--buy-color); }
        .strategy-item.sell h4 { color: var(--sell-color); }
        .strategy-item.hold h4 { color: var(--hold-color); }

        .strategy-item ul {
            list-style: none;
            font-size: 0.8em;
            color: var(--text-secondary);
        }

        .strategy-item li {
            padding: 4px 0;
            padding-left: 16px;
            position: relative;
        }

        .strategy-item li::before {
            content: '›';
            position: absolute;
            left: 0;
            color: var(--text-muted);
        }

        /* Price Range Section */
        .price-range-section {
            margin-top: 32px;
        }

        .price-range-card {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 16px;
        }

        .price-range-card h3 {
            font-size: 1em;
            margin-bottom: 16px;
        }

        .price-range-card.buy h3 { color: var(--buy-color); }
        .price-range-card.sell h3 { color: var(--sell-color); }
        .price-range-card.hold h3 { color: var(--hold-color); }

        .price-range-card table {
            font-size: 0.8em;
        }

        .price-range-card th {
            background: transparent;
            border-bottom: 1px solid var(--border-color);
            padding: 8px 6px;
            font-size: 0.75em;
        }

        .price-range-card td {
            padding: 8px 6px;
        }

        /* Best performance highlight */
        .best-cell {
            background: linear-gradient(135deg, rgba(0, 212, 170, 0.25) 0%, rgba(0, 212, 170, 0.1) 100%) !important;
            border: 1px solid var(--accent-cyan) !important;
            border-radius: 4px;
            box-shadow: 0 0 12px rgba(0, 212, 170, 0.3);
            position: relative;
        }

        .best-cell::after {
            content: '★';
            position: absolute;
            top: 2px;
            right: 4px;
            font-size: 0.6em;
            color: var(--accent-cyan);
        }

        .best-row {
            background: linear-gradient(90deg, rgba(0, 212, 170, 0.15) 0%, transparent 50%) !important;
        }
    """

    def make_profit_cards(summary_df, action):
        """アクション別の日別損益カード"""
        row = summary_df[summary_df['action'] == action]
        if len(row) == 0:
            return ""
        row = row.iloc[0]

        cards = ""
        for d in [0, 1, 2, 3, 4, 5]:
            label = f'{d+1}日目'
            avg = row.get(f'day{d}_avg')
            win = row.get(f'day{d}_win')
            val_cls = 'positive' if avg and avg > 0 else 'negative' if avg and avg < 0 else ''
            cards += f"""
            <div class="profit-card">
                <div class="day-label">{label}</div>
                <div class="profit-value {val_cls}">{fmt_yen(avg)}</div>
                <div class="win-rate">勝率 {fmt_pct(win)}</div>
            </div>
            """
        return cards

    def make_summary_card(summary_df, action, card_class):
        """サマリーカード生成（合計表示）"""
        row = summary_df[summary_df['action'] == action]
        if len(row) == 0:
            return ""
        row = row.iloc[0]

        # アクション表示名
        if action == '買い':
            title = f"買いシグナル（{int(row['count'])}件）"
            subtitle = "買い→売り（ロング）"
        elif action == '売り':
            title = f"売りシグナル（{int(row['count'])}件）"
            subtitle = "売り→買い（空売り）"
        else:
            title = f"静観シグナル（{int(row['count'])}件）"
            subtitle = "買い→売り（ロング）"

        # 各日の損益表示（合計）
        day_rows = ""
        for d in [0, 1, 2, 3, 4, 5]:
            label = f'{d+1}日目'
            total = row.get(f'day{d}_total')
            win = row.get(f'day{d}_win')

            total_cls = 'style="color: var(--positive);"' if total and total > 0 else 'style="color: var(--negative);"' if total and total < 0 else ''

            day_rows += f"""
            <div class="stat-row">
                <span class="stat-label">{label} <small style="color:var(--text-muted);">({fmt_pct(win)})</small></span>
                <span class="stat-value" {total_cls}>{fmt_yen(total)}</span>
            </div>
            """

        return f"""
        <div class="summary-card {card_class}">
            <h3>{title}</h3>
            <p style="color:var(--text-secondary); margin-bottom:12px; font-size:0.85em;">{subtitle}</p>
            {day_rows}
        </div>
        """

    def make_swing_summary_table(summary_df):
        """スイング損益サマリーテーブル"""
        rows = ""
        for action in ['買い', '静観', '売り']:
            row = summary_df[summary_df['action'] == action]
            if len(row) == 0:
                continue
            row = row.iloc[0]

            action_cls = 'action-買い' if action == '買い' else 'action-売り' if action == '売り' else 'action-静観'
            badge_cls = 'action-買い-badge' if action == '買い' else 'action-売り-badge' if action == '売り' else 'action-静観-badge'

            cols = f'<td><span class="action-badge {badge_cls}">{action}</span></td>'
            cols += f'<td class="number">{int(row["count"])}</td>'

            for d in [0, 1, 2, 3, 4, 5]:
                avg = row.get(f'day{d}_avg')
                win = row.get(f'day{d}_win')
                val_cls = 'positive' if avg and avg > 0 else 'negative' if avg and avg < 0 else ''
                cols += f'<td class="number {val_cls}">{fmt_yen(avg)}<br><small style="color:var(--text-secondary);">{fmt_pct(win)}</small></td>'

            rows += f'<tr class="{action_cls}">{cols}</tr>'

        return f"""
        <table class="swing-table">
            <thead>
                <tr>
                    <th>判定</th>
                    <th class="number">件数</th>
                    <th class="number">1日目</th>
                    <th class="number">2日目</th>
                    <th class="number">3日目</th>
                    <th class="number">4日目</th>
                    <th class="number">5日目</th>
                    <th class="number">6日目</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
        """

    def make_detail_table(df):
        """銘柄別詳細テーブル"""
        df_sorted = df.copy()
        df_sorted['action_order'] = df_sorted['action'].map({'買い': 0, '静観': 1, '売り': 2})
        df_sorted = df_sorted.sort_values(['backtest_date', 'action_order'], ascending=[False, True])

        rows = ""
        current_date = None

        for _, row in df_sorted.iterrows():
            # 日付セパレーター
            if current_date != row['backtest_date']:
                current_date = row['backtest_date']
                rows += f'<tr class="date-separator"><td colspan="12">{current_date}</td></tr>'

            action = row['action']
            action_cls = 'action-買い' if action == '買い' else 'action-売り' if action == '売り' else 'action-静観'
            badge_cls = 'action-買い-badge' if action == '買い' else 'action-売り-badge' if action == '売り' else 'action-静観-badge'

            # 信用制限マーク
            margin_mark = ""
            if row.get('margin_restricted'):
                margin_mark = "<span style='color:var(--negative); font-size:0.8em;' title='信用取引制限'>🚫</span>"

            # 価格帯ステータスマーク
            price_status = row.get('price_range_status', 'neutral')
            price_mark = ""
            row_extra_style = ""
            if price_status == 'optimal_day':
                price_mark = "<span style='color:var(--positive); font-size:0.8em;' title='当日推奨価格帯'>★</span>"
                row_extra_style = "background: linear-gradient(90deg, rgba(0,212,170,0.1) 0%, transparent 100%) !important;"
            elif price_status == 'optimal_swing':
                price_mark = "<span style='color:var(--accent-blue); font-size:0.8em;' title='スイング推奨価格帯'>◎</span>"
                row_extra_style = "background: linear-gradient(90deg, rgba(84,160,255,0.1) 0%, transparent 100%) !important;"
            elif price_status == 'avoid':
                price_mark = "<span style='color:var(--negative); font-size:0.8em;' title='避けるべき価格帯'>✗</span>"
                row_extra_style = "background: linear-gradient(90deg, rgba(255,107,157,0.1) 0%, transparent 100%) !important;"

            # 前日終値表示
            price_level = row.get('price_level', row['buy_price'])
            price_level_str = f"{price_level:,.0f}" if price_level and not pd.isna(price_level) else "-"

            cols = f"<td>{row['ticker']}{margin_mark}{price_mark}</td>"
            cols += f"<td>{row['stock_name'][:10]}</td>"
            cols += f"<td><span class='action-badge {badge_cls}'>{action}</span></td>"
            cols += f"<td class='number'>{price_level_str}円</td>"

            for d in [0, 1, 2, 3, 4, 5]:
                profit = row.get(f'day{d}_profit')
                if profit is not None and not pd.isna(profit):
                    val_cls = 'positive' if profit > 0 else 'negative'
                    cols += f"<td class='number {val_cls}'>{fmt_yen(profit)}</td>"
                elif row.get('margin_restricted'):
                    # 制限銘柄は「制限」表示
                    cols += "<td class='number' style='color:var(--text-muted);'>制限</td>"
                else:
                    cols += "<td class='number'>-</td>"

            style_attr = f' style="{row_extra_style}"' if row_extra_style else ''
            rows += f"<tr class='{action_cls}'{style_attr}>{cols}</tr>"

        return f"""
        <table>
            <thead>
                <tr>
                    <th>コード</th>
                    <th>銘柄名</th>
                    <th>判定</th>
                    <th class="number">前日終値</th>
                    <th class="number">1日目</th>
                    <th class="number">2日目</th>
                    <th class="number">3日目</th>
                    <th class="number">4日目</th>
                    <th class="number">5日目</th>
                    <th class="number">6日目</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
        """

    # 期間取得
    v203_start = results_v203['backtest_date'].min() if len(results_v203) > 0 else '-'
    v203_end = results_v203['backtest_date'].max() if len(results_v203) > 0 else '-'
    v21_start = results_v21['backtest_date'].min() if len(results_v21) > 0 else '-'
    v21_end = results_v21['backtest_date'].max() if len(results_v21) > 0 else '-'
    v2101_start = results_v2101['backtest_date'].min() if len(results_v2101) > 0 else '-'
    v2101_end = results_v2101['backtest_date'].max() if len(results_v2101) > 0 else '-'
    v3_start = results_v3['backtest_date'].min() if len(results_v3) > 0 else '-'
    v3_end = results_v3['backtest_date'].max() if len(results_v3) > 0 else '-'

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GROK銘柄 デイスイング分析レポート</title>
    <style>{css}</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>GROK SWING ANALYSIS</h1>
        <div class="subtitle">v3.0 価格帯最適化戦略 | 100株あたり損益</div>
        <div class="meta-info">
            <div class="meta-item">◆ 買い・静観 = <span>ロング</span></div>
            <div class="meta-item">◆ 売り = <span>空売り</span></div>
            <div class="meta-item">◆ 🚫 = <span>信用制限</span></div>
            <div class="meta-item">◆ 除外: <span>4570.T</span></div>
        </div>
    </div>

    <!-- v3.0 セクション（トップ） -->
    <div class="version-header v3">
        <h2>v3.0 価格帯最適化戦略</h2>
        <div class="subtitle">期間: {v3_start} ~ {v3_end} | {len(results_v3)}件 | シグナル + 価格帯 → 最適保有期間</div>
    </div>

    <div class="summary-section">
        <h3 class="section-title">v3.0 パフォーマンスサマリー</h3>

        <div style="margin-bottom:24px;">
            {make_v3_strategy_card()}
        </div>

        {make_v3_summary_table(summary_v3)}

        {make_v3_price_range_section(price_summary_v3)}
    </div>

    <div class="summary-section">
        <h3 class="section-title">9時（寄付き〜9:30）詳細</h3>
        <p style="color:var(--text-secondary); margin-bottom:12px; font-size:0.85em;">
            寄付き後の高値・安値の順番。<span style="color:var(--negative);">高値先↓</span>=先に上がって下落、<span style="color:var(--positive);">安値先↑</span>=先に下がって上昇
        </p>
        {make_9am_detail_table()}
    </div>

    <div class="table-section">
        <h3 class="section-title">v3.0 銘柄別詳細</h3>
        {make_v3_detail_table(results_v3)}
    </div>

    <!-- v2.0.3 セクション -->
    <div class="version-header">
        <h2>v2.0.3 スイング分析</h2>
        <div class="subtitle">期間: {v203_start} ~ {v203_end} | {len(results_v203)}件</div>
    </div>

    <div class="summary-section">
        <h3 class="section-title">保有期間別 平均損益（100株）</h3>
        {make_swing_summary_table(summary_v203)}

        <div class="summary-grid" style="margin-top:24px;">
            {make_summary_card(summary_v203, '買い', 'buy')}
            {make_summary_card(summary_v203, '静観', 'hold')}
            {make_summary_card(summary_v203, '売り', 'sell')}
        </div>

        {make_price_range_section(price_summary_v203)}
    </div>

    <div class="table-section">
        <h3 class="section-title">v2.0.3 銘柄別詳細</h3>
        {make_detail_table(results_v203)}
    </div>

    <!-- v2.1 セクション -->
    <div class="version-header">
        <h2>v2.1 スイング分析</h2>
        <div class="subtitle">期間: {v21_start} ~ {v21_end} | {len(results_v21)}件</div>
    </div>

    <div class="summary-section">
        <h3 class="section-title">保有期間別 平均損益（100株）</h3>
        {make_swing_summary_table(summary_v21)}

        <div class="summary-grid" style="margin-top:24px;">
            {make_summary_card(summary_v21, '買い', 'buy')}
            {make_summary_card(summary_v21, '静観', 'hold')}
            {make_summary_card(summary_v21, '売り', 'sell')}
        </div>

        {make_price_range_section(price_summary_v21)}
    </div>

    <div class="table-section">
        <h3 class="section-title">v2.1 銘柄別詳細</h3>
        {make_detail_table(results_v21)}
    </div>

    <!-- V2.1.0.1 ハイブリッド セクション -->
    <div class="version-header v2101">
        <h2>V2.1.0.1 ハイブリッド戦略</h2>
        <div class="subtitle">期間: {v2101_start} ~ {v2101_end} | {len(results_v2101)}件 | v2.0.3ベース + v2.1売りシグナル強化</div>
    </div>

    <div class="summary-section">
        <h3 class="section-title">保有期間別 平均損益（100株）</h3>
        {make_swing_summary_table(summary_v2101)}

        <div class="summary-grid" style="margin-top:24px;">
            {make_summary_card(summary_v2101, '買い', 'buy')}
            {make_summary_card(summary_v2101, '静観', 'hold')}
            {make_summary_card(summary_v2101, '売り', 'sell')}
        </div>

        {make_price_range_section(price_summary_v2101)}
    </div>

    <div class="table-section">
        <h3 class="section-title">V2.1.0.1 銘柄別詳細</h3>
        {make_detail_table(results_v2101)}
    </div>

    <div class="footer">
        <p>Generated: <span>{now}</span> | GROK Swing Analysis Terminal</p>
    </div>
</div>
</body>
</html>
"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    return output_path


def main():
    print("=" * 60)
    print("GROK銘柄 デイスイング分析（ライトテーマHTML）")
    print("=" * 60)

    grok, prices, jsf_stop_codes = load_data()
    print(f"GROK銘柄: {len(grok)}件")
    print(f"価格データ: {len(prices)}件")
    print(f"空売り可能（売りシグナル対象）: {len(grok[(grok['margin_code']=='2') & (~grok['jsf_restricted'])])}件")

    # v2.0.3分析
    print("\n--- v2.0.3 分析 ---")
    results_v203 = calculate_swing_returns(grok, prices, action_col='v2_0_3_action')
    summary_v203 = generate_summary(results_v203)
    price_summary_v203 = generate_price_range_summary(results_v203)

    # v2.1分析
    print("--- v2.1 分析 ---")
    results_v21 = calculate_swing_returns(grok, prices, action_col='v2_1_action')
    summary_v21 = generate_summary(results_v21)
    price_summary_v21 = generate_price_range_summary(results_v21)

    # V2.1.0.1 ハイブリッド分析
    print("--- V2.1.0.1 ハイブリッド 分析 ---")
    results_v2101 = calculate_swing_returns(grok, prices, action_col='v2_1_0_1_action')
    summary_v2101 = generate_summary(results_v2101)
    price_summary_v2101 = generate_price_range_summary(results_v2101)

    # サマリー表示
    print("\n=== v2.0.3 サマリー ===")
    for _, row in summary_v203.iterrows():
        action = row['action']
        print(f"  {action}: 件数={int(row['count'])}")
        for d in [0, 1, 5]:
            label = f'{d+1}日目'
            print(f"    {label}: 平均={fmt_yen(row.get(f'day{d}_avg'))}, 勝率={fmt_pct(row.get(f'day{d}_win'))}")

    print("\n=== v2.1 サマリー ===")
    for _, row in summary_v21.iterrows():
        action = row['action']
        print(f"  {action}: 件数={int(row['count'])}")
        for d in [0, 1, 5]:
            label = f'{d+1}日目'
            print(f"    {label}: 平均={fmt_yen(row.get(f'day{d}_avg'))}, 勝率={fmt_pct(row.get(f'day{d}_win'))}")

    print("\n=== V2.1.0.1 ハイブリッド サマリー ===")
    for _, row in summary_v2101.iterrows():
        action = row['action']
        print(f"  {action}: 件数={int(row['count'])}")
        for d in [0, 1, 5]:
            label = f'{d+1}日目'
            print(f"    {label}: 平均={fmt_yen(row.get(f'day{d}_avg'))}, 勝率={fmt_pct(row.get(f'day{d}_win'))}")

    # v3.0分析
    print("\n--- v3.0 価格帯最適化 分析 ---")
    results_v3 = calculate_v3_returns(grok, prices)
    summary_v3 = generate_v3_summary(results_v3)
    price_summary_v3 = generate_v3_price_range_summary(results_v3)

    print("\n=== v3.0 サマリー ===")
    for _, row in summary_v3.iterrows():
        label = row['label']
        avg = row.get('avg_profit')
        win = row.get('win_rate')
        total = row.get('total_profit')
        print(f"  {label}: シグナル={int(row['count'])}件, 取引={int(row['trade_count'])}件")
        print(f"    平均={fmt_yen(avg)}, 合計={fmt_yen(total)}, 勝率={fmt_pct(win)}")

    # HTML保存
    html_path = OUTPUT_DIR / "grok_swing_analysis_light.html"
    generate_html_report(
        results_v203, results_v21, results_v2101,
        summary_v203, summary_v21, summary_v2101,
        price_summary_v203, price_summary_v21, price_summary_v2101,
        results_v3, summary_v3, price_summary_v3,
        html_path
    )
    print(f"\n✅ HTML: {html_path}")

    return results_v203, results_v21, results_v2101, summary_v203, summary_v21, summary_v2101, results_v3, summary_v3


if __name__ == "__main__":
    main()
