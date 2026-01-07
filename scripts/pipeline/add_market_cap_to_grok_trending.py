#!/usr/bin/env python3
"""
add_market_cap_to_grok_trending.py

grok_trending.parquetに時価総額(market_cap)カラムを追加する

処理:
1. grok_trending.parquet を読み込み
2. prices_max_1d.parquet から終値を取得
3. J-Quants APIで発行済株式数を取得
4. 終値 × 発行済株式数 で時価総額を計算
5. market_capカラムを追加して保存

実行タイミング: 23:00パイプライン（grok_trending.parquet生成後）
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
import os

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import requests
from dotenv import load_dotenv

from scripts.lib.price_limit import calc_price_limit, calc_upper_limit_price, calc_max_cost_100

# .env.jquants 読み込み
load_dotenv(ROOT / ".env.jquants")

# パス設定
GROK_TRENDING_FILE = Path(os.getenv(
    "GROK_TRENDING_FILE",
    ROOT / "data" / "parquet" / "grok_trending.parquet"
))
PRICES_FILE = ROOT / "data" / "parquet" / "prices_max_1d.parquet"

# J-Quants認証情報
JQUANTS_MAIL = os.getenv("JQUANTS_MAIL_ADDRESS")
JQUANTS_PASSWORD = os.getenv("JQUANTS_PASSWORD")

_id_token_cache = None


def get_id_token() -> str:
    """J-Quants IDトークンを取得（キャッシュ付き）"""
    global _id_token_cache
    if _id_token_cache:
        return _id_token_cache

    # リフレッシュトークン取得
    res = requests.post(
        "https://api.jquants.com/v1/token/auth_user",
        json={"mailaddress": JQUANTS_MAIL, "password": JQUANTS_PASSWORD}
    )
    res.raise_for_status()
    refresh_token = res.json()["refreshToken"]

    # IDトークン取得
    res2 = requests.post(
        "https://api.jquants.com/v1/token/auth_refresh",
        params={"refreshtoken": refresh_token}
    )
    res2.raise_for_status()
    _id_token_cache = res2.json()["idToken"]
    return _id_token_cache


def fetch_issued_shares(ticker: str) -> Optional[float]:
    """
    J-Quants APIから発行済株式数を取得

    Args:
        ticker: 銘柄コード (例: "7203.T")

    Returns:
        発行済株式数、取得失敗時はNone
    """
    try:
        code = ticker.replace('.T', '').ljust(5, '0')
        id_token = get_id_token()
        headers = {"Authorization": f"Bearer {id_token}"}

        # 決算データから発行済株式数を取得
        res = requests.get(
            "https://api.jquants.com/v1/fins/statements",
            headers=headers,
            params={"code": code},
            timeout=15
        )
        res.raise_for_status()
        data = res.json()

        if 'statements' not in data or not data['statements']:
            return None

        # 最新のデータを取得（日付順でソート）
        statements = sorted(
            data['statements'],
            key=lambda x: x.get('DisclosedDate', ''),
            reverse=True
        )

        for statement in statements:
            issued_shares = statement.get('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
            if issued_shares:
                return float(issued_shares)

        return None

    except Exception as e:
        print(f"[WARN] Failed to fetch issued shares for {ticker}: {e}")
        return None


def fetch_adjustment_factor(ticker: str, date_str: str) -> float:
    """
    J-Quants APIから調整係数を取得

    Args:
        ticker: 銘柄コード
        date_str: 日付 (YYYY-MM-DD)

    Returns:
        調整係数（デフォルト1.0）
    """
    try:
        code = ticker.replace('.T', '').ljust(5, '0')
        id_token = get_id_token()
        headers = {"Authorization": f"Bearer {id_token}"}

        res = requests.get(
            "https://api.jquants.com/v1/prices/daily_quotes",
            headers=headers,
            params={"code": code, "date": date_str},
            timeout=15
        )
        res.raise_for_status()
        data = res.json()

        if 'daily_quotes' not in data or not data['daily_quotes']:
            return 1.0

        return float(data['daily_quotes'][0].get('AdjustmentFactor', 1.0))

    except Exception:
        return 1.0


def calculate_market_cap(close_price: float, issued_shares: float, adjustment_factor: float = 1.0) -> float:
    """
    時価総額を計算

    Args:
        close_price: 終値
        issued_shares: 発行済株式数
        adjustment_factor: 調整係数

    Returns:
        時価総額（円）
    """
    return close_price * (issued_shares / adjustment_factor)


def get_close_from_prices(ticker: str, prices_df: pd.DataFrame) -> Optional[float]:
    """prices_max_1d.parquetから最新終値を取得"""
    try:
        ticker_df = prices_df[prices_df['ticker'] == ticker].copy()
        if ticker_df.empty:
            return None
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])
        ticker_df = ticker_df.sort_values('date', ascending=False)
        return float(ticker_df.iloc[0]['Close'])
    except Exception:
        return None


def main():
    """メイン処理"""
    print("=== grok_trending.parquet に market_cap カラムを追加 ===\n")

    # 1. grok_trending.parquet 読み込み
    print(f"1. 読み込み: {GROK_TRENDING_FILE}")
    if not GROK_TRENDING_FILE.exists():
        print(f"   エラー: ファイルが見つかりません")
        return 1

    df = pd.read_parquet(GROK_TRENDING_FILE)
    print(f"   銘柄数: {len(df)}")

    # prices_max_1d.parquet 読み込み
    print(f"   prices_max_1d.parquet 読み込み中...")
    if not PRICES_FILE.exists():
        print(f"   エラー: prices_max_1d.parquet が見つかりません")
        return 1
    prices_df = pd.read_parquet(PRICES_FILE)
    print(f"   prices_max_1d: {len(prices_df)} レコード")

    # 日付取得
    if 'date' in df.columns:
        date_str = pd.to_datetime(df['date'].iloc[0]).strftime('%Y-%m-%d')
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
    print(f"   対象日: {date_str}")

    # 2. 各銘柄の時価総額を計算
    print("\n2. 時価総額を計算中...")
    market_caps = []
    success_count = 0

    for idx, row in df.iterrows():
        ticker = row['ticker']

        # grok_trending.parquetのCloseがあればそれを使用、なければprices_max_1dから取得
        close_price = row.get('Close')
        if pd.isna(close_price) or close_price is None or close_price <= 0:
            close_price = get_close_from_prices(ticker, prices_df)

        if close_price is None or close_price <= 0:
            market_caps.append(None)
            print(f"   {ticker}: 終値取得失敗、スキップ")
            continue

        # 発行済株式数を取得
        issued_shares = fetch_issued_shares(ticker)
        if issued_shares is None or issued_shares <= 0:
            market_caps.append(None)
            print(f"   {ticker}: 発行済株式数取得失敗")
            continue

        # 調整係数を取得
        adjustment_factor = fetch_adjustment_factor(ticker, date_str)

        # 時価総額を計算
        market_cap = calculate_market_cap(close_price, issued_shares, adjustment_factor)
        market_caps.append(market_cap)
        success_count += 1

        # 億円換算で表示
        mc_oku = market_cap / 1e8
        print(f"   {ticker}: {mc_oku:.0f}億円")

    # 3. カラム追加
    df['market_cap'] = market_caps
    print(f"\n3. market_capカラム追加完了")
    print(f"   成功: {success_count}/{len(df)} 銘柄")

    # 3.5. 制限値幅・必要資金カラムを追加
    print(f"\n3.5. 制限値幅・必要資金カラムを追加中...")
    if 'Close' not in df.columns:
        print("   ⚠️ Closeカラムが存在しません。制限値幅の計算をスキップします。")
        df['price_limit'] = None
        df['limit_price_upper'] = None
        df['max_cost_100'] = None
    else:
        df['price_limit'] = df['Close'].apply(lambda x: calc_price_limit(x) if pd.notna(x) and x > 0 else None)
        df['limit_price_upper'] = df.apply(
            lambda r: calc_upper_limit_price(r['Close']) if pd.notna(r['Close']) and r['Close'] > 0 else None, axis=1
        )
        df['max_cost_100'] = df.apply(
            lambda r: calc_max_cost_100(r['Close']) if pd.notna(r['Close']) and r['Close'] > 0 else None, axis=1
        )
        valid_costs = df[df['max_cost_100'].notna()]
        print(f"   制限値幅: {len(valid_costs)}/{len(df)} 銘柄で計算完了")

    # 4. 保存
    print(f"\n4. 保存: {GROK_TRENDING_FILE}")
    df.to_parquet(GROK_TRENDING_FILE, index=False)
    print("   完了")

    # 5. サマリー
    valid_caps = df[df['market_cap'].notna()]['market_cap']
    if len(valid_caps) > 0:
        print(f"\n=== サマリー ===")
        print(f"時価総額 中央値: {valid_caps.median() / 1e8:.0f}億円")
        print(f"時価総額 最小: {valid_caps.min() / 1e8:.0f}億円")
        print(f"時価総額 最大: {valid_caps.max() / 1e8:.0f}億円")

        # 500-1000億の銘柄数
        skip_range = valid_caps[(valid_caps >= 500e8) & (valid_caps < 1000e8)]
        print(f"500-1000億: {len(skip_range)}銘柄（見送り対象）")

    return 0


if __name__ == "__main__":
    sys.exit(main())
