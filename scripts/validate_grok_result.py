#!/usr/bin/env python3
"""
validate_grok_result.py
Grok選定結果を検証するスクリプト

使い方:
    1. Grokから返ってきたJSON結果をファイルに保存
       例: data/parquet/grok_result_test.json

    2. スクリプト実行
       python3 scripts/validate_grok_result.py data/parquet/grok_result_test.json

    3. 検証レポートが表示される

検証項目:
    - 時価総額が50〜500億円の範囲内か
    - 除外銘柄（Core30、高配当リスト）に含まれていないか
    - 重複銘柄がないか
    - （オプション）翌営業日の実際の値動き
"""

from __future__ import annotations

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yfinance as yf
from scripts.lib.jquants_fetcher import JQuantsFetcher

# 除外銘柄リスト（銘柄コード）
EXCLUDED_CODES = {
    # Core30/TOPIX Core30 & 日経225主力株
    "7203", "6758", "8306", "9984", "6861", "7974", "4063", "8035",
    "6367", "6098", "9433", "9432", "9020", "9983", "4568",

    # 高配当・政策関連株
    "7011", "7012", "7013", "6954", "6506", "5401", "8031", "8058",
    "8001", "8053", "8002"
}


def load_grok_result(file_path: Path) -> list[dict[str, Any]]:
    """Grok結果のJSONファイルを読み込む"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array, got {type(data)}")

    print(f"[OK] Loaded {len(data)} stocks from {file_path}")
    return data


def validate_market_cap(ticker_code: str, fetcher: JQuantsFetcher) -> dict[str, Any]:
    """
    時価総額を取得して50〜500億円の範囲内かチェック

    Returns:
        {
            "market_cap_billion": float or None,
            "in_range": bool,
            "error": str or None
        }
    """
    try:
        # J-Quantsから銘柄情報を取得
        response = fetcher.client.request("/listed/info", params={"code": ticker_code})

        if not response or "info" not in response or len(response["info"]) == 0:
            return {
                "market_cap_billion": None,
                "in_range": False,
                "error": "銘柄情報が取得できませんでした"
            }

        info = response["info"][0]
        market_cap = info.get("MarketCapitalization")  # 時価総額（百万円単位）

        if market_cap is None:
            return {
                "market_cap_billion": None,
                "in_range": False,
                "error": "時価総額データが存在しません"
            }

        # 百万円 → 億円に変換
        market_cap_billion = market_cap / 100

        # 50〜500億円の範囲内か
        in_range = 50 <= market_cap_billion <= 500

        return {
            "market_cap_billion": round(market_cap_billion, 1),
            "in_range": in_range,
            "error": None
        }

    except Exception as e:
        return {
            "market_cap_billion": None,
            "in_range": False,
            "error": str(e)
        }


def validate_exclusion(ticker_code: str) -> dict[str, Any]:
    """
    除外銘柄リストに含まれていないかチェック

    Returns:
        {
            "is_excluded": bool,
            "reason": str or None
        }
    """
    if ticker_code in EXCLUDED_CODES:
        return {
            "is_excluded": True,
            "reason": "Core30/高配当リストに含まれています"
        }

    return {
        "is_excluded": False,
        "reason": None
    }


def validate_next_day_movement(ticker_symbol: str, target_date: str) -> dict[str, Any]:
    """
    翌営業日の値動きをチェック

    Args:
        ticker_symbol: 銘柄コード（例: "2459.T"）
        target_date: 対象日（YYYY-MM-DD）

    Returns:
        {
            "open": float or None,
            "high": float or None,
            "low": float or None,
            "close": float or None,
            "volume": int or None,
            "change_pct": float or None,  # 終値変化率
            "range_pct": float or None,   # 値幅率（高値-安値）/始値
            "error": str or None
        }
    """
    try:
        # yfinanceで翌営業日のデータを取得
        target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        end_dt = target_dt + timedelta(days=5)  # 余裕を持って5日後まで取得

        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(start=target_date, end=end_dt.strftime("%Y-%m-%d"))

        if hist.empty:
            return {
                "open": None, "high": None, "low": None, "close": None,
                "volume": None, "change_pct": None, "range_pct": None,
                "error": f"{target_date}のデータが取得できませんでした（休場の可能性）"
            }

        # 最初の行が対象日のデータ
        row = hist.iloc[0]

        open_price = row["Open"]
        high_price = row["High"]
        low_price = row["Low"]
        close_price = row["Close"]
        volume = int(row["Volume"])

        # 変化率を計算
        change_pct = ((close_price - open_price) / open_price) * 100 if open_price > 0 else None
        range_pct = ((high_price - low_price) / open_price) * 100 if open_price > 0 else None

        return {
            "open": round(open_price, 1),
            "high": round(high_price, 1),
            "low": round(low_price, 1),
            "close": round(close_price, 1),
            "volume": volume,
            "change_pct": round(change_pct, 2) if change_pct is not None else None,
            "range_pct": round(range_pct, 2) if range_pct is not None else None,
            "error": None
        }

    except Exception as e:
        return {
            "open": None, "high": None, "low": None, "close": None,
            "volume": None, "change_pct": None, "range_pct": None,
            "error": str(e)
        }


def validate_grok_stocks(grok_data: list[dict], target_date: str = None) -> pd.DataFrame:
    """
    Grok選定結果を一括検証

    Args:
        grok_data: Grok選定結果のJSON配列
        target_date: 翌営業日の日付（YYYY-MM-DD）。Noneの場合は値動きチェックをスキップ

    Returns:
        検証結果のDataFrame
    """
    fetcher = JQuantsFetcher()
    results = []

    print("\n" + "=" * 80)
    print("Grok選定結果の検証を開始")
    print("=" * 80)

    for i, stock in enumerate(grok_data, 1):
        ticker_code = stock.get("ticker_symbol", "").replace(".T", "")
        company_name = stock.get("company_name", "")
        mentioned_by = stock.get("mentioned_by", [])

        print(f"\n[{i}/{len(grok_data)}] {ticker_code} - {company_name}")

        # 1. 時価総額チェック
        print("  - 時価総額チェック中...")
        market_cap_result = validate_market_cap(ticker_code, fetcher)

        # 2. 除外銘柄チェック
        print("  - 除外銘柄チェック中...")
        exclusion_result = validate_exclusion(ticker_code)

        # 3. 翌日値動きチェック（target_dateが指定されている場合のみ）
        next_day_result = {}
        if target_date:
            print(f"  - 翌営業日（{target_date}）の値動きチェック中...")
            ticker_symbol = f"{ticker_code}.T"
            next_day_result = validate_next_day_movement(ticker_symbol, target_date)

        # 結果を集約
        result = {
            "ticker_code": ticker_code,
            "company_name": company_name,
            "mentioned_by": ", ".join(mentioned_by) if mentioned_by else "",
            "market_cap_billion": market_cap_result["market_cap_billion"],
            "market_cap_in_range": market_cap_result["in_range"],
            "market_cap_error": market_cap_result["error"],
            "is_excluded": exclusion_result["is_excluded"],
            "exclusion_reason": exclusion_result["reason"],
        }

        # 翌日値動き結果を追加
        if target_date:
            result.update({
                "next_day_open": next_day_result.get("open"),
                "next_day_high": next_day_result.get("high"),
                "next_day_low": next_day_result.get("low"),
                "next_day_close": next_day_result.get("close"),
                "next_day_volume": next_day_result.get("volume"),
                "next_day_change_pct": next_day_result.get("change_pct"),
                "next_day_range_pct": next_day_result.get("range_pct"),
                "next_day_error": next_day_result.get("error"),
            })

        results.append(result)

    df = pd.DataFrame(results)
    return df


def print_validation_summary(df: pd.DataFrame, target_date: str = None):
    """検証結果のサマリーを表示"""
    print("\n" + "=" * 80)
    print("検証結果サマリー")
    print("=" * 80)

    total = len(df)

    # 1. 時価総額チェック
    market_cap_ok = df["market_cap_in_range"].sum()
    market_cap_ng = total - market_cap_ok
    print(f"\n【時価総額チェック（50〜500億円）】")
    print(f"  ✅ 範囲内: {market_cap_ok}銘柄 ({market_cap_ok/total*100:.1f}%)")
    print(f"  ❌ 範囲外: {market_cap_ng}銘柄 ({market_cap_ng/total*100:.1f}%)")

    if market_cap_ng > 0:
        print(f"\n  範囲外の銘柄:")
        ng_stocks = df[~df["market_cap_in_range"]]
        for _, row in ng_stocks.iterrows():
            cap = row["market_cap_billion"]
            cap_str = f"{cap:.1f}億円" if cap is not None else "取得不可"
            print(f"    - {row['ticker_code']} ({row['company_name']}): {cap_str}")

    # 2. 除外銘柄チェック
    excluded_count = df["is_excluded"].sum()
    print(f"\n【除外銘柄チェック】")
    print(f"  ✅ 除外リスト非該当: {total - excluded_count}銘柄")
    print(f"  ❌ 除外リスト該当: {excluded_count}銘柄")

    if excluded_count > 0:
        print(f"\n  除外リストに該当する銘柄:")
        excluded_stocks = df[df["is_excluded"]]
        for _, row in excluded_stocks.iterrows():
            print(f"    - {row['ticker_code']} ({row['company_name']}): {row['exclusion_reason']}")

    # 3. 重複チェック
    duplicates = df[df.duplicated(subset=["ticker_code"], keep=False)]
    duplicate_count = len(duplicates)
    print(f"\n【重複チェック】")
    print(f"  ✅ 重複なし: {duplicate_count == 0}")
    print(f"  ❌ 重複あり: {duplicate_count}銘柄")

    if duplicate_count > 0:
        print(f"\n  重複している銘柄:")
        for code in duplicates["ticker_code"].unique():
            count = len(df[df["ticker_code"] == code])
            print(f"    - {code}: {count}回出現")

    # 4. プレミアムユーザー言及
    mentioned_count = df[df["mentioned_by"] != ""].shape[0]
    mentioned_rate = mentioned_count / total * 100
    print(f"\n【プレミアムユーザー言及】")
    print(f"  言及あり: {mentioned_count}銘柄 ({mentioned_rate:.1f}%)")
    print(f"  言及なし: {total - mentioned_count}銘柄 ({100 - mentioned_rate:.1f}%)")

    # 5. 翌日値動き（target_dateが指定されている場合のみ）
    if target_date:
        print(f"\n【翌営業日（{target_date}）の値動き】")

        # データ取得成功率
        has_data = df["next_day_change_pct"].notna().sum()
        print(f"  データ取得成功: {has_data}銘柄 ({has_data/total*100:.1f}%)")

        if has_data > 0:
            # 平均変化率
            avg_change = df["next_day_change_pct"].mean()
            print(f"  平均変化率: {avg_change:.2f}%")

            # 平均値幅率
            avg_range = df["next_day_range_pct"].mean()
            print(f"  平均値幅率: {avg_range:.2f}%")

            # 上昇・下落の内訳
            up_count = (df["next_day_change_pct"] > 0).sum()
            down_count = (df["next_day_change_pct"] < 0).sum()
            print(f"  上昇: {up_count}銘柄")
            print(f"  下落: {down_count}銘柄")

            # 値幅2%以上の銘柄
            volatile_count = (df["next_day_range_pct"] >= 2.0).sum()
            print(f"  値幅2%以上: {volatile_count}銘柄 ({volatile_count/has_data*100:.1f}%)")

    print("\n" + "=" * 80)


def main():
    """メイン処理"""
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/validate_grok_result.py <grok_result.json> [target_date]")
        print()
        print("Examples:")
        print("  # 時価総額・除外チェックのみ")
        print("  python3 scripts/validate_grok_result.py data/parquet/grok_result_test.json")
        print()
        print("  # 翌営業日の値動きもチェック")
        print("  python3 scripts/validate_grok_result.py data/parquet/grok_result_test.json 2025-10-27")
        return 1

    json_path = Path(sys.argv[1])
    target_date = sys.argv[2] if len(sys.argv) >= 3 else None

    if not json_path.exists():
        print(f"[ERROR] File not found: {json_path}")
        return 1

    try:
        # 1. Grok結果を読み込み
        grok_data = load_grok_result(json_path)

        # 2. 検証実行
        df = validate_grok_stocks(grok_data, target_date)

        # 3. サマリー表示
        print_validation_summary(df, target_date)

        # 4. 詳細結果をCSVで保存
        output_csv = json_path.parent / f"{json_path.stem}_validation.csv"
        df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        print(f"\n[OK] 詳細結果を保存: {output_csv}")

        return 0

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
