"""
VI（ボラティリティ指数） × Grok戦略 相関分析

目的:
1. 当日のVI値とGrok戦略利益の相関（連関）
2. 前営業日のVI値が先行指標として有用か

データソース:
- yfinance: ^VIX（米国VIX）、^N225（日経平均）
- ローカル/S3: grok_trending_archive.parquet
"""

import pandas as pd
import numpy as np
import boto3
import tempfile
import os
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf

# パス設定
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "parquet"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

S3_BUCKET = os.getenv("S3_BUCKET", "stock-api-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")


def load_from_s3(key: str) -> pd.DataFrame:
    """S3からparquetを読み込み"""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        s3.download_fileobj(S3_BUCKET, key, tmp)
        tmp_path = tmp.name
    df = pd.read_parquet(tmp_path)
    os.unlink(tmp_path)
    return df


def load_grok_archive() -> pd.DataFrame:
    """Grokアーカイブを読み込み"""
    local_path = DATA_DIR / "backtest" / "grok_trending_archive.parquet"
    if local_path.exists():
        df = pd.read_parquet(local_path)
        print(f"[ローカル] grok_trending_archive: {len(df)}件")
    else:
        df = load_from_s3("parquet/backtest/grok_trending_archive.parquet")
        print(f"[S3] grok_trending_archive: {len(df)}件")
    return df


def load_vix_data(start_date: str, end_date: str) -> pd.DataFrame:
    """米国VIXデータを取得"""
    ticker = yf.Ticker("^VIX")
    df = ticker.history(start=start_date, end=end_date)
    df = df.reset_index()
    df["date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()
    df = df.rename(columns={"Close": "vix_close", "Open": "vix_open"})
    print(f"[yfinance] VIX: {len(df)}件 ({df['date'].min()} ~ {df['date'].max()})")
    return df[["date", "vix_open", "vix_close"]]


def load_n225_data(start_date: str, end_date: str) -> pd.DataFrame:
    """日経平均データを取得（ヒストリカルボラティリティ計算用）"""
    ticker = yf.Ticker("^N225")
    df = ticker.history(start=start_date, end=end_date)
    df = df.reset_index()
    df["date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()

    # 日次リターン
    df["daily_return"] = df["Close"].pct_change()

    # 5日ヒストリカルボラティリティ（年率換算）
    df["hv_5d"] = df["daily_return"].rolling(window=5).std() * np.sqrt(252) * 100

    # 10日ヒストリカルボラティリティ
    df["hv_10d"] = df["daily_return"].rolling(window=10).std() * np.sqrt(252) * 100

    print(f"[yfinance] N225: {len(df)}件")
    return df[["date", "Close", "daily_return", "hv_5d", "hv_10d"]]


def prepare_grok_data(df_grok: pd.DataFrame) -> pd.DataFrame:
    """Grokデータを準備（ショート戦略用に符号反転）"""
    df = df_grok.copy()

    # 日付カラム正規化
    if "selection_date" in df.columns:
        df["date"] = pd.to_datetime(df["selection_date"])
    elif "backtest_date" in df.columns:
        df["date"] = pd.to_datetime(df["backtest_date"])

    # 2025-11-04以降のみ
    df = df[df["date"] >= "2025-11-04"]

    # 制度信用 or いちにち信用のみ
    df = df[(df["shortable"] == True) | ((df["day_trade"] == True) & (df["shortable"] == False))]

    # 信用区分
    df["margin_type"] = df.apply(lambda r: "制度信用" if r["shortable"] else "いちにち信用", axis=1)

    # 除0株フラグ
    df["is_ex0"] = df.apply(
        lambda r: True if r["shortable"] else (
            pd.isna(r.get("day_trade_available_shares")) or r.get("day_trade_available_shares", 0) > 0
        ),
        axis=1
    )

    # ショート戦略用に符号反転
    df["short_p1"] = -df["profit_per_100_shares_phase1"].fillna(0)
    df["short_p2"] = -df["profit_per_100_shares_phase2"].fillna(0)
    df["short_me"] = -df["profit_per_100_shares_morning_early"].fillna(0)
    df["short_ae"] = -df["profit_per_100_shares_afternoon_early"].fillna(0)

    return df


def analyze_by_vix_range(df_merged: pd.DataFrame, vix_col: str, profit_col: str = "short_p2") -> pd.DataFrame:
    """VIX水準別に利益を集計"""

    # VIXの区間定義
    bins = [0, 12, 15, 18, 22, 30, np.inf]
    labels = ["<12(低)", "12-15", "15-18", "18-22", "22-30", ">30(高)"]

    df = df_merged.copy()
    df["vix_range"] = pd.cut(df[vix_col], bins=bins, labels=labels)

    result = df.groupby("vix_range", observed=True).agg(
        件数=(profit_col, "count"),
        利益合計=(profit_col, "sum"),
        利益平均=(profit_col, "mean"),
        勝率=(profit_col, lambda x: (x > 0).mean() * 100),
    ).round(1)

    result["利益合計"] = result["利益合計"].astype(int)
    result["利益平均"] = result["利益平均"].astype(int)

    return result


def analyze_by_hv_range(df_merged: pd.DataFrame, hv_col: str, profit_col: str = "short_p2") -> pd.DataFrame:
    """ヒストリカルボラティリティ水準別に利益を集計"""

    bins = [0, 10, 15, 20, 25, 35, np.inf]
    labels = ["<10%(低)", "10-15%", "15-20%", "20-25%", "25-35%", ">35%(高)"]

    df = df_merged.copy()
    df["hv_range"] = pd.cut(df[hv_col], bins=bins, labels=labels)

    result = df.groupby("hv_range", observed=True).agg(
        件数=(profit_col, "count"),
        利益合計=(profit_col, "sum"),
        利益平均=(profit_col, "mean"),
        勝率=(profit_col, lambda x: (x > 0).mean() * 100),
    ).round(1)

    result["利益合計"] = result["利益合計"].astype(int)
    result["利益平均"] = result["利益平均"].astype(int)

    return result


def analyze_prev_day_indicator(df_merged: pd.DataFrame, indicator_col: str, profit_col: str = "short_p2") -> pd.DataFrame:
    """前営業日の指標値が先行指標として有用か検証"""

    df = df_merged.copy()

    # 前営業日の値を取得
    df = df.sort_values("date")

    # 日次集計（同じ日の銘柄をまとめる）
    daily = df.groupby("date").agg({
        indicator_col: "first",
        profit_col: "sum",
    }).reset_index()

    # 前日の指標値
    daily["prev_indicator"] = daily[indicator_col].shift(1)
    daily["prev_indicator_change"] = daily[indicator_col] - daily["prev_indicator"]

    # 前日VIX変化で区分
    bins = [-np.inf, -2, 0, 2, np.inf]
    labels = ["急落(<-2)", "下落(-2~0)", "上昇(0~+2)", "急騰(>+2)"]
    daily["prev_change_range"] = pd.cut(daily["prev_indicator_change"], bins=bins, labels=labels)

    result = daily.groupby("prev_change_range", observed=True).agg(
        日数=(profit_col, "count"),
        利益合計=(profit_col, "sum"),
        利益平均=(profit_col, "mean"),
    ).round(1)

    result["利益合計"] = result["利益合計"].astype(int)
    result["利益平均"] = result["利益平均"].astype(int)

    return result


def calc_correlation(df_merged: pd.DataFrame, indicator_col: str, profit_col: str = "short_p2") -> dict:
    """相関係数を計算"""
    df = df_merged.copy()

    # 日次集計
    daily = df.groupby("date").agg({
        indicator_col: "first",
        profit_col: "sum",
    }).reset_index()

    # 欠損除去
    daily = daily.dropna(subset=[indicator_col, profit_col])

    if len(daily) < 5:
        return {"n": len(daily), "correlation": None, "message": "データ不足"}

    corr = daily[indicator_col].corr(daily[profit_col])

    return {
        "n": len(daily),
        "correlation": round(corr, 3),
        "interpretation": "負の相関（VI高→利益減）" if corr < -0.3 else
                         "正の相関（VI高→利益増）" if corr > 0.3 else "相関弱い"
    }


def main():
    print("=" * 60)
    print("VI（ボラティリティ指数） × Grok戦略 相関分析")
    print("=" * 60)

    # データ読み込み
    df_grok = load_grok_archive()
    df_grok_prep = prepare_grok_data(df_grok)

    # 日付範囲
    min_date = df_grok_prep["date"].min()
    max_date = df_grok_prep["date"].max()
    print(f"\nGrokデータ期間: {min_date.strftime('%Y-%m-%d')} ~ {max_date.strftime('%Y-%m-%d')}")

    # VIXと日経平均データ取得
    start_date = (min_date - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = (max_date + timedelta(days=1)).strftime("%Y-%m-%d")

    df_vix = load_vix_data(start_date, end_date)
    df_n225 = load_n225_data(start_date, end_date)

    # 日付でマージ
    df_grok_prep["date"] = pd.to_datetime(df_grok_prep["date"]).dt.normalize()

    df_merged = df_grok_prep.merge(df_vix, on="date", how="left")
    df_merged = df_merged.merge(df_n225[["date", "hv_5d", "hv_10d"]], on="date", how="left")

    print(f"\nマージ後: {len(df_merged)}件")
    print(f"VIXデータあり: {df_merged['vix_close'].notna().sum()}件")
    print(f"HVデータあり: {df_merged['hv_5d'].notna().sum()}件")

    # VIX/HVデータがない行を除外
    df_merged = df_merged[df_merged["vix_close"].notna() & df_merged["hv_5d"].notna()]

    # ===== 1. 当日の相関分析 =====
    print("\n" + "=" * 60)
    print("【1. 当日のVI値とGrok利益の相関】")
    print("=" * 60)

    # VIX相関
    print("\n--- 米国VIX ---")
    corr_vix = calc_correlation(df_merged, "vix_close", "short_p2")
    print(f"  相関係数: {corr_vix['correlation']} (n={corr_vix['n']})")
    print(f"  解釈: {corr_vix['interpretation']}")

    # HV相関
    print("\n--- 日経HV(5日) ---")
    corr_hv = calc_correlation(df_merged, "hv_5d", "short_p2")
    print(f"  相関係数: {corr_hv['correlation']} (n={corr_hv['n']})")
    print(f"  解釈: {corr_hv['interpretation']}")

    # VIX水準別
    print("\n--- VIX水準別 利益 ---")
    result_vix = analyze_by_vix_range(df_merged, "vix_close", "short_p2")
    print(result_vix.to_string())

    # HV水準別
    print("\n--- 日経HV(5日)水準別 利益 ---")
    result_hv = analyze_by_hv_range(df_merged, "hv_5d", "short_p2")
    print(result_hv.to_string())

    # ===== 2. 前営業日の先行指標分析 =====
    print("\n" + "=" * 60)
    print("【2. 前営業日VIXの先行指標分析】")
    print("  ※前日のVIX変化が翌日のGrok利益を予測できるか")
    print("=" * 60)

    result_prev = analyze_prev_day_indicator(df_merged, "vix_close", "short_p2")
    print(result_prev.to_string())

    # ===== 3. 特異日の分析（高市報道 2026-01-09） =====
    print("\n" + "=" * 60)
    print("【3. 特異日の確認（高市総選挙報道 2026-01-09夜）】")
    print("=" * 60)

    # 1/10のデータ（報道翌営業日 = 1/14?）
    # 1/9(木)夜報道 → 1/10(金) が影響日
    special_dates = ["2026-01-10", "2026-01-14"]  # 金曜と翌週月曜（祝日なら火曜）

    for date_str in special_dates:
        df_day = df_merged[df_merged["date"] == date_str]
        if len(df_day) > 0:
            profit = df_day["short_p2"].sum()
            count = len(df_day)
            vix = df_day["vix_close"].iloc[0] if not df_day["vix_close"].isna().all() else "N/A"
            print(f"\n{date_str}:")
            print(f"  銘柄数: {count}")
            print(f"  ショート利益: {int(profit):+,}円")
            print(f"  VIX: {vix}")
        else:
            print(f"\n{date_str}: データなし（祝日?）")

    # ===== 結論 =====
    print("\n" + "=" * 60)
    print("【結論】")
    print("=" * 60)

    print(f"""
1. 当日VIXとの相関: {corr_vix['correlation']} ({corr_vix['interpretation']})
   → VIX水準別の利益差を確認して判断

2. 前営業日VIX変化の先行指標:
   → 上記テーブルから「VIX急騰/急落後の翌日利益」を確認

3. 特異日（高市報道）:
   → 通常のVI指標では予測困難なイベントリスク
""")

    # CSVエクスポート
    output_file = OUTPUT_DIR / f"vi_correlation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_merged.to_csv(output_file, index=False)
    print(f"\n詳細データ出力: {output_file}")


if __name__ == "__main__":
    main()
