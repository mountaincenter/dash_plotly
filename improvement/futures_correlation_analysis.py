"""
日経先物変動 × Grok戦略 利益額分析

目的:
- 日経先物の変動率と、Grok推奨銘柄のショート戦略利益額の相関を分析
- 「先物+X%以上の日は見送り」というルールの検証

データソース:
- S3: futures_prices_max_1d.parquet (日経先物日足)
- ローカル/S3: grok_trending_archive.parquet (Grok推奨結果)
"""

import pandas as pd
import numpy as np
import boto3
import tempfile
import os
from pathlib import Path
from datetime import datetime

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


def load_futures() -> pd.DataFrame:
    """日経先物データを読み込み（S3優先）"""
    try:
        df = load_from_s3("parquet/futures_prices_max_1d.parquet")
        print(f"[S3] futures_prices_max_1d: {df['date'].max()}")
    except Exception as e:
        local_path = DATA_DIR / "futures_prices_max_1d.parquet"
        df = pd.read_parquet(local_path)
        print(f"[ローカル] futures_prices_max_1d: {df['date'].max()}")
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


def calc_futures_change(df_futures: pd.DataFrame) -> pd.DataFrame:
    """先物の変動率を計算"""
    df = df_futures.copy()
    df = df.sort_values("date")

    # 前日終値→当日始値（ギャップ）
    df["prev_close"] = df["Close"].shift(1)
    df["gap_pct"] = (df["Open"] - df["prev_close"]) / df["prev_close"] * 100

    # 前日終値→当日終値（日次変動）
    df["daily_change_pct"] = (df["Close"] - df["prev_close"]) / df["prev_close"] * 100

    # 当日の日中変動（始値→終値）
    df["intraday_pct"] = (df["Close"] - df["Open"]) / df["Open"] * 100

    return df[["date", "Open", "Close", "prev_close", "gap_pct", "daily_change_pct", "intraday_pct"]]


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


def analyze_by_futures_range(df_merged: pd.DataFrame, profit_col: str = "short_p2") -> pd.DataFrame:
    """先物変動率の区間別に利益を集計"""

    # 区間定義
    bins = [-np.inf, -2, -1, 0, 1, 2, np.inf]
    labels = ["<-2%", "-2~-1%", "-1~0%", "0~+1%", "+1~+2%", ">+2%"]

    df = df_merged.copy()
    df["gap_range"] = pd.cut(df["gap_pct"], bins=bins, labels=labels)

    # 区間別集計
    result = df.groupby("gap_range", observed=True).agg(
        件数=(profit_col, "count"),
        利益合計=(profit_col, "sum"),
        利益平均=(profit_col, "mean"),
        勝率=(profit_col, lambda x: (x > 0).mean() * 100),
    ).round(1)

    result["利益合計"] = result["利益合計"].astype(int)
    result["利益平均"] = result["利益平均"].astype(int)

    return result


def analyze_by_daily_change(df_merged: pd.DataFrame, profit_col: str = "short_p2") -> pd.DataFrame:
    """先物の日次変動（前日比）別に利益を集計"""

    bins = [-np.inf, -2, -1, 0, 1, 2, np.inf]
    labels = ["<-2%", "-2~-1%", "-1~0%", "0~+1%", "+1~+2%", ">+2%"]

    df = df_merged.copy()
    df["change_range"] = pd.cut(df["daily_change_pct"], bins=bins, labels=labels)

    result = df.groupby("change_range", observed=True).agg(
        件数=(profit_col, "count"),
        利益合計=(profit_col, "sum"),
        利益平均=(profit_col, "mean"),
        勝率=(profit_col, lambda x: (x > 0).mean() * 100),
    ).round(1)

    result["利益合計"] = result["利益合計"].astype(int)
    result["利益平均"] = result["利益平均"].astype(int)

    return result


def simulate_skip_rule(df_merged: pd.DataFrame, threshold: float, profit_col: str = "short_p2") -> dict:
    """
    「先物ギャップがthreshold%以上の日は見送り」ルールをシミュレーション
    """
    df = df_merged.copy()

    # 全日エントリー
    all_profit = df[profit_col].sum()
    all_count = len(df)

    # 見送りルール適用
    df_filtered = df[df["gap_pct"] < threshold]
    filtered_profit = df_filtered[profit_col].sum()
    filtered_count = len(df_filtered)

    skipped_count = all_count - filtered_count
    skipped_profit = all_profit - filtered_profit  # 見送った分の損益

    return {
        "閾値": f"+{threshold}%",
        "全日利益": int(all_profit),
        "全日件数": all_count,
        "適用後利益": int(filtered_profit),
        "適用後件数": filtered_count,
        "見送り件数": skipped_count,
        "見送り損益": int(skipped_profit),
        "改善額": int(filtered_profit - all_profit) if skipped_profit < 0 else 0,
    }


def main():
    print("=" * 60)
    print("日経先物変動 × Grok戦略 利益額分析")
    print("=" * 60)

    # データ読み込み
    df_futures = load_futures()
    df_grok = load_grok_archive()

    # 前処理
    df_futures_calc = calc_futures_change(df_futures)
    df_grok_prep = prepare_grok_data(df_grok)

    # 日付でマージ
    df_futures_calc["date"] = pd.to_datetime(df_futures_calc["date"]).dt.normalize()
    df_grok_prep["date"] = pd.to_datetime(df_grok_prep["date"]).dt.normalize()

    df_merged = df_grok_prep.merge(
        df_futures_calc[["date", "gap_pct", "daily_change_pct", "intraday_pct"]],
        on="date",
        how="left"
    )

    print(f"\nマージ後: {len(df_merged)}件")
    print(f"先物データあり: {df_merged['gap_pct'].notna().sum()}件")

    # 先物データがない行を除外
    df_merged = df_merged[df_merged["gap_pct"].notna()]

    # ===== 全体分析 =====
    print("\n" + "=" * 60)
    print("【全体】ショート戦略 利益額")
    print("=" * 60)
    for col, label in [("short_me", "10:25"), ("short_p1", "前場引け"), ("short_ae", "14:45"), ("short_p2", "大引け")]:
        total = df_merged[col].sum()
        avg = df_merged[col].mean()
        win_rate = (df_merged[col] > 0).mean() * 100
        print(f"  {label}: 合計{int(total):+,}円, 平均{int(avg):+}円, 勝率{win_rate:.1f}%")

    # ===== 先物ギャップ別分析 =====
    print("\n" + "=" * 60)
    print("【先物ギャップ別】ショート利益（大引け）")
    print("=" * 60)
    result_gap = analyze_by_futures_range(df_merged, "short_p2")
    print(result_gap.to_string())

    # ===== 先物日次変動別分析 =====
    print("\n" + "=" * 60)
    print("【先物日次変動別】ショート利益（大引け）")
    print("=" * 60)
    result_change = analyze_by_daily_change(df_merged, "short_p2")
    print(result_change.to_string())

    # ===== 見送りルールシミュレーション =====
    print("\n" + "=" * 60)
    print("【見送りルール シミュレーション】")
    print("  ルール: 先物ギャップが+X%以上の日は見送り")
    print("=" * 60)

    thresholds = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
    results = [simulate_skip_rule(df_merged, t, "short_p2") for t in thresholds]
    df_sim = pd.DataFrame(results)
    print(df_sim.to_string(index=False))

    # ===== 制度/いちにち別分析 =====
    print("\n" + "=" * 60)
    print("【信用区分別】先物ギャップ別ショート利益")
    print("=" * 60)

    for margin_type in ["制度信用", "いちにち信用"]:
        df_sub = df_merged[df_merged["margin_type"] == margin_type]
        if margin_type == "いちにち信用":
            df_sub = df_sub[df_sub["is_ex0"]]  # 0株除外
        print(f"\n--- {margin_type} ({len(df_sub)}件) ---")
        result = analyze_by_futures_range(df_sub, "short_p2")
        print(result.to_string())

    # ===== 結論 =====
    print("\n" + "=" * 60)
    print("【結論】")
    print("=" * 60)

    # 最適な閾値を探す
    best = max(results, key=lambda x: x["適用後利益"])
    print(f"最適閾値: {best['閾値']}")
    print(f"  → 全日エントリー利益: {best['全日利益']:+,}円")
    print(f"  → 適用後利益: {best['適用後利益']:+,}円")
    print(f"  → 改善額: {best['改善額']:+,}円")

    # CSVエクスポート
    output_file = OUTPUT_DIR / f"futures_correlation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_merged.to_csv(output_file, index=False)
    print(f"\n詳細データ出力: {output_file}")


if __name__ == "__main__":
    main()
