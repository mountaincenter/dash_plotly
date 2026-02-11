#!/usr/bin/env python3
"""
fetch_supply_demand.py
J-Quants API + 日証金CSVで需給データを取得してparquet保存

取得データ:
1. 業種別空売り比率       → short_ratio_max_1d.parquet
2. 日々公表信用取引残高   → margin_alert_max_1d.parquet
3. プットコールレシオ     → put_call_ratio_max_1d.parquet (option_pricesから算出)
4. 投資部門別売買動向     → market_breakdown_max_1d.parquet (Premium - 失敗時スキップ)
5. 日証金 貸借残高        → jsf_lending_max_1d.parquet (taisyaku.jp CSV)

実行:
    python3 scripts/pipeline/fetch_supply_demand.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import requests
from scripts.lib.jquants_fetcher import JQuantsFetcher

PARQUET_DIR = ROOT / "data" / "parquet"


def save_parquet_append(
    df: pd.DataFrame, path: Path, date_col: str = "date"
) -> None:
    """既存parquetに追記保存（同日データは上書き）"""
    if df.empty:
        print(f"  [SKIP] No data to save for {path.name}")
        return

    if path.exists():
        existing = pd.read_parquet(path)
        print(f"  Existing rows: {len(existing)}")

        # 同日データを除外してから追記
        if date_col in existing.columns and date_col in df.columns:
            new_dates = df[date_col].unique()
            existing = existing[~existing[date_col].isin(new_dates)]

        df = pd.concat([existing, df], ignore_index=True)

    df.to_parquet(path, index=False)
    print(f"  Saved {path.name}: {len(df)} rows ({path.stat().st_size:,} bytes)")


# ── 1. 業種別空売り比率 ──


def fetch_short_ratio(fetcher: JQuantsFetcher, target_date: str) -> None:
    """業種別空売り比率を取得"""
    print(f"\n[1/5] 業種別空売り比率 (date={target_date})")
    try:
        df = fetcher.get_short_ratio(date_val=target_date)
        if df.empty:
            print("  [WARN] No data")
            return

        df.columns = df.columns.str.lower()
        print(f"  Retrieved {len(df)} sector records")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # 主要カラムを表示
        if "sector33codename" in df.columns and "sellingratio" in df.columns:
            top5 = df.nlargest(5, "sellingratio")
            print("  空売り比率 Top5:")
            for _, row in top5.iterrows():
                print(f"    {row.get('sector33codename', '?')}: {row['sellingratio']:.1f}%")

        save_parquet_append(df, PARQUET_DIR / "short_ratio_max_1d.parquet")

    except Exception as e:
        print(f"  [ERROR] {e}")


# ── 2. 日々公表信用取引残高 ──


def fetch_margin_alert(fetcher: JQuantsFetcher, target_date: str) -> None:
    """日々公表信用取引残高を取得"""
    print(f"\n[2/5] 日々公表信用取引残高 (date={target_date})")
    try:
        df = fetcher.get_margin_alert(date_val=target_date)
        if df.empty:
            print("  [WARN] No data (対象銘柄なし or 非公表日)")
            return

        df.columns = df.columns.str.lower()
        print(f"  Retrieved {len(df)} stock records")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # 数値カラムの型変換（NaN混在対策）
        for col in df.columns:
            if col in ("date", "code", "codename", "sector33code", "sector33codename"):
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")

        save_parquet_append(df, PARQUET_DIR / "margin_alert_max_1d.parquet")

    except Exception as e:
        print(f"  [ERROR] {e}")


# ── 3. プットコールレシオ ──


def compute_put_call_ratio() -> None:
    """オプションデータからプットコールレシオを算出"""
    print("\n[3/5] プットコールレシオ算出")
    option_path = PARQUET_DIR / "option_prices_max_1d.parquet"

    if not option_path.exists():
        print("  [SKIP] option_prices_max_1d.parquet not found")
        print("  → 先に update_option_prices.py を実行してください")
        return

    try:
        df = pd.read_parquet(option_path)
        if df.empty:
            print("  [WARN] Empty option data")
            return

        pc_col = "putcalldivision"
        if pc_col not in df.columns:
            print(f"  [ERROR] '{pc_col}' column not found. Columns: {df.columns.tolist()}")
            return

        # 日付別にPut/Callの出来高・建玉を集計
        grouped = (
            df.groupby(["date", pc_col])
            .agg(volume_sum=("volume", "sum"), oi_sum=("openinterest", "sum"))
            .reset_index()
        )

        puts = (
            grouped[grouped[pc_col] == "Put"]
            .rename(columns={"volume_sum": "put_volume", "oi_sum": "put_oi"})
            .drop(columns=[pc_col])
        )
        calls = (
            grouped[grouped[pc_col] == "Call"]
            .rename(columns={"volume_sum": "call_volume", "oi_sum": "call_oi"})
            .drop(columns=[pc_col])
        )

        pcr = puts.merge(calls, on="date", how="outer").fillna(0)
        pcr["pcr_volume"] = pcr["put_volume"] / pcr["call_volume"].replace(0, float("nan"))
        pcr["pcr_oi"] = pcr["put_oi"] / pcr["call_oi"].replace(0, float("nan"))

        if "date" in pcr.columns:
            pcr["date"] = pd.to_datetime(pcr["date"])
        pcr = pcr.sort_values("date")

        print(f"  Computed PCR for {len(pcr)} dates")
        if not pcr.empty:
            latest = pcr.iloc[-1]
            print(f"  Latest: date={latest['date']}")
            print(f"    PCR(volume) = {latest['pcr_volume']:.3f}")
            print(f"    PCR(OI)     = {latest['pcr_oi']:.3f}")
            print(f"    Put vol={int(latest['put_volume'])}, Call vol={int(latest['call_volume'])}")

        save_parquet_append(pcr, PARQUET_DIR / "put_call_ratio_max_1d.parquet")

    except Exception as e:
        print(f"  [ERROR] {e}")


# ── 4. 投資部門別売買動向 ──


def fetch_market_breakdown(fetcher: JQuantsFetcher, target_date: str) -> None:
    """投資部門別売買動向（Premiumプラン以上）"""
    print(f"\n[4/5] 投資部門別売買動向 (date={target_date}) — Premium only")
    try:
        df = fetcher.get_market_breakdown(date_val=target_date)
        if df.empty:
            print("  [INFO] No data (Premiumプランが必要)")
            return

        df.columns = df.columns.str.lower()
        print(f"  Retrieved {len(df)} records")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        save_parquet_append(df, PARQUET_DIR / "market_breakdown_max_1d.parquet")

    except Exception as e:
        if "403" in str(e) or "Forbidden" in str(e):
            print("  [INFO] 403 Forbidden — Premiumプランが必要です")
        else:
            print(f"  [INFO] Not available: {e}")


# ── 5. 日証金 貸借残高CSV ──


def fetch_jsf_lending() -> None:
    """日証金 (taisyaku.jp) から貸借残高CSVを取得"""
    print("\n[5/5] 日証金 貸借残高CSV")

    urls = {
        "zandaka": "https://www.taisyaku.jp/data/zandaka.csv",
        "shina": "https://www.taisyaku.jp/data/shina.csv",
    }

    for name, url in urls.items():
        print(f"  Fetching {name}.csv from taisyaku.jp...")
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()

            # Shift-JIS encoding (日本語サイト)
            from io import StringIO

            text = resp.content.decode("shift_jis", errors="replace")
            df = pd.read_csv(StringIO(text))

            if df.empty:
                print(f"    [WARN] Empty CSV for {name}")
                continue

            print(f"    Retrieved {len(df)} rows, columns: {df.columns.tolist()[:6]}...")

            # 日付カラムを推定して変換
            date_candidates = [c for c in df.columns if "日" in c or "date" in c.lower()]
            if date_candidates:
                date_col = date_candidates[0]
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                df = df.rename(columns={date_col: "date"})

            output_path = PARQUET_DIR / f"jsf_{name}_latest.parquet"
            df.to_parquet(output_path, index=False)
            print(f"    Saved {output_path.name}: {len(df)} rows ({output_path.stat().st_size:,} bytes)")

        except Exception as e:
            print(f"    [ERROR] {name}: {e}")


# ── main ──


def main() -> None:
    print("=" * 60)
    print("Fetch Supply & Demand Data (J-Quants Standard + 日証金)")
    print("=" * 60)

    fetcher = JQuantsFetcher()

    # 最新営業日を取得
    target_date = fetcher.get_latest_trading_day()
    print(f"Target date: {target_date}")

    # J-Quants API
    fetch_short_ratio(fetcher, target_date)
    fetch_margin_alert(fetcher, target_date)
    compute_put_call_ratio()
    fetch_market_breakdown(fetcher, target_date)

    # 日証金 CSV（APIキー不要）
    fetch_jsf_lending()

    # サマリー
    print("\n" + "=" * 60)
    print("Output parquet files:")
    for name in [
        "short_ratio_max_1d",
        "margin_alert_max_1d",
        "put_call_ratio_max_1d",
        "market_breakdown_max_1d",
        "jsf_zandaka_latest",
        "jsf_shina_latest",
    ]:
        p = PARQUET_DIR / f"{name}.parquet"
        if p.exists():
            print(f"  ✓ {p.name} ({p.stat().st_size:,} bytes)")
        else:
            print(f"  ✗ {p.name} (not created)")

    print("\n※ 日経VIはJ-Quants/yfinanceで取得不可。")
    print("  Investing.com (jp.investing.com) から手動CSVダウンロードが必要。")
    print("=" * 60)


if __name__ == "__main__":
    main()
