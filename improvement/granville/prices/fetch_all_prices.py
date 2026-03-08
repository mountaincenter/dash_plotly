"""
全東証銘柄の日足データをyfinanceで取得し、セグメント別parquetに保存する。

ソース: data/parquet/meta_jquants.parquet (3,769銘柄)
出力:   improvement/granville/prices/<segment>.parquet
フォーマット: [date, Open, High, Low, Close, Volume, ticker]
         (data/parquet/prices_max_1d.parquet と同一スキーマ)

セグメント分類:
  core30.parquet       TOPIX Core30 (31)
  large70.parquet      TOPIX Large70 (69)
  mid400.parquet       TOPIX Mid400 (396)
  small1.parquet       TOPIX Small 1 (494)
  small2.parquet       TOPIX Small 2 (671)
  prime_other.parquet  プライム・TOPIX外 (41)
  standard.parquet     スタンダード (1,464)
  growth.parquet       グロース (603)
"""

import sys
import time
import json
from pathlib import Path

import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[3]  # dash_plotly/
META_PATH = ROOT / "data" / "parquet" / "meta_jquants.parquet"
OUT_DIR = Path(__file__).resolve().parent  # prices/
PROGRESS_FILE = OUT_DIR / "_progress.json"

BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 2  # seconds


def classify_segment(row: pd.Series) -> str:
    topix = row["topixnewindexseries"]
    market = row["market"]

    if topix == "TOPIX Core30":
        return "core30"
    elif topix == "TOPIX Large70":
        return "large70"
    elif topix == "TOPIX Mid400":
        return "mid400"
    elif topix == "TOPIX Small 1":
        return "small1"
    elif topix == "TOPIX Small 2":
        return "small2"
    elif market == "プライム":
        return "prime_other"
    elif market == "スタンダード":
        return "standard"
    elif market == "グロース":
        return "growth"
    else:
        return "other"


def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"completed_segments": [], "current_segment": None, "current_offset": 0}


def save_progress(progress: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False, indent=2))


def fetch_batch(tickers: list[str], period: str = "max") -> pd.DataFrame:
    """yfinance.download で一括取得し、既存フォーマットに揃える。"""
    if not tickers:
        return pd.DataFrame()

    raw = yf.download(
        tickers,
        period=period,
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        threads=True,
        progress=False,
    )

    frames = []
    if len(tickers) == 1:
        # 単一銘柄の場合、columnsがMultiIndexにならない
        t = tickers[0]
        df = raw.copy()
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df["ticker"] = t
        df = df.reset_index()
        df.columns = [c if c != "Date" and c != "Price" else "date" for c in df.columns]
        if "date" not in df.columns:
            df = df.rename(columns={df.columns[0]: "date"})
        frames.append(df)
    else:
        for t in tickers:
            try:
                if t not in raw.columns.get_level_values(0):
                    continue
                df = raw[t].copy()
                df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
                df["ticker"] = t
                df = df.reset_index()
                df.columns = [c if c != "Date" and c != "Price" else "date" for c in df.columns]
                if "date" not in df.columns:
                    df = df.rename(columns={df.columns[0]: "date"})
                # NaN行を除外（上場前データ等）
                df = df.dropna(subset=["Close"])
                frames.append(df)
            except Exception as e:
                print(f"  SKIP {t}: {e}")

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    # スキーマ統一
    result["date"] = pd.to_datetime(result["date"]).dt.tz_localize(None)
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        result[col] = result[col].astype(float)
    return result[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]]


def process_segment(segment_name: str, tickers: list[str], start_offset: int = 0) -> None:
    out_path = OUT_DIR / f"{segment_name}.parquet"
    total = len(tickers)
    all_frames = []

    # 既存の途中結果があれば読み込む
    tmp_path = OUT_DIR / f"_{segment_name}_tmp.parquet"
    if start_offset > 0 and tmp_path.exists():
        all_frames.append(pd.read_parquet(tmp_path))
        print(f"  Resumed from offset {start_offset} ({tmp_path.name})")

    for i in range(start_offset, total, BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        print(f"  [{segment_name}] {i+1}-{min(i+BATCH_SIZE, total)}/{total} ({len(batch)} tickers)")

        df = fetch_batch(batch)
        if not df.empty:
            all_frames.append(df)

        # 途中保存（10バッチごと）
        if (i // BATCH_SIZE + 1) % 10 == 0 and all_frames:
            tmp = pd.concat(all_frames, ignore_index=True)
            tmp.to_parquet(tmp_path, index=False)
            save_progress({
                "completed_segments": progress["completed_segments"],
                "current_segment": segment_name,
                "current_offset": i + BATCH_SIZE,
            })

        time.sleep(SLEEP_BETWEEN_BATCHES)

    if all_frames:
        result = pd.concat(all_frames, ignore_index=True)
        result = result.sort_values(["ticker", "date"]).reset_index(drop=True)
        result.to_parquet(out_path, index=False)
        print(f"  => {out_path.name}: {result.shape[0]:,} rows, {result['ticker'].nunique()} tickers")
    else:
        print(f"  => {segment_name}: NO DATA")

    # tmp削除
    if tmp_path.exists():
        tmp_path.unlink()


if __name__ == "__main__":
    # 対象セグメントを引数で絞れる（未指定なら全部）
    target_segments = sys.argv[1:] if len(sys.argv) > 1 else None

    meta = pd.read_parquet(META_PATH)
    meta["segment"] = meta.apply(classify_segment, axis=1)

    segment_order = ["core30", "large70", "mid400", "small1", "small2", "prime_other", "standard", "growth"]

    print("=== セグメント別銘柄数 ===")
    for seg in segment_order:
        n = (meta["segment"] == seg).sum()
        print(f"  {seg:15s}: {n:>5}")
    print(f"  {'TOTAL':15s}: {len(meta):>5}")
    print()

    # メタ情報を保存（後で使えるように）
    meta_out = meta[["ticker", "code", "stock_name", "market", "sectors", "series", "topixnewindexseries", "segment"]]
    meta_out.to_parquet(OUT_DIR / "meta_all.parquet", index=False)
    print(f"meta_all.parquet saved ({len(meta_out)} rows)")
    print()

    progress = load_progress()

    for seg in segment_order:
        if target_segments and seg not in target_segments:
            continue
        if seg in progress.get("completed_segments", []):
            print(f"[SKIP] {seg} (already completed)")
            continue

        tickers = sorted(meta[meta["segment"] == seg]["ticker"].tolist())
        offset = 0
        if progress.get("current_segment") == seg:
            offset = progress.get("current_offset", 0)

        print(f"\n[START] {seg} ({len(tickers)} tickers, offset={offset})")
        process_segment(seg, tickers, start_offset=offset)

        progress["completed_segments"].append(seg)
        progress["current_segment"] = None
        progress["current_offset"] = 0
        save_progress(progress)

    # 完了後にprogressファイル削除
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
    print("\n=== ALL DONE ===")
