"""
Trade Review HTML 生成
- CSV (stock_results＿today.csv) から当日約定を読み込み
- 戦略: grok(grok_trending.parquet) / pair(signals.parquet) で分類
- 5分足: JST + 11:30-12:30 を詰めて連続表示
- 建玉位置スコア: 正しい方向で計算（SHORTは高いほど有利）
- 終値: J-Quants AdjC を正として表示、決済値と区別
- MOC保持時損益: 決済を大引けまで引っ張ったらどうなったか
"""
from __future__ import annotations

import base64
import io
import os
import subprocess
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
for fp in ["/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
           "/System/Library/Fonts/ヒラギノ角ゴ ProN W3.otf",
           "/Library/Fonts/ヒラギノ角ゴ ProN W3.otf"]:
    if Path(fp).exists():
        plt.rcParams["font.family"] = fm.FontProperties(fname=fp).get_name()
        break
else:
    plt.rcParams["font.family"] = "Hiragino Sans"

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[3]
CSV_PATH = ROOT / "data" / "csv" / "stock_results__today.csv"
HOLD_CSV_PATH = ROOT / "data" / "csv" / "hold_stocks.csv"
OUT_DIR = ROOT / "data" / "reports" / "trade_review"

# main() で上書き
DATE: str = ""       # "2026-04-21"
OUT: Path | None = None

# grok_trending.parquet (grok 判定用)。production S3 から都度DL
GROK_S3 = "s3://stock-api-data/parquet/grok_trending.parquet"
GROK_LOCAL = Path("/tmp/grok_trending.parquet")
GROK_DATED_LOCAL = Path("/tmp/grok_trending_dated.parquet")
GROK_ARCHIVE_S3 = "s3://stock-api-data/parquet/backtest/grok_trending_archive.parquet"
GROK_ARCHIVE_LOCAL = Path("/tmp/grok_trending_archive.parquet")

# signals.parquet (pair/granville/reversal 判定用)。staging S3 から都度DL
SIGNALS_S3 = "s3://stock-api-data-staging/parquet/signals.parquet"
SIGNALS_LOCAL = Path("/tmp/signals.parquet")

# peers 絞り込み: TOPIX 全階層（pair/granville 生成と同じ universe）
TOPIX_TIERS = ("TOPIX Core30", "TOPIX Large70", "TOPIX Mid400", "TOPIX Small 1", "TOPIX Small 2")
TOPIX_TIER_ORDER = {name: i for i, name in enumerate(TOPIX_TIERS)}


def _num(s) -> str:
    """'149,500' → '149500'、NaN/None → ''"""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).replace(",", "").strip()


def load_trades_from_csv(csv_path: Path) -> tuple[list[dict], str]:
    """
    MarketSpeed CSV を読んでトレードリストと日付 (YYYY-MM-DD) を返す。

    - 買埋 → 当初 SHORT、売埋 → 当初 LONG
    - 複数日混在 / 未知の取引種別はエラー
    - CSV 約定日 != 今日 (JST) もエラー（過去日生成は禁止：grok/signals/eq master/yfinance の
      データ制約上、過去日の正確な再現は不可能）
    """
    df = pd.read_csv(csv_path, dtype=str).dropna(subset=["約定日"])
    if df.empty:
        raise ValueError(f"CSV が空です: {csv_path}")

    dates = df["約定日"].unique()
    if len(dates) > 1:
        raise ValueError(f"約定日が混在しています: {list(dates)}")
    yyyymmdd = dates[0].replace("/", "")
    date_iso = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"

    today_jst = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d")
    allow_date_mismatch = os.environ.get("ALLOW_TRADE_REVIEW_DATE_MISMATCH") == "1"
    if date_iso != today_jst and not allow_date_mismatch:
        raise ValueError(
            f"CSV 約定日 ({date_iso}) が今日 JST ({today_jst}) と不一致。"
            f"過去日生成は禁止（grok_trending.parquet/signals.parquet/eq master が翌朝上書きされ正確な再現不可）"
        )
    if date_iso != today_jst and allow_date_mismatch:
        print(f"[WARN] CSV 約定日 ({date_iso}) と今日 JST ({today_jst}) が不一致。明示フラグにより続行")

    direction_map = {"買埋": "SHORT", "売埋": "LONG"}
    trades: list[dict] = []
    for _, r in df.iterrows():
        torihiki = str(r["取引"]).strip()
        if torihiki not in direction_map:
            raise ValueError(f"未知の取引種別: {torihiki} (code={r.get('コード')})")
        trades.append({
            "code": str(r["コード"]).strip(),
            "name": str(r["銘柄名"]).strip(),
            "direction": direction_map[torihiki],
            "qty": int(_num(r["数量(株/口)"])),
            "entry": float(_num(r["平均取得価額(円)"])),
            "exit": float(_num(r["単価(円)"])),
            "pl": int(_num(r["実現損益(円)"])),
        })
    return trades, date_iso


def load_open_positions(csv_path: Path) -> list[dict]:
    """hold_stocks.csv から未決済建玉を読む。ペア片脚の持ち越し検出に使う。"""
    if not csv_path.exists():
        return []
    df = pd.read_csv(csv_path, dtype=str)
    if df.empty:
        return []

    direction_map = {"買建": "LONG", "売建": "SHORT"}
    positions: list[dict] = []
    for _, r in df.iterrows():
        side = str(r.get("売買", "")).strip()
        if side not in direction_map:
            continue
        qty = int(float(_num(r.get("建玉数量合計(株/口)", "0")) or 0))
        if qty <= 0:
            continue
        code = str(r.get("コード", "")).strip().zfill(4)
        entry_total = float(_num(r.get("建玉金額合計(円)", "0")) or 0)
        current_total = float(_num(r.get("時価評価額(円)", "0")) or 0)
        positions.append({
            "code": code,
            "name": str(r.get("銘柄名", "")).strip(),
            "direction": direction_map[side],
            "qty": qty,
            "entry": entry_total / qty if qty else 0.0,
            "current": current_total / qty if qty else 0.0,
            "unrealized_pl": int(float(_num(r.get("評価損益額合計(円)", "0")) or 0)),
            "raw_side": side,
        })
    return positions


def detect_manual_trade_note(t: dict) -> tuple[str | None, str | None]:
    """当日レビューで戦略損益から分離すべきオペレーションミスを検出。"""
    if DATE == "2026-05-27" and t["code"] in {"6055", "6981"}:
        return (
            "semicon",
            "ユーザー指定: 2026-05-27 のジャパンM・村田製作所は半導体テーマ（semicon）として扱う。"
            "実現損益は半導体テーマの取引結果として集計。",
        )
    if DATE == "2026-05-27" and t["code"] == "3979":
        return (
            "grok",
            "ユーザー指定: 2026-05-27 のうるるはgrokとして扱う。"
            "実現損益はgrok取引結果として集計。",
        )
    if DATE == "2026-05-26" and t["code"] in {"200A", "4062", "6981"}:
        return (
            "semicon",
            "ユーザー指定: 2026-05-26 の取引3銘柄はすべて半導体テーマ（semicon）として扱う。"
            "実現損益は半導体テーマの取引結果として集計。",
        )
    if DATE == "2026-05-25" and t["code"] == "4062":
        return (
            "semicon",
            "ユーザー指定: イビデンは半導体テーマ（semicon）として扱う。"
            "実現損益は半導体テーマの利益確定として分離。",
        )
    if DATE == "2026-05-25" and t["code"] == "4208":
        return (
            "loss_cut",
            "ユーザー指定: UBEは損切りとして扱う。"
            "実現損益は戦略分類とは分け、損失圧縮/撤退判断として評価。",
        )
    if DATE == "2026-05-18" and t["code"] == "8850" and t["direction"] == "LONG" and t["pl"] < 0:
        return (
            "mistake",
            "取引ミス: 本来は8804/8850ペアのSHORT脚として売建すべきところを買建。"
            "この-12,000円は戦略判断ではなく発注方向ミスとして分離。",
        )
    return None, None


def _read_grok_source(path: Path, expected_date: str, source_name: str) -> pd.DataFrame:
    """grok parquet を expected_date で絞り込む。current / dated / archive の列差を吸収する。"""
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)

    date_col = next((c for c in ("date", "backtest_date", "selection_date") if c in df.columns), None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
        dates = sorted(d for d in df[date_col].dropna().unique().tolist())
        if expected_date not in dates:
            print(f"[WARN] {source_name} {date_col}={dates[:8]} に {expected_date} が無い")
            return pd.DataFrame()
        df = df[df[date_col] == expected_date].copy()

    if "code" not in df.columns:
        if "ticker" not in df.columns:
            print(f"[WARN] {source_name} に code/ticker が無い。grok 判定をスキップ")
            return pd.DataFrame()
        df["code"] = df["ticker"].astype(str).str.replace(".T", "", regex=False)

    return df


def _grok_info_from_df(df: pd.DataFrame) -> dict:
    prob_cols = ("prob_up", "ml_prob_live", "ml_prob", "ml_prob_wfcv")
    out = {}
    for _, row in df.iterrows():
        code = str(row.get("code", "")).replace(".T", "").strip()
        if not code:
            continue
        if code.isdigit():
            code = code.zfill(4)

        prob = None
        for col in prob_cols:
            if col in df.columns and pd.notna(row.get(col)):
                prob = float(row.get(col))
                break

        bucket = row.get("bucket") if "bucket" in df.columns and pd.notna(row.get("bucket")) else None
        if bucket is None:
            if prob is None:
                bucket = None
            elif prob < 0.45:
                bucket = "SHORT"
            elif prob > 0.70:
                bucket = "LONG"
            else:
                bucket = "DISC"
        out[code] = {"bucket": bucket, "prob": prob}
    return out


def load_grok_info(expected_date: str) -> dict:
    """grok_trending から code→{bucket, prob} の辞書を作る。
    current が翌日分に上書き済みの場合は、日付別 backup、archive の順で補完する。"""
    r = subprocess.run(
        ["aws", "s3", "cp", GROK_S3, str(GROK_LOCAL)],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode == 0:
        df = _read_grok_source(GROK_LOCAL, expected_date, "grok_trending.parquet")
        if not df.empty:
            print("[INFO] grok source: current grok_trending.parquet")
            return _grok_info_from_df(df)
    else:
        print(f"[WARN] grok_trending.parquet DL失敗: {r.stderr}")

    yyyymmdd = expected_date.replace("-", "")
    dated_local = ROOT / "data" / "parquet" / "backtest" / f"grok_trending_{yyyymmdd}.parquet"
    df = _read_grok_source(dated_local, expected_date, f"local grok_trending_{yyyymmdd}.parquet")
    if not df.empty:
        print(f"[INFO] grok source: local grok_trending_{yyyymmdd}.parquet")
        return _grok_info_from_df(df)

    dated_s3 = f"s3://stock-api-data/parquet/backtest/grok_trending_{yyyymmdd}.parquet"
    r = subprocess.run(
        ["aws", "s3", "cp", dated_s3, str(GROK_DATED_LOCAL)],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode == 0:
        df = _read_grok_source(GROK_DATED_LOCAL, expected_date, f"s3 grok_trending_{yyyymmdd}.parquet")
        if not df.empty:
            print(f"[INFO] grok source: s3 grok_trending_{yyyymmdd}.parquet")
            return _grok_info_from_df(df)
    else:
        print(f"[WARN] grok_trending_{yyyymmdd}.parquet DL失敗: {r.stderr}")

    archive_local = ROOT / "data" / "parquet" / "backtest" / "grok_trending_archive.parquet"
    df = _read_grok_source(archive_local, expected_date, "local grok_trending_archive.parquet")
    if not df.empty:
        print("[INFO] grok source: local grok_trending_archive.parquet")
        return _grok_info_from_df(df)

    r = subprocess.run(
        ["aws", "s3", "cp", GROK_ARCHIVE_S3, str(GROK_ARCHIVE_LOCAL)],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode == 0:
        df = _read_grok_source(GROK_ARCHIVE_LOCAL, expected_date, "s3 grok_trending_archive.parquet")
        if not df.empty:
            print("[INFO] grok source: s3 grok_trending_archive.parquet")
            return _grok_info_from_df(df)
    else:
        print(f"[WARN] grok_trending_archive.parquet DL失敗: {r.stderr}")

    print(f"[WARN] {expected_date} の grok 情報が current/date/archive のいずれにも無い。grok 判定をスキップ")
    return {}


def load_signals(date_iso: str) -> pd.DataFrame:
    """signals.parquet を指定日で絞り込む。ローカル同期済み当日データを優先する。"""
    local_path = ROOT / "data" / "parquet" / "signals.parquet"
    if local_path.exists():
        df = pd.read_parquet(local_path)
        if "signal_date" in df.columns:
            df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.strftime("%Y-%m-%d")
            hit = df[df["signal_date"] == date_iso].copy()
            if not hit.empty:
                return hit

    r = subprocess.run(
        ["aws", "s3", "cp", SIGNALS_S3, str(SIGNALS_LOCAL)],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode != 0:
        print(f"[WARN] signals.parquet DL失敗: {r.stderr}")
        return pd.DataFrame()
    df = pd.read_parquet(SIGNALS_LOCAL)
    df["signal_date"] = pd.to_datetime(df["signal_date"]).dt.strftime("%Y-%m-%d")
    return df[df["signal_date"] == date_iso].copy()


def load_eq_master(date_iso: str) -> pd.DataFrame:
    """銘柄マスタを日付付きでキャッシュ（/tmp/eq_master_YYYYMMDD.csv）。
    J-Quants の eq master は 17:30 頃更新。同日内でキャッシュが 17:30 より前に作られていて
    現在が 17:30 以降なら再取得（当日中に新規上場/廃止を取り込むため）。"""
    yyyymmdd = date_iso.replace("-", "")
    path = Path(f"/tmp/eq_master_{yyyymmdd}.csv")
    now = datetime.now()
    date_dt = datetime.strptime(date_iso, "%Y-%m-%d")
    stale = False
    if path.exists() and now.date() == date_dt.date():
        cutoff = date_dt.replace(hour=17, minute=30)
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
        if mtime < cutoff <= now:
            stale = True
    if not path.exists() or stale:
        r = subprocess.run(
            ["jquants", "--output", "csv", "eq", "master"],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            raise RuntimeError(f"jquants eq master 失敗: {r.stderr}")
        path.write_text(r.stdout)
    return pd.read_csv(path, dtype=str)


def classify_trade(
    code: str, trade_direction: str, trade_codes: set[str], trade_dirs: dict[str, set[str]],
    signals_df: pd.DataFrame, grok_info: dict,
) -> tuple[str, dict | None]:
    """分類: pair → grok → granville → reversal → other。
    pair 判定は (a) 相手銘柄も約定 (b) signals.direction と両脚の実トレード方向が整合、の両条件。
    複数 pair_id 候補がある場合は |z_latest| 最大のものを採用。"""
    ticker = f"{code}.T"
    if not signals_df.empty:
        pairs = signals_df[signals_df["strategy"] == "pairs"]
        candidates = []
        for _, row in pairs.iterrows():
            tk1, tk2 = row["tk1"], row["tk2"]
            c1, c2 = tk1.replace(".T", ""), tk2.replace(".T", "")
            direction = row["direction"]  # "short_tk1" or "long_tk1"

            if direction not in ("short_tk1", "long_tk1"):
                continue
            if code == c1:
                my_expect = "SHORT" if direction == "short_tk1" else "LONG"
                partner = c2
                partner_expect = "LONG" if direction == "short_tk1" else "SHORT"
            elif code == c2:
                my_expect = "LONG" if direction == "short_tk1" else "SHORT"
                partner = c1
                partner_expect = "SHORT" if direction == "short_tk1" else "LONG"
            else:
                continue

            if (
                partner in trade_codes
                and trade_direction == my_expect
                and partner_expect in trade_dirs.get(partner, set())
            ):
                z = abs(float(row["z_latest"])) if pd.notna(row["z_latest"]) else 0.0
                candidates.append((z, row, partner))

        if candidates:
            candidates.sort(key=lambda x: -x[0])
            _, row, partner = candidates[0]
            return "pair", {
                "pair_id": row["pair_id"], "partner": partner, "direction": row["direction"],
                "name1": row["name1"], "name2": row["name2"],
                "z_latest": float(row["z_latest"]) if pd.notna(row["z_latest"]) else None,
            }

    if grok_info.get(code):
        return "grok", None

    if not signals_df.empty:
        granv = signals_df[(signals_df["strategy"] == "granville") & (signals_df["ticker"] == ticker)]
        if not granv.empty:
            return "granville", None

        rev = signals_df[(signals_df["strategy"] == "reversal") & (signals_df["ticker"] == ticker)]
        if not rev.empty:
            return "reversal", None

    return "other", None


def load_peers_by_s33(target_code: str, eq_master_df: pd.DataFrame, top_n: int = 8) -> tuple[list, str]:
    """対象コードと同S33 + TOPIX全階層に絞り、ScaleCat階層→コード昇順で top_n 件。
    戻り値: (peers, sector_name)。peers = [(code4, name), ...]。"""
    code5 = target_code + "0"
    row = eq_master_df[eq_master_df["Code"] == code5]
    if row.empty:
        return [], ""
    s33 = row.iloc[0]["S33"]
    sector_name = row.iloc[0]["S33Nm"]

    same = eq_master_df[
        (eq_master_df["S33"] == s33) & (eq_master_df["ScaleCat"].isin(TOPIX_TIERS))
    ].copy()
    same["_tier_rank"] = same["ScaleCat"].map(TOPIX_TIER_ORDER)
    same = same.sort_values(["_tier_rank", "Code"])

    peers = []
    for _, r in same.head(top_n).iterrows():
        code4 = str(r["Code"])[:-1]  # 5桁→4桁
        peers.append((code4, str(r["CoName"])))
    return peers, sector_name


def sector_moves(peers: list, date: str) -> list:
    """指定コードリストの当日騰落率を取得（前日比%）"""
    d_from = n_business_days_before(date, 3)
    out = []
    for code, name in peers:
        d = jq_daily(code, d_from, date)
        if d.empty:
            continue
        d["Date"] = pd.to_datetime(d["Date"])
        d = d.sort_values("Date")
        hit = d[d["Date"].dt.strftime("%Y-%m-%d") == date]
        if hit.empty or len(d) < 2:
            continue
        today = hit.iloc[-1]
        d_before = d[d["Date"] < pd.Timestamp(date)]
        if d_before.empty:
            continue
        prev = d_before.iloc[-1]
        pct = (today["AdjC"] - prev["AdjC"]) / prev["AdjC"] * 100
        out.append({
            "code": code, "name": name,
            "prev": float(prev["AdjC"]), "close": float(today["AdjC"]),
            "pct": float(pct),
        })
    return out


def jq_daily(code4: str, from_: str, to_: str) -> pd.DataFrame:
    code5 = code4 + "0"
    r = subprocess.run(
        ["jquants", "--output", "csv", "eq", "daily", "--code", code5, "--from", from_, "--to", to_],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0 or not r.stdout.strip():
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(r.stdout))


def jq_business_days(date_from: str, date_to: str) -> list[str]:
    """jquants mkt calendar から営業日リストを取得（YYYY-MM-DD 昇順）。
    HolDiv は実データで 1=営業日, 0=休業日（CLI schema の説明は逆なので要注意）。"""
    r = subprocess.run(
        ["jquants", "--output", "csv", "mkt", "calendar", "--from", date_from, "--to", date_to],
        capture_output=True, text=True, timeout=30,
    )
    if r.returncode != 0 or not r.stdout.strip():
        return []
    df = pd.read_csv(io.StringIO(r.stdout))
    return df[df["HolDiv"] == 1]["Date"].tolist()


def n_business_days_before(date_iso: str, n: int) -> str:
    """date_iso (YYYY-MM-DD) から n 営業日前の日付を返す。
    カレンダー取得は当日から 60 暦日前を上限に検索する。"""
    d_to = date_iso
    d_from = (datetime.strptime(date_iso, "%Y-%m-%d") - timedelta(days=60)).strftime("%Y-%m-%d")
    bdays = jq_business_days(d_from, d_to)
    if date_iso in bdays:
        idx = bdays.index(date_iso)
    else:
        idx = len(bdays)
    target_idx = max(idx - n, 0)
    return bdays[target_idx] if bdays else d_from


def yf_intraday(code4: str) -> pd.DataFrame:
    """前日+当日の2営業日分を取得（RSI/MACDの初期値を安定させるため）"""
    t = yf.Ticker(f"{code4}.T")
    df = t.history(period="5d", interval="5m")
    if df.empty:
        return df
    df.index = df.index.tz_convert("Asia/Tokyo")
    return df


def prepare_5m(intraday: pd.DataFrame, daily: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    5分足整形。前日含む複数営業日を受け取り:
    - 各日 15:20以降（板寄せ周辺ノイズ）をカット
    - 昼休み詰め
    - 当日のみ: 先頭バーのOpenをJ-Quants AdjOで上書き、15:30大引けバー(AdjC)を追加
    - 前日 15:30 大引けバー(前日 AdjC)を追加
    戻り値: (full=前日+当日, today_only=当日のみ)
    """
    if intraday.empty:
        return intraday, intraday
    d = compact_lunch_break(intraday)
    tz = "Asia/Tokyo"

    # 各日ごとに15:20以降カット
    d = d[d.index.map(lambda ts: ts.time() < pd.Timestamp("15:20").time())]

    # 対象日リスト: 当日とその直前営業日
    if not len(daily):
        return d, d
    daily2 = daily.copy()
    daily2["Date"] = pd.to_datetime(daily2["Date"])
    daily2 = daily2.sort_values("Date")

    # 各営業日について、5分足末尾に 15:30 大引けバー(AdjC)を追加、
    # 各営業日の9:00 寄付バー Open を AdjO で上書き（当日のみ必要だが前日もやっておく）
    rebuilt = []
    for _, row in daily2.iterrows():
        dt = row["Date"]
        day_str = dt.strftime("%Y-%m-%d")
        day_d = d[d.index.date == dt.date()]
        o_val = float(row["AdjO"])
        c_val = float(row["AdjC"])
        open_t = pd.Timestamp(f"{day_str} 09:00:00", tz=tz)
        close_t = pd.Timestamp(f"{day_str} 15:30:00", tz=tz)
        if not day_d.empty:
            first = day_d.iloc[0].copy()
            first["Open"] = o_val
            first["Low"] = min(first["Low"], o_val)
            first["High"] = max(first["High"], o_val)
            day_d = day_d.iloc[1:]
            day_d = pd.concat([pd.DataFrame([first.to_dict()], index=[open_t]), day_d])
            day_d = pd.concat([day_d, pd.DataFrame({"Open":[c_val],"High":[c_val],"Low":[c_val],"Close":[c_val]}, index=[close_t])])
            rebuilt.append(day_d)

    if not rebuilt:
        return d, d
    full = pd.concat(rebuilt).sort_index()
    today = full[full.index.date == pd.Timestamp(DATE).date()]
    return full, today


def calc_indicators(close: pd.Series) -> pd.DataFrame:
    """RSI(9) と MACD(12,26,9)"""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_g = gain.ewm(alpha=1/9, adjust=False).mean()
    avg_l = loss.ewm(alpha=1/9, adjust=False).mean()
    rs = avg_g / avg_l.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    ema_f = close.ewm(span=12, adjust=False).mean()
    ema_s = close.ewm(span=26, adjust=False).mean()
    macd = ema_f - ema_s
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    return pd.DataFrame({"RSI": rsi, "MACD": macd, "Signal": signal, "Hist": hist}, index=close.index)


def compact_lunch_break(df: pd.DataFrame) -> pd.DataFrame:
    """11:30-12:30 の昼休みを詰める。index=Datetime → 連続番号化"""
    if df.empty:
        return df
    # 前場(9:00-11:30) と 後場(12:30-15:30) のみ残す
    d = df.copy()
    times = d.index.time
    mask_morning = [(t >= pd.Timestamp("09:00").time()) and (t <= pd.Timestamp("11:30").time()) for t in times]
    mask_afternoon = [(t >= pd.Timestamp("12:30").time()) and (t <= pd.Timestamp("15:30").time()) for t in times]
    mask = [m or a for m, a in zip(mask_morning, mask_afternoon)]
    d = d[mask]
    return d


def plot_chart(daily: pd.DataFrame, intraday: pd.DataFrame, trade: dict) -> str:
    fig, axes = plt.subplots(1, 2, figsize=(12, 3.5), gridspec_kw={"width_ratios": [2, 3]})
    plt.subplots_adjust(wspace=0.28)
    fig.patch.set_facecolor("#18181b")

    # ===== 日足 (非営業日は詰めて連番x軸) =====
    ax = axes[0]
    ax.set_facecolor("#09090b")
    if not daily.empty:
        d = daily.copy()
        d["Date"] = pd.to_datetime(d["Date"])
        d = d.sort_values("Date").reset_index(drop=True)
        for i, r in d.iterrows():
            color = "#34d399" if r["AdjC"] >= r["AdjO"] else "#fb7185"
            ax.plot([i, i], [r["AdjL"], r["AdjH"]], color=color, linewidth=0.8)
            ax.plot([i, i], [r["AdjO"], r["AdjC"]], color=color, linewidth=3.5)
        ax.set_title(f"日足 (直近{len(d)}営業日)", color="#fafafa", fontsize=10)
        step = max(1, len(d) // 6)
        ticks = list(range(0, len(d), step))
        ax.set_xticks(ticks)
        ax.set_xticklabels([d.iloc[i]["Date"].strftime("%m/%d") for i in ticks], rotation=45, fontsize=8)
        ax.tick_params(axis="x", colors="#a1a1aa")
        ax.tick_params(axis="y", labelsize=8, colors="#a1a1aa")
        ax.axhline(trade["entry"], color="#60a5fa", linestyle="--", linewidth=0.8, alpha=0.6)
        ax.axhline(trade["exit"], color="#fbbf24", linestyle="--", linewidth=0.8, alpha=0.6)
    for sp in ax.spines.values():
        sp.set_color("#27272a")
    ax.grid(True, alpha=0.15, color="#a1a1aa", linestyle=":")

    # ===== 5分足 (JST, 昼休み詰め) =====
    ax = axes[1]
    ax.set_facecolor("#09090b")
    d = intraday  # 呼び出し側で prepare_5m 済み
    if not d.empty:
        if "Volume" in d.columns:
            vol = pd.to_numeric(d["Volume"], errors="coerce").fillna(0)
            typical = (d["High"] + d["Low"] + d["Close"]) / 3
            cum_vol = vol.cumsum()
            vwap = (typical * vol).cumsum() / cum_vol.replace(0, np.nan)
        else:
            vwap = pd.Series(np.nan, index=d.index)
        x = list(range(len(d)))
        labels = [t.strftime("%H:%M") for t in d.index]
        for i in range(len(d)):
            r = d.iloc[i]
            color = "#34d399" if r["Close"] >= r["Open"] else "#fb7185"
            ax.plot([i, i], [r["Low"], r["High"]], color=color, linewidth=0.6)
            ax.plot([i, i], [r["Open"], r["Close"]], color=color, linewidth=2.2)
        if vwap.notna().any():
            ax.plot(x, vwap.values, color="#2dd4bf", linewidth=1.1, alpha=0.85, label="VWAP")
        ax.axhline(trade["entry"], color="#60a5fa", linestyle="--", linewidth=1, alpha=0.8,
                   label=f"建値 {trade['entry']:.1f}")
        ax.axhline(trade["exit"], color="#fbbf24", linestyle="--", linewidth=1, alpha=0.8,
                   label=f"決済 {trade['exit']:.1f}")
        # 大引けを別線で
        if trade.get("daily_close"):
            ax.axhline(trade["daily_close"], color="#a78bfa", linestyle=":", linewidth=1, alpha=0.9,
                       label=f"大引け {trade['daily_close']:.1f}")
        ax.legend(loc="best", facecolor="#18181b", edgecolor="#27272a",
                  labelcolor="#fafafa", fontsize=7)
        ax.set_title("5分足 JST (11:30-12:30 詰め)", color="#fafafa", fontsize=10)
        # X軸: 30分刻みで目盛
        step = max(1, len(d) // 10)
        ax.set_xticks(x[::step])
        ax.set_xticklabels(labels[::step], rotation=0, fontsize=8)
        ax.tick_params(axis="x", colors="#a1a1aa")
        ax.tick_params(axis="y", labelsize=8, colors="#a1a1aa")
    for sp in ax.spines.values():
        sp.set_color("#27272a")
    ax.grid(True, alpha=0.15, color="#a1a1aa", linestyle=":")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, facecolor="#18181b", bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def fmt_pl(v):
    cls = "num-pos" if v > 0 else ("num-neg" if v < 0 else "num-neutral")
    sign = "+" if v > 0 else ""
    return f'<span class="{cls}">{sign}{v:,} 円</span>'


def fmt_price(p) -> str:
    """小数部が 0 なら整数表記、そうでなければ .1f"""
    if p is None or (isinstance(p, float) and pd.isna(p)):
        return "—"
    try:
        pf = float(p)
    except (TypeError, ValueError):
        return str(p)
    if pf == int(pf):
        return f"{int(pf):,}"
    return f"{pf:,.1f}"


def verdict_for_grok(t):
    """grok 戦略用の評価"""
    pl = t["pl"]
    direction = t["direction"]
    entry = t["entry"]
    exit_ = t["exit"]
    dc = t.get("daily_close")
    dl = t.get("daily_low")
    dh = t.get("daily_high")
    notes = []

    # 決済 vs 大引けの比較（MOC保持したらどうだったか）
    if dc is not None:
        if direction == "SHORT":
            moc_pl = int((entry - dc) * t["qty"])
            diff = moc_pl - pl
            if diff > 0:
                notes.append(f"早期決済で{diff:+,}円取り逃し（MOC保持=大引け決済ならさらに +{diff:,}円）")
            elif diff < -500:
                notes.append(f"早期決済が奏功、MOC保持ならあと {diff:+,}円悪化していた")
        else:
            moc_pl = int((dc - entry) * t["qty"])
            diff = moc_pl - pl
            if diff > 0:
                notes.append(f"早期決済で{diff:+,}円取り逃し（大引け決済ならさらに +{diff:,}円）")

    # 安値/高値との距離
    if direction == "SHORT" and dl is not None:
        miss = int((exit_ - dl) * t["qty"])
        if miss > 1000:
            notes.append(f"安値{fmt_price(dl)} まで下げた → 決済 {fmt_price(exit_)} は {miss:+,}円取りこぼし")
    if direction == "LONG" and dh is not None:
        miss = int((dh - exit_) * t["qty"])
        if miss > 1000:
            notes.append(f"高値{fmt_price(dh)} まで上げた → 決済 {fmt_price(exit_)} は {miss:+,}円取りこぼし")

    if pl > 5000:
        cls = "ok"
        head = f"grok: 利益確保 {pl:+,}円"
    elif pl > 0:
        cls = "ok"
        head = f"grok: 小幅利益 {pl:+,}円"
    elif pl > -3000:
        cls = "bad"
        head = f"grok: 小幅損失 {pl:+,}円"
    else:
        cls = "bad"
        head = f"grok: 大幅損失 {pl:+,}円"

    body = head + ("。" + "、".join(notes) if notes else "")
    return cls, body


def entry_score_label(direction: str, entry: float, l: float, h: float, exit_: float):
    """
    案D（主指標・0%=最良・100%=最悪）+ 案B（補助・±100・中央0）
    戻り値: dict
      edge_entry, edge_exit: 案D（小さいほど良い）
      dev_entry, dev_exit:   案B（中央からの偏差 ±100）
      entry_label, exit_label: 評価テキスト
      capture: 取り幅達成度（+100=完璧、0=無駄、負=逆）
    """
    if h <= l:
        return None
    rng = h - l
    m = (h + l) / 2
    w = rng / 2
    dev_entry = (entry - m) / w * 100
    dev_exit = (exit_ - m) / w * 100
    if direction == "SHORT":
        edge_entry = (h - entry) / rng * 100  # 0=天井で売れた
        edge_exit = (exit_ - l) / rng * 100   # 0=底で買戻した
    else:
        edge_entry = (entry - l) / rng * 100  # 0=底で買えた
        edge_exit = (h - exit_) / rng * 100   # 0=天井で売れた
    capture = 100 - (edge_entry + edge_exit) / 2

    def level(edge: float) -> str:
        if edge <= 20: return "完璧"
        if edge <= 40: return "良好"
        if edge <= 60: return "普通"
        if edge <= 80: return "悪い"
        return "最悪"

    entry_label = f"建玉 {level(edge_entry)}"
    exit_label = f"決済 {level(edge_exit)}"
    return {
        "edge_entry": edge_entry, "edge_exit": edge_exit,
        "dev_entry": dev_entry, "dev_exit": dev_exit,
        "entry_label": entry_label, "exit_label": exit_label,
        "capture": capture,
    }


def fmt_edge(edge: float) -> str:
    """0%=最良 100%=最悪 のヒートマップ色分け"""
    if edge <= 20: cls = "num-pos"
    elif edge <= 40: cls = "num-pos"
    elif edge <= 60: cls = "num-neutral"
    elif edge <= 80: cls = "num-neg"
    else: cls = "num-neg"
    return f'<span class="{cls}">{edge:.1f}%</span>'


def fmt_dev(dev: float) -> str:
    sign = "+" if dev >= 0 else ""
    return f'<span style="color:var(--text-muted);font-size:.85em">（中央比 {sign}{dev:.0f}）</span>'


def fmt_capture(capture: float) -> str:
    if capture >= 70: cls = "num-pos"
    elif capture >= 30: cls = "num-neutral"
    elif capture >= 0: cls = "num-neg"
    else: cls = "num-neg"
    sign = "+" if capture >= 0 else ""
    return f'<span class="{cls}">{sign}{capture:.1f}%</span>'


def render_indicators_table(t: dict) -> str:
    """5分足 RSI(9)/MACD(12,26,9)/VWAP の特徴的ポイントを表示（前日5分足で助走計算）"""
    full = t.get("d_5m_full")
    today = t.get("d_5m")
    if full is None or today is None or len(full) < 25 or len(today) < 5:
        return ""
    ind = calc_indicators(full["Close"])
    m_full = full.join(ind)
    m = m_full[m_full.index.date == pd.Timestamp(DATE).date()]
    if m.empty:
        return ""
    if "Volume" in m.columns:
        vol = pd.to_numeric(m["Volume"], errors="coerce").fillna(0)
        typical = (m["High"] + m["Low"] + m["Close"]) / 3
        cum_vol = vol.cumsum()
        m = m.copy()
        m["VWAP"] = (typical * vol).cumsum() / cum_vol.replace(0, np.nan)
    else:
        m = m.copy()
        m["VWAP"] = np.nan
    direction = t["direction"]

    def row(label, r, highlight=False):
        ts = r.name.strftime("%H:%M")
        cls = " class='hl'" if highlight else ""
        rsi = r["RSI"]
        rsi_cls = "num-neg" if rsi <= 30 else ("num-pos" if rsi >= 70 else "")
        hist = r["Hist"]
        hist_cls = "num-pos" if hist > 0 else ("num-neg" if hist < 0 else "")
        vwap = r.get("VWAP")
        vwap_txt = fmt_price(vwap) if pd.notna(vwap) else "—"
        vwap_diff = r["Close"] - vwap if pd.notna(vwap) else np.nan
        diff_cls = "num-pos" if pd.notna(vwap_diff) and vwap_diff > 0 else ("num-neg" if pd.notna(vwap_diff) and vwap_diff < 0 else "")
        diff_txt = f"{vwap_diff:+.1f}" if pd.notna(vwap_diff) else "—"
        return (f"<tr{cls}><td>{ts}</td><td>{escape(label)}</td>"
                f"<td class='r'>{fmt_price(r['Close'])}</td>"
                f"<td class='r {rsi_cls}'>{rsi:.1f}</td>"
                f"<td class='r'>{r['MACD']:+.2f}</td>"
                f"<td class='r'>{r['Signal']:+.2f}</td>"
                f"<td class='r {hist_cls}'>{hist:+.2f}</td>"
                f"<td class='r'>{vwap_txt}</td>"
                f"<td class='r {diff_cls}'>{diff_txt}</td></tr>")

    rows = []
    # 方向別の「最良手仕舞い地点」
    if direction == "SHORT":
        best_idx = m["Low"].idxmin()
        best_r = m.loc[best_idx].copy()
        best_r["Close"] = m.loc[best_idx, "Low"]  # 安値価格を表示
        rows.append(row(f"日中最安値（ショート利確ベスト）", best_r, highlight=True))
        # MACD ヒスト プラ転（Hist<0 → >=0、反発の初期サイン）
        prev = m["Hist"].shift(1)
        flips = m[(prev < 0) & (m["Hist"] >= 0)]
        if len(flips):
            rows.append(row("MACDヒスト プラ転（反発シグナル）", flips.iloc[0]))
        # RSI 底打ち反転（min後に最初に+5上がった点）
        rsi_min_idx = m["RSI"].idxmin()
        after = m.loc[rsi_min_idx:]
        rsi_min = m.loc[rsi_min_idx, "RSI"]
        bounce = after[after["RSI"] >= rsi_min + 5]
        if len(bounce) > 0 and rsi_min_idx != m.index[-1]:
            rows.append(row(f"RSI底打ち+5反転（{rsi_min:.1f}→）", bounce.iloc[0]))
    else:  # LONG
        best_idx = m["High"].idxmax()
        best_r = m.loc[best_idx].copy()
        best_r["Close"] = m.loc[best_idx, "High"]
        rows.append(row(f"日中最高値（ロング利確ベスト）", best_r, highlight=True))
        prev = m["Hist"].shift(1)
        flips = m[(prev >= 0) & (m["Hist"] < 0)]
        if len(flips):
            rows.append(row("MACDヒスト マイ転（反落シグナル）", flips.iloc[0]))

    # 大引け
    rows.append(row("大引け 15:30", m.iloc[-1]))

    legend = (
        "RSI(9): ≤30=売られすぎ(ショート利確圏) / ≥70=買われすぎ(ロング利確圏) "
        "／ MACD(12,26,9)ヒスト: SHORTは<0→≥0のプラ転が反発初動、LONGは≥0→<0のマイ転が反落初動 "
        "／ VWAP乖離: 終値-VWAP"
    )

    return f"""
    <h3>テクニカル指標（5分足・RSI(9)・MACD(12,26,9)・VWAP）</h3>
    <table>
      <tr><th>時刻</th><th>イベント</th><th class="r">株価</th><th class="r">RSI(9)</th><th class="r">MACD</th><th class="r">Signal</th><th class="r">Hist</th><th class="r">VWAP</th><th class="r">終値-VWAP</th></tr>
      {''.join(rows)}
    </table>
    <p style="color:var(--text-muted);font-size:.82rem;margin-top:6px">{legend}</p>
    """


STRATEGY_BADGE_CLASS = {
    "grok": "badge-grok",
    "pair": "badge-pair",
    "semicon": "badge-semicon",
    "loss_cut": "badge-loss-cut",
    "mistake": "badge-mistake",
    "granville": "badge-granville",
    "reversal": "badge-reversal",
    "other": "",
}


def render_single_section(t: dict) -> str:
    """grok/granville/reversal/other 用の共通セクション。戦略バッジのみ切替、
    verdict は grok のみ（他戦略は専用判定ロジック未整備のため省略）。"""
    code = t["code"]
    name = t["name"]
    direction = t["direction"]
    qty = t["qty"]
    entry = t["entry"]
    exit_ = t["exit"]
    pl = t["pl"]
    pl_pct = t["pl_pct"]
    strategy = t.get("strategy", "other")

    badge_dir = "badge-long" if direction == "LONG" else "badge-short"
    if strategy == "grok":
        verdict_cls, verdict_text = verdict_for_grok(t)
    else:
        verdict_cls, verdict_text = "", ""

    ohlc_row = ""
    exec_quality = ""
    if t.get("daily_close") is not None:
        o, h, l, c, v = t["daily_open"], t["daily_high"], t["daily_low"], t["daily_close"], t["daily_volume"]
        # 直近10営業日分
        dd = t["daily_df"].copy()
        dd["Date"] = pd.to_datetime(dd["Date"])
        dd = dd.sort_values("Date").tail(10).reset_index(drop=True)
        prev_close = dd["AdjC"].shift(1)
        rows_html = []
        for i, row in dd.iterrows():
            dt = row["Date"].strftime("%m/%d")
            oo, hh, ll, cc, vv = row["AdjO"], row["AdjH"], row["AdjL"], row["AdjC"], row["AdjVo"]
            oc_ = (cc - oo) / oo * 100
            rng_ = (hh - ll) / ll * 100
            if pd.notna(prev_close.iloc[i]):
                pc_ = (cc - prev_close.iloc[i]) / prev_close.iloc[i] * 100
                pc_html = f'<td class="r {"num-pos" if pc_>0 else "num-neg"}">{pc_:+.2f}%</td>'
            else:
                pc_html = '<td class="r" style="color:var(--text-muted)">—</td>'
            is_today = row["Date"].strftime("%Y-%m-%d") == DATE
            tr_cls = " class=\"hl\"" if is_today else ""
            dt_cell = f"<b>{dt}</b>" if is_today else dt
            cc_str = fmt_price(cc)
            c_cell = f"<b>{cc_str}</b>" if is_today else cc_str
            oc_cls = "num-pos" if oc_ > 0 else "num-neg"
            v_cell = f"{int(vv):,}" if pd.notna(vv) else "—"
            rows_html.append(
                f"<tr{tr_cls}><td>{dt_cell}</td>"
                f"<td class=\"r\">{fmt_price(oo)}</td><td class=\"r\">{fmt_price(hh)}</td>"
                f"<td class=\"r\">{fmt_price(ll)}</td><td class=\"r\">{c_cell}</td>"
                f"<td class=\"r\">{v_cell}</td>"
                f"<td class=\"r {oc_cls}\">{oc_:+.2f}%</td>"
                f"{pc_html}<td class=\"r\">{rng_:.2f}%</td></tr>"
            )
        ohlc_row = f"""
        <h3>日足 OHLC（直近{len(dd)}営業日, J-Quants AdjC）</h3>
        <table>
          <tr><th>日付</th><th class="r">始値</th><th class="r">高値</th><th class="r">安値</th>
              <th class="r">終値</th><th class="r">出来高</th>
              <th class="r">寄り引け</th><th class="r">前日比</th><th class="r">レンジ</th></tr>
          {''.join(rows_html)}
        </table>
        """

        # 執行位置スコア（主=案D 0%最良 / 補助=案B ±100）
        s = entry_score_label(direction, entry, l, h, exit_)

        # MOC比較
        if direction == "SHORT":
            moc_pl = int((entry - c) * qty)
        else:
            moc_pl = int((c - entry) * qty)

        if s is None:
            exec_quality = "<h3>執行品質</h3><p style='color:var(--text-muted)'>レンジがゼロのためスコア算出不可</p>"
        else:
            exec_quality = f"""
        <h3>執行品質（0%=理想 / 100%=最悪）</h3>
        <table>
          <tr><th>指標</th><th class="r">値</th><th>所見</th></tr>
          <tr><td>建玉位置</td><td class="r">{fmt_edge(s['edge_entry'])} {fmt_dev(s['dev_entry'])}</td><td>{s['entry_label']}</td></tr>
          <tr><td>決済位置</td><td class="r">{fmt_edge(s['edge_exit'])} {fmt_dev(s['dev_exit'])}</td><td>{s['exit_label']}</td></tr>
          <tr><td>取り幅達成度</td><td class="r">{fmt_capture(s['capture'])}</td><td>+100=完璧 / 0=無駄 / −=逆走</td></tr>
          <tr><td>MOC保持時損益</td><td class="r">{fmt_pl(moc_pl)}</td><td>建値→大引けまで保持した場合</td></tr>
          <tr><td>実現 vs MOC差</td><td class="r">{fmt_pl(pl - moc_pl)}</td><td>{'早期決済で取れなかった分' if pl < moc_pl else '早期決済が良かった分'}</td></tr>
        </table>
        <p style="color:var(--text-muted);font-size:.78rem;margin-top:6px">
          建玉/決済位置: 日中レンジ内で「理想地点からどれだけ離れたか」。0%=完璧、100%=最悪。中央比は補助（+100=高値/−100=安値/0=中央）。
        </p>
        """

    chart_tag = f'<img src="data:image/png;base64,{t["chart_b64"]}" alt="chart" style="width:100%;max-width:1100px;border-radius:8px;margin:8px 0">' if t.get("chart_b64") else ""

    bucket = t.get("bucket") if strategy == "grok" else None
    prob = t.get("prob") if strategy == "grok" else None
    bucket_badge = f'<span class="badge badge-bucket-{bucket.lower()}">{bucket}</span>' if bucket else ""
    prob_badge = f'<span class="badge badge-prob">{prob:.2f}</span>' if prob is not None else ""
    strat_badge_cls = STRATEGY_BADGE_CLASS.get(strategy, "")
    strat_badge = f'<span class="badge {strat_badge_cls}">{strategy}</span>' if strat_badge_cls else f'<span class="badge" style="color:var(--text-muted);border:1px solid var(--card-border)">{strategy}</span>'
    verdict_html = f'<div class="verdict {verdict_cls}">{escape(verdict_text)}</div>' if verdict_text else ""

    return f"""
    <div class="section">
      <h2>{code} {escape(name)}
        <span class="badge {badge_dir}">{direction}</span>
        {strat_badge}
        {bucket_badge}
        {prob_badge}
      </h2>

      <div class="grid">
        <div class="stat-card"><div class="label">建値 → 決済</div><div class="value">{fmt_price(entry)} → {fmt_price(exit_)}</div><div class="sub">{qty}株</div></div>
        <div class="stat-card"><div class="label">実現損益</div><div class="value">{fmt_pl(pl)}</div><div class="sub">{pl_pct:+.2f}%</div></div>
        <div class="stat-card"><div class="label">大引け</div><div class="value">{fmt_price(t.get('daily_close'))}</div><div class="sub">終値</div></div>
        <div class="stat-card"><div class="label">日中レンジ</div><div class="value" style="font-size:.95rem">{fmt_price(t.get('daily_low'))} 〜 {fmt_price(t.get('daily_high'))}</div><div class="sub">L 〜 H</div></div>
      </div>
      {ohlc_row}
      <h3>日足 + 5分足 (JST・昼休み詰め)</h3>
      {chart_tag}
      {render_indicators_table(t)}
      {exec_quality}
      {verdict_html}
      <h3>メモ</h3>
      <div class="memo">{escape(t.get("manual_note") or "（ここに反省・学びを書く）")}</div>
    </div>
    """


def render_pair_group(label: str, sector: str, trade_keys: list, trades: dict, peers_data: list = None) -> str:
    legs = [trades[k] for k in trade_keys if k in trades]
    if len(legs) < 2:
        return ""
    pair_codes = {l["code"] for l in legs}
    total_pl = sum(l["pl"] for l in legs)
    short_legs = [l for l in legs if l["direction"] == "SHORT"]
    long_legs = [l for l in legs if l["direction"] == "LONG"]
    if not short_legs or not long_legs:
        print(f"[WARN] pair direction 不整合でスキップ: {label} / {[(l['code'], l['direction']) for l in legs]}")
        return ""
    short_leg = short_legs[0]
    long_leg = long_legs[0]

    rows_html = ""
    for l in legs:
        direction_badge = "badge-short" if l["direction"] == "SHORT" else "badge-long"
        rows_html += f"""
        <tr>
          <td>{l["code"]}</td>
          <td>{escape(l["name"])}</td>
          <td><span class="badge {direction_badge}">{l["direction"]}</span></td>
          <td class="r">{fmt_price(l["entry"])}</td>
          <td class="r">{fmt_price(l["exit"])}</td>
          <td class="r">{fmt_price(l.get("daily_close"))}</td>
          <td class="r">{fmt_pl(l["pl"])}</td>
        </tr>
        """

    # セクター内騰落
    sector_html = ""
    if peers_data:
        sec_rows = []
        avg = sum(p["pct"] for p in peers_data) / len(peers_data)
        for p in sorted(peers_data, key=lambda x: -x["pct"]):
            pair_flag = p["code"] in pair_codes
            tr_cls = " class=\"hl\"" if pair_flag else ""
            cls = "num-pos" if p["pct"] > 0 else ("num-neg" if p["pct"] < 0 else "num-neutral")
            tag = " <span class=\"badge badge-pair\" style=\"font-size:.65rem\">本日脚</span>" if pair_flag else ""
            sec_rows.append(
                f"<tr{tr_cls}><td>{p['code']}</td><td>{escape(p['name'])}{tag}</td>"
                f"<td class=\"r\">{fmt_price(p['prev'])}</td>"
                f"<td class=\"r\">{fmt_price(p['close'])}</td>"
                f"<td class=\"r {cls}\">{p['pct']:+.2f}%</td></tr>"
            )
        avg_cls = "num-pos" if avg > 0 else ("num-neg" if avg < 0 else "num-neutral")
        winners = sum(1 for p in peers_data if p["pct"] > 0)
        breadth = f"{winners}/{len(peers_data)} 銘柄 上昇"
        sector_html = f"""
        <h3>セクター内騰落（{escape(sector)} n={len(peers_data)}, 前日比）</h3>
        <table>
          <tr><th>コード</th><th>銘柄</th><th class="r">前日終値</th><th class="r">当日終値</th><th class="r">前日比</th></tr>
          {''.join(sec_rows)}
          <tr style="border-top:2px solid var(--card-border)">
            <td colspan="4" class="r"><b>セクター単純平均</b></td>
            <td class="r {avg_cls}"><b>{avg:+.2f}%</b></td>
          </tr>
        </table>
        <p style="color:var(--text-muted);font-size:.82rem">
          騰落幅: {breadth} / 強いセクター={avg:+.2f}% なら SHORT脚は地合い逆風。本日脚は黄ハイライト。
        </p>
        """

    spread_verdict = "収束" if total_pl > 0 else "発散"
    verdict_cls = "ok" if total_pl > 0 else "bad"

    return f"""
    <div class="section">
      <h2>{escape(label)}
        <span class="badge badge-pair">pair</span>
      </h2>

      <div class="grid">
        <div class="stat-card"><div class="label">ペア合計</div><div class="value">{fmt_pl(total_pl)}</div><div class="sub">スプレッド {spread_verdict}</div></div>
        <div class="stat-card"><div class="label">SHORT脚</div><div class="value" style="font-size:.95rem">{short_leg["code"]}</div><div class="sub">{fmt_pl(short_leg["pl"])}</div></div>
        <div class="stat-card"><div class="label">LONG脚</div><div class="value" style="font-size:.95rem">{long_leg["code"]}</div><div class="sub">{fmt_pl(long_leg["pl"])}</div></div>
        <div class="stat-card"><div class="label">セクター</div><div class="value" style="font-size:.95rem">{escape(sector)}</div><div class="sub">両脚同セクター</div></div>
      </div>

      <h3>両脚の約定</h3>
      <table>
        <tr><th>コード</th><th>銘柄</th><th>方向</th><th class="r">建値</th><th class="r">決済</th><th class="r">大引け</th><th class="r">損益</th></tr>
        {rows_html}
      </table>

      {sector_html}
      <h3>セクター観察（{escape(sector)}）</h3>
      <p style="color:var(--text-muted);font-size:.88rem;line-height:1.7">
        両脚とも同セクター。個別材料ではなくセクター全体の相対強弱で動く。<br>
        ペアP&Lは <b>両脚スプレッドの収束/発散</b> で評価する。片脚単体の勝敗は意味なし（メモリー規則: 片脚利確=部分最適）。<br>
        今日のスプレッド: SHORT脚 {fmt_price(short_leg["entry"])}→{fmt_price(short_leg["exit"])} ({(short_leg["entry"]-short_leg["exit"])/short_leg["entry"]*100:+.2f}%) / LONG脚 {fmt_price(long_leg["entry"])}→{fmt_price(long_leg["exit"])} ({(long_leg["exit"]-long_leg["entry"])/long_leg["entry"]*100:+.2f}%)
      </p>
      <div class="verdict {verdict_cls}">
        ペア: スプレッド{spread_verdict} / 合計 {total_pl:+,}円 / MOC決済ルール遵守
      </div>
      <h3>メモ</h3>
      <div class="memo">（ここに反省・学びを書く）</div>
    </div>
    """


def render_open_position_section(open_positions: list[dict], trade_dict: dict) -> str:
    """当日ペアの片脚だけが未決済で残った場合の注意セクション。"""
    if not open_positions:
        return ""

    rows = []
    for p in open_positions:
        note = ""
        if DATE == "2026-05-18" and p["code"] == "8345":
            note = "8387四国銀行とのペア残り脚。返済注文が大引不成ではなく本日中となり持ち越し。"
        else:
            paired_today = any(
                t.get("pair_info") and p["code"] in (t["code"], t["pair_info"].get("partner"))
                for t in trade_dict.values()
            )
            if not paired_today:
                continue
            note = "当日ペアの未決済残り脚。"

        badge_dir = "badge-long" if p["direction"] == "LONG" else "badge-short"
        rows.append(f"""
        <tr>
          <td>{p["code"]}</td>
          <td>{escape(p["name"])}</td>
          <td><span class="badge {badge_dir}">{p["direction"]}</span></td>
          <td class="r">{p["qty"]:,}</td>
          <td class="r">{fmt_price(p["entry"])}</td>
          <td class="r">{fmt_price(p["current"])}</td>
          <td class="r">{fmt_pl(p["unrealized_pl"])}</td>
          <td>{escape(note)}</td>
        </tr>
        """)

    if not rows:
        return ""

    return f"""
    <h2 style="margin:24px 0 12px">未決済ペア残り脚（{len(rows)}件）</h2>
    <div class="section">
      <h2>持ち越し確認 <span class="badge badge-pair">open pair leg</span></h2>
      <table>
        <tr><th>コード</th><th>銘柄</th><th>方向</th><th class="r">数量</th><th class="r">建値</th><th class="r">時価</th><th class="r">含み損益</th><th>メモ</th></tr>
        {''.join(rows)}
      </table>
      <div class="memo">大引不成の返済条件漏れは、発注前 order.csv チェックで「返済条件」「株数」「片脚だけ本日中になっていないか」を確認する。</div>
    </div>
    """


def main():
    global DATE, OUT
    trades_raw, DATE = load_trades_from_csv(CSV_PATH)
    yyyymmdd = DATE.replace("-", "")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT = OUT_DIR / f"{yyyymmdd}.html"
    d_from = n_business_days_before(DATE, 10)

    print(f"[INFO] 対象日 {DATE} / 約定 {len(trades_raw)}件")
    print("[INFO] データソース読み込み (signals / grok / eq master)...")
    grok_info = load_grok_info(DATE)
    signals_df = load_signals(DATE)
    eq_master_df = load_eq_master(DATE)
    open_positions = load_open_positions(HOLD_CSV_PATH)
    trade_codes = {t["code"] for t in trades_raw} | {p["code"] for p in open_positions}
    trade_dirs: dict[str, set[str]] = {}
    for t in trades_raw:
        trade_dirs.setdefault(t["code"], set()).add(t["direction"])
    for p in open_positions:
        trade_dirs.setdefault(p["code"], set()).add(p["direction"])

    print("[INFO] 日足取得 (J-Quants)...")
    trade_dict = {}
    for idx, t_raw in enumerate(trades_raw):
        code = t_raw["code"]
        d = jq_daily(code, d_from, DATE)
        today = None
        if not d.empty:
            d["Date"] = pd.to_datetime(d["Date"])
            hit = d[d["Date"].dt.strftime("%Y-%m-%d") == DATE]
            if not hit.empty:
                today = hit.iloc[-1]
        strategy, pair_info = classify_trade(
            code, t_raw["direction"], trade_codes, trade_dirs, signals_df, grok_info
        )
        manual_strategy, manual_note = detect_manual_trade_note(t_raw)
        if manual_strategy:
            strategy = manual_strategy
            pair_info = None

        t = {
            **t_raw,
            "trade_key": f"{code}-{idx}",
            "pl_pct": t_raw["pl"] / (t_raw["entry"] * t_raw["qty"]) * 100,
            "strategy": strategy,
            "pair_info": pair_info,
            "manual_note": manual_note,
            "daily_df": d,
            "daily_today": today,
            "daily_open": today["AdjO"] if today is not None else None,
            "daily_high": today["AdjH"] if today is not None else None,
            "daily_low": today["AdjL"] if today is not None else None,
            "daily_close": today["AdjC"] if today is not None else None,
            "daily_volume": today["AdjVo"] if today is not None else None,
            "bucket": grok_info.get(code, {}).get("bucket"),
            "prob": grok_info.get(code, {}).get("prob"),
        }
        trade_dict[t["trade_key"]] = t

    print(f"[INFO] 分類結果: " + " / ".join(
        f"{s}={sum(1 for t in trade_dict.values() if t['strategy']==s)}"
        for s in ("grok", "pair", "semicon", "loss_cut", "mistake", "granville", "reversal", "other")
    ))

    intraday_tech_strategies = {"grok", "semicon", "loss_cut"}
    print("[INFO] 5分足取得 (yfinance)...")
    for t in trade_dict.values():
        if t["strategy"] in intraday_tech_strategies:
            try:
                intr = yf_intraday(t["code"])
            except Exception as e:
                intr = pd.DataFrame()
                print(f"[WARN] yf {t['code']}: {e}")
            d5_full, d5_today = prepare_5m(intr, t["daily_df"])
            t["d_5m_full"] = d5_full
            t["d_5m"] = d5_today
            t["chart_b64"] = plot_chart(t["daily_df"], d5_today, t)

    # 集計
    total_pl = sum(t["pl"] for t in trade_dict.values())
    wins = sum(1 for t in trade_dict.values() if t["pl"] > 0)
    losses = sum(1 for t in trade_dict.values() if t["pl"] < 0)
    strategies = ("grok", "pair", "semicon", "loss_cut", "mistake", "granville", "reversal", "other")
    strategy_pl = {s: sum(t["pl"] for t in trade_dict.values() if t["strategy"] == s)
                   for s in strategies}
    strategy_cnt = {s: sum(1 for t in trade_dict.values() if t["strategy"] == s)
                    for s in strategies}
    max_loss = min(t["pl"] for t in trade_dict.values())
    max_win = max(t["pl"] for t in trade_dict.values())
    loss_code = next(t["code"] for t in trade_dict.values() if t["pl"] == max_loss)
    win_code = next(t["code"] for t in trade_dict.values() if t["pl"] == max_win)

    cnt_sub = " / ".join(f"{s} {strategy_cnt[s]}件" for s in strategies if strategy_cnt[s])
    pl_sub = " / ".join(f"{s} {strategy_pl[s]:+,}" for s in strategies if strategy_cnt[s])

    # セクション構築
    # (1) サマリー
    total_html = f"""
    <div class="section">
      <div class="grid">
        <div class="stat-card"><div class="label">約定件数</div><div class="value">{len(trade_dict)}</div><div class="sub">{cnt_sub}</div></div>
        <div class="stat-card"><div class="label">合計損益</div><div class="value">{fmt_pl(total_pl)}</div><div class="sub">{pl_sub}</div></div>
        <div class="stat-card"><div class="label">勝ち / 負け</div><div class="value">{wins} / {losses}</div><div class="sub">勝率 {wins/len(trade_dict)*100:.1f}%</div></div>
        <div class="stat-card"><div class="label">最大損失 / 最大利益</div><div class="value" style="font-size:.95rem">{max_loss:+,} / {max_win:+,}</div><div class="sub">{loss_code} / {win_code}</div></div>
      </div>
    </div>
    """

    # (2) grok セクション（損益絶対値順）
    grok_trades = [t for t in trade_dict.values() if t["strategy"] == "grok"]
    grok_trades.sort(key=lambda x: -abs(x["pl"]))
    grok_sections = "".join(render_single_section(t) for t in grok_trades)

    mistake_trades = [t for t in trade_dict.values() if t["strategy"] == "mistake"]
    mistake_trades.sort(key=lambda x: -abs(x["pl"]))
    mistake_sections = "".join(render_single_section(t) for t in mistake_trades)

    semicon_trades = [t for t in trade_dict.values() if t["strategy"] == "semicon"]
    semicon_trades.sort(key=lambda x: -abs(x["pl"]))
    semicon_sections = "".join(render_single_section(t) for t in semicon_trades)

    loss_cut_trades = [t for t in trade_dict.values() if t["strategy"] == "loss_cut"]
    loss_cut_trades.sort(key=lambda x: -abs(x["pl"]))
    loss_cut_sections = "".join(render_single_section(t) for t in loss_cut_trades)

    # (3) pair セクション（signals.parquet の pair_id でグルーピング）
    pair_groups: dict[str, list[str]] = {}
    for trade_key, t in trade_dict.items():
        if t["strategy"] != "pair" or not t.get("pair_info"):
            continue
        pair_id = t["pair_info"]["pair_id"]
        pair_groups.setdefault(pair_id, []).append(trade_key)

    pair_sections = ""
    rendered_pair_count = 0
    single_pair_count = 0
    for pair_id, trade_keys in pair_groups.items():
        if len(trade_keys) < 2:
            single_pair_count += len(trade_keys)
            for trade_key in trade_keys:
                pair_sections += render_single_section(trade_dict[trade_key])
            continue
        first_key = trade_keys[0]
        pi = trade_dict[first_key]["pair_info"]
        peers, sector_name = load_peers_by_s33(trade_dict[first_key]["code"], eq_master_df, top_n=8)
        peers_data = sector_moves(peers, DATE)
        label = f"{pi['name1']}×{pi['name2']} ペア"
        section_html = render_pair_group(label, sector_name, trade_keys, trade_dict, peers_data)
        if section_html:
            pair_sections += section_html
            rendered_pair_count += 1

    # (4) granville / reversal / other セクション（損益絶対値順）
    def _render_group(kind: str) -> tuple[str, int]:
        items = [t for t in trade_dict.values() if t["strategy"] == kind]
        items.sort(key=lambda x: -abs(x["pl"]))
        return "".join(render_single_section(t) for t in items), len(items)

    granville_sections, granville_count = _render_group("granville")
    reversal_sections, reversal_count = _render_group("reversal")
    other_sections, other_count = _render_group("other")
    open_position_sections = render_open_position_section(open_positions, trade_dict)

    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    dt = datetime.strptime(DATE, "%Y-%m-%d")
    date_slash = dt.strftime("%Y/%m/%d")
    weekday = weekdays[dt.weekday()]

    breakdown_parts = []
    for s in strategies:
        if not strategy_cnt[s]:
            continue
        if s == "pair":
            if rendered_pair_count and single_pair_count:
                unit_label = f"{rendered_pair_count}ペア+片脚{single_pair_count}件"
            elif rendered_pair_count:
                unit_label = f"{rendered_pair_count}ペア"
            else:
                unit_label = f"{single_pair_count}件"
            breakdown_parts.append(f"pair {unit_label}({strategy_pl[s]:+,})")
        else:
            breakdown_parts.append(f"{s} {strategy_cnt[s]}件({strategy_pl[s]:+,})")
    breakdown = " / ".join(breakdown_parts)
    title_str = f"Trade Review {date_slash}（{weekday}） - {total_pl:+,}円 {len(trade_dict)}件・{breakdown}"
    pair_heading = (
        f"pair 銘柄（{rendered_pair_count}ペア / 片脚{single_pair_count}件）"
        if single_pair_count
        else f"pair 銘柄（{rendered_pair_count}ペア）"
    )

    html = f"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><title>{title_str}</title>
<style>
:root{{--bg:#09090b;--card:#18181b;--card-border:#27272a;--text:#fafafa;--text-muted:#a1a1aa;
--up:#34d399;--up-bg:rgba(52,211,153,0.1);--down:#fb7185;--down-bg:rgba(251,113,133,0.1);
--amber:#fbbf24;--amber-bg:rgba(251,191,36,0.15);--blue:#60a5fa;--blue-bg:rgba(96,165,250,0.1);
--purple:#a78bfa;--purple-bg:rgba(167,139,250,0.1);}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--text);font-family:"Helvetica Neue",-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif;
font-feature-settings:"tnum" 1,"lnum" 1;-webkit-font-smoothing:antialiased;line-height:1.6;padding:24px;max-width:1200px;margin:0 auto}}
h1{{font-size:1.5rem;font-weight:700;margin-bottom:6px}}
.subtitle{{font-size:.875rem;color:var(--text-muted);margin-bottom:20px}}
.section{{background:var(--card);border:1px solid var(--card-border);border-radius:12px;padding:24px;margin-bottom:20px}}
.section h2{{font-size:1.1rem;font-weight:700;margin-bottom:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}}
.section h3{{font-size:.95rem;color:var(--text-muted);margin:16px 0 8px}}
.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}}
.stat-card{{background:rgba(255,255,255,0.02);border:1px solid var(--card-border);border-radius:8px;padding:16px;text-align:center}}
.stat-card .label{{color:var(--text-muted);font-size:.75rem;margin-bottom:4px}}
.stat-card .value{{font-size:1.35rem;font-weight:700;font-variant-numeric:tabular-nums}}
.stat-card .sub{{color:var(--text-muted);font-size:.7rem;margin-top:2px}}
table{{width:100%;border-collapse:collapse;font-size:.85rem;margin:8px 0}}
th{{text-align:left;padding:8px 12px;background:rgba(255,255,255,0.03);color:var(--text-muted);font-weight:600;border-bottom:1px solid var(--card-border)}}
td{{padding:8px 12px;border-bottom:1px solid rgba(255,255,255,0.05)}}
td.r,th.r{{text-align:right;font-variant-numeric:tabular-nums}}
.num-pos{{color:var(--up);font-weight:600}}.num-neg{{color:var(--down);font-weight:600}}.num-neutral{{color:var(--text-muted)}}
tr.hl td{{background:rgba(251,191,36,0.08)}}
.badge{{display:inline-block;padding:2px 10px;border-radius:9999px;font-size:.72rem;font-weight:600}}
.badge-long{{background:var(--up-bg);color:var(--up);border:1px solid rgba(52,211,153,0.3)}}
.badge-short{{background:var(--down-bg);color:var(--down);border:1px solid rgba(251,113,133,0.3)}}
.badge-grok{{background:var(--amber-bg);color:var(--amber);border:1px solid rgba(251,191,36,0.3)}}
.badge-pair{{background:var(--purple-bg);color:var(--purple);border:1px solid rgba(167,139,250,0.3)}}
.badge-semicon{{background:var(--cyan-bg);color:var(--cyan);border:1px solid rgba(34,211,238,0.3)}}
.badge-loss-cut{{background:var(--down-bg);color:var(--down);border:1px solid rgba(251,113,133,0.3)}}
.badge-mistake{{background:var(--down-bg);color:var(--down);border:1px solid rgba(251,113,133,0.3)}}
.badge-granville{{background:var(--blue-bg);color:var(--blue);border:1px solid rgba(96,165,250,0.3)}}
.badge-reversal{{background:rgba(244,114,182,0.1);color:#f472b6;border:1px solid rgba(244,114,182,0.3)}}
.badge-bucket-short{{color:#fb7185;border:1px solid rgba(251,113,133,0.4);background:transparent}}
.badge-bucket-disc{{color:#a1a1aa;border:1px solid rgba(161,161,170,0.4);background:transparent}}
.badge-bucket-long{{color:#34d399;border:1px solid rgba(52,211,153,0.4);background:transparent}}
.badge-prob{{background:rgba(167,139,250,0.1);color:#a78bfa;border:1px solid rgba(167,139,250,0.3)}}
.verdict{{background:rgba(255,255,255,0.02);border-left:3px solid var(--blue);padding:10px 14px;margin:10px 0;font-size:.88rem}}
.verdict.ok{{border-left-color:var(--up)}}
.verdict.bad{{border-left-color:var(--down)}}
.memo{{background:rgba(255,255,255,0.02);border-left:3px solid var(--amber);padding:12px 16px;margin-top:12px;font-size:.88rem;color:var(--text-muted);white-space:pre-wrap}}
</style></head>
<body>
<h1>Trade Review — {date_slash}（{weekday}）</h1>
<div class="subtitle">戦略: grok / pair / semicon / loss_cut / granville / reversal / other（signals.parquet + grok_trending.parquet + 明示指定で分類）／ 日足: J-Quants AdjC ／ 5分足: yfinance JST 昼休み詰め</div>
{total_html}
{'<h2 style="margin:24px 0 12px">取引ミス（' + str(len(mistake_trades)) + '件）</h2>' + mistake_sections if mistake_trades else ''}
{'<h2 style="margin:24px 0 12px">semicon 銘柄（' + str(len(semicon_trades)) + '件）</h2>' + semicon_sections if semicon_trades else ''}
{'<h2 style="margin:24px 0 12px">損切り（' + str(len(loss_cut_trades)) + '件）</h2>' + loss_cut_sections if loss_cut_trades else ''}
<h2 style="margin:24px 0 12px">grok 銘柄（{len(grok_trades)}件）</h2>
{grok_sections}
<h2 style="margin:24px 0 12px">{pair_heading}</h2>
{pair_sections}
{open_position_sections}
{'<h2 style="margin:24px 0 12px">granville 銘柄（' + str(granville_count) + '件）</h2>' + granville_sections if granville_count else ''}
{'<h2 style="margin:24px 0 12px">reversal 銘柄（' + str(reversal_count) + '件）</h2>' + reversal_sections if reversal_count else ''}
{'<h2 style="margin:24px 0 12px">other 銘柄（' + str(other_count) + '件）</h2>' + other_sections if other_count else ''}
</body></html>
"""

    OUT.write_text(html, encoding="utf-8")
    print(f"[OUT] {OUT}")
    print(f"total_pl={total_pl:+,} " + " ".join(f"{s}={strategy_pl[s]:+,}" for s in strategy_pl if strategy_cnt[s]))


if __name__ == "__main__":
    main()
