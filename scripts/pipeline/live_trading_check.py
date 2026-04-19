#!/usr/bin/env python3
"""
live_trading_check.py
=====================
sox_research/live_trading 用の毎朝判定スクリプト。

実行タイミング: 07:00 JST (data-pipeline-staging.yml の cme_update フェーズ)

処理フロー:
  1. yfinance で SOX / CME_NK / NVDA / VIX / N225 の最新引け取得
  2. SOX/CME/NVDA の day_pct (前日比) を計算
  3. CME日経 vs N225 前日終値 でギャップ推定
  4. earnings_dates.json で決算±2営業日 window 判定
  5. 発火判定 (SOX+2.0 / CME+2.0 / NVDA+0.75 の OR)
  6. サイズ判定 (gap_pct ルール)
  7. 棄却条件 (VIX>=25 / earnings window / gap>+7%)
  8. signals_log.csv に append または update (手動確定行は保護)
  9. Slack 用メッセージを /tmp/live_trading_slack.json に出力

ルール定義は sox_research/live_trading/RULES.md と同期すること。
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
LIVE_DIR = ROOT / "sox_research" / "live_trading"
SIGNALS_LOG = LIVE_DIR / "signals_log.csv"
EARNINGS_JSON = LIVE_DIR / "earnings_dates.json"
SLACK_OUT = Path("/tmp/live_trading_slack.json")

# ルール閾値 (RULES.md と同期)
SOX_THRESHOLD = 2.0
CME_THRESHOLD = 2.0
NVDA_THRESHOLD = 0.75
VIX_MAX = 25.0
GAP_100_MAX = 5.0   # gap < +5% → 100株GO
GAP_50_MAX = 7.0    # gap +5-7% → 50株 or 見送り (PENDING手動判断)
EARNINGS_WINDOW_BD = 2  # 決算±2営業日 で NG
TICKER = "6857"

JST = ZoneInfo("Asia/Tokyo")

CSV_COLUMNS = [
    "date", "sox_day_pct", "cme_day_pct", "nvda_day_pct",
    "sox_fired", "cme_fired", "nvda_fired",
    "vix_close", "gap_pct", "earnings_week", "action", "reason",
]


def fetch_closes() -> dict[str, pd.Series | None]:
    import yfinance as yf
    tickers = {
        "SOX": "^SOX",
        "CME_NK": "NKD=F",
        "NVDA": "NVDA",
        "VIX": "^VIX",
        "N225": "^N225",
    }
    out: dict[str, pd.Series | None] = {}
    for label, tk in tickers.items():
        try:
            df = yf.download(tk, period="10d", interval="1d", progress=False, auto_adjust=False)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]
            if df.empty or "Close" not in df.columns:
                out[label] = None
                continue
            out[label] = df["Close"].dropna()
        except Exception as e:
            print(f"[WARN] yfinance download failed for {label} ({tk}): {e}")
            out[label] = None
    return out


def day_pct(s: pd.Series | None) -> float | None:
    if s is None or len(s) < 2:
        return None
    return round((float(s.iloc[-1]) / float(s.iloc[-2]) - 1) * 100, 2)


def last_value(s: pd.Series | None) -> float | None:
    if s is None or len(s) < 1:
        return None
    return float(s.iloc[-1])


def busdays_between(d1: date, d2: date) -> int:
    """d1, d2 の間の営業日数 (絶対値, 両端含まない日数)"""
    lo, hi = sorted([d1, d2])
    return max(0, len(pd.bdate_range(lo, hi)) - 1)


def load_earnings_dates(code: str) -> list[date]:
    if not EARNINGS_JSON.exists():
        return []
    with open(EARNINGS_JSON) as f:
        cfg = json.load(f)
    entry = cfg.get(code)
    if not entry:
        return []
    return [date.fromisoformat(e["date"]) for e in entry.get("earnings", [])]


def judge_earnings_window(target: date, earnings: list[date]) -> tuple[bool, str]:
    for ed in earnings:
        bd = busdays_between(target, ed)
        if bd <= EARNINGS_WINDOW_BD:
            return True, f"決算{ed.isoformat()}の±{bd}営業日"
    return False, ""


def decide_action(
    any_fired: bool,
    in_earnings: bool,
    earnings_reason: str,
    vix: float | None,
    gap: float | None,
    fire_detail: str,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if not any_fired:
        return "SKIP", ["no signal fire"]
    if in_earnings:
        return "SKIP", [earnings_reason]
    if vix is not None and vix >= VIX_MAX:
        return "SKIP", [f"VIX {vix:.1f} >= {VIX_MAX}"]
    if gap is not None and gap > GAP_50_MAX:
        return "SKIP", [f"gap {gap:+.2f}% > +{GAP_50_MAX}% (wipeout risk)"]
    if gap is not None and gap >= GAP_100_MAX:
        reasons.append(f"gap {gap:+.2f}% = +{GAP_100_MAX}〜{GAP_50_MAX}% → 50株 or 見送り判断")
        reasons.append(fire_detail)
        return "PENDING_50", reasons
    reasons.append(fire_detail)
    return "PENDING_100", reasons


def upsert_signals_log(new_row: dict) -> tuple[pd.DataFrame, bool]:
    if SIGNALS_LOG.exists():
        df = pd.read_csv(SIGNALS_LOG)
    else:
        df = pd.DataFrame(columns=CSV_COLUMNS)

    today_str = new_row["date"]
    today_mask = df.get("date", pd.Series(dtype=str)).astype(str) == today_str

    protected = False
    if today_mask.any():
        existing = str(df.loc[today_mask, "action"].iloc[0])
        if existing in ("TRADED", "SKIP_CONFIRMED"):
            protected = True
        else:
            for k, v in new_row.items():
                df.loc[today_mask, k] = v
    else:
        new_df = pd.DataFrame([new_row], columns=CSV_COLUMNS)
        df = new_df if df.empty else pd.concat([df, new_df], ignore_index=True)

    df = df[CSV_COLUMNS]
    df.to_csv(SIGNALS_LOG, index=False)
    return df, protected


def build_slack_message(
    today_str: str,
    sox_chg: float | None, cme_chg: float | None, nvda_chg: float | None,
    sox_fired: bool, cme_fired: bool, nvda_fired: bool,
    vix: float | None, gap: float | None,
    earnings_reason: str, in_earnings: bool,
    action: str, reasons: list[str], protected: bool,
) -> str:
    if protected:
        emoji = "🔒"
        headline = f"{emoji} Live Trading [{today_str}] manual override 保持 (自動判定スキップ)"
        return headline

    if action.startswith("PENDING"):
        emoji = "🟢" if action == "PENDING_100" else "🟡"
    else:
        emoji = "⚪"

    lines = [f"{emoji} *Live Trading Signal* {today_str} ({TICKER} アドバンテスト)"]
    lines.append("")
    lines.append("*▼ シグナル発火 (閾値)*")
    lines.append(f"SOX    {_fmt_pct(sox_chg)} [{_mark(sox_fired)}] (+{SOX_THRESHOLD}%)")
    lines.append(f"CME日経 {_fmt_pct(cme_chg)} [{_mark(cme_fired)}] (+{CME_THRESHOLD}%)")
    lines.append(f"NVDA   {_fmt_pct(nvda_chg)} [{_mark(nvda_fired)}] (+{NVDA_THRESHOLD}%)")
    lines.append("")
    lines.append("*▼ 市場環境*")
    lines.append(f"VIX: {vix:.1f}" if vix is not None else "VIX: n/a")
    lines.append(f"CME日経 gap推定: {_fmt_pct(gap)}" if gap is not None else "gap: n/a")
    lines.append(f"決算window: {'🚫 NG (' + earnings_reason + ')' if in_earnings else '✅ OK'}")
    lines.append("")
    lines.append(f"*▼ 自動判定: {action}*")
    for r in reasons:
        lines.append(f"・{r}")

    if action == "PENDING_100":
        lines.append("")
        lines.append("*▼ 最終確認 (手動)*")
        lines.append("・地政学イベント (Hormuz / Iran / Israel)")
        lines.append("・CME日経の最終レベル (寄付直前)")
        lines.append("・個別材料 (6857決算関連の速報等)")
        lines.append(f"・エントリー: {TICKER} 100株 / SL -2.5% (寄値基準) / 利確+3%半ドテン")
    elif action == "PENDING_50":
        lines.append("")
        lines.append("*▼ 最終確認 (手動)*")
        lines.append("・gap +5-7% ゾーン → 50株 or 見送り判断")
        lines.append("・高ボラ前提でSLが狩られやすい局面")

    return "\n".join(lines)


def _fmt_pct(v: float | None) -> str:
    return "n/a" if v is None else f"{v:+.2f}%"


def _mark(fired: bool) -> str:
    return "✓" if fired else " "


def main() -> int:
    print("=" * 60)
    print("Live Trading Signal Check")
    print("=" * 60)

    LIVE_DIR.mkdir(parents=True, exist_ok=True)

    today_jst = datetime.now(JST).date()
    today_str = today_jst.isoformat()

    closes = fetch_closes()
    sox_chg = day_pct(closes.get("SOX"))
    cme_chg = day_pct(closes.get("CME_NK"))
    nvda_chg = day_pct(closes.get("NVDA"))
    vix = last_value(closes.get("VIX"))

    gap_pct: float | None = None
    nkd_last = last_value(closes.get("CME_NK"))
    n225_last = last_value(closes.get("N225"))
    if nkd_last is not None and n225_last is not None and n225_last > 0:
        gap_pct = round((nkd_last - n225_last) / n225_last * 100, 2)

    sox_fired = sox_chg is not None and sox_chg >= SOX_THRESHOLD
    cme_fired = cme_chg is not None and cme_chg >= CME_THRESHOLD
    nvda_fired = nvda_chg is not None and nvda_chg >= NVDA_THRESHOLD
    any_fired = sox_fired or cme_fired or nvda_fired

    earnings_list = load_earnings_dates(TICKER)
    in_earnings, earnings_reason = judge_earnings_window(today_jst, earnings_list)

    fire_parts = []
    if sox_fired: fire_parts.append(f"SOX{_fmt_pct(sox_chg)}")
    if cme_fired: fire_parts.append(f"CME{_fmt_pct(cme_chg)}")
    if nvda_fired: fire_parts.append(f"NVDA{_fmt_pct(nvda_chg)}")
    fire_detail = "GO候補: " + " / ".join(fire_parts) if fire_parts else "no fire"

    action, reasons = decide_action(any_fired, in_earnings, earnings_reason, vix, gap_pct, fire_detail)

    new_row = {
        "date": today_str,
        "sox_day_pct": sox_chg if sox_chg is not None else "",
        "cme_day_pct": cme_chg if cme_chg is not None else "",
        "nvda_day_pct": nvda_chg if nvda_chg is not None else "",
        "sox_fired": "Y" if sox_fired else "N",
        "cme_fired": "Y" if cme_fired else "N",
        "nvda_fired": "Y" if nvda_fired else "N",
        "vix_close": round(vix, 2) if vix is not None else "",
        "gap_pct": gap_pct if gap_pct is not None else "",
        "earnings_week": "Y" if in_earnings else "N",
        "action": action,
        "reason": "; ".join(reasons),
    }

    _, protected = upsert_signals_log(new_row)

    message = build_slack_message(
        today_str, sox_chg, cme_chg, nvda_chg,
        sox_fired, cme_fired, nvda_fired,
        vix, gap_pct, earnings_reason, in_earnings,
        action, reasons, protected,
    )

    with open(SLACK_OUT, "w") as f:
        json.dump({"text": message}, f, ensure_ascii=False)

    print(message)
    print("=" * 60)
    print(f"[OK] signals_log: {SIGNALS_LOG}  (protected={protected})")
    print(f"[OK] slack payload: {SLACK_OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
