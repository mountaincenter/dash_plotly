#!/usr/bin/env python3
"""
format_granville_b4_slack.py
============================
B4(-15%)エントリー判定のSlack通知セクションを生成。
/tmp/granville_b4_section.txt に出力。

パイプライン（18:00/22:00/07:00）から呼ばれる。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common_cfg.paths import PARQUET_DIR

GRANVILLE_DIR = PARQUET_DIR / "granville"
OUTPUT_PATH = Path("/tmp/granville_b4_section.txt")


def _load_vi() -> float | None:
    """日経VIを取得"""
    vi_path = PARQUET_DIR / "nikkei_vi_max_1d.parquet"
    if vi_path.exists():
        try:
            df = pd.read_parquet(vi_path)
            df["date"] = pd.to_datetime(df["date"])
            return float(df.sort_values("date").iloc[-1]["close"])
        except Exception:
            pass
    return None


def _load_cme_gap() -> tuple[float | None, float | None]:
    """CMEギャップとN225変化率"""
    try:
        import yfinance as yf
        nkd = yf.download("NKD=F", period="5d", interval="1d", progress=False)
        n225 = yf.download("^N225", period="5d", interval="1d", progress=False)
        if isinstance(nkd.columns, pd.MultiIndex):
            nkd.columns = [c[0] for c in nkd.columns]
        if isinstance(n225.columns, pd.MultiIndex):
            n225.columns = [c[0] for c in n225.columns]
        if nkd.empty or n225.empty:
            return None, None

        nkd_close = float(nkd["Close"].iloc[-1])
        n225_close = float(n225["Close"].iloc[-1])
        gap = round((nkd_close - n225_close) / n225_close * 100, 2)

        n225_chg = None
        if len(n225) >= 2:
            n225_chg = round((float(n225["Close"].iloc[-1]) / float(n225["Close"].iloc[-2]) - 1) * 100, 2)

        return gap, n225_chg
    except Exception:
        return None, None


def main():
    # B4シグナル読み込み
    signals_files = sorted(GRANVILLE_DIR.glob("signals_*.parquet"))
    if not signals_files:
        print("No signals files found")
        return

    latest = pd.read_parquet(signals_files[-1])
    b4 = latest[latest["rule"] == "B4"] if "rule" in latest.columns else pd.DataFrame()
    sig_date = pd.to_datetime(latest["signal_date"].iloc[0]).strftime("%Y-%m-%d") if "signal_date" in latest.columns else "?"
    weekday = pd.to_datetime(sig_date).strftime("%a") if sig_date != "?" else ""

    # 市場環境
    vi = _load_vi()
    cme_gap, n225_chg = _load_cme_gap()

    # 除外ルール判定
    excluded = []
    if vi and cme_gap is not None:
        if (vi >= 30) and (vi < 40) and (cme_gap >= -1) and (cme_gap < 1):
            excluded.append("VI30-40×膠着")
        if (vi >= 30) and (vi < 40) and (cme_gap >= 1):
            excluded.append("VI30-40×GU")
    if n225_chg is not None and n225_chg < -3:
        excluded.append("N225<-3%")

    # 判定
    if excluded:
        decision = "🚫 回避"
        decision_emoji = "🚫"
    elif vi and vi >= 40:
        decision = "🟢 強エントリー"
        decision_emoji = "🟢"
    elif vi and vi >= 30:
        decision = "🟡 エントリー"
        decision_emoji = "🟡"
    elif vi and vi >= 25:
        decision = "🔵 検討"
        decision_emoji = "🔵"
    else:
        decision = "⚪ 待機"
        decision_emoji = "⚪"

    # 金曜警告
    is_friday = pd.to_datetime(sig_date).weekday() == 4 if sig_date != "?" else False

    # Slack Block生成
    blocks = []

    # ヘッダー
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*{decision_emoji} B4(-15%) エントリー判定* ({sig_date} {weekday})"
        }
    })

    # 市場環境
    vi_str = f"{vi:.1f}" if vi else "-"
    gap_str = f"{cme_gap:+.2f}%" if cme_gap is not None else "-"
    n225_str = f"{n225_chg:+.2f}%" if n225_chg is not None else "-"
    env_text = f"VI: *{vi_str}* | CME: *{gap_str}* | N225: *{n225_str}* | B4: *{len(b4)}件*"
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": env_text}
    })

    # 判定理由
    if excluded:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"⚠️ 除外ルール: {', '.join(excluded)}"}
        })

    if is_friday:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "⚠️ 金曜シグナル (PF1.89) — 週末リスク注意"}
        })

    # B4候補銘柄
    if len(b4) > 0 and not excluded:
        from scripts.lib.price_limit import calc_max_cost_100
        lines = []
        for _, r in b4.head(5).iterrows():
            tk = r.get("ticker", "").replace(".T", "")
            name = r.get("stock_name", "")[:8]
            dev = r.get("dev_from_sma20", 0)
            close = r.get("close", 0)
            cost = calc_max_cost_100(close)
            lines.append(f"`{tk}` {name} {dev:+.1f}% ¥{close:,.0f} (上限¥{cost:,})")

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "📋 *候補（乖離深い順）*\n" + "\n".join(lines)}
        })

    # 出力
    section_text = json.dumps(blocks, ensure_ascii=False)
    OUTPUT_PATH.write_text(section_text, encoding="utf-8")
    print(f"[OK] B4 Slack section: {OUTPUT_PATH} ({len(blocks)} blocks)")
    print(f"  Decision: {decision}")
    print(f"  B4 signals: {len(b4)}")


if __name__ == "__main__":
    main()
