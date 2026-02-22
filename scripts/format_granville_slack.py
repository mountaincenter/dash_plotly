#!/usr/bin/env python3
"""
グランビルIFD戦略の結果をSlack Block形式にフォーマット
- フィルター状態（uptrend / CI）
- 翌日エントリー候補（シグナル）
- バックテスト統計（2025/1~）

出力: /tmp/granville_section.txt（先頭カンマ付きJSON blocks）
"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = ROOT / "data" / "parquet"
TEMP_FILE = "/tmp/granville_section.txt"


def main():
    try:
        blocks = []

        # === 1. シグナル読み込み ===
        signals_path = PARQUET_DIR / "granville_ifd_signals.parquet"
        signals = pd.DataFrame()
        if signals_path.exists():
            signals = pd.read_parquet(signals_path)

        # === 2. フィルター状態 ===
        uptrend_str = "-"
        idx_path = PARQUET_DIR / "index_prices_max_1d.parquet"
        if idx_path.exists():
            try:
                idx = pd.read_parquet(idx_path)
                nk = idx[idx["ticker"] == "^N225"][["date", "Close"]].copy()
                nk["date"] = pd.to_datetime(nk["date"])
                nk = nk.sort_values("date")
                nk["sma20"] = nk["Close"].rolling(20).mean()
                latest = nk.dropna(subset=["sma20"]).iloc[-1]
                up = latest["Close"] > latest["sma20"]
                uptrend_str = (
                    f"{'○' if up else '×'} ¥{latest['Close']:,.0f} "
                    f"(SMA20 ¥{latest['sma20']:,.0f})"
                )
            except Exception:
                pass

        ci_str = "-"
        ci_path = ROOT / "improvement" / "data" / "macro" / "estat_ci_index.parquet"
        if ci_path.exists():
            try:
                ci = pd.read_parquet(ci_path)
                ci = ci[["date", "leading"]].sort_values("date")
                ci["chg3m"] = ci["leading"].diff(3)
                row = ci.dropna(subset=["chg3m"]).iloc[-1]
                expand = row["chg3m"] > 0
                ci_str = (
                    f"{'○' if expand else '×'} {row['leading']:.1f} "
                    f"(3ヶ月変化: {row['chg3m']:+.2f})"
                )
            except Exception:
                pass

        # === 3. バックテスト統計 (2025/1~) ===
        archive_str = ""
        archive_path = PARQUET_DIR / "backtest" / "granville_ifd_archive.parquet"
        if archive_path.exists():
            try:
                arc = pd.read_parquet(archive_path)
                if not arc.empty:
                    arc["entry_date"] = pd.to_datetime(arc["entry_date"])
                    recent = arc[arc["entry_date"] >= "2025-01-01"]
                    if not recent.empty:
                        n = len(recent)
                        pnl = int(recent["pnl_yen"].astype(float).sum())
                        wins = int((recent["ret_pct"].astype(float) > 0).sum())
                        sl = int((recent["exit_type"] == "SL").sum())
                        archive_str = (
                            f"{n}件 | {wins}勝{n - wins}敗 | "
                            f"¥{pnl:+,} | SL {sl}件"
                        )
            except Exception:
                pass

        # === ブロック構築 ===
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "header",
            "text": {"type": "plain_text", "text": "📊 グランビルIFD ロング戦略"},
        })

        # フィルター状態
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*市場トレンド:*\n{uptrend_str}"},
                {"type": "mrkdwn", "text": f"*CI先行指数:*\n{ci_str}"},
            ],
        })

        # シグナル
        if not signals.empty:
            sig_text = f"*翌日エントリー候補: {len(signals)}件*\n"
            for _, s in signals.iterrows():
                sig_text += (
                    f"`{s.get('ticker', '')}` {s.get('stock_name', '')} "
                    f"[{s.get('signal_type', '')}] "
                    f"¥{s.get('close', 0):,.0f} "
                    f"SL ¥{s.get('sl_price', 0):,.0f} "
                    f"({s.get('dev_from_sma20', 0):+.1f}%)\n"
                )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": sig_text.strip()},
            })
        else:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "_本日のシグナルはありません_"},
            })

        # バックテスト統計
        if archive_str:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*バックテスト (2025/1~):*\n{archive_str}",
                },
            })

        blocks.append({"type": "divider"})

        # 出力（先頭カンマ付き = 既存blocksに連結可能）
        with open(TEMP_FILE, "w", encoding="utf-8") as f:
            blocks_json = ",".join(
                json.dumps(b, ensure_ascii=False) for b in blocks
            )
            f.write("," + blocks_json)

        print(f"[OK] Granville Slack section written to {TEMP_FILE}")
        return 0

    except Exception as e:
        print(f"[WARN] format_granville_slack.py failed: {e}", file=sys.stderr)
        open(TEMP_FILE, "w").close()
        return 0


if __name__ == "__main__":
    sys.exit(main())
