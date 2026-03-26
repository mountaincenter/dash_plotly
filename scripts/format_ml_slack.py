#!/usr/bin/env python3
"""
ML再学習結果をSlack Block形式にフォーマット
- Grade別ショート成績テーブル
- 前回比較（data/parquet/ml/grok_lgbm_meta.json vs models/grok_lgbm_meta.json）

出力: /tmp/ml_section.txt（先頭カンマ付きJSON blocks）
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMP_FILE = "/tmp/ml_section.txt"


def main():
    try:
        # 再学習後のメタ（models/に出力される）
        new_meta_path = ROOT / "models" / "grok_lgbm_meta.json"
        if not new_meta_path.exists():
            open(TEMP_FILE, "w").close()
            return 0

        with open(new_meta_path) as f:
            new_meta = json.load(f)

        nm = new_meta["metrics"]
        grades = nm.get("grade_analysis", [])

        # 前回のメタ（data/parquet/ml/に保存されている）
        prev_meta_path = ROOT / "data" / "parquet" / "ml" / "grok_lgbm_meta.json"
        prev_meta = None
        if prev_meta_path.exists():
            try:
                with open(prev_meta_path) as f:
                    prev_meta = json.load(f)
            except Exception:
                pass

        blocks = []

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "header",
            "text": {"type": "plain_text", "text": "🤖 ML Model Retraining"},
        })

        # Grade別テーブル
        table_text = "```\n"
        table_text += "Grade │ 件数 │ SHORT勝率 │ SHORT損益\n"
        table_text += "──────┼──────┼──────────┼────────────\n"
        for g in grades:
            wr = g["short_win_rate"] * 100
            pnl = g["short_pnl_total"]
            table_text += f"  {g['grade']}  │ {g['count']:>4} │   {wr:>5.1f}%  │ ¥{pnl:>10,.0f}\n"

        g12_wr = nm.get("g12_win_rate", 0) * 100
        g12_pnl = nm.get("g12_pnl_total", 0)
        g12_count = nm.get("g12_count", 0)
        table_text += "──────┼──────┼──────────┼────────────\n"
        table_text += f"G1+G2 │ {g12_count:>4} │   {g12_wr:>5.1f}%  │ ¥{g12_pnl:>10,.0f}\n"
        table_text += "```"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": table_text},
        })

        # モデル指標
        metrics_text = (
            f"*AUC:* `{nm['auc_mean']:.4f}` (±{nm['auc_std']:.4f})  "
            f"*Acc:* `{nm['accuracy_mean']:.4f}`  "
            f"*件数:* `{nm['total_evaluated']:,}`"
        )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": metrics_text},
        })

        # 前回比較
        if prev_meta:
            pm = prev_meta["metrics"]
            prev_g12_wr = pm.get("g12_win_rate", 0) * 100
            prev_g12_pnl = pm.get("g12_pnl_total", 0)
            prev_auc = pm.get("auc_mean", 0)
            prev_total = pm.get("total_evaluated", 0)

            wr_diff = g12_wr - prev_g12_wr
            auc_diff = nm["auc_mean"] - prev_auc
            total_diff = nm["total_evaluated"] - prev_total

            wr_emoji = "📈" if wr_diff > 0 else "📉" if wr_diff < 0 else "➡️"
            auc_emoji = "📈" if auc_diff > 0 else "📉" if auc_diff < 0 else "➡️"

            compare_text = (
                f"*前回比較:*\n"
                f"• G1+G2勝率: `{prev_g12_wr:.1f}%` → `{g12_wr:.1f}%` ({wr_diff:+.1f}pp) {wr_emoji}\n"
                f"• AUC: `{prev_auc:.4f}` → `{nm['auc_mean']:.4f}` ({auc_diff:+.4f}) {auc_emoji}\n"
                f"• 評価件数: `{prev_total:,}` → `{nm['total_evaluated']:,}` (+{total_diff})"
            )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": compare_text},
            })

        blocks.append({"type": "divider"})

        # 出力（先頭カンマ付き = 既存blocksに連結可能）
        with open(TEMP_FILE, "w", encoding="utf-8") as f:
            blocks_json = ",".join(
                json.dumps(b, ensure_ascii=False) for b in blocks
            )
            f.write("," + blocks_json)

        print(f"[OK] ML Slack section written to {TEMP_FILE}")
        return 0

    except Exception as e:
        print(f"[WARN] format_ml_slack.py failed: {e}", file=sys.stderr)
        open(TEMP_FILE, "w").close()
        return 0


if __name__ == "__main__":
    sys.exit(main())
