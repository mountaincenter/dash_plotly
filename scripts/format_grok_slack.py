#!/usr/bin/env python3
"""
GROK銘柄情報をSlack Block形式にフォーマット
"""
import json
import sys

TEMP_FILE = "/tmp/grok_section.txt"

def main():
    if len(sys.argv) < 2:
        open(TEMP_FILE, "w").close()
        return 0

    grok_info_json = sys.argv[1]

    try:
        data = json.loads(grok_info_json)

        if not data or data.get("total", 0) == 0:
            open(TEMP_FILE, "w").close()
            return 0

        # ヘッダー
        lines = []
        lines.append("📈 *GROK銘柄更新*")
        lines.append("")

        # 時刻別集計
        time_counts = data.get("time_counts", {})
        for time, count in sorted(time_counts.items()):
            lines.append(f"  {time}更新: {count}銘柄")
        lines.append(f"  合計: {data['total']}銘柄")
        lines.append("")

        # 銘柄リスト（全銘柄）
        stocks = data.get("stocks", [])
        for i, stock in enumerate(stocks, 1):
            ticker = stock.get("ticker", "")
            name = stock.get("stock_name", "")
            tags = stock.get("tags", "")
            reason = stock.get("reason", "")
            time = stock.get("selected_time", "")

            lines.append(f"{i}. *{ticker}* {name} [{time}]")
            lines.append(f"   _{tags}_")
            lines.append(f"   {reason}")
            lines.append("")

        # JSON blockとして出力
        text = "\\n".join(lines)
        block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": text
            }
        }

        # ファイルに出力（先頭にカンマを付ける）
        with open(TEMP_FILE, "w", encoding="utf-8") as f:
            f.write("," + json.dumps(block, ensure_ascii=False))

        return 0

    except Exception as e:
        print(f"Error formatting GROK info: {e}", file=sys.stderr)
        open(TEMP_FILE, "w").close()
        return 0

if __name__ == "__main__":
    sys.exit(main())
