#!/usr/bin/env python3
"""
GROK銘柄情報をSlack Block形式にフォーマット（改善版）
全銘柄を個別セクションブロックで表示
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

        # ブロックリストを作成
        blocks = []

        # 1. ヘッダーブロック
        blocks.append({
            "type": "divider"
        })
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📈 GROK銘柄更新"
            }
        })

        # 2. 集計情報
        time_counts = data.get("time_counts", {})
        fields = []
        for time in sorted(time_counts.keys()):
            count = time_counts[time]
            fields.append({
                "type": "mrkdwn",
                "text": f"*{time}更新:*\n{count}銘柄"
            })
        fields.append({
            "type": "mrkdwn",
            "text": f"*合計:*\n{data['total']}銘柄"
        })

        blocks.append({
            "type": "section",
            "fields": fields
        })

        blocks.append({
            "type": "divider"
        })

        # 3. 各銘柄を個別セクションで表示
        stocks = data.get("stocks", [])
        for i, stock in enumerate(stocks, 1):
            ticker = stock.get("ticker", "")
            name = stock.get("stock_name", "")
            tags = stock.get("tags", "")
            reason = stock.get("reason", "")
            time = stock.get("selected_time", "")

            # タグをインラインコード形式に、理由を引用ブロック形式に
            # タグの "+" を " + " に変換してスペースを追加
            formatted_tags = tags.replace("+", " + ") if tags else ""

            text = f"*{i}. {ticker} {name}* `[{time}]`\n"
            if formatted_tags:
                text += f"`{formatted_tags}`\n"
            if reason:
                text += f"> {reason}"

            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": text
                }
            })

        # 4. フッター
        blocks.append({
            "type": "divider"
        })
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "💡 セクション分割で読みやすく表示"
                }
            ]
        })

        # JSON配列として出力（先頭にカンマを付ける）
        with open(TEMP_FILE, "w", encoding="utf-8") as f:
            # 各ブロックをカンマ区切りで連結
            blocks_json = ",".join([json.dumps(block, ensure_ascii=False) for block in blocks])
            f.write("," + blocks_json)

        return 0

    except Exception as e:
        print(f"Error formatting GROK info: {e}", file=sys.stderr)
        open(TEMP_FILE, "w").close()
        return 0

if __name__ == "__main__":
    sys.exit(main())
