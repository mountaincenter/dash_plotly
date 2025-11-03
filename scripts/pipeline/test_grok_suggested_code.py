#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from xai_sdk import Client
from xai_sdk.chat import system, user, tool  # ヘルパー関数をインポート（toolも追加）

# APIキーを環境変数から設定（.env.xaiから読み込み）
ENV_XAI_PATH = ROOT / ".env.xai"
config = dotenv_values(ENV_XAI_PATH)
if not config.get("XAI_API_KEY"):
    raise ValueError("XAI_API_KEY が .env.xai に設定されていません。https://x.ai/api で取得してください。")
os.environ["XAI_API_KEY"] = config.get("XAI_API_KEY")

# クライアント初期化
client = Client(api_key=os.environ.get("XAI_API_KEY"))

# ツール定義: web_search を使用して株価検索（xAI SDKのツール形式に準拠）
tools = [
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Webから最新の株価データを検索",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "検索クエリ（例: latest Nikkei 225 closing price）"},
                    "num_results": {"type": "integer", "description": "結果数（デフォルト10）"}
                },
                "required": ["query"]
            }
        }
    }
]

# クエリ: 最新の東証株価終値を取得（ヘルパー関数でmessages構築）
messages = [
    user("2025年11月2日現在の最新東証終値（日経平均株価とTOPIX）をweb_searchツールを使って取得し、終値と日付をJSON形式で出力せよ。市場休場時は前日分を明記。")
]

# チャットインスタンス作成（ツール呼び出し有効）
try:
    chat = client.chat.create(
        model="grok-4",  # Grok-4を使用（Premium+必要）
        messages=messages,  # ヘルパー関数で作成
        tools=tools,
        tool_choice="auto",  # モデルが自動でツールを選択
        max_tokens=500,
        temperature=0.1  # 低温度で正確性を重視
    )

    # ツール呼び出しループ: ツールが発生したら結果を追加して再実行（簡易推論ループ）
    full_response = ""
    while True:
        response = chat.sample()  # 応答生成（同期モード）
        full_response += response.content or ""
        
        # ツール呼び出しのチェックとハンドリング
        if hasattr(response, 'tool_calls') and response.tool_calls:
            tool_call = response.tool_calls[0]
            print(f"ツール呼び出し: {tool_call.function.name} with args: {tool_call.function.arguments}")
            # ツール結果をmessagesに追加（実際のAPIではサーバーサイド注入、例としてダミーJSON）
            tool_result = "{'nikkei': {'close': 52411.34, 'date': '2025-10-31'}, 'topix': {'close': 3332.0, 'date': '2025-10-31'}}"
            messages.append(
                tool(
                    content=tool_result,
                    tool_call_id=tool_call.id  # tool_call.id を指定
                )
            )
            chat.append(messages[-1])  # 最新ツールメッセージをchatに追加
            continue  # ループ継続で再サンプリング
        
        break  # ツールなしで終了

    print("取得結果:")
    print(full_response)  # JSON形式の出力例

except Exception as e:
    print(f"エラー: {e}")
    print("トラブルシューティング: SDKバージョンを確認（pip show xai-sdk）。ヘルパー関数を使用し、https://github.com/xai-org/xai-sdk-python を参照。")
    