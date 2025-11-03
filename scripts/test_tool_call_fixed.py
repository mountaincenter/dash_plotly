#!/usr/bin/env python3
# 最小修正: argumentsをstrからdictにパース

import json

class DummyToolCall:
    def __init__(self):
        self.id = "test_id"
        self.function = type('obj', (object,), {'name': 'web_search', 'arguments': '{"query": "test query", "num_results": 5}'})()  # str arguments

tool_call = DummyToolCall()

# 修正: パース処理
if isinstance(tool_call.function.arguments, str):
    try:
        parsed_args = json.loads(tool_call.function.arguments)
        tool_call.function.arguments = parsed_args  # dictに置き換え
        print(f"Parsed successfully: {parsed_args}")
    except json.JSONDecodeError as e:
        print(f"Parse error: {e}")
        tool_call.function.arguments = {}  # フォールバック
else:
    print("Arguments already dict")

# 安全にget()
try:
    query = tool_call.function.arguments.get('query', 'N/A')
    print(f"Fixed: Query = {query}")
except AttributeError as e:
    print(f"Still error: {e}")

print("Fixed test completed.")