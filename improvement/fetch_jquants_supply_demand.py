#!/usr/bin/env python3
"""
J-Quants API v2 需給関連データの取得・HTML出力

Standardプランで取得可能な需給データ:
- 信用取引週末残高 (weekly-margin-interest)
- 日々公表信用取引残高 (margin-transactions)
- 業種別空売り比率 (short-selling-by-sector)
- 空売り残高報告 (short-selling)
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from datetime import date, timedelta
from scripts.lib.jquants_client import JQuantsClient
import pandas as pd
import json


def get_sample_codes():
    """grok_trending.parquetからサンプル銘柄コードを取得"""
    parquet_path = project_root / "data/parquet/grok_trending.parquet"
    df = pd.read_parquet(parquet_path)
    # tickerから5桁コードを生成（7280.T → 72800）
    codes = df['ticker'].str.replace('.T', '0', regex=False).head(5).tolist()
    return codes


def fetch_supply_demand_data():
    """需給関連データを取得"""
    client = JQuantsClient()

    # grok_trendingからサンプル銘柄コードを取得
    sample_codes = get_sample_codes()
    sample_code = sample_codes[0]  # 最初の1銘柄
    print(f"サンプル銘柄コード: {sample_codes}")

    # 取得期間設定
    to_date = date.today()
    from_date = to_date - timedelta(days=30)
    sample_date = str(to_date - timedelta(days=7))  # 1週間前

    results = {}

    # V2 エンドポイント（V1からの変更）
    # /markets/weekly_margin_interest → /markets/margin-interest
    # /markets/daily_margin_interest → /markets/margin-alert
    # /markets/short_selling → /markets/short-ratio
    # /markets/short_selling_positions → /markets/short-sale-report

    # 1. 信用取引週末残高 (code指定が必須)
    print(f"[1/5] 信用取引週末残高を取得中... (code={sample_code})")
    try:
        data = client.request(
            "/markets/margin-interest",
            params={"code": sample_code, "from": str(from_date), "to": str(to_date)}
        )
        results["margin_interest"] = {
            "endpoint": "/markets/margin-interest",
            "description": f"信用取引週末残高（週次）銘柄: {sample_code}",
            "sample_data": data.get("data", [])[:5],
            "total_count": len(data.get("data", [])),
            "fields": list(data.get("data", [{}])[0].keys()) if data.get("data") else []
        }
    except Exception as e:
        results["margin_interest"] = {"error": str(e)}

    # 2. 日々公表信用取引残高 (date指定で試す)
    print(f"[2/5] 日々公表信用取引残高を取得中... (date={sample_date})")
    try:
        data = client.request(
            "/markets/margin-alert",
            params={"date": sample_date}
        )
        results["margin_alert"] = {
            "endpoint": "/markets/margin-alert",
            "description": f"日々公表信用取引残高（日次）date: {sample_date}",
            "sample_data": data.get("data", [])[:5],
            "total_count": len(data.get("data", [])),
            "fields": list(data.get("data", [{}])[0].keys()) if data.get("data") else []
        }
    except Exception as e:
        results["margin_alert"] = {"error": str(e)}

    # 3. 業種別空売り比率 (date指定で試す)
    print(f"[3/5] 業種別空売り比率を取得中... (date={sample_date})")
    try:
        data = client.request(
            "/markets/short-ratio",
            params={"date": sample_date}
        )
        results["short_ratio"] = {
            "endpoint": "/markets/short-ratio",
            "description": f"業種別空売り比率（日次）date: {sample_date}",
            "sample_data": data.get("data", [])[:5],
            "total_count": len(data.get("data", [])),
            "fields": list(data.get("data", [{}])[0].keys()) if data.get("data") else []
        }
    except Exception as e:
        results["short_ratio"] = {"error": str(e)}

    # 4. 空売り残高報告 (code指定)
    print(f"[4/5] 空売り残高報告を取得中... (code={sample_code})")
    try:
        data = client.request(
            "/markets/short-sale-report",
            params={"code": sample_code}
        )
        results["short_sale_report"] = {
            "endpoint": "/markets/short-sale-report",
            "description": f"空売り残高報告（銘柄別）code: {sample_code}",
            "sample_data": data.get("data", [])[:5],
            "total_count": len(data.get("data", [])),
            "fields": list(data.get("data", [{}])[0].keys()) if data.get("data") else []
        }
    except Exception as e:
        results["short_sale_report"] = {"error": str(e)}

    # 5. 売買内訳（Premiumのみ）
    print("[5/5] 売買内訳を取得中（Premiumのみ）...")
    try:
        data = client.request(
            "/markets/breakdown",
            params={"date": sample_date}
        )
        results["breakdown"] = {
            "endpoint": "/markets/breakdown",
            "description": "売買内訳データ（投資部門別）※Premiumのみ",
            "sample_data": data.get("data", [])[:5],
            "total_count": len(data.get("data", [])),
            "fields": list(data.get("data", [{}])[0].keys()) if data.get("data") else []
        }
    except Exception as e:
        results["breakdown"] = {"error": str(e), "note": "Premiumプランのみ利用可能"}

    return results


def generate_html(results: dict) -> str:
    """HTMLを生成"""
    html = """<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>J-Quants API v2 需給データ仕様</title>
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0d1117;
            color: #c9d1d9;
            padding: 20px;
            max-width: 1400px;
            margin: 0 auto;
        }
        h1 { color: #58a6ff; border-bottom: 1px solid #30363d; padding-bottom: 10px; }
        h2 { color: #8b949e; margin-top: 40px; }
        .api-section {
            background: #161b22;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
        }
        .endpoint {
            background: #21262d;
            padding: 8px 12px;
            border-radius: 4px;
            font-family: monospace;
            color: #7ee787;
            display: inline-block;
            margin-bottom: 10px;
        }
        .description { color: #8b949e; margin-bottom: 15px; }
        .fields-table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        .fields-table th, .fields-table td {
            border: 1px solid #30363d;
            padding: 10px;
            text-align: left;
        }
        .fields-table th {
            background: #21262d;
            color: #58a6ff;
        }
        .fields-table tr:nth-child(even) { background: #161b22; }
        .sample-data {
            background: #0d1117;
            padding: 15px;
            border-radius: 4px;
            overflow-x: auto;
            font-family: monospace;
            font-size: 12px;
            white-space: pre-wrap;
        }
        .error {
            background: #3d1f1f;
            border: 1px solid #f85149;
            color: #f85149;
            padding: 10px;
            border-radius: 4px;
        }
        .note {
            background: #1f2d1f;
            border: 1px solid #3fb950;
            color: #3fb950;
            padding: 10px;
            border-radius: 4px;
            margin-top: 10px;
        }
        .count { color: #8b949e; font-size: 14px; }
        .field-name { color: #ffa657; }
        .plan-badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 12px;
            margin-left: 10px;
        }
        .standard { background: #238636; color: white; }
        .premium { background: #a371f7; color: white; }
    </style>
</head>
<body>
    <h1>J-Quants API v2 需給関連データ仕様</h1>
    <p class="description">Standardプラン（3,300円/月）で取得可能な需給データの詳細</p>
"""

    plan_map = {
        "margin_interest": "standard",
        "margin_alert": "standard",
        "short_ratio": "standard",
        "short_sale_report": "standard",
        "breakdown": "premium"
    }

    for key, data in results.items():
        plan = plan_map.get(key, "standard")
        plan_label = "Standard" if plan == "standard" else "Premium"

        html += f"""
    <div class="api-section">
        <h2>{data.get('description', key)} <span class="plan-badge {plan}">{plan_label}</span></h2>
        <div class="endpoint">{data.get('endpoint', 'N/A')}</div>
"""

        if "error" in data:
            html += f"""
        <div class="error">エラー: {data['error']}</div>
"""
            if "note" in data:
                html += f"""
        <div class="note">{data['note']}</div>
"""
        else:
            html += f"""
        <p class="count">取得件数: {data.get('total_count', 0)}件（過去30日間）</p>

        <h3>フィールド一覧</h3>
        <table class="fields-table">
            <tr>
                <th>フィールド名</th>
                <th>サンプル値</th>
            </tr>
"""
            fields = data.get("fields", [])
            sample = data.get("sample_data", [{}])[0] if data.get("sample_data") else {}

            for field in fields:
                value = sample.get(field, "")
                html += f"""
            <tr>
                <td class="field-name">{field}</td>
                <td>{value}</td>
            </tr>
"""

            html += """
        </table>

        <h3>サンプルデータ（最大5件）</h3>
        <div class="sample-data">"""
            html += json.dumps(data.get("sample_data", []), indent=2, ensure_ascii=False)
            html += """</div>
"""

        html += """
    </div>
"""

    html += """
    <div class="api-section">
        <h2>需給分析での活用方法</h2>
        <table class="fields-table">
            <tr>
                <th>データ</th>
                <th>分析指標</th>
                <th>見方</th>
            </tr>
            <tr>
                <td>信用買残</td>
                <td>将来の売り圧力</td>
                <td>買残増加 → 将来の売り圧力上昇</td>
            </tr>
            <tr>
                <td>信用売残</td>
                <td>将来の買い圧力</td>
                <td>売残増加 → 将来の買い戻し需要</td>
            </tr>
            <tr>
                <td>貸借倍率（買残÷売残）</td>
                <td>需給バランス</td>
                <td>1未満: 売り優勢、1超: 買い優勢</td>
            </tr>
            <tr>
                <td>空売り比率</td>
                <td>下落圧力</td>
                <td>高い → 下落圧力大 / 逆張り買いシグナル</td>
            </tr>
            <tr>
                <td>空売り残高</td>
                <td>将来の買い戻し需要</td>
                <td>残高増加 → 将来の買い戻し需要</td>
            </tr>
        </table>
    </div>
</body>
</html>
"""
    return html


def main():
    print("=" * 60)
    print("J-Quants API v2 需給データ取得")
    print("=" * 60)

    results = fetch_supply_demand_data()

    html = generate_html(results)

    output_path = Path(__file__).parent / "output" / "jquants_supply_demand_spec.html"
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(html, encoding="utf-8")

    print(f"\n出力完了: {output_path}")
    print(f"ブラウザで開く: file://{output_path.resolve()}")


if __name__ == "__main__":
    main()
