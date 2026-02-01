#!/usr/bin/env python3
"""
需給 × パフォーマンス 相関分析（4区分対応）
/dev/analysis スタイルのHTML出力
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import datetime


def load_backtest_with_margin() -> pd.DataFrame:
    """マージ済みデータを読み込み、4区分カラムを追加（ショート視点で符号反転）"""
    parquet_path = project_root / "improvement/output/backtest_with_supply_demand.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(f"マージ済みデータがありません: {parquet_path}")

    df = pd.read_parquet(parquet_path)

    # 4区分カラム名を統一（profit_per_100_shares_* → me, p1, ae, p2）
    # me: 10:25, p1: 前場引け, ae: 14:45, p2: 大引け
    # ※ショート戦略なので符号を反転（ロング損益 × -1 = ショート損益）
    df["me"] = -1 * df.get("profit_per_100_shares_morning_early", 0).fillna(0)
    df["p1"] = -1 * df.get("profit_per_100_shares_phase1", 0).fillna(0)
    df["ae"] = -1 * df.get("profit_per_100_shares_afternoon_early", 0).fillna(0)
    df["p2"] = -1 * df.get("profit_per_100_shares_phase2", 0).fillna(0)

    return df


def analyze_by_margin_ratio(df: pd.DataFrame) -> dict:
    """貸借倍率別の分析"""
    df_with_margin = df[df["margin_sl_ratio"].notna()].copy()

    # 貸借倍率のビン分け
    bins = [0, 1, 3, 10, float("inf")]
    labels = ["<1 (売り優勢)", "1-3", "3-10", ">10 (買い優勢)"]
    df_with_margin["sl_ratio_bin"] = pd.cut(
        df_with_margin["margin_sl_ratio"],
        bins=bins,
        labels=labels,
        include_lowest=True
    )

    results = []
    for label in labels:
        subset = df_with_margin[df_with_margin["sl_ratio_bin"] == label]
        if len(subset) == 0:
            continue

        results.append({
            "label": label,
            "count": len(subset),
            "me": int(subset["me"].sum()),
            "p1": int(subset["p1"].sum()),
            "ae": int(subset["ae"].sum()),
            "p2": int(subset["p2"].sum()),
            "win_me": (subset["me"] > 0).mean() * 100,
            "win_p1": (subset["p1"] > 0).mean() * 100,
            "win_ae": (subset["ae"] > 0).mean() * 100,
            "win_p2": (subset["p2"] > 0).mean() * 100,
        })

    return {
        "total": len(df),
        "with_margin": len(df_with_margin),
        "results": results,
        "correlation": {
            "me": df_with_margin["margin_sl_ratio"].corr(df_with_margin["me"]),
            "p1": df_with_margin["margin_sl_ratio"].corr(df_with_margin["p1"]),
            "ae": df_with_margin["margin_sl_ratio"].corr(df_with_margin["ae"]),
            "p2": df_with_margin["margin_sl_ratio"].corr(df_with_margin["p2"]),
        }
    }


def format_profit(val: int) -> str:
    """損益フォーマット"""
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:,}"


def profit_class(val: int) -> str:
    """損益の色クラス"""
    if val > 0:
        return "text-emerald-400"
    elif val < 0:
        return "text-rose-400"
    return "text-foreground"


def winrate_class(rate: float) -> str:
    """勝率の色クラス"""
    if rate > 50:
        return "text-emerald-400"
    elif rate < 50:
        return "text-rose-400"
    return "text-foreground"


def get_quadrant_classes(me: int, p1: int, ae: int, p2: int) -> tuple:
    """4区分の色分けルール"""
    values = [me, p1, ae, p2]
    positives = [v for v in values if v > 0]
    negatives = [v for v in values if v < 0]

    WHITE = "text-foreground"
    GREEN = "text-emerald-400"
    RED = "text-rose-400"

    if len(positives) == 4:
        max_val = max(values)
        return tuple(GREEN if v == max_val else WHITE for v in values)

    if len(negatives) == 4:
        min_val = min(values)
        return tuple(RED if v == min_val else WHITE for v in values)

    if len(positives) == 0 and len(negatives) == 0:
        return (WHITE, WHITE, WHITE, WHITE)

    max_positive = max(positives) if positives else None
    min_negative = min(negatives) if negatives else None

    result = []
    for v in values:
        if v > 0 and v == max_positive:
            result.append(GREEN)
        elif v < 0 and v == min_negative:
            result.append(RED)
        else:
            result.append(WHITE)
    return tuple(result)


def generate_html(analysis: dict) -> str:
    """/dev/analysis スタイルのHTML生成"""

    # 全体合計
    total_me = sum(r["me"] for r in analysis["results"])
    total_p1 = sum(r["p1"] for r in analysis["results"])
    total_ae = sum(r["ae"] for r in analysis["results"])
    total_p2 = sum(r["p2"] for r in analysis["results"])

    me_c, p1_c, ae_c, p2_c = get_quadrant_classes(total_me, total_p1, total_ae, total_p2)

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>需給 × パフォーマンス分析（4区分）</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {{
            theme: {{
                extend: {{
                    colors: {{
                        background: '#0d1117',
                        foreground: '#c9d1d9',
                        card: '#161b22',
                        border: '#30363d',
                        muted: '#8b949e',
                        primary: '#58a6ff',
                    }}
                }}
            }}
        }}
    </script>
    <style>
        body {{ background: #0d1117; color: #c9d1d9; }}
        .text-foreground {{ color: #c9d1d9; }}
        .text-emerald-400 {{ color: #34d399; }}
        .text-rose-400 {{ color: #fb7185; }}
        .text-amber-400 {{ color: #fbbf24; }}
        .text-sky-400 {{ color: #38bdf8; }}
    </style>
</head>
<body class="min-h-screen p-4 md:p-6">
    <div class="max-w-7xl mx-auto">
        <!-- Header -->
        <header class="mb-6 pb-4 border-b border-gray-700">
            <h1 class="text-xl font-bold text-sky-400">需給 × パフォーマンス分析</h1>
            <p class="text-gray-500 text-sm mt-1">
                総件数: {analysis["total"]}件 / 需給データあり: {analysis["with_margin"]}件
                <span class="ml-2 text-amber-400">※ショート損益（符号反転済み）</span>
            </p>
        </header>

        <!-- Summary Cards -->
        <div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-6">
            <div class="rounded-xl border border-gray-700 bg-gray-800/50 p-4">
                <div class="text-gray-500 text-sm mb-1">需給データあり</div>
                <div class="text-2xl font-bold tabular-nums">{analysis["with_margin"]}</div>
            </div>
            <div class="rounded-xl border border-gray-700 bg-gray-800/50 p-4">
                <div class="text-gray-500 text-sm mb-1">10:25</div>
                <div class="text-2xl font-bold tabular-nums {me_c}">{format_profit(total_me)}円</div>
            </div>
            <div class="rounded-xl border border-gray-700 bg-gray-800/50 p-4">
                <div class="text-gray-500 text-sm mb-1">前場引け</div>
                <div class="text-2xl font-bold tabular-nums {p1_c}">{format_profit(total_p1)}円</div>
            </div>
            <div class="rounded-xl border border-gray-700 bg-gray-800/50 p-4">
                <div class="text-gray-500 text-sm mb-1">14:45</div>
                <div class="text-2xl font-bold tabular-nums {ae_c}">{format_profit(total_ae)}円</div>
            </div>
            <div class="rounded-xl border border-gray-700 bg-gray-800/50 p-4">
                <div class="text-gray-500 text-sm mb-1">大引け</div>
                <div class="text-2xl font-bold tabular-nums {p2_c}">{format_profit(total_p2)}円</div>
            </div>
        </div>

        <!-- 貸借倍率別 -->
        <div class="mb-6">
            <h2 class="text-base font-semibold mb-3">貸借倍率別パフォーマンス</h2>
            <div class="rounded-xl border border-gray-700 bg-gray-800/50 p-4">
                <p class="text-gray-500 text-sm mb-3">
                    貸借倍率 = 買残 ÷ 売残（損益はショート視点: +は利益、-は損失）<br>
                    <span class="text-amber-400">期待: 買い優勢（>10）→ 将来の売り圧力 → ショート有利、売り優勢（<1）→ 買い戻し → ショート不利</span>
                </p>
                <div class="overflow-x-auto">
                    <table class="w-full text-sm">
                        <thead>
                            <tr class="border-b border-gray-700">
                                <th class="py-2 text-left text-gray-500 font-medium">貸借倍率</th>
                                <th class="py-2 text-right text-gray-500 font-medium">件数</th>
                                <th class="py-2 text-right text-gray-500 font-medium">10:25</th>
                                <th class="py-2 text-right text-gray-500 font-medium">%</th>
                                <th class="py-2 text-right text-gray-500 font-medium">前場引</th>
                                <th class="py-2 text-right text-gray-500 font-medium">%</th>
                                <th class="py-2 text-right text-gray-500 font-medium">14:45</th>
                                <th class="py-2 text-right text-gray-500 font-medium">%</th>
                                <th class="py-2 text-right text-gray-500 font-medium">大引</th>
                                <th class="py-2 text-right text-gray-500 font-medium">%</th>
                            </tr>
                        </thead>
                        <tbody>'''

    for r in analysis["results"]:
        me_c, p1_c, ae_c, p2_c = get_quadrant_classes(r["me"], r["p1"], r["ae"], r["p2"])

        # ショート視点でのハイライト
        highlight = ""
        if ">10" in r["label"]:
            highlight = "bg-emerald-900/20"  # ショートに有利と期待
        elif "<1" in r["label"]:
            highlight = "bg-rose-900/20"  # ショートに不利と期待

        html += f'''
                            <tr class="border-b border-gray-700/50 {highlight}">
                                <td class="py-2.5 text-left">{r["label"]}</td>
                                <td class="py-2.5 text-right tabular-nums">{r["count"]}</td>
                                <td class="py-2.5 text-right tabular-nums {me_c}">{format_profit(r["me"])}</td>
                                <td class="py-2.5 text-right tabular-nums {winrate_class(r["win_me"])}">{r["win_me"]:.0f}%</td>
                                <td class="py-2.5 text-right tabular-nums {p1_c}">{format_profit(r["p1"])}</td>
                                <td class="py-2.5 text-right tabular-nums {winrate_class(r["win_p1"])}">{r["win_p1"]:.0f}%</td>
                                <td class="py-2.5 text-right tabular-nums {ae_c}">{format_profit(r["ae"])}</td>
                                <td class="py-2.5 text-right tabular-nums {winrate_class(r["win_ae"])}">{r["win_ae"]:.0f}%</td>
                                <td class="py-2.5 text-right tabular-nums {p2_c}">{format_profit(r["p2"])}</td>
                                <td class="py-2.5 text-right tabular-nums {winrate_class(r["win_p2"])}">{r["win_p2"]:.0f}%</td>
                            </tr>'''

    html += '''
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- 相関係数 -->
        <div class="mb-6">
            <h2 class="text-base font-semibold mb-3">相関係数（貸借倍率 vs 損益）</h2>
            <div class="rounded-xl border border-gray-700 bg-gray-800/50 p-4">
                <div class="grid grid-cols-2 md:grid-cols-4 gap-4">'''

    corr_labels = [("10:25", "me"), ("前場引け", "p1"), ("14:45", "ae"), ("大引け", "p2")]
    for label, key in corr_labels:
        corr = analysis["correlation"][key]
        corr_class = "text-emerald-400" if corr > 0 else "text-rose-400" if corr < 0 else "text-foreground"
        interpretation = "正の相関" if corr > 0.1 else "負の相関" if corr < -0.1 else "相関なし"
        html += f'''
                    <div class="text-center">
                        <div class="text-gray-500 text-sm">{label}</div>
                        <div class="text-xl font-bold tabular-nums {corr_class}">{corr:.3f}</div>
                        <div class="text-xs text-gray-500">{interpretation}</div>
                    </div>'''

    html += '''
                </div>
                <p class="text-gray-500 text-xs mt-4">
                    ※ショート戦略では正の相関が期待される（貸借倍率↑ = 買い優勢 → 将来の売り圧力 → 株価下落 → ショート勝利）
                </p>
            </div>
        </div>

        <!-- 解釈 -->
        <div class="mb-6">
            <h2 class="text-base font-semibold mb-3">分析の解釈</h2>
            <div class="rounded-xl border border-gray-700 bg-gray-800/50 p-4">
                <div class="grid md:grid-cols-2 gap-4">
                    <div>
                        <h3 class="font-semibold text-amber-400 mb-2">ショート戦略での期待</h3>
                        <ul class="text-sm text-gray-400 space-y-1">
                            <li>• 貸借倍率 > 10（買い優勢）→ 将来の売り圧力 → <span class="text-emerald-400">ショートに有利</span></li>
                            <li>• 貸借倍率 < 1（売り優勢）→ 買い戻し需要 → <span class="text-rose-400">ショートに不利</span></li>
                            <li>• 相関係数が正なら、需給分析が有効</li>
                        </ul>
                    </div>
                    <div>
                        <h3 class="font-semibold text-sky-400 mb-2">注意点</h3>
                        <ul class="text-sm text-gray-400 space-y-1">
                            <li>• J-Quantsの需給データは週次更新（タイムラグあり）</li>
                            <li>• サンプル数が少ない場合は統計的有意性に注意</li>
                            <li>• grok_trending銘柄は元々ボラティリティが高い</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>

        <!-- Footer -->
        <div class="text-center text-gray-500 text-xs mt-8">
            Generated: ''' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '''
        </div>
    </div>
</body>
</html>'''

    return html


def main():
    print("=" * 60)
    print("需給 × パフォーマンス分析（4区分対応）")
    print("=" * 60)

    print("[1/3] データ読み込み...")
    df = load_backtest_with_margin()
    print(f"  総件数: {len(df)}")

    print("[2/3] 貸借倍率別分析...")
    analysis = analyze_by_margin_ratio(df)
    print(f"  需給データあり: {analysis['with_margin']}件")

    print("[3/3] HTML生成...")
    html = generate_html(analysis)

    output_path = Path(__file__).parent / "output" / "supply_demand_4seg_analysis.html"
    output_path.write_text(html, encoding="utf-8")

    print(f"\n出力完了: {output_path}")
    print(f"ブラウザで開く: file://{output_path.resolve()}")


if __name__ == "__main__":
    main()
