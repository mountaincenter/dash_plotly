#!/usr/bin/env python3
"""
deep_analysis JSONファイルを2025-11-19の構造に統一する
"""
import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

def get_baseline_structure(baseline_file: Path) -> Dict:
    """ベースライン（2025-11-19）の構造を取得"""
    with open(baseline_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if 'stockAnalyses' in data and data['stockAnalyses']:
        return data['stockAnalyses'][0]
    return {}

def create_default_value(key: str, value: Any) -> Any:
    """欠損フィールドのデフォルト値を作成"""
    if isinstance(value, dict):
        return {}
    elif isinstance(value, list):
        return []
    elif isinstance(value, str):
        return ""
    elif isinstance(value, (int, float)):
        return 0 if isinstance(value, int) else 0.0
    elif isinstance(value, bool):
        return False
    else:
        return None

def deep_copy_structure(baseline_value: Any) -> Any:
    """ベースライン構造の深いコピーを作成（デフォルト値で）"""
    if isinstance(baseline_value, dict):
        return {k: deep_copy_structure(v) for k, v in baseline_value.items()}
    elif isinstance(baseline_value, list):
        if baseline_value and isinstance(baseline_value[0], dict):
            # リストの要素が辞書の場合は1つ目をテンプレートとして使用
            return []
        return []
    elif isinstance(baseline_value, str):
        return ""
    elif isinstance(baseline_value, bool):
        return False
    elif isinstance(baseline_value, int):
        return 0
    elif isinstance(baseline_value, float):
        return 0.0
    else:
        return None

def normalize_stock_data(stock: Dict, baseline: Dict) -> Dict:
    """1銘柄のデータをベースライン構造に統一"""
    normalized = {}

    # ベースラインの全フィールドを確保
    for key, baseline_value in baseline.items():
        if key in stock and stock[key]:
            if isinstance(baseline_value, dict):
                if isinstance(stock[key], dict):
                    # ネストした辞書は再帰的に処理
                    normalized[key] = normalize_stock_data(stock[key], baseline_value)
                else:
                    # 型が一致しない場合はベースライン構造で埋める
                    normalized[key] = deep_copy_structure(baseline_value)
            elif isinstance(baseline_value, list):
                # リストはそのまま使用
                normalized[key] = stock[key] if isinstance(stock[key], list) else []
            else:
                # 値をそのまま使用
                normalized[key] = stock[key]
        else:
            # 欠損フィールドまたは空の場合はベースライン構造で埋める
            normalized[key] = deep_copy_structure(baseline_value)

    return normalized

def normalize_file(input_file: Path, baseline: Dict, output_file: Path):
    """ファイル全体を正規化"""
    print(f"\nProcessing: {input_file.name}")

    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if 'stockAnalyses' not in data:
        print(f"  ❌ No stockAnalyses found")
        return

    original_count = len(data['stockAnalyses'])

    # 各銘柄を正規化
    normalized_stocks = []
    for stock in data['stockAnalyses']:
        normalized_stock = normalize_stock_data(stock, baseline)
        normalized_stocks.append(normalized_stock)

    data['stockAnalyses'] = normalized_stocks

    # バックアップ作成
    backup_file = input_file.with_suffix('.json.backup')
    if not backup_file.exists():
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(json.load(open(input_file, 'r', encoding='utf-8')), f, ensure_ascii=False, indent=2)
        print(f"  ✅ Backup created: {backup_file.name}")

    # 正規化したファイルを保存
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"  ✅ Normalized: {original_count} stocks")
    print(f"  ✅ Saved to: {output_file.name}")

def main():
    base_dir = Path('/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/data/parquet/backtest/analysis')

    # ベースライン構造を取得
    baseline_file = base_dir / 'deep_analysis_2025-11-19.json'
    baseline = get_baseline_structure(baseline_file)

    print("=" * 80)
    print("Normalizing Deep Analysis Files to 2025-11-19 Schema")
    print("=" * 80)
    print(f"Baseline: {baseline_file.name}")
    print(f"Baseline fields: {len(baseline)}")

    # 各ファイルを正規化
    files_to_normalize = [
        'deep_analysis_2025-11-17.json',
        'deep_analysis_2025-11-18.json'
    ]

    for filename in files_to_normalize:
        input_file = base_dir / filename
        if input_file.exists():
            output_file = input_file  # 上書き
            normalize_file(input_file, baseline, output_file)
        else:
            print(f"\n❌ File not found: {filename}")

    print("\n" + "=" * 80)
    print("Normalization Complete")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Review the normalized files")
    print("2. Run compare_deep_analysis_schema.py to verify")
    print("3. If OK, delete .backup files")

if __name__ == '__main__':
    main()
