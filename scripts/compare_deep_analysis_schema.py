#!/usr/bin/env python3
"""
deep_analysis JSONファイルの構造を比較し、フィールドの差異を特定する
"""
import json
from pathlib import Path
from typing import Dict, Set, Any

def flatten_keys(obj: Any, parent_key: str = '', sep: str = '.') -> Set[str]:
    """
    ネストされた辞書のキーをフラット化してセットで返す
    """
    keys = set()

    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            keys.add(new_key)
            if isinstance(v, (dict, list)):
                keys.update(flatten_keys(v, new_key, sep))
    elif isinstance(obj, list) and obj:
        # リストの最初の要素の構造を代表として使用
        if isinstance(obj[0], (dict, list)):
            keys.update(flatten_keys(obj[0], f"{parent_key}[]", sep))

    return keys

def analyze_json_structure(file_path: Path) -> Dict:
    """
    JSONファイルの構造を分析
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # stockAnalyses配列の最初の要素を分析
    if 'stockAnalyses' in data and data['stockAnalyses']:
        first_stock = data['stockAnalyses'][0]
        keys = flatten_keys(first_stock)

        return {
            'file': file_path.name,
            'total_stocks': len(data['stockAnalyses']),
            'field_count': len(keys),
            'fields': sorted(keys)
        }

    return None

def main():
    base_dir = Path('/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/data/parquet/backtest/analysis')

    files = [
        '2025-11-17',
        '2025-11-18',
        '2025-11-19'
    ]

    results = {}

    print("=" * 80)
    print("Deep Analysis Schema Comparison")
    print("=" * 80)

    for date in files:
        file_path = base_dir / f'deep_analysis_{date}.json'
        if file_path.exists():
            result = analyze_json_structure(file_path)
            if result:
                results[date] = result
                print(f"\n{date}:")
                print(f"  Total stocks: {result['total_stocks']}")
                print(f"  Field count: {result['field_count']}")

    # フィールドの差分を分析
    if len(results) >= 2:
        dates = sorted(results.keys())
        baseline = '2025-11-19'  # 95項目が正しいベースライン

        print("\n" + "=" * 80)
        print(f"Baseline: {baseline} ({results[baseline]['field_count']} fields)")
        print("=" * 80)

        baseline_fields = set(results[baseline]['fields'])

        for date in dates:
            if date == baseline:
                continue

            current_fields = set(results[date]['fields'])

            extra = current_fields - baseline_fields
            missing = baseline_fields - current_fields

            print(f"\n{date} vs {baseline}:")
            print(f"  Extra fields ({len(extra)}):")
            for field in sorted(extra):
                print(f"    + {field}")

            print(f"  Missing fields ({len(missing)}):")
            for field in sorted(missing):
                print(f"    - {field}")

    # 詳細なフィールドリストを出力
    print("\n" + "=" * 80)
    print("Complete field list for each date:")
    print("=" * 80)

    for date in sorted(results.keys()):
        output_file = base_dir / f'schema_{date}.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"Fields in {date} ({results[date]['field_count']} total):\n")
            f.write("\n".join(results[date]['fields']))
        print(f"\n{date}: {output_file}")

if __name__ == '__main__':
    main()
