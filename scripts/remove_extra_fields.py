#!/usr/bin/env python3
"""
ベースラインにない余分なフィールドを削除
"""
import json
from pathlib import Path
from typing import Dict, Set, Any

def get_all_keys(obj: Any, parent_key: str = '', sep: str = '.') -> Set[str]:
    """全てのキーパスを取得"""
    keys = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            keys.add(new_key)
            if isinstance(v, (dict, list)):
                keys.update(get_all_keys(v, new_key, sep))
    elif isinstance(obj, list) and obj:
        if isinstance(obj[0], (dict, list)):
            keys.update(get_all_keys(obj[0], f"{parent_key}[]", sep))
    return keys

def remove_keys_by_path(obj: Any, paths_to_remove: Set[str], current_path: str = '') -> Any:
    """指定されたパスのキーを削除"""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            full_path = f"{current_path}.{key}" if current_path else key

            # このキーを削除対象かチェック
            should_remove = False
            for path in paths_to_remove:
                if full_path == path or full_path.startswith(path + '.'):
                    should_remove = True
                    break

            if not should_remove:
                if isinstance(value, (dict, list)):
                    result[key] = remove_keys_by_path(value, paths_to_remove, full_path)
                else:
                    result[key] = value
        return result
    elif isinstance(obj, list):
        return [remove_keys_by_path(item, paths_to_remove, f"{current_path}[]") for item in obj]
    else:
        return obj

def main():
    base_dir = Path('/Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/data/parquet/backtest/analysis')

    # ベースラインのキーを取得
    baseline_file = base_dir / 'deep_analysis_2025-11-19.json'
    with open(baseline_file, 'r', encoding='utf-8') as f:
        baseline_data = json.load(f)

    baseline_keys = get_all_keys(baseline_data['stockAnalyses'][0])

    # 2025-11-17の余分なキーを削除
    target_file = base_dir / 'deep_analysis_2025-11-17.json'

    print("=" * 80)
    print("Removing Extra Fields from 2025-11-17")
    print("=" * 80)

    with open(target_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 余分なキーを特定
    target_keys = get_all_keys(data['stockAnalyses'][0])
    extra_keys = target_keys - baseline_keys

    print(f"\nExtra fields to remove: {len(extra_keys)}")
    for key in sorted(extra_keys):
        print(f"  - {key}")

    # 余分なキーを削除
    cleaned_stocks = []
    for stock in data['stockAnalyses']:
        cleaned_stock = remove_keys_by_path(stock, extra_keys)
        cleaned_stocks.append(cleaned_stock)

    data['stockAnalyses'] = cleaned_stocks

    # 保存
    with open(target_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Cleaned and saved: {target_file.name}")

    print("\n" + "=" * 80)
    print("Complete")
    print("=" * 80)

if __name__ == '__main__':
    main()
