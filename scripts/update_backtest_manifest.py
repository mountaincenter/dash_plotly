"""
バックテストparquetファイルのmanifest.jsonを更新するスクリプト
"""

import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq

# データディレクトリ
DATA_DIR = Path(__file__).parent.parent / "data" / "parquet" / "backtest"

def generate_manifest():
    """manifest.jsonを生成"""
    print("=" * 80)
    print("バックテストmanifest.json生成スクリプト")
    print("=" * 80)

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "directory": "backtest",
        "description": "GROK AI株式選定システムのバックテストデータ（前場・全日メトリクス含む）",
        "total_files": 0,
        "total_records": 0,
        "files": {}
    }

    # parquetファイルをスキャン
    parquet_files = sorted(DATA_DIR.glob("*.parquet"))

    for file_path in parquet_files:
        print(f"\n処理中: {file_path.name}")

        try:
            # ファイル情報を取得
            df = pd.read_parquet(file_path)
            parquet_file = pq.ParquetFile(file_path)

            # 日付範囲を取得
            if 'backtest_date' in df.columns:
                df['backtest_date'] = pd.to_datetime(df['backtest_date'])
                date_range = {
                    "start": df['backtest_date'].min().strftime('%Y-%m-%d'),
                    "end": df['backtest_date'].max().strftime('%Y-%m-%d')
                }
            else:
                date_range = None

            # カラム情報を取得
            column_info = {}
            for col in df.columns:
                dtype_str = str(df[col].dtype)
                non_null_count = df[col].notna().sum()
                column_info[col] = {
                    "dtype": dtype_str,
                    "non_null_count": int(non_null_count),
                    "null_count": int(len(df) - non_null_count)
                }

            # ファイル情報を追加
            file_info = {
                "size_bytes": file_path.stat().st_size,
                "size_mb": round(file_path.stat().st_size / 1024 / 1024, 2),
                "record_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "column_info": column_info,
                "date_range": date_range,
                "last_modified": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
            }

            manifest["files"][file_path.name] = file_info
            manifest["total_records"] += len(df)

            print(f"  ✓ {len(df)} レコード, {len(df.columns)} カラム")

        except Exception as e:
            print(f"  ✗ エラー: {e}")
            continue

    manifest["total_files"] = len(manifest["files"])

    # manifest.jsonを保存
    manifest_path = DATA_DIR / "manifest.json"
    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 80)
    print(f"✓ manifest.json を生成しました: {manifest_path}")
    print("=" * 80)
    print(f"総ファイル数: {manifest['total_files']}")
    print(f"総レコード数: {manifest['total_records']}")
    print(f"\nファイル一覧:")
    for file_name, info in manifest["files"].items():
        print(f"  - {file_name}: {info['record_count']} レコード ({info['column_count']} カラム)")

if __name__ == "__main__":
    generate_manifest()
