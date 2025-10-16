#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
meta.parquetのスキーマ変更スクリプト
tag1 → categories (配列化、重複削除)
tag2/tag3 → tags (配列化)
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd
from common_cfg.paths import PARQUET_DIR

CURRENT_META = PARQUET_DIR / "meta.parquet"
BACKUP_META = PARQUET_DIR / "meta_backup.parquet"
NEW_META = PARQUET_DIR / "meta_new.parquet"


def migrate_meta_schema():
    """meta.parquetのスキーマを変更"""

    print("=" * 60)
    print("Meta.parquet Schema Migration")
    print("=" * 60)

    # 1. バックアップ作成
    print("\n[STEP 1] Creating backup...")
    if CURRENT_META.exists():
        import shutil
        shutil.copy2(CURRENT_META, BACKUP_META)
        print(f"  ✓ Backup created: {BACKUP_META}")
    else:
        print(f"  ✗ Original file not found: {CURRENT_META}")
        return 1

    # 2. データ読み込み
    print("\n[STEP 2] Loading current meta.parquet...")
    df = pd.read_parquet(CURRENT_META)
    print(f"  ✓ Loaded {len(df)} rows")
    print(f"  ✓ Columns: {list(df.columns)}")

    # 3. 重複削除とcategories配列化
    print("\n[STEP 3] Converting tag1 → categories (array)...")

    # tickerごとにtag1を集約
    grouped = df.groupby('ticker').agg({
        'code': 'first',
        'stock_name': 'first',
        'market': 'first',
        'sectors': 'first',
        'series': 'first',
        'topixnewindexseries': 'first',
        'tag1': lambda x: list(x.dropna().unique()),  # 配列化
        'tag2': 'first',  # 一旦firstで取得（後で配列化）
        'tag3': 'first',
    }).reset_index()

    print(f"  ✓ Deduplicated: {len(df)} rows → {len(grouped)} rows")
    print(f"  ✓ Removed {len(df) - len(grouped)} duplicate rows")

    # 4. tag2/tag3をtags配列に変換
    print("\n[STEP 4] Converting tag2/tag3 → tags (array)...")

    def merge_tags(row):
        """tag2とtag3を配列に統合"""
        tags = []
        if pd.notna(row['tag2']) and str(row['tag2']).strip():
            tags.append(str(row['tag2']).strip())
        if pd.notna(row['tag3']) and str(row['tag3']).strip():
            tags.append(str(row['tag3']).strip())
        return tags if tags else None

    grouped['tags'] = grouped.apply(merge_tags, axis=1)

    # 5. categoriesにリネーム
    print("\n[STEP 5] Renaming columns...")
    grouped = grouped.rename(columns={'tag1': 'categories'})

    # 6. 不要なカラム削除
    grouped = grouped.drop(columns=['tag2', 'tag3'])

    # 7. カラム順序整理
    column_order = [
        'ticker', 'code', 'stock_name', 'market', 'sectors',
        'series', 'topixnewindexseries', 'categories', 'tags'
    ]
    grouped = grouped[column_order]

    print(f"  ✓ Final columns: {list(grouped.columns)}")

    # 8. データ確認
    print("\n[STEP 6] Data validation...")
    print(f"  ✓ Total rows: {len(grouped)}")
    print(f"  ✓ Unique tickers: {grouped['ticker'].nunique()}")

    # categories分布
    print("\n  Categories distribution:")
    categories_flat = [cat for cats in grouped['categories'] if cats for cat in cats]
    from collections import Counter
    cat_counts = Counter(categories_flat)
    for cat, count in cat_counts.most_common():
        print(f"    - {cat}: {count} stocks")

    # tags分布（高市銘柄のみ）
    takaichi_rows = grouped[grouped['categories'].apply(lambda x: '高市銘柄' in x if x else False)]
    if not takaichi_rows.empty:
        print(f"\n  高市銘柄 tags distribution (total {len(takaichi_rows)} stocks):")
        tags_flat = [tag for tags in takaichi_rows['tags'].dropna() for tag in tags]
        tag_counts = Counter(tags_flat)
        for tag, count in tag_counts.most_common():
            print(f"    - {tag}: {count} stocks")

    # 9. サンプル表示
    print("\n[STEP 7] Sample data:")
    print("\n  CORE30 + 高市銘柄 の重複例:")
    both = grouped[grouped['categories'].apply(
        lambda x: 'TOPIX_CORE30' in x and '高市銘柄' in x if x else False
    )]
    if not both.empty:
        print(both[['ticker', 'stock_name', 'categories', 'tags']].head(3).to_string(index=False))

    print("\n  高市銘柄のみの例:")
    takaichi_only = grouped[grouped['categories'].apply(
        lambda x: '高市銘柄' in x and 'TOPIX_CORE30' not in x if x else False
    )]
    if not takaichi_only.empty:
        print(takaichi_only[['ticker', 'stock_name', 'categories', 'tags']].head(3).to_string(index=False))

    # 10. 保存
    print("\n[STEP 8] Saving new meta.parquet...")
    grouped.to_parquet(NEW_META, index=False)
    print(f"  ✓ Saved to: {NEW_META}")

    # 11. 確認プロンプト
    print("\n" + "=" * 60)
    print("Migration completed successfully!")
    print("=" * 60)
    print(f"\nBackup: {BACKUP_META}")
    print(f"New file: {NEW_META}")
    print("\nTo apply the changes, run:")
    print(f"  mv {NEW_META} {CURRENT_META}")
    print("\nTo rollback, run:")
    print(f"  mv {BACKUP_META} {CURRENT_META}")

    return 0


if __name__ == "__main__":
    raise SystemExit(migrate_meta_schema())
