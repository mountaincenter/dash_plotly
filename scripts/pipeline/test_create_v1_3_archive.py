#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path
import pandas as pd

# Add project root to Python path
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))


def consolidate_v1_3_archive():
    """Consolidate v1.3 test results into archive"""

    # Setup paths
    test_output_dir = project_root / "test_output"
    output_dir = project_root / "data" / "parquet" / "backtest"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find v1.3 files
    v1_3_files = sorted(test_output_dir.glob("v1.3_grok_trending_*.parquet"))

    if not v1_3_files:
        print("Error: No v1.3_grok_trending_*.parquet files found in test_output/")
        return

    print(f"Found {len(v1_3_files)} v1.3 files:")
    for f in v1_3_files:
        print(f"  - {f.name}")

    # Load all files
    dfs = []
    for file_path in v1_3_files:
        df = pd.read_parquet(file_path)
        print(f"\n{file_path.name}: {len(df)} stocks")

        # Show columns
        if "date" in df.columns:
            print(f"  date: {df['date'].unique()}")
        if "twitter_mentions" in df.columns:
            print(f"  twitter_mentions: {df['twitter_mentions'].min()}-{df['twitter_mentions'].max()}")
        if "previous_day_change_pct" in df.columns:
            print(f"  previous_day_change_pct: {df['previous_day_change_pct'].min():.2f}%-{df['previous_day_change_pct'].max():.2f}%")

        dfs.append(df)

    # Concatenate
    archive_df = pd.concat(dfs, ignore_index=True)

    # Sort by date
    if "date" in archive_df.columns:
        archive_df = archive_df.sort_values("date").reset_index(drop=True)

    # Save
    output_path = output_dir / "v1.3_grok_trending_archive.parquet"
    archive_df.to_parquet(output_path, index=False)

    print(f"\nArchive created:")
    print(f"  Path: {output_path}")
    print(f"  Total: {len(archive_df)} stocks")

    # Summary
    if "date" in archive_df.columns:
        print(f"\nStocks by date:")
        date_counts = archive_df.groupby("date").size()
        for date, count in date_counts.items():
            print(f"  {date}: {count} stocks")

    # Column list
    print(f"\nColumns ({len(archive_df.columns)}):")
    for col in archive_df.columns:
        print(f"  - {col}")

    return output_path


if __name__ == "__main__":
    print("=" * 60)
    print("v1.3 Archive Consolidation Script")
    print("=" * 60)
    print()

    output_path = consolidate_v1_3_archive()

    if output_path:
        print(f"\nCompleted successfully!")
        print(f"Next step: Jupyter notebook visualization")
