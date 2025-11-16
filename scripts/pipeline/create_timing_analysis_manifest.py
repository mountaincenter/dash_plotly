#!/usr/bin/env python3
"""
タイミング分析用のmanifest.jsonを生成
"""

import json
from datetime import datetime
from pathlib import Path

def create_manifest():
    timing_dir = Path("data/parquet/timing_analysis")
    html_file = timing_dir / "timing_analysis_report.html"

    if not html_file.exists():
        print(f"❌ Error: {html_file} not found")
        return False

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "directory": "timing_analysis",
        "description": "売買タイミング最適化分析レポート（前場終値 vs 大引値）",
        "files": {
            "timing_analysis_report.html": {
                "size_bytes": html_file.stat().st_size,
                "last_modified": datetime.fromtimestamp(html_file.stat().st_mtime).isoformat()
            }
        }
    }

    manifest_path = timing_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    print(f"  ✅ Created manifest.json")
    return True

if __name__ == "__main__":
    success = create_manifest()
    exit(0 if success else 1)
