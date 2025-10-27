#!/usr/bin/env python3
"""
run_pipeline_scalping_skip_add_grok.py
スキャルピング銘柄選定をスキップし、Grok銘柄を追加するデータパイプライン
GitHub Actionsとローカル開発の両方で使用

実行順序:
  1. create_meta_jquants  - J-Quants APIから全銘柄メタ情報取得
  2. generate_grok_trending - xAI Grok APIでトレンド銘柄選定
  3. create_all_stocks    - meta + grok統合
  4. fetch_prices         - yfinanceでall_stocks.parquetの価格データ取得
  5. update_manifest      - manifest.json生成・S3一括アップロード

注意:
  - generate_scalpingは実行されません（スキップ）
  - scalping_*.parquetは空のまま維持されます
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class PipelineRunner:
    """パイプライン実行管理クラス"""

    def __init__(self):
        # 環境変数でGrok生成をスキップするか判定
        skip_grok = os.getenv("SKIP_GROK_GENERATION", "false").lower() == "true"

        # 基本ステップ
        self.steps = [
            ("pipeline.create_meta_jquants", "Meta情報取得（J-Quants全銘柄）"),
        ]

        # Grok選定ステップ（23:00 JST実行時のみ）
        if not skip_grok:
            self.steps.append(
                ("pipeline.generate_grok_trending", "Grok銘柄選定（xAI API）")
            )

        # 共通ステップ
        self.steps.extend([
            ("pipeline.create_all_stocks", "銘柄統合（Meta + Grok）"),
            ("pipeline.fetch_prices", "価格データ取得（yfinance）"),
        ])

        # バックテストメタ情報生成（常に実行）
        # 16:00 JST: backtest/アーカイブから計算
        # 23:00 JST: 新規選定直後に空のメタを生成（翌日16:00で上書きされる）
        self.steps.append(
            ("pipeline.save_grok_backtest_meta", "Grokバックテストメタ情報生成")
        )

        # Manifest生成・S3アップロード
        self.steps.append(
            ("pipeline.update_manifest", "Manifest生成・S3アップロード")
        )

        self.skip_grok = skip_grok
        self.results: List[Tuple[str, bool, float, str]] = []

    def run_step(self, module_name: str, description: str) -> Tuple[bool, float, str]:
        """
        個別ステップを実行

        Returns:
            (成功/失敗, 実行時間(秒), エラーメッセージ)
        """
        print("\n" + "=" * 60)
        print(f"[STEP] {module_name}")
        print(f"[DESC] {description}")
        print("=" * 60)
        print(f"[DEBUG] Step started at {datetime.now().isoformat()}")

        start_time = time.time()

        try:
            # 動的インポート
            print(f"[DEBUG] Importing module: scripts.{module_name}")
            module = __import__(f"scripts.{module_name}", fromlist=["main"])
            print(f"[DEBUG] Module imported successfully")

            # main()を実行
            print(f"[DEBUG] Executing main() function...")
            exit_code = module.main()
            print(f"[DEBUG] main() returned exit code: {exit_code}")

            elapsed_time = time.time() - start_time

            if exit_code == 0:
                print(f"\n✅ SUCCESS: {module_name} ({elapsed_time:.2f}s)")
                return True, elapsed_time, ""
            else:
                error_msg = f"Exit code: {exit_code}"
                print(f"\n❌ FAILED: {module_name} - {error_msg}")
                return False, elapsed_time, error_msg

        except Exception as e:
            elapsed_time = time.time() - start_time
            error_msg = str(e)
            print(f"\n❌ EXCEPTION: {module_name} - {error_msg}")
            import traceback
            traceback.print_exc()
            return False, elapsed_time, error_msg

    def run(self) -> int:
        """パイプライン全体を実行"""
        print("=" * 60)
        print("Data Pipeline Execution (Scalping Skip + Grok)")
        print(f"Started at: {datetime.now().isoformat()}")
        print("=" * 60)
        print(f"[DEBUG] Total steps to run: {len(self.steps)}")
        print("[INFO] Scalping generation is SKIPPED in this pipeline")

        if self.skip_grok:
            print("[INFO] ⏭️  SKIP_GROK_GENERATION=true - Grok selection SKIPPED (16:00 JST mode)")
            print("[INFO] Using previous Grok trending data from yesterday's 23:00 selection")
            print("[INFO] Backtest meta will be updated from backtest/ archives")
        else:
            print("[INFO] ✅ SKIP_GROK_GENERATION=false - Grok selection ENABLED (23:00 JST mode)")
            print("[INFO] Grok trending stocks will be freshly selected")
            print("[INFO] Backtest meta will be generated (empty until tomorrow 16:00)")

        total_start = time.time()

        for idx, (module_name, description) in enumerate(self.steps, 1):
            print(f"\n[DEBUG] ===== Starting step {idx}/{len(self.steps)}: {module_name} =====")
            success, elapsed, error = self.run_step(module_name, description)
            self.results.append((module_name, success, elapsed, error))
            print(f"[DEBUG] Step {idx} result: success={success}, elapsed={elapsed:.2f}s")

            if not success:
                print(f"\n⚠️  Pipeline stopped at {module_name}")
                break

        total_elapsed = time.time() - total_start

        # 結果サマリー
        self.print_summary(total_elapsed)

        # 全ステップ成功なら0、失敗があれば1を返す
        all_success = all(success for _, success, _, _ in self.results)
        return 0 if all_success else 1

    def print_summary(self, total_time: float):
        """実行結果のサマリーを表示"""
        print("\n" + "=" * 60)
        print("Pipeline Execution Summary")
        print("=" * 60)

        for module_name, success, elapsed, error in self.results:
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"{status} | {module_name:35s} | {elapsed:6.2f}s")
            if error:
                print(f"         └─ Error: {error}")

        print("-" * 60)
        print(f"Total execution time: {total_time:.2f}s")
        print(f"Completed at: {datetime.now().isoformat()}")
        print("=" * 60)

        # 成功/失敗の統計
        success_count = sum(1 for _, success, _, _ in self.results if success)
        total_count = len(self.results)

        if success_count == total_count:
            print("\n🎉 All steps completed successfully!")
        else:
            print(f"\n⚠️  {total_count - success_count}/{total_count} step(s) failed")


def main() -> int:
    """エントリーポイント"""
    runner = PipelineRunner()
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
