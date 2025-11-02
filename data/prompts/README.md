# Grok Prompt Version Management

GROKプロンプトのバージョン管理システム

## 概要

プロンプトを改善するたびに新しいバージョンとして保存し、過去のバージョンのバックテストデータと比較できるようにします。

## プロンプトの種類

このディレクトリでは2種類のプロンプトを管理しています:

1. **銘柄選定用（Grok Trending）** - `v1_0_baseline.py`, `v1_1_xxx.py` など
   - 目的: TOPIX500銘柄から注目銘柄を選定
   - 使用モデル: grok-beta
   - バックテスト: 過去データで精度検証

2. **市場サマリー用（Market Summary）** - `v1_2_market_summary.py` など
   - 目的: 東証大引け後に市場サマリーレポート生成
   - 使用モデル: grok-4-fast
   - データソース: J-Quants API + web_search()
   - 詳細: [MARKET_SUMMARY_SPEC.md](./MARKET_SUMMARY_SPEC.md) 参照

## ディレクトリ構造

```
data/prompts/
├── README.md                       # このファイル（プロンプト管理全般）
├── MARKET_SUMMARY_SPEC.md          # 市場サマリー仕様書
├── __init__.py
├── v1_0_baseline.py                # 銘柄選定: v1.0初期バージョン
├── v1_1_xxx.py                     # 銘柄選定: v1.1次のバージョン
├── v1_2_market_summary.py          # 市場サマリー: v1.2最新版
└── v2_0_xxx.py                     # メジャーアップデート
```

## 使い方

### 1. 新しいプロンプトバージョンの作成

#### 銘柄選定用（Grok Trending）

既存のバージョンをコピーして、新しいファイルを作成します：

```bash
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly/data/prompts
cp v1_0_baseline.py v1_1_enhanced_volatility.py
```

新しいファイル `v1_1_enhanced_volatility.py` を編集：
- ファイル冒頭のdocstringを更新（バージョン、説明、変更内容）
- `build_grok_prompt()` 関数内のプロンプトを修正

#### 市場サマリー用（Market Summary）

詳細は [MARKET_SUMMARY_SPEC.md](./MARKET_SUMMARY_SPEC.md) を参照してください。

```bash
# テスト実行
python3 scripts/pipeline/test_market_summary_v1_2.py

# 本番実行（実装予定）
python3 scripts/pipeline/generate_market_summary.py
```

### 2. バージョンを指定して実行

#### 銘柄選定用（Grok Trending）

環境変数 `PROMPT_VERSION` でバージョンを指定します：

```bash
# v1.0を使用（デフォルト）
python3 scripts/pipeline/generate_grok_trending.py

# v1.1を使用
PROMPT_VERSION=v1_1_enhanced_volatility python3 scripts/pipeline/generate_grok_trending.py

# v2.0を使用
PROMPT_VERSION=v2_0_sentiment_focus python3 scripts/pipeline/generate_grok_trending.py
```

#### 市場サマリー用（Market Summary）

バージョン管理は `v1_2_market_summary.py` 内の `get_prompt_metadata()` で行います。

```bash
# v1.2を使用（現在の最新版）
python3 scripts/pipeline/test_market_summary_v1_2.py
```

### 3. バージョン別にデータを蓄積

**重要**: `prompt_version` カラムが自動的に追加されます。

```bash
# v1.0で銘柄選定
python3 scripts/pipeline/generate_grok_trending.py

# バックテスト実行（prompt_version は自動的に引き継がれる）
python3 scripts/pipeline/save_backtest_to_archive.py

# v1.1で銘柄選定
PROMPT_VERSION=v1_1_enhanced_volatility python3 scripts/pipeline/generate_grok_trending.py

# バックテスト実行
python3 scripts/pipeline/save_backtest_to_archive.py
```

データは全て `grok_trending_archive.parquet` に追記され、`prompt_version` カラムでフィルタできます。

## バージョン命名規則

- **メジャーバージョン**: `vX_0_name` - プロンプトの大幅な変更
  - 例: `v1_0_baseline`, `v2_0_complete_redesign`

- **マイナーバージョン**: `vX_Y_name` - 部分的な改善・調整
  - 例: `v1_1_enhanced_volatility`, `v1_2_sentiment_focus`

- **name**: バージョンの特徴を表す簡潔な名前（スネークケース）
  - 例: `baseline`, `enhanced_volatility`, `sentiment_focus`

## プロンプトファイルの構造

各バージョンファイルは以下の構造に従います：

```python
"""
Grok Prompt vX.Y - Name
説明

Created: YYYY-MM-DD
Description:
    - 変更点1
    - 変更点2
    - 変更点3
"""

from typing import Any


def build_grok_prompt(context: dict[str, str], backtest: dict[str, Any]) -> str:
    """
    動的にGrokプロンプトを生成

    Args:
        context: get_trading_context() で取得したコンテキスト
        backtest: load_backtest_patterns() で取得したバックテストデータ

    Returns:
        str: Grok APIに送信するプロンプト
    """
    # プロンプト生成ロジック
    return f"""【タスク】..."""
```

## バージョン履歴

### 銘柄選定用（Grok Trending）

#### v1.0 - baseline (2025-10-31)
- 初期バージョン
- 時価総額50億円〜500億円
- ATR ≧ 3%
- バックテストフィードバック統合

#### v1.1 - enhanced_volatility (未実装)
- ボラティリティ重視
- ATR基準を5%に引き上げ
- 出来高急増の閾値を3倍に

#### v2.0 - sentiment_focus (未実装)
- センチメント重視
- 株クラバズの閾値を200件に
- プレミアムユーザー言及を必須に

### 市場サマリー用（Market Summary）

#### v1.2 - market_summary (2025-11-02)
- xAI SDK 1.3.1対応（web_search()使用）
- J-Quants API統合設計（v1.3で実装予定）
- 検索回数最適化（5-7回）
- 詳細: [MARKET_SUMMARY_SPEC.md](./MARKET_SUMMARY_SPEC.md)

## トラブルシューティング

### ModuleNotFoundError が出る

```bash
# PYTHONPATH を設定してから実行
cd /Users/hiroyukiyamanaka/Desktop/python_stock/dash_plotly
PYTHONPATH=. python3 scripts/pipeline/generate_grok_trending.py
```

### バージョンが見つからない

```
[ERROR] Failed to import prompt module: data.prompts.v1_1_xxx
[INFO] Falling back to v1_0_baseline
```

→ ファイル名が正しいか確認してください（アンダースコア区切り、`.py`拡張子）

## 今後の拡張

### 銘柄選定用
- [ ] バックテストデータをバージョン別に保存
- [ ] バージョン比較ダッシュボード
- [ ] versions.json によるメタデータ管理
- [ ] API経由でバージョン切り替え

### 市場サマリー用
- [ ] J-Quants API統合実装（v1.3）
- [ ] GitHub Actions自動実行（毎営業日16:30）
- [ ] S3保存機能
- [ ] x_search()統合（v2.0）
- [ ] グラフ生成機能（v2.0）

## 関連ドキュメント

- [MARKET_SUMMARY_SPEC.md](./MARKET_SUMMARY_SPEC.md) - 市場サマリー仕様書（詳細）
- [v1_2_market_summary.py](./v1_2_market_summary.py) - 市場サマリープロンプト実装
