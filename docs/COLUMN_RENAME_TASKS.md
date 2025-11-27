# カラム名統一作業一覧

## 概要
grok_trending.parquet（マスターデータ）のカラム名に統一する。

### 変更内容
- `company_name` → `stock_name`
- `category` → `categories`

---

## Phase 1: パイプラインスクリプト（本番稼働中・最優先）

### 1-1. 日次パイプライン（必須）
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| scripts/pipeline/update_grok_analysis_merged_v2_1.py | company_name, category | 完了 | L345-346, L488-489: `company_name`→`stock_name`, `category`→`categories` |
| scripts/pipeline/merge_json_to_grok_analysis.py | company_name, category | 完了 | L405, L431, L524-525: `company_name`→`stock_name`, `category`→`categories` |
| scripts/pipeline/merge_json_to_grok_analysis_v2.py | company_name, category | 完了 | L397, L424, L470-471: `company_name`→`stock_name`, `category`→`categories` |
| scripts/pipeline/generate_grok_trending.py | company_name, category | 完了 | L143-144, L158-159, L428-452: `company_name`→`stock_name_val`, `category`→`categories_val` |
| scripts/pipeline/generate_trading_recommendation_v2_1.py | company_name | 完了 | L409: `'company_name'`→`'stock_name'` |
| scripts/pipeline/save_backtest_to_archive.py | company_name, category | 完了 | L474-475: `company_name`→`stock_name`, `category`→`categories` |
| scripts/pipeline/extract_backtest_patterns.py | company_name, category | 完了 | L169,187,226,247,280,296-298,324,336-338: `company_name`→`stock_name`, `category`→`categories` |
| scripts/pipeline/save_grok_backtest_meta.py | company_name, category | 完了 | L401-402: `company_name`→`stock_name`, `category`→`categories` |
| scripts/pipeline/save_political_backtest_to_archive.py | company_name, category | 完了 | L92-93: `company_name`→`stock_name`, `category`→`categories` |

### 1-2. データファイル（parquet）
| ファイル | 変更内容 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| data/parquet/backtest/grok_analysis_merged.parquet | カラム名変更 | 完了 | pandas rename: `company_name`→`stock_name`, `category`→`categories` |
| data/parquet/backtest/grok_analysis_merged_v2_1.parquet | カラム名変更 | 完了 | pandas rename: `company_name`→`stock_name`, `category`→`categories` |

---

## Phase 2: サーバー/API

### 2-1. APIルーター
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| server/routers/dev_grok_analysis.py | company_name, category | 完了 | L182: `groupby('category')`→`groupby('categories')`, L195: `'category':`→`'categories':`, L473,615,900,966: `row['company_name']`→`row['stock_name']` |
| server/routers/dev_analyze.py | company_name | 完了 | L770,783: `row['company_name']`→`row['stock_name']` |
| server/routers/dev_backtest.py | company_name | 完了 | L567: `row.get("company_name")`→`row.get("stock_name")` |
| server/routers/dev_recommendations.py | company_name | 完了 | L62: `stock.get("company_name",...)`→`stock.get("stock_name",...)` |
| server/routers/dev_timing_analysis.py | company_name | 完了 | L43,48,49: `'company_name'`→`'stock_name'`, L87: `drop(columns=['company_name']`→`drop(columns=['stock_name']`, L383: `row.get('company_name',...)`→`row.get('stock_name',...)` |
| server/routers/dev_trading_recommendation.py | company_name | 完了 | L75: `stock.get("company_name",...)`→`stock.get("stock_name",...)` |
| server/utils.py | category | 対象外 | categoryはパラメータ名、カラム名ではない |

---

## Phase 3: フロントエンド（stock-frontend）

### 3-1. TSXコンポーネント
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| app/dev/grok-analysis-v2/page.tsx | companyName | 完了 | L92,161: `companyName:`→`stockName:`, L861,973: `{stock.companyName}`→`{stock.stockName}` |
| app/dev/grok-analysis/page.tsx | companyName | 完了 | L698,864: `{stock.companyName}`→`{stock.stockName}` |
| app/dev/timing-analysis/page.tsx | companyName | 完了 | L18: `companyName:`→`stockName:`, L533: `{stock.companyName}`→`{stock.stockName}` |
| app/dev/analyze/page.tsx | company_name, category | 完了 | L116,124: `company_name:`→`stock_name:`, L119,127: `category:`→`categories:`, L507: usage変更 |
| app/components/GrokBacktestBanner.tsx | company_name, category | 完了 | L15: `company_name:`→`stock_name:`, L18: `category:`→`categories:`, L214,260: usage変更 |

---

## Phase 4: 補助スクリプト

### 4-1. バックテスト・分析スクリプト
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| scripts/backtest_grok_with_morning.py | company_name, category | 完了 | L272-273: `'company_name'`→`'stock_name'`, `'category'`→`'categories'` |
| scripts/backtest/backtest_buy_signals.py | company_name | 完了 | L33,96,148: `company_name`→`stock_name` |
| scripts/generate_grok_analysis_html.py | company_name, category | 完了 | L112,654: groupby, L906,917,918: column refs |
| scripts/generate_timing_analysis_html.py | company_name | 完了 | L285,720: `company_name`→`stock_name` |
| scripts/generate_trade_strategy_report.py | company_name, category | 完了 | L44,102: groupby, L107: row ref |
| scripts/generate_factor_analysis_html.py | category | 対象外 | categoryカラム参照なし |
| scripts/analyze_grok_segments.py | category | 完了 | L139-140: `'category'`→`'categories'` |
| scripts/generate_trading_recommendation.py | category | 完了 | L121: groupby `'category'`→`'categories'` |
| scripts/validate_grok_result.py | company_name | 完了 | L223,226,246,295,307: `company_name`→`stock_name` |

### 4-2. ノートブック
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| notebooks/create_complete_analysis.py | category | 完了 | L19-30,76: `"category"`→`"categories"` |
| notebooks/create_unified_html.py | category | 完了 | L19-30,76: `"category"`→`"categories"` |
| notebooks/generate_plotly_html.py | category | 完了 | L19-30,76: `"category"`→`"categories"` |
| notebooks/generate_unified_report.py | category | 完了 | L19-30,76: `"category"`→`"categories"` |

### 4-3. その他
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| recommended_market_tickers.py | category | 対象外 | categoryカラム参照なし |

---

## Phase 5: プロンプト・改善スクリプト

### 5-1. プロンプトファイル
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| data/prompts/v1_0_baseline.py | company_name, category | 完了 | L44,48: `stock['category']`→`stock['categories']`, L133,135,143,145: JSON例 |
| data/prompts/v1_1_web_search.py | company_name, category | 完了 | L81,85: `stock['category']`→`stock['categories']`, L285,287,295,297: JSON例 |
| data/prompts/v1_2_quality_over_hype.py | company_name, category | 完了 | L264,266,274,276: JSON例 |
| data/prompts/v1_3_zero_label.py | company_name | 完了 | L309,318: JSON例 |

### 5-2. 改善スクリプト（アクティブ）
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| improvement/scripts/fill_17_columns.py | company_name | 完了 | L275: `'company_name'`→`'stock_name'` |
| improvement/scripts/generate_complete_v2_0_3_comparison.py | company_name | 完了 | L776: `row.get('company_name')`→`row.get('stock_name')` |
| improvement/scripts/generate_trading_recommendation_v2_1.py | company_name | 完了 | L385: `'company_name':`→`'stock_name':` |

---

## Phase 6: アーカイブ・テストファイル（低優先度）

### 6-1. アーカイブスクリプト
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| improvement/scripts/archive/analyze_prev_day_trend.py | - | 対象外 | 既にstock_name使用 |
| improvement/scripts/archive/create_grok_analysis_base_latest.py | - | 対象外 | 既にstock_name/categories使用 |
| improvement/scripts/archive/generate_v2_0_3_hybrid.py | - | 対象外 | 既にstock_name使用 |
| improvement/scripts/archive/generate_v2_1_0_1_backtest_report.py | - | 対象外 | 既にstock_name使用 |
| improvement/scripts/archive/generate_v2_1_0_2_backtest_report.py | - | 対象外 | 既にstock_name使用 |
| improvement/scripts/archive/generate_v2_1_0_html_report.py | - | 対象外 | 既にstock_name使用 |
| improvement/scripts/archive/generate_v2_1_1_backtest_report.py | - | 対象外 | 既にstock_name使用 |
| improvement/scripts/archive/simulate_stop_loss_v2_1_0_2.py | - | 対象外 | 既にstock_name使用 |
| improvement/scripts/archive/update_hold_to_sell_analysis.py | - | 対象外 | 既にstock_name使用 |

### 6-2. テストスクリプト
| ファイル | 変更箇所 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| scripts/pipeline/test_generate_anthropic_trending.py | - | 対象外 | 既にstock_name使用 |
| scripts/pipeline/test_generate_grok_trending.py | - | 対象外 | 既にstock_name/categories使用 |
| scripts/pipeline/test_save_backtest_archive.py | - | 対象外 | 既にstock_name/categories使用 |
| scripts/pipeline/test_save_backtest_archive_fixed.py | - | 対象外 | 既にstock_name/categories使用 |
| scripts/test_create_grok_analysis_base_20251107.py | - | 対象外 | 既にstock_name/categories使用 |
| scripts/test_create_grok_analysis_base_20251107_v2.py | - | 対象外 | 既にstock_name/categories使用 |
| scripts/test_create_grok_analysis_base_20251107_v3.py | - | 対象外 | 既にstock_name/categories使用 |

---

## Phase 7: JSONデータファイル

### 7-1. trading_recommendation系
| ファイル | 変更内容 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| improvement/trading_recommendation.json | - | 対象外 | 既にstock_name使用 |
| improvement/data/trading_recommendation.json | - | 対象外 | 既にstock_name使用 |
| improvement/archive/trading_recommendation.json | - | 対象外 | 既にstock_name使用 |

### 7-2. テストデータ（履歴）
| ファイル | 変更内容 | 状態 | 修正詳細 |
|---------|---------|------|---------|
| data/test_output/grok_trending_20251029.json | - | 対象外 | 既にstock_name/categories使用 |
| data/test_output/grok_trending_20251030.json | - | 対象外 | 既にstock_name/categories使用 |
| data/test_output/grok_trending_20251031.json | - | 対象外 | 既にstock_name/categories使用 |
| data/test_output/grok_trending_20251104.json | - | 対象外 | 既にstock_name/categories使用 |
| data/test_output/grok_trending_add_x_20251028.json | company_name, category | 未着手 | 旧形式の履歴データ |
| data/test_output/grok_trending_add_x_20251029.json | company_name, category | 未着手 | 旧形式の履歴データ |
| data/test_output/grok_trending_add_x_20251030.json | company_name, category | 未着手 | 旧形式の履歴データ |
| data/test_output/grok_trending_202510312300.json | company_name, category | 未着手 | 旧形式の履歴データ |

---

## 作業順序

1. **Phase 1-1**: パイプラインスクリプト修正 → テスト → コミット
2. **Phase 1-2**: parquetファイルのカラム名変更スクリプト作成・実行
3. **Phase 2**: APIルーター修正 → テスト → コミット
4. **Phase 3**: フロントエンド修正 → テスト → コミット
5. **Phase 4-7**: 残りのファイル修正

---

## 合計ファイル数

| Phase | 対象ファイル数 | 状態 |
|-------|--------------|------|
| Phase 1 | 11 | 完了 |
| Phase 2 | 6 | 完了 |
| Phase 3 | 5 | 完了 |
| Phase 4 | 12 | 完了 |
| Phase 5 | 7 | 完了 |
| Phase 6 | 0（全て対象外） | 完了 |
| Phase 7 | 4（履歴データのみ） | 未着手 |
| **合計** | **45ファイル完了、4ファイル未着手** |

---

## 注意事項

- API レスポンスの `companyName` → `stockName` 変更はフロントエンドとの整合性が必要
- 既存parquetファイルのカラム名変更にはマイグレーションスクリプトが必要
- Phase 6（アーカイブ）は動作確認不要だが、将来の参照時のために修正推奨
