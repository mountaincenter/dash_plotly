# V2スコアリング改善プロジェクト

**作成日**: 2025-11-20
**目的**: デイトレードで収益を上げるためのv2.0 → v2.1改善

---

## ディレクトリ構成

```
improvement/
├── README.md                                    ← このファイル
├── grok_analysis_schema_v2_requirements.md      ← v2要件定義書（コピー）
├── generate_trading_recommendation_v2_1.py      ← v2.1実装（作成予定）
└── backtest_v2_1.py                             ← v2.1検証スクリプト（作成予定）
```

---

## 現状

### v2.0の問題点

| 判断 | 勝率 | 問題 |
|------|------|------|
| 売り | 71.4% | ✅ 機能している |
| 買い | 39.0% | ❌ 機能していない |

### 原因

- v2スコアがGrokランク勝率を軽視
- ファンダメンタルズ（ROE、成長率）偏重
- テクニカル指標不足（RSI、出来高変化なし）

---

## v2.1改善内容

### 1. Grokランク勝率を最重視

```python
# 変更前
if backtest_win_rate > 0.70:
    score += 30

# 変更後
if backtest_win_rate > 0.60:
    score += 50  # 大幅に重み増加
```

### 2. RSI追加（売られすぎ検知）

```python
rsi_14d = calculate_rsi(ticker_df, period=14)
if rsi_14d < 30:
    score += 20
    reasons.append("RSI 30以下（売られすぎ）")
```

### 3. 出来高急増検知

```python
volume_change_20d = current_volume / volume_sma_20d
if volume_change_20d > 2.0:
    score += 15
    reasons.append("出来高2倍以上（注目急増）")
```

### 4. 5日線押し目買い

```python
price_vs_sma5_pct = (current_price - sma_5d) / sma_5d * 100
if -2.0 < price_vs_sma5_pct < 0:
    score += 15
    reasons.append("5日線押し目（短期反発）")
```

---

## 期待効果

| | v2.0 | v2.1目標 |
|---|------|---------|
| 売りシグナル | 71.4% | 70%以上（維持） |
| 買いシグナル | 39.0% | **50-55%** |

---

## 作業手順

### Step 1: v2.1実装（今日）

- [ ] `generate_trading_recommendation_v2_1.py` 作成
- [ ] 4カラム追加（RSI、出来高変化、SMA5、乖離率）
- [ ] スコアリングロジック修正

### Step 2: バックテスト検証（明日）

- [ ] `backtest_v2_1.py` 作成
- [ ] 過去11日間で買い勝率50%超を確認
- [ ] 売り勝率70%以上維持を確認

### Step 3: 実運用テスト（2-3日後）

- [ ] 2025-11-20のGrok推奨でv2.1実行
- [ ] シェアリングテクノロジー（Grok勝率63.6%）が上位に来るか確認
- [ ] 1週間の実績を記録

### Step 4: 本番環境へマージ（1週間後）

- [ ] v2.1が成功を確認
- [ ] `scripts/generate_trading_recommendation_v2.py` を更新
- [ ] 本番運用開始

---

## 実験記録

### 2025-11-20

#### 実施内容
- improvementディレクトリ作成
- v2要件定義書コピー
- README.md作成

#### 次のステップ
- v2.1実装開始

---

## 注意事項

1. **既存のv2.0は触らない**
   - `scripts/generate_trading_recommendation_v2.py` は保持
   - 成功したらマージ、失敗したら削除

2. **データソースは共通**
   - grok_trending.parquet
   - prices_max_1d.parquet
   - J-Quants API

3. **バックテストで検証必須**
   - 過去データで50%超を確認
   - 実運用前に必ず検証

4. **段階的に改善**
   - v2.1で成功 → v2.2へ
   - 失敗したら原因分析してやり直し
