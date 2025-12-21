# スコアリング戦略 v2.0.3 / v2.1 / v2.2 比較仕様書

**作成日**: 2025-11-28
**バージョン**: 1.0.0
**目的**: 信用取引データを活用した売買判定精度向上

---

## 1. バージョン概要

| バージョン | リリース日 | 主な特徴 | 目標勝率 |
|-----------|-----------|---------|---------|
| **v2.0.3** | 2025-11-17 | ROE・営業利益成長重視 | 売り70% / 買い40% |
| **v2.1** | 2025-11-21 | Grok勝率重視 + RSI/出来高 | 売り70% / 買い50% |
| **v2.2** | TBD | 信用取引データ + 規制銘柄検知 | 売り75% / 買い55% |

---

## 2. 使用カラム比較

### 2.1 入力カラム一覧

| # | カラム名 | v2.0.3 | v2.1 | v2.2 | データソース |
|---|---------|:------:|:----:|:----:|-------------|
| 1 | `grok_rank` | ✅ | ✅ | ✅ | grok_trending.parquet |
| 2 | `backtest_win_rate` | ✅ | ✅ | ✅ | grok_analysis_base |
| 3 | `roe` | ✅ | ✅ | ✅ | J-Quants /fins/statements |
| 4 | `operating_profit_growth` | ✅ | ✅ | ✅ | J-Quants /fins/statements |
| 5 | `daily_change_pct` | ✅ | ✅ | ✅ | prices_max_1d.parquet |
| 6 | `atr_pct` | ✅ | ✅ | ✅ | prices_max_1d.parquet |
| 7 | `current_price` | ✅ | ✅ | ✅ | prices_max_1d.parquet |
| 8 | `ma25` | ✅ | ✅ | ✅ | prices_max_1d.parquet |
| 9 | `rsi_14d` | - | ✅ | ✅ | prices_max_1d 計算 |
| 10 | `volume_change_20d` | - | ✅ | ✅ | prices_max_1d 計算 |
| 11 | `sma_5d` | - | ✅ | ✅ | prices_max_1d 計算 |
| 12 | `price_vs_sma5_pct` | - | ✅ | ✅ | prices_max_1d 計算 |
| 13 | `margin_balance_ratio` | - | - | ✅ | J-Quants /markets/weekly_margin_interest |
| 14 | `margin_balance_change_pct` | - | - | ✅ | J-Quants /markets/weekly_margin_interest |
| 15 | `sector_short_ratio` | - | - | ✅ | J-Quants /markets/short_selling |
| 16 | `issue_type` | - | - | ✅ | J-Quants /markets/weekly_margin_interest |
| 17 | `is_restricted` | - | - | ✅ | J-Quants /markets/daily_margin_interest |
| 18 | `regulation_class` | - | - | ✅ | J-Quants /markets/daily_margin_interest |

**カラム数**: v2.0.3: 8 → v2.1: 12 (+4) → v2.2: 18 (+6)

---

## 3. v2.2 追加カラム詳細

### 3.1 信用取引残高データ

| カラム名 | 計算式 | 説明 |
|---------|--------|------|
| `margin_balance_ratio` | 買い残 / 売り残 | 信用倍率。高いほど将来の売り圧力 |
| `margin_balance_change_pct` | (今週買い残 - 前週買い残) / 前週買い残 × 100 | 買い残増減率 |

**データソース**: `/markets/weekly_margin_interest`

```python
# 取得例
response = client.request("/markets/weekly_margin_interest", params={"code": "4570"})
latest = response["weekly_margin_interest"][-1]

margin_balance_ratio = latest["LongMarginTradeVolume"] / latest["ShortMarginTradeVolume"]
```

### 3.2 セクター空売り比率

| カラム名 | 計算式 | 説明 |
|---------|--------|------|
| `sector_short_ratio` | (規制空売り + 非規制空売り) / 総売り × 100 | セクター全体の空売り比率(%) |

**データソース**: `/markets/short_selling` + `/listed/info`

```python
# 取得例
# 1. 銘柄のSector33Code取得
listed = client.request("/listed/info")
sector_code = get_sector_code(ticker, listed)

# 2. セクター空売り比率取得
short = client.request("/markets/short_selling", params={"date": "2025-11-27"})
sector_data = find_by_sector(sector_code, short)
sector_short_ratio = (
    sector_data["ShortSellingWithRestrictionsTurnoverValue"] +
    sector_data["ShortSellingWithoutRestrictionsTurnoverValue"]
) / total_selling * 100
```

### 3.3 銘柄区分・規制情報

| カラム名 | 値 | 説明 |
|---------|-----|------|
| `issue_type` | "1": 信用銘柄, "2": 貸借銘柄, "3": その他 | 空売り可否の判定 |
| `is_restricted` | True/False | 取引規制銘柄かどうか |
| `regulation_class` | "001"〜"102" | 東証規制区分コード |

**規制区分コード**:
| コード | 意味 | 対応 |
|-------|------|------|
| 001 | 日証金注意喚起/申込制限 | 警告表示 |
| 002 | 日々公表銘柄 | 注意表示 |
| 003-006 | 規制第1〜4弾 | **取引禁止** |
| 101 | 規制解除銘柄 | 通常 |
| 102 | 監視銘柄 | 警告表示 |

---

## 4. スコアリングロジック比較

### 4.1 v2.0.3 ロジック（8カラム）

```python
def calculate_score_v2_0_3(data):
    score = 0

    # 1. Grokランク基礎スコア
    if grok_rank <= 2:
        score += 40
    elif grok_rank <= 5:
        score += 20
    elif grok_rank >= 8:
        score -= 10

    # 2. バックテスト勝率補正
    if backtest_win_rate >= 0.70:
        score += 30
    elif backtest_win_rate <= 0.30:
        score -= 20

    # 3. ファンダメンタルズ
    if roe > 15:
        score += 20
    elif roe < 0:
        score -= 15

    if operating_profit_growth > 50:
        score += 25
    elif operating_profit_growth < -30:
        score -= 20

    # 4. テクニカル
    if daily_change_pct < -3:
        score += 15  # リバウンド期待
    elif daily_change_pct > 10:
        score -= 10  # 過熱感

    if atr_pct < 3.0:
        score += 10
    elif atr_pct > 8.0:
        score -= 15

    # 5. 移動平均乖離
    ma25_deviation = (current_price - ma25) / ma25 * 100
    if ma25_deviation > 5:
        score -= 10
    elif ma25_deviation < -5:
        score += 10

    return score
```

### 4.2 v2.1 ロジック（12カラム）

```python
def calculate_score_v2_1(data):
    score = calculate_score_v2_0_3(data)  # 基本ロジック継承

    # 追加1: Grok勝率をより重視
    if backtest_win_rate > 0.60:
        score += 20  # 追加ボーナス

    # 追加2: RSI
    if rsi_14d < 30:
        score += 20
        reasons.append("RSI 30以下（売られすぎ）")
    elif rsi_14d > 70:
        score -= 10
        reasons.append("RSI 70以上（買われすぎ）")

    # 追加3: 出来高急増
    if volume_change_20d > 2.0:
        score += 15
        reasons.append(f"出来高{volume_change_20d:.1f}倍（注目急増）")

    # 追加4: 5日線押し目
    if -2.0 < price_vs_sma5_pct < 0:
        score += 15
        reasons.append("5日線押し目")

    return score
```

### 4.3 v2.2 ロジック（18カラム）

```python
def calculate_score_v2_2(data):
    score = calculate_score_v2_1(data)  # v2.1ロジック継承

    # === 規制チェック（最優先） ===
    if is_restricted or regulation_class in ["003", "004", "005", "006"]:
        return None  # 取引対象外
        flags.append("RESTRICTED")

    if regulation_class == "002":
        flags.append("DAILY_PUBLICATION")  # 日々公表銘柄

    if regulation_class == "001":
        flags.append("JSF_CAUTION")  # 日証金注意喚起

    # === 銘柄区分チェック ===
    if issue_type == "1":  # 信用銘柄（空売り不可）
        flags.append("NO_SHORT")
        if score < 0:  # 売りシグナルだが空売り不可
            reasons.append("空売り不可銘柄のため静観")
            return 0

    # === 信用倍率 ===
    if margin_balance_ratio < 1.5:
        score -= 15
        reasons.append(f"信用倍率{margin_balance_ratio:.1f}倍（売り圧力大）")
    elif margin_balance_ratio > 5.0:
        score += 10
        reasons.append(f"信用倍率{margin_balance_ratio:.1f}倍（踏み上げ期待）")

    # === 買い残急増 ===
    if margin_balance_change_pct > 30:
        score -= 10
        reasons.append(f"買い残+{margin_balance_change_pct:.0f}%（将来売り圧力）")
    elif margin_balance_change_pct < -20:
        score += 5
        reasons.append(f"買い残{margin_balance_change_pct:.0f}%（需給改善）")

    # === セクター空売り比率 ===
    if sector_short_ratio >= 45:
        score -= 15
        reasons.append(f"セクター空売り{sector_short_ratio:.0f}%（売り圧力大）")
    elif sector_short_ratio >= 40:
        score -= 5
        reasons.append(f"セクター空売り{sector_short_ratio:.0f}%（やや注意）")
    elif sector_short_ratio <= 30:
        score += 5
        reasons.append(f"セクター空売り{sector_short_ratio:.0f}%（需給良好）")

    return score
```

---

## 5. 判定閾値

### 5.1 スコアベース閾値

| 判定 | v2.0.3 | v2.1 | v2.2 |
|------|--------|------|------|
| 買い（高） | score >= 40 | - | - |
| 買い（中） | score >= 20 | score >= 30 | score >= 30 (継承) |
| 静観 | -15 < score < 20 | -20 < score < 30 | -20 < score < 30 (継承) |
| 売り（中） | score <= -15 | score <= -20 | score <= -20 (継承) |
| 売り（強） | score <= -30 | - | - |
| **取引禁止** | - | - | is_restricted = True |

※ v2.2はv2.1の閾値を継承。変更点は信用取引データによるスコア加減点のみ。

### 5.2 v2.1 追加ルール（2階建てアーキテクチャ）

```
優先度1: 価格帯強制判定（最優先）
  - 10,000円以上 → 売り強制
  - 5,000-10,000円 → 買い強制

優先度2: 2段階変化阻止
  - v2.0.3が買い → v2.1で売りにならない

優先度3: v2.0.3売り判定の保持
  - v2.0.3が売り → v2.1も売り維持
```

### 5.3 v2.2 追加ルール

```
優先度0: 規制銘柄チェック（最優先）
  - 規制第1-4弾（003-006） → 取引対象外
  - 日証金注意喚起（001） → 警告フラグ

優先度1: 空売り不可チェック
  - 信用銘柄（issue_type=1）かつ売りシグナル → 静観に変更
```

---

## 6. 出力フォーマット

### 6.1 trading_recommendation.json 拡張

```json
{
  "ticker": "4570.T",
  "company_name": "免疫生物研究所",
  "grok_rank": 4,

  "v2_0_3_action": "買い",
  "v2_0_3_score": 30,
  "v2_0_3_reasons": "Grokランク4/10（過去勝率18.2%）...",

  "v2_1_action": "買い",
  "v2_1_score": 40,
  "v2_1_reasons": ["Grokランク上位50%", "RSI 97.5（買われすぎ）"],

  "v2_2_action": "静観",
  "v2_2_score": 25,
  "v2_2_reasons": [
    "Grokランク上位50%",
    "RSI 97.5（買われすぎ）",
    "セクター空売り31.9%（需給良好）"
  ],
  "v2_2_flags": ["DAILY_PUBLICATION"],

  "margin_data": {
    "margin_balance_ratio": 32.76,
    "margin_balance_change_pct": 15.2,
    "sector_short_ratio": 31.9,
    "issue_type": "信用銘柄",
    "regulation_class": "002"
  }
}
```

---

## 7. データ取得フロー

```
┌─────────────────────────────────────────────────────────────┐
│ 1. 基礎データ取得                                             │
├─────────────────────────────────────────────────────────────┤
│ grok_trending.parquet → ticker, grok_rank                    │
│ prices_max_1d.parquet → price, volume, RSI, SMA              │
│ J-Quants /fins/statements → ROE, 営業利益成長率               │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. 信用取引データ取得（v2.2追加）                              │
├─────────────────────────────────────────────────────────────┤
│ J-Quants /listed/info → Sector33Code, MarginCode             │
│ J-Quants /markets/weekly_margin_interest → 信用倍率           │
│ J-Quants /markets/daily_margin_interest → 規制情報            │
│ J-Quants /markets/short_selling → セクター空売り比率          │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. スコアリング                                               │
├─────────────────────────────────────────────────────────────┤
│ calculate_score_v2_0_3() → v2.0.3スコア                       │
│ calculate_score_v2_1() → v2.1スコア                           │
│ calculate_score_v2_2() → v2.2スコア + flags                   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. 出力                                                       │
├─────────────────────────────────────────────────────────────┤
│ trading_recommendation.json（3バージョン比較可能）             │
│ backtest/comparison_v2_versions.parquet（検証用）             │
└─────────────────────────────────────────────────────────────┘
```

---

## 8. 実装ファイル

```
improvement/
├── docs/
│   └── SCORING_V2_COMPARISON_SPEC.md   # 本仕様書
├── scripts/
│   ├── fetch_margin_data.py            # 信用取引データ取得
│   ├── scoring_v2_2.py                 # v2.2スコアリング
│   └── compare_v2_versions.py          # バージョン比較
└── data/
    ├── margin_weekly.parquet           # 週次信用残高
    ├── margin_daily.parquet            # 日次信用残高（規制銘柄）
    ├── sector_short_selling.parquet    # セクター空売り比率
    └── backtest/
        └── comparison_v2_versions.parquet
```

---

## 9. 成功基準

### 9.1 技術的成功基準
- [ ] 全Grok銘柄の信用取引データ取得完了
- [ ] v2.0.3 / v2.1 / v2.2 の3バージョン並列出力
- [ ] 規制銘柄の正確な検知

### 9.2 戦略評価基準
| 指標 | v2.0.3 | v2.1目標 | v2.2目標 |
|------|--------|---------|---------|
| 買いシグナル勝率 | 39.0% | 50%+ | **55%+** |
| 売りシグナル勝率 | 71.4% | 70%+ | **75%+** |
| 取引禁止銘柄検知 | - | - | 100% |

---

## 10. 変更履歴

| 日付 | バージョン | 変更内容 |
|------|-----------|----------|
| 2025-11-28 | 1.0.0 | 初版作成 |

---

## 11. 承認

- [ ] ユーザー承認待ち

**承認後、実装を開始します。**
