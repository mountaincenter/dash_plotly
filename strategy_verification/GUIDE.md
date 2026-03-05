# トレード戦略の検証手法ガイド

## 目的

グランビル8原則に基づくトレード戦略を、データサイエンスの原則に従って根本から検証する。
SLなし・フィルターなしの生データから始め、異常値排除→分布分析→パラメータ導出の順で進める。

## ディレクトリ構造

```
strategy_verification/
├── GUIDE.md                  # 本ファイル（目次・進捗・用語定義）
├── data/
│   ├── raw/                  # 生データ（絶対不変）
│   │   └── metadata/         # データ出典・カラム定義
│   ├── interim/              # 中間処理データ
│   └── processed/            # 分析用最終データ
├── notebooks/                # 探索・試行錯誤
├── scripts/                  # 確定処理の再現用
│   └── master.sh             # raw → processed → chapters 一気通貫
├── chapters/                 # 各章の成果物（レポート・図表）
└── references/               # 参考資料・判断ログ
```

## 原則

1. **data/raw/ は絶対に書き換えない** — 何度でもやり直せる起点
2. **notebooks = 試行錯誤** — 結論が出たら scripts/ + chapters/ へ昇格
3. **master.sh で全再現可能** — raw → processed → chapters を1コマンドで
4. **metadata 必須** — データ出典・カラム定義を記録
5. **判断ログは references/ に残す** — なぜそのパラメータにしたか

## 章立て

| 章 | ディレクトリ | 内容 | 状態 |
|----|-------------|------|------|
| 1 | `01_data_quality` | データ品質検証（異常値・欠損・バイアス） | 完了 |
| 2 | `02_mae_mfe_raw` | MAE/MFE生分布（SLなし・全8原則） | 完了 |
| 3 | `03_sl_optimization` | SL幅の導出 | 完了 |
| 4 | `04_exit_strategy` | 利確戦略の導出 | 完了 |
| 5 | `05_time_analysis` | 時間軸分析（保有期間・MFEピーク日） | 未着手 |
| 6 | `06_current_strategy_comparison` | 現行戦略（SL -3%, signal A/B）との比較 | 未着手 |

## 用語

- **MAE** (Maximum Adverse Excursion): 保有中の最大逆行幅
- **MFE** (Maximum Favorable Excursion): 保有中の最大含み益幅
- **発射台**: フィルターなし・SLなしのバックテスト生データ
- **グランビル8原則**: 移動平均線と株価の位置関係による8分類（B1-B4: 買い、S1-S4: 売り）

## 既知の残課題（進行に影響なし）

| # | 内容 | 影響度 | 対応方針 |
|---|------|--------|----------|
| K1 | yfinance配当落ち日のシグナル発火タイミング微差（0.6%） | 統計的に無視可 | JQuants生値でシグナル再生成する場合は scripts/01 の上流を差し替え。Chapter 2以降は再実行のみ |
| K2 | 2016年以前のデータはJQuantsで検証不可 | 低（yfinance単独だが内部整合） | 記録のみ。10年超の長期分布には十分 |
