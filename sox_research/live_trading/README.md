# Live Trading 実運用記録

**研究フェーズ (sox_research/multi_signal) から得られた候補ルールの実運用検証**

## ディレクトリ構成

```
live_trading/
├── RULES.md          # 現行ルール定義 (v1.0: 2026-04-20〜)
├── trades.csv        # 実トレード記録 (1トレード = 1行)
├── signals_log.csv   # 日次シグナル発火ログ (取引有無に関わらず全日)
├── monthly/          # 月次振り返り
└── README.md         # このファイル
```

## signals_log.csv の入力例

```csv
date,sox_day_pct,cme_day_pct,nvda_day_pct,sox_fired,cme_fired,nvda_fired,vix_close,gap_pct,earnings_week,action,reason
2026-04-20,2.40,0.85,1.68,Y,N,Y,16.5,0.3,N,TRADED,SOX+NVDA double fire
2026-04-21,-1.1,0.2,0.3,N,N,N,17.0,,N,SKIP,no signal
2026-04-24,3.1,2.5,2.8,Y,Y,Y,18.0,1.5,Y,SKIP,earnings week (2026-04-24)
```

**重要:** 発火したが取引しなかった日こそ記録する。見送り理由が段階1学習の核心。

## trades.csv の入力例

```csv
trade_date,trigger,trigger_value_pct,vix,gap_pct,ticker,size,entry_price,exit_price,day_ret_pct,pnl_gross_yen,pnl_after_fee_yen,earnings_week,notes
2026-04-20,SOX_up,2.40,16.5,0.3,6857,100,28000,28350,1.25,35000,34200,N,SOX+NVDA double fire. entry smooth.
```

## 月次振り返り (monthly/YYYY-MM.md)

各月末に以下を記入:

```markdown
# 2026-04 月次振り返り

## 取引実績
- 発火日数: 7
- 取引実施: 5
- 見送り: 2 (決算週1 / VIX高騰1)
- 勝敗: 3勝2敗
- 月間PnL: +XX,XXX円
- 実績PF: 1.X

## ルール違反
- なし / あり: [詳細]

## 気づき
- ...

## 翌月への申し送り
- ...
```

## 3ヶ月ゲート (2026-07-20)

RULES.md の判定基準に従い、継続・調整・撤退を決める。
感情的撤退はしない。記録に従う。
