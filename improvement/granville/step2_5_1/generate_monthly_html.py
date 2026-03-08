#!/usr/bin/env python3
"""
Step 2.5.1: ATR + 株価フィルター 月次パフォーマンスHTML生成
==========================================================
フィルター: B1/B3(Uptrend) ATR<2.5%, B4(Downtrend) ATR>6.0%, 株価<¥20,000
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]  # improvement/
PARQUET_DIR = ROOT.parent / "data" / "parquet"
STEP1_DIR = ROOT / "granville" / "step1"
OUT_DIR = Path(__file__).resolve().parent

# フィルター設定
PRICE_LIMIT = 20_000
ATR_B1B3_MAX = 2.5
ATR_B4_MIN = 6.0
TARGETS = [("B1", "Uptrend"), ("B3", "Uptrend"), ("B4", "Downtrend")]
START_DATE = "2025-01-01"


def load_and_filter() -> tuple[pd.DataFrame, pd.Timestamp]:
    """トレードデータ読み込み + ATR/株価フィルター適用"""
    # 価格データ → ATR
    p = pd.read_parquet(PARQUET_DIR / "prices_max_1d.parquet")
    p["date"] = pd.to_datetime(p["date"])
    p = p.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = p.groupby("ticker")

    high_low = p["High"] - p["Low"]
    high_pc = abs(p["High"] - g["Close"].shift(1))
    low_pc = abs(p["Low"] - g["Close"].shift(1))
    tr = pd.concat([high_low, high_pc, low_pc], axis=1).max(axis=1)
    p["atr14"] = tr.groupby(p["ticker"]).transform(
        lambda x: x.ewm(span=14, adjust=False).mean()
    )
    p["atr_pct"] = p["atr14"] / p["Close"] * 100
    data_end = p["date"].max()

    # 最新終値取得（OPEN損益計算用）
    latest_closes = (
        p.sort_values("date").groupby("ticker").last()[["Close"]].reset_index()
    )
    latest_closes.columns = ["ticker", "latest_close"]

    # トレードデータ
    t = pd.read_parquet(STEP1_DIR / "trades_sl3.parquet")
    long = t[t["direction"] == "LONG"].copy()
    long["signal_date"] = pd.to_datetime(long["signal_date"])
    long["entry_date"] = pd.to_datetime(long["entry_date"])
    long["exit_date"] = pd.to_datetime(long["exit_date"])

    # ATR結合
    tech = p[["ticker", "date", "atr_pct"]].rename(columns={"date": "signal_date"})
    long = long.merge(tech, on=["ticker", "signal_date"], how="left")
    long = long.dropna(subset=["atr_pct"])

    # メタ結合（銘柄名）
    meta = pd.read_parquet(PARQUET_DIR / "meta.parquet")
    long = long.merge(
        meta[["ticker", "stock_name"]].drop_duplicates("ticker"),
        on="ticker",
        how="left",
    )

    # 期間フィルター
    long = long[long["signal_date"] >= START_DATE]

    # レジーム限定
    mask = pd.Series(False, index=long.index)
    for rule, regime in TARGETS:
        mask = mask | ((long["rule"] == rule) & (long["regime"] == regime))
    long = long[mask].copy()

    # ATRフィルター
    def atr_pass(row: pd.Series) -> bool:
        if row["rule"] in ("B1", "B3"):
            return row["atr_pct"] <= ATR_B1B3_MAX
        elif row["rule"] == "B4":
            return row["atr_pct"] >= ATR_B4_MIN
        return False

    long = long[long.apply(atr_pass, axis=1)].copy()

    # 株価フィルター
    long = long[long["entry_price"] < PRICE_LIMIT].copy()

    # OPEN判定 + 含み損益計算
    long = long.merge(latest_closes, on="ticker", how="left")
    open_mask = (
        (long["signal_date"] > (data_end - pd.Timedelta(days=14)))
        & (long["exit_type"] == "expire")
        & (long["pnl"] == 0)
    )
    long["is_open"] = open_mask
    # OPEN: 含み損益 = (latest_close - entry_price) * 100
    long["unrealized_pnl"] = np.where(
        long["is_open"],
        ((long["latest_close"] - long["entry_price"]) * 100).round().astype(int),
        0,
    )
    long["unrealized_ret"] = np.where(
        long["is_open"],
        ((long["latest_close"] / long["entry_price"] - 1) * 100).round(2),
        0,
    )

    long = long.sort_values(["signal_date", "ticker"]).reset_index(drop=True)
    return long, data_end


def esc(s: str) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_yen(v: int) -> str:
    return f"{v:+,}円"


def css_class(v: float) -> str:
    if v > 0:
        return "up"
    elif v < 0:
        return "down"
    return "neu"


def generate_html(trades: pd.DataFrame, data_end: pd.Timestamp) -> str:
    """月次展開式HTML生成"""
    trades["ym"] = trades["signal_date"].dt.to_period("M")

    # 全体サマリー（確定のみ）
    confirmed = trades[~trades["is_open"]]
    open_trades = trades[trades["is_open"]]

    total_pnl = int(confirmed["pnl"].sum())
    total_unrealized = int(open_trades["unrealized_pnl"].sum())
    n_confirmed = len(confirmed)
    n_open = len(open_trades)
    wins = int((confirmed["ret_pct"] > 0).sum())
    losses = n_confirmed - wins
    w_sum = confirmed[confirmed["ret_pct"] > 0]["pnl"].sum()
    l_sum = abs(confirmed[confirmed["ret_pct"] <= 0]["pnl"].sum())
    pf = round(w_sum / l_sum, 2) if l_sum > 0 else 999
    wr = round(wins / n_confirmed * 100, 1) if n_confirmed > 0 else 0
    avg_pnl = int(total_pnl / n_confirmed) if n_confirmed > 0 else 0

    # 月別集計
    months = sorted(trades["ym"].unique())

    # HTML構築
    rows = []
    for ym in months:
        mt = trades[trades["ym"] == ym]
        mc = mt[~mt["is_open"]]
        mo = mt[mt["is_open"]]
        m_pnl = int(mc["pnl"].sum())
        m_unr = int(mo["unrealized_pnl"].sum())
        m_n = len(mc)
        m_open = len(mo)
        m_wins = int((mc["ret_pct"] > 0).sum())
        m_losses = m_n - m_wins
        mw = mc[mc["ret_pct"] > 0]["pnl"].sum()
        ml = abs(mc[mc["ret_pct"] <= 0]["pnl"].sum())
        m_pf = round(mw / ml, 2) if ml > 0 else 999
        m_wr = round(m_wins / m_n * 100, 1) if m_n > 0 else 0
        m_avg = int(m_pnl / m_n) if m_n > 0 else 0
        ym_key = "m" + str(ym).replace("-", "")

        pnl_cls = css_class(m_pnl)
        unr_cls = "open-c" if m_unr != 0 else "neu"

        rows.append(
            f'<tr class="month-row" onclick="toggleMonth(\'{ym_key}\')">'
            f'<td><span class="toggle" id="arrow_{ym_key}">▶</span></td>'
            f"<td>{ym}</td>"
            f'<td class="num">{m_n}</td>'
            f"<td>{m_wins}W{m_losses}L</td>"
            f'<td class="num {pnl_cls}">{fmt_yen(m_pnl)}</td>'
            f'<td class="num">{m_open}</td>'
            f'<td class="num {unr_cls}">{fmt_yen(m_unr) if m_unr != 0 else "+0円"}</td>'
            f'<td class="num">{m_pf:.2f}</td>'
            f'<td class="num">{m_wr}%</td>'
            f'<td class="num {css_class(m_avg)}">{fmt_yen(m_avg)}</td>'
            f"</tr>"
        )

        # 個別トレード行
        for _, r in mt.iterrows():
            is_open = r["is_open"]
            rule_tag = f'tag-{r["rule"].lower()}'
            sig_tag = "tag-open" if is_open else f'tag-{r["exit_type"]}'
            exit_label = "OPEN" if is_open else r["exit_type"]

            if is_open:
                pnl_val = int(r["unrealized_pnl"])
                ret_val = r["unrealized_ret"]
                pnl_display = fmt_yen(pnl_val)
                cls = "open-c"
                hold_str = f'{int((data_end - r["entry_date"]).days)}日~'
                price_str = f'entry {r["entry_price"]:,.0f} → 含み {r["latest_close"]:,.1f} ({ret_val:+.1f}%)'
            else:
                pnl_val = int(r["pnl"])
                ret_pct = r["ret_pct"]
                pnl_display = fmt_yen(pnl_val)
                cls = css_class(pnl_val)
                hold_str = f'{int(r["hold_days"])}日'
                price_str = f'entry {r["entry_price"]:,.0f} → exit {r["exit_price"]:,.1f} ({ret_pct:+.1f}%)'

            name = esc(str(r.get("stock_name", r["ticker"]))[:12])
            rows.append(
                f'<tr class="detail-row {ym_key}">'
                f"<td></td>"
                f'<td>{r["signal_date"].strftime("%m/%d")}</td>'
                f'<td><span class="tag {rule_tag}">{r["rule"]}</span> {r["ticker"]} {name}</td>'
                f'<td class="num">ATR {r["atr_pct"]:.1f}%</td>'
                f'<td class="num {cls}">{pnl_display}</td>'
                f'<td class="num">{hold_str}</td>'
                f'<td>{"含み" if is_open else "—"}</td>'
                f'<td colspan="2" class="num">{price_str}</td>'
                f'<td><span class="tag {sig_tag}">{exit_label}</span></td>'
                f"</tr>"
            )

    tbody = "\n".join(rows)

    # 除外銘柄リスト（株価>20000で除外されたもの）
    excluded_note = f"株価≥¥{PRICE_LIMIT:,}の銘柄を除外"

    html = f"""<!DOCTYPE html>
<html lang="ja"><head><meta charset="UTF-8">
<title>Step 2.5.1 ATR+株価フィルター 月次パフォーマンス 2025/01~</title>
<style>
:root{{--bg:#0d1117;--card:#161b22;--border:#30363d;--text:#e6edf3;--dim:#8b949e;--green:#3fb950;--red:#f85149;--yellow:#d29922;--blue:#58a6ff;--purple:#bc8cff;--orange:#f0883e}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--text);font-family:'Segoe UI',sans-serif;padding:20px;max-width:1500px;margin:0 auto}}
h1{{font-size:1.3em;margin-bottom:6px}}
.sub{{color:var(--dim);font-size:0.82em;margin-bottom:20px}}
table{{width:100%;border-collapse:collapse;font-size:0.8em;margin-bottom:4px}}
th{{background:var(--card);color:var(--dim);text-align:left;padding:7px 5px;border-bottom:1px solid var(--border);font-weight:600;white-space:nowrap}}
td{{padding:6px 5px;border-bottom:1px solid var(--border)}}
tr:hover{{background:rgba(88,166,255,0.05)}}
.num{{text-align:right;font-variant-numeric:tabular-nums}}
.up{{color:var(--green)}}.down{{color:var(--red)}}.neu{{color:var(--dim)}}.open-c{{color:var(--purple)}}
.tag{{display:inline-block;padding:1px 6px;border-radius:3px;font-size:0.78em;font-weight:600}}
.tag-b1{{background:rgba(63,185,80,0.15);color:var(--green)}}
.tag-b3{{background:rgba(88,166,255,0.15);color:var(--blue)}}
.tag-b4{{background:rgba(240,136,62,0.15);color:var(--orange)}}
.tag-sl{{background:rgba(248,81,73,0.15);color:var(--red)}}
.tag-sig{{background:rgba(188,140,255,0.15);color:var(--purple)}}
.tag-signal{{background:rgba(188,140,255,0.15);color:var(--purple)}}
.tag-exp{{background:rgba(139,148,158,0.15);color:var(--dim)}}
.tag-expire{{background:rgba(139,148,158,0.15);color:var(--dim)}}
.tag-open{{background:rgba(188,140,255,0.2);color:var(--purple);border:1px solid var(--purple)}}
.month-row{{background:var(--card);cursor:pointer;user-select:none}}
.month-row:hover{{background:rgba(88,166,255,0.1)}}
.month-row td{{font-weight:700;padding:10px 5px}}
.detail-row{{display:none}}
.detail-row.show{{display:table-row}}
.toggle{{color:var(--blue);font-size:0.9em;margin-right:6px}}
.summary{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:20px}}
.summary h2{{font-size:1em;color:var(--blue);margin-bottom:10px}}
.summary-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}}
.s-item{{background:var(--bg);border-radius:6px;padding:10px}}
.s-label{{font-size:0.75em;color:var(--dim)}}
.s-val{{font-size:1.3em;font-weight:700}}
.filter-badge{{display:inline-block;padding:3px 10px;border-radius:12px;font-size:0.78em;font-weight:600;margin-right:6px;margin-bottom:4px}}
.badge-atr{{background:rgba(88,166,255,0.15);color:var(--blue)}}
.badge-price{{background:rgba(63,185,80,0.15);color:var(--green)}}
.badge-regime{{background:rgba(240,136,62,0.15);color:var(--orange)}}
.comparison{{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:20px}}
.comparison h2{{font-size:1em;color:var(--orange);margin-bottom:10px}}
.comp-table{{font-size:0.82em}}
.comp-table th,.comp-table td{{padding:8px 12px}}
.comp-table tr:last-child td{{border-bottom:none}}
.highlight-row{{background:rgba(63,185,80,0.08)}}
.footer{{margin-top:20px;padding-top:12px;border-top:1px solid var(--border);font-size:0.72em;color:var(--dim)}}
</style></head><body>
<h1>Granville ATR+株価フィルター 月次パフォーマンス</h1>
<p class="sub">期間: 2025/01 ~ {data_end.strftime('%Y/%m/%d')} | データ最終日: {data_end.strftime('%Y/%m/%d')}</p>

<div style="margin-bottom:16px">
<span class="filter-badge badge-regime">B1/B3: Uptrend | B4: Downtrend</span>
<span class="filter-badge badge-atr">B1/B3: ATR&lt;{ATR_B1B3_MAX}% | B4: ATR≥{ATR_B4_MIN}%</span>
<span class="filter-badge badge-price">株価 &lt; ¥{PRICE_LIMIT:,}</span>
</div>

<div class="summary"><h2>全体サマリー</h2>
<div class="summary-grid">
<div class="s-item"><div class="s-label">確定損益</div><div class="s-val {css_class(total_pnl)}">{fmt_yen(total_pnl)}</div></div>
<div class="s-item"><div class="s-label">含み損益(OPEN)</div><div class="s-val {"open-c" if total_unrealized != 0 else "neu"}">{fmt_yen(total_unrealized)}</div></div>
<div class="s-item"><div class="s-label">確定件数</div><div class="s-val">{n_confirmed}件</div></div>
<div class="s-item"><div class="s-label">勝敗</div><div class="s-val">{wins}勝{losses}敗</div></div>
<div class="s-item"><div class="s-label">勝率</div><div class="s-val">{wr}%</div></div>
<div class="s-item"><div class="s-label">PF</div><div class="s-val {css_class(pf - 1)}">{pf}</div></div>
<div class="s-item"><div class="s-label">平均確定損益</div><div class="s-val {css_class(avg_pnl)}">{fmt_yen(avg_pnl)}</div></div>
<div class="s-item"><div class="s-label">OPEN件数</div><div class="s-val open-c">{n_open}件</div></div>
</div></div>

<table id="mainTable"><thead><tr><th></th><th>月</th><th>確定件数</th><th>勝敗</th><th>確定損益</th><th>OPEN</th><th>含み損益</th><th>PF</th><th>勝率</th><th>平均損益</th></tr></thead><tbody>
{tbody}
</tbody></table>

<div class="footer">
Step 2.5.1 | {excluded_note} | SL: -3% | Exit: シグナル or 60日expire<br>
生成: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}
</div>

<script>
function toggleMonth(ym){{
  const rows=document.querySelectorAll('.detail-row.'+ym);
  const arrow=document.getElementById('arrow_'+ym);
  const show=rows.length>0&&!rows[0].classList.contains('show');
  rows.forEach(r=>r.classList.toggle('show',show));
  if(arrow)arrow.textContent=show?'▼':'▶';
}}
</script>
</body></html>"""
    return html


def main():
    t0 = time.time()
    print("=" * 70)
    print("Step 2.5.1: ATR + 株価フィルター 月次HTML生成")
    print(f"  ATR: B1/B3 < {ATR_B1B3_MAX}%, B4 >= {ATR_B4_MIN}%")
    print(f"  株価: < ¥{PRICE_LIMIT:,}")
    print("=" * 70)

    trades, data_end = load_and_filter()
    confirmed = trades[~trades["is_open"]]
    open_t = trades[trades["is_open"]]
    n_conf = len(confirmed)
    w = confirmed[confirmed["ret_pct"] > 0]["pnl"].sum()
    l = abs(confirmed[confirmed["ret_pct"] <= 0]["pnl"].sum())
    pf = round(w / l, 2) if l > 0 else 999
    print(f"  {n_conf} confirmed + {len(open_t)} open trades")
    print(f"  PnL: {confirmed['pnl'].sum():+,}円, PF: {pf}")

    html = generate_html(trades, data_end)
    out_path = OUT_DIR / "atr_price_monthly_2025.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"\n[OK] {out_path} ({len(html):,} bytes)")
    print(f"Time: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
