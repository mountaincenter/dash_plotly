from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = Path(__file__).resolve().parent / "output"
PRICE_PATH = ROOT / "data" / "parquet" / "prices_topix500_oc.parquet"
FUTURES_PATH = ROOT / "data" / "parquet" / "futures_prices_max_1d.parquet"
MARKET_PATH = OUT_DIR / "semicon_market_momentum.parquet"
MARKET_TICKERS = ["^SOX", "MU", "NVDA"]
MARKET_REQUIRED_COLUMNS = {
    "date",
    "sox_ret1",
    "sox_ret5",
    "mu_ret1",
    "mu_ret5",
    "nvda_ret1",
    "nvda_ret5",
    "cme_ret1",
    "semicon_market_up",
    "chimp_up",
}

UNIVERSE = {
    "6857": ("アドバンテスト", "A", "テスタ/検査", "indicator"),
    "6146": ("ディスコ", "A", "後工程装置", "indicator"),
    "8035": ("東京エレクトロン", "A", "エッチング/成膜/塗布", "indicator"),
    "6920": ("レーザーテック", "A", "EUVマスク検査", "indicator"),
    "7735": ("SCREEN", "A", "洗浄装置", "indicator"),
    "6525": ("KOKUSAI ELECTRIC", "A", "成膜装置", "tradable"),
    "4062": ("イビデン", "A", "パッケージ基板", "heavy_watch"),
    "4186": ("東京応化工業", "A", "フォトレジスト", "tradable"),
    "4004": ("レゾナック", "A", "パッケージ/CMP材料", "tradable"),
    "3436": ("SUMCO", "A", "シリコンウエハ", "tradable"),
    "4063": ("信越化学工業", "A", "シリコンウエハ", "tradable"),
    "6723": ("ルネサス", "A", "マイコン/アナログ", "tradable"),
    "285A": ("キオクシア", "A", "NAND", "indicator"),
    "6503": ("三菱電機", "A", "パワー半導体/重電", "tradable"),
    "6504": ("富士電機", "A", "パワー半導体", "tradable"),
    "6501": ("日立製作所", "A/B", "検査装置/電力", "tradable"),
    "5801": ("古河電工", "B", "光通信/電力ケーブル/冷却", "heavy_watch"),
    "5803": ("フジクラ", "B", "光通信/高密度配線", "tradable"),
    "6981": ("村田製作所", "B", "MLCC/電源部品/EMI", "tradable"),
    "6367": ("ダイキン工業", "B", "冷却/空調", "tradable"),
    "5802": ("住友電工", "C", "光通信/電線", "tradable"),
    "5805": ("SWCC", "C", "電線/電力ケーブル", "tradable"),
    "6645": ("オムロン", "C", "電源/制御", "tradable"),
    "1963": ("日揮HD", "C", "設備/EPC", "tradable"),
    "1979": ("大気社", "C", "空調/クリーンルーム", "tradable"),
    "1802": ("大林組", "D", "建設", "tradable"),
    "1925": ("大和ハウス", "D", "建設/不動産", "tradable"),
    "8801": ("三井不動産", "D", "不動産/DC", "tradable"),
    "8802": ("三菱地所", "D", "不動産/DC", "tradable"),
}

LABEL_SCORE = {"A": 3.0, "A/B": 2.5, "B": 2.0, "C": 0.0, "D": -1.0}


def pct(now: pd.Series, before: pd.Series) -> pd.Series:
    return (now / before - 1.0) * 100.0


def max_drawdown(pnl: pd.Series) -> float:
    curve = pnl.cumsum()
    return float((curve - curve.cummax()).min()) if not curve.empty else np.nan


def pf(pnl: pd.Series) -> float:
    gain = float(pnl[pnl > 0].sum())
    loss = float(-pnl[pnl < 0].sum())
    return gain / loss if loss > 0 else np.inf


def cvar05(pnl: pd.Series) -> float:
    if pnl.empty:
        return np.nan
    q = pnl.quantile(0.05)
    tail = pnl[pnl <= q]
    return float(tail.mean()) if not tail.empty else np.nan


def build_signals() -> pd.DataFrame:
    df = pd.read_parquet(PRICE_PATH)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["_code4"] = df["Code"].astype(str).str[:4]
    df = df[df["_code4"].isin(UNIVERSE)].dropna(subset=["AdjO", "AdjC"]).copy()

    frames = []
    for code, g in df.sort_values("Date").groupby("_code4"):
        if len(g) < 90:
            continue
        name, label, segment, role = UNIVERSE[code]
        close = g["AdjC"].astype(float)
        open_ = g["AdjO"].astype(float)
        ma25 = close.rolling(25).mean()
        hi20 = close.rolling(20).max()
        ret5 = pct(close, close.shift(5))
        ret20 = pct(close, close.shift(20))
        vs25 = pct(close, ma25)
        slope25 = pct(ma25, ma25.shift(5))
        dist20hi = pct(close, hi20)
        score = pd.Series(LABEL_SCORE.get(label, 0.0), index=g.index)
        score += np.where(vs25 > 0, 2, 0)
        score += np.where(slope25 > 0, 2, 0)
        score += np.where(ret5 > 0, 1, 0)
        score += np.where(ret20 > 0, 1, 0)
        score += np.where((dist20hi >= -5) & (dist20hi <= 0), 2, 0)
        score += np.where(dist20hi < -15, -2, 0)
        score += np.where(vs25 > 20, -2, np.where(vs25 > 12, -1, 0))
        action = np.where(
            pd.Series(label, index=g.index).isin(["C", "D"]),
            np.where(score >= 9, "WATCH_ONLY", "AVOID"),
            np.where((score >= 12) & (vs25 <= 18) & (ret5 >= 0), "BUY", np.where(score >= 9, "WATCH", "AVOID")),
        )
        out = pd.DataFrame(
            {
                "signal_date": g["Date"],
                "trade_date": g["Date"].shift(-1),
                "code": code,
                "name": name,
                "label": label,
                "segment": segment,
                "role": role,
                "score": score,
                "action": action,
                "close": close,
                "next_open": open_.shift(-1),
                "next_close": close.shift(-1),
                "ret5": ret5,
                "ret20": ret20,
                "vs25": vs25,
                "dist20hi": dist20hi,
            }
        )
        out["pnl_pct"] = (out["next_close"] / out["next_open"] - 1.0) * 100.0
        out["pnl_100"] = (out["next_close"] - out["next_open"]) * 100.0
        frames.append(out)
    all_signals = pd.concat(frames, ignore_index=True)
    return all_signals.dropna(subset=["trade_date", "next_open", "next_close", "pnl_100"])


def build_market_momentum() -> pd.DataFrame:
    if MARKET_PATH.exists():
        market = pd.read_parquet(MARKET_PATH)
        market["date"] = pd.to_datetime(market["date"], errors="coerce")
        if MARKET_REQUIRED_COLUMNS.issubset(market.columns):
            return market

    raw = yf.download(
        MARKET_TICKERS,
        start="2022-01-01",
        progress=False,
        auto_adjust=False,
        group_by="ticker",
        threads=True,
    )
    rows = []
    for ticker in MARKET_TICKERS:
        if isinstance(raw.columns, pd.MultiIndex):
            if ticker not in raw.columns.get_level_values(0):
                continue
            close = raw[ticker]["Close"].dropna().astype(float)
        else:
            close = raw["Close"].dropna().astype(float)
        if close.empty:
            continue
        rows.append(pd.DataFrame({"date": pd.to_datetime(close.index).tz_localize(None), "ticker": ticker, "close": close.values}))
    if not rows:
        raise RuntimeError("SOX/MU/NVDA market momentum data is unavailable")

    long = pd.concat(rows, ignore_index=True).sort_values(["ticker", "date"])
    long["ret1"] = long.groupby("ticker")["close"].pct_change(1) * 100.0
    long["ret5"] = long.groupby("ticker")["close"].pct_change(5) * 100.0
    wide = long.pivot(index="date", columns="ticker", values=["ret1", "ret5"])
    wide.columns = [f"{ticker.lower().replace('^', '')}_{metric}" for metric, ticker in wide.columns]
    wide = wide.reset_index().rename(columns={"sox_ret1": "sox_ret1", "sox_ret5": "sox_ret5"})

    if FUTURES_PATH.exists():
        futures = pd.read_parquet(FUTURES_PATH)
        futures["date"] = pd.to_datetime(futures["date"], errors="coerce")
        cme = futures[futures["ticker"] == "NKD=F"].dropna(subset=["Close"]).sort_values("date").copy()
        cme["cme_ret1"] = cme["Close"].astype(float).pct_change(1) * 100.0
        wide = wide.merge(cme[["date", "cme_ret1"]], on="date", how="left")
    else:
        wide["cme_ret1"] = np.nan

    wide["semicon_market_up"] = (wide.get("sox_ret5", np.nan) > 0) & ((wide.get("mu_ret5", np.nan) > 0) | (wide.get("nvda_ret5", np.nan) > 0))
    wide["chimp_up"] = (wide.get("sox_ret1", np.nan) > 0) & (wide.get("cme_ret1", np.nan) > 0)
    MARKET_PATH.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(MARKET_PATH, index=False)
    return wide


def attach_market(signals: pd.DataFrame) -> pd.DataFrame:
    market = build_market_momentum()
    out = signals.copy()
    out["signal_date"] = pd.to_datetime(out["signal_date"], errors="coerce")
    market["date"] = pd.to_datetime(market["date"], errors="coerce")
    out = out.merge(
        market[["date", "sox_ret1", "sox_ret5", "mu_ret1", "mu_ret5", "nvda_ret1", "nvda_ret5", "cme_ret1", "semicon_market_up", "chimp_up"]],
        left_on="signal_date",
        right_on="date",
        how="left",
    ).drop(columns=["date"])
    return out


def choose(df: pd.DataFrame, variant: str) -> pd.DataFrame:
    d = df[df["signal_date"] >= pd.Timestamp("2022-07-01")].copy()
    if variant.startswith("chimp_"):
        d = d[(d["role"] == "tradable") & (d["chimp_up"] == True)].copy()
        if variant == "chimp_all_tradable":
            return d
        if variant == "chimp_label_a_all":
            return d[d["label"].isin(["A", "A/B"])]
        if variant == "chimp_top1_score":
            return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(1)
        if variant == "chimp_top3_score":
            return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(3)
        if variant == "chimp_top3_low_price":
            d = d[d["next_open"] <= 20000]
            return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(3)
    d = d[d["action"].isin(["BUY", "WATCH"])].copy()
    if variant == "all_top3":
        return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(3)
    if variant == "tradable_top1":
        d = d[d["role"] == "tradable"]
        return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(1)
    if variant == "tradable_top3":
        d = d[d["role"] == "tradable"]
        return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(3)
    if variant == "tradable_left_tail_guard":
        d = d[(d["role"] == "tradable") & (d["vs25"] <= 18) & (d["ret5"] <= 18)]
        return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(3)
    if variant == "tradable_price_guard":
        d = d[(d["role"] == "tradable") & (d["next_open"] <= 20000)]
        return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(3)
    if variant == "market_momentum_top1":
        d = d[(d["role"] == "tradable") & (d["semicon_market_up"] == True)]
        return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(1)
    if variant == "market_momentum_top3":
        d = d[(d["role"] == "tradable") & (d["semicon_market_up"] == True)]
        return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(3)
    if variant == "market_momentum_guard_top1":
        d = d[
            (d["role"] == "tradable")
            & (d["semicon_market_up"] == True)
            & (d["vs25"] <= 18)
            & (d["ret5"] <= 18)
            & (d["next_open"] <= 20000)
        ]
        return d.sort_values(["signal_date", "score"], ascending=[True, False]).groupby("signal_date").head(1)
    raise ValueError(variant)


def summarize(name: str, trades: pd.DataFrame) -> dict[str, object]:
    pnl = trades["pnl_100"].astype(float)
    daily = pnl.groupby(trades["trade_date"]).sum().sort_index()
    return {
        "variant": name,
        "n": len(trades),
        "days": trades["trade_date"].nunique(),
        "pf": pf(pnl),
        "win_rate": float((pnl > 0).mean() * 100.0) if len(pnl) else np.nan,
        "sum_pnl_100": float(pnl.sum()) if len(pnl) else np.nan,
        "avg_pnl_100": float(pnl.mean()) if len(pnl) else np.nan,
        "max_dd_100": max_drawdown(daily),
        "worst_trade_100": float(pnl.min()) if len(pnl) else np.nan,
        "q05_100": float(pnl.quantile(0.05)) if len(pnl) else np.nan,
        "cvar05_100": cvar05(pnl),
        "from": str(trades["trade_date"].min().date()) if len(trades) else "",
        "to": str(trades["trade_date"].max().date()) if len(trades) else "",
    }


def write_html(summary: pd.DataFrame, trades: pd.DataFrame) -> None:
    def money(v: object) -> str:
        return "-" if pd.isna(v) else f"{float(v):,.0f}"

    rows = []
    for r in summary.to_dict("records"):
        rows.append(
            "<tr>"
            f"<td>{r['variant']}</td><td class='r'>{int(r['n']):,}</td><td class='r'>{int(r['days']):,}</td>"
            f"<td class='r'>{r['pf']:.2f}</td><td class='r'>{r['win_rate']:.1f}%</td>"
            f"<td class='r'>{money(r['sum_pnl_100'])}</td><td class='r'>{money(r['max_dd_100'])}</td>"
            f"<td class='r'>{money(r['worst_trade_100'])}</td><td class='r'>{money(r['cvar05_100'])}</td>"
            f"<td>{r['from']}..{r['to']}</td></tr>"
        )
    worst = trades.nsmallest(30, "pnl_100")
    worst_rows = []
    for r in worst.itertuples():
        worst_rows.append(
            "<tr>"
            f"<td>{r.trade_date.date()}</td><td>{r.code}</td><td>{r.name}</td><td>{r.role}</td>"
            f"<td class='r'>{r.score:.1f}</td><td class='r'>{r.next_open:,.1f}</td>"
            f"<td class='r'>{r.next_close:,.1f}</td><td class='r'>{r.pnl_pct:+.2f}%</td><td class='r'>{r.pnl_100:,.0f}</td>"
            "</tr>"
        )
    html = f"""<!doctype html><html lang="ja"><head><meta charset="utf-8"><title>半導体順張りバックテスト</title>
<style>body{{background:#09090b;color:#fafafa;font-family:-apple-system,BlinkMacSystemFont,'Noto Sans JP',sans-serif;padding:24px;max-width:1500px;margin:auto}}section{{background:#18181b;border:1px solid #27272a;border-radius:10px;padding:18px;margin:16px 0;overflow:auto}}table{{border-collapse:collapse;width:100%;font-size:14px}}th,td{{border-bottom:1px solid #27272a;padding:8px 10px;white-space:nowrap}}th{{color:#a1a1aa;text-align:left}}.r{{text-align:right;font-variant-numeric:tabular-nums}}p{{color:#a1a1aa;line-height:1.7}}</style></head><body>
<h1>AI/半導体 順張りバックテスト</h1>
<p>指標専用の値嵩株は地合い判定に残し、実弾候補から外す前提で比較。検証は前日シグナル、翌営業日寄付買い、大引け売り。chimp系は SOX 1日&gt;0 かつ CME(NKD=F) 1日&gt;0 だけで判定。market_momentum系は SOX 5日&gt;0 かつ MU 5日&gt;0 または NVDA 5日&gt;0。High/VWAPはこのローカルOCデータに無いため未反映。</p>
<section><h2>サマリー</h2><table><thead><tr><th>variant</th><th class='r'>n</th><th class='r'>days</th><th class='r'>PF</th><th class='r'>勝率</th><th class='r'>損益/100株</th><th class='r'>最大DD</th><th class='r'>最大損失</th><th class='r'>CVaR5</th><th>期間</th></tr></thead><tbody>{''.join(rows)}</tbody></table></section>
<section><h2>ワーストトレード</h2><table><thead><tr><th>日付</th><th>code</th><th>銘柄</th><th>role</th><th class='r'>score</th><th class='r'>寄付</th><th class='r'>大引</th><th class='r'>%</th><th class='r'>損益/100株</th></tr></thead><tbody>{''.join(worst_rows)}</tbody></table></section>
</body></html>"""
    (OUT_DIR / "semicon_trend_backtest_report.html").write_text(html, encoding="utf-8")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    signals = attach_market(build_signals())
    summaries = []
    trades_all = []
    for variant in [
        "all_top3",
        "tradable_top1",
        "tradable_top3",
        "tradable_left_tail_guard",
        "tradable_price_guard",
        "chimp_all_tradable",
        "chimp_label_a_all",
        "chimp_top1_score",
        "chimp_top3_score",
        "chimp_top3_low_price",
        "market_momentum_top1",
        "market_momentum_top3",
        "market_momentum_guard_top1",
    ]:
        trades = choose(signals, variant).copy()
        trades["variant"] = variant
        summaries.append(summarize(variant, trades))
        trades_all.append(trades)
    summary = pd.DataFrame(summaries)
    trades = pd.concat(trades_all, ignore_index=True)
    summary.to_csv(OUT_DIR / "semicon_trend_backtest_summary.csv", index=False)
    trades.to_csv(OUT_DIR / "semicon_trend_backtest_trades.csv", index=False)
    write_html(summary, trades)
    print(summary.to_string(index=False))
    print(f"Wrote {OUT_DIR / 'semicon_trend_backtest_report.html'}")


if __name__ == "__main__":
    main()
