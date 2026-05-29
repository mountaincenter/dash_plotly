"""Find early signs by combining weekly theme memos with semicon money flow.

This is an analysis prototype, not a production signal.  It intentionally uses
round, explainable thresholds instead of optimized cutoffs.
"""
from __future__ import annotations

import json
from html import escape
from pathlib import Path
import sys

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
MEMO_PATH = ROOT / "data" / "analysis" / "semicon_weekly_theme_memos.json"
PRICE_PATH = ROOT / "data" / "parquet" / "prices_topix500_oc.parquet"
OUT_DIR = ROOT / "scripts" / "analysis" / "semiconductor" / "output"
LEADS_PATH = OUT_DIR / "semicon_theme_flow_leads.csv"
SUMMARY_PATH = OUT_DIR / "semicon_theme_flow_lead_summary.csv"
REPORT_PATH = OUT_DIR / "semicon_theme_flow_lead_report.html"

sys.path.insert(0, str(ROOT))
from server.routers.dev_semicon import UNIVERSE, _theme_flow_for_code  # noqa: E402
from scripts.analysis.semiconductor.backtest_semicon_weekly_theme_memos import (  # noqa: E402
    expanded_codes,
    memo_theme_hits,
)


def fmt_pct(v: object, digits: int = 2) -> str:
    if pd.isna(v):
        return "-"
    return f"{float(v):+.{digits}f}%"


def fmt_x(v: object) -> str:
    if pd.isna(v):
        return "-"
    return f"{float(v):.2f}x"


def cls(v: object) -> str:
    if pd.isna(v):
        return ""
    return "pos" if float(v) > 0 else "neg" if float(v) < 0 else ""


def build_meta() -> pd.DataFrame:
    rows = []
    for stock in UNIVERSE:
        theme_layer, flow_group = _theme_flow_for_code(stock.code, stock)
        rows.append(
            {
                "code": stock.code,
                "ticker": f"{stock.code}.T",
                "name": stock.name,
                "label": stock.label,
                "core_segment": stock.core_segment,
                "sub_segment": stock.sub_segment,
                "theme_driver": stock.theme_driver,
                "theme_layer": theme_layer,
                "flow_group": flow_group,
            }
        )
    return pd.DataFrame(rows)


def load_stock_history(meta: pd.DataFrame) -> pd.DataFrame:
    px = pd.read_parquet(PRICE_PATH)
    px["Date"] = pd.to_datetime(px["Date"], errors="coerce")
    px["Code"] = px["Code"].astype(str).str.replace(r"0$", "", regex=True)
    for col in ["AdjO", "AdjH", "AdjL", "AdjC", "AdjVo", "Va"]:
        px[col] = pd.to_numeric(px[col], errors="coerce")
    px = px.dropna(subset=["Date", "Code", "AdjO", "AdjC", "Va"])
    px = px[px["Code"].isin(set(meta["code"]))].merge(meta, left_on="Code", right_on="code", how="left")
    px = px.sort_values(["Code", "Date"]).copy()

    g = px.groupby("Code")
    px["ret1"] = g["AdjC"].pct_change() * 100.0
    px["ret5"] = g["AdjC"].pct_change(5) * 100.0
    px["ret20"] = g["AdjC"].pct_change(20) * 100.0
    ma25 = g["AdjC"].transform(lambda s: s.rolling(25, min_periods=20).mean())
    px["vs25"] = (px["AdjC"] / ma25 - 1.0) * 100.0
    for n in [5, 20, 60]:
        px[f"va{n}_prev"] = g["Va"].transform(lambda s, w=n: s.shift(1).rolling(w, min_periods=max(3, w // 2)).mean())
        px[f"va_ratio{n}"] = px["Va"] / px[f"va{n}_prev"]

    px["next_open"] = g["AdjO"].shift(-1)
    px["next_close_1"] = g["AdjC"].shift(-1)
    px["next_close_5"] = g["AdjC"].shift(-5)
    px["next_ret1"] = (px["next_close_1"] / px["next_open"] - 1.0) * 100.0
    px["next_ret5"] = (px["next_close_5"] / px["next_open"] - 1.0) * 100.0
    return px


def add_flow_history(px: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    daily = (
        px.groupby(["flow_group", "Date"], as_index=False)
        .agg(
            flow_va=("Va", "sum"),
            flow_ret1=("ret1", "mean"),
            flow_ret5=("ret5", "mean"),
            flow_up_ratio=("ret1", lambda x: float((x > 0).mean() * 100.0)),
            flow_n=("Code", "nunique"),
            flow_next_ret1=("next_ret1", "mean"),
            flow_next_ret5=("next_ret5", "mean"),
        )
        .sort_values(["flow_group", "Date"])
    )
    gd = daily.groupby("flow_group")["flow_va"]
    for n in [5, 20, 60]:
        daily[f"flow_tv{n}"] = daily["flow_va"] / gd.transform(
            lambda s, w=n: s.shift(1).rolling(w, min_periods=max(3, w // 2)).mean()
        )

    top = (
        px.sort_values(["flow_group", "Date", "Va"], ascending=[True, True, False])
        .groupby(["flow_group", "Date"], as_index=False)
        .first()[["flow_group", "Date", "Code", "name", "Va", "ret1", "ret5", "vs25"]]
        .rename(
            columns={
                "Code": "top_code",
                "name": "top_name",
                "Va": "top_va",
                "ret1": "top_ret1",
                "ret5": "top_ret5",
                "vs25": "top_vs25",
            }
        )
    )
    daily = daily.merge(top, on=["flow_group", "Date"], how="left")
    daily["top_share"] = daily["top_va"] / daily["flow_va"] * 100.0
    px = px.merge(
        daily[
            [
                "flow_group",
                "Date",
                "flow_va",
                "flow_ret1",
                "flow_ret5",
                "flow_up_ratio",
                "flow_n",
                "flow_next_ret1",
                "flow_next_ret5",
                "flow_tv5",
                "flow_tv20",
                "flow_tv60",
                "top_code",
                "top_name",
                "top_share",
            ]
        ],
        on=["flow_group", "Date"],
        how="left",
    )
    return px, daily


def classify(row: pd.Series, direct_count: int) -> tuple[str, float]:
    tv5 = float(row.get("flow_tv5", np.nan))
    tv20 = float(row.get("flow_tv20", np.nan))
    tv60 = float(row.get("flow_tv60", np.nan))
    ret = float(row.get("flow_ret1", np.nan))
    up = float(row.get("flow_up_ratio", np.nan))
    top = float(row.get("top_share", np.nan))

    score = 0.0
    if direct_count > 0:
        score += 2.0
    if tv20 >= 1.35:
        score += 2.0
    elif tv20 >= 1.15:
        score += 1.0
    if tv60 >= 1.25:
        score += 1.5
    elif tv60 >= 1.10:
        score += 0.5
    if tv5 >= 1.25:
        score += 1.0
    if up >= 60:
        score += 1.5
    elif up >= 50:
        score += 0.5
    if ret >= 0.5:
        score += 1.0
    elif ret < -1.0:
        score -= 1.0
    if 20 <= top <= 60:
        score += 0.5
    elif top >= 70:
        score -= 1.0

    if tv20 >= 1.35 and tv60 >= 1.20 and up >= 60 and ret >= 0:
        stage = "active_rotation"
    elif direct_count > 0 and tv20 >= 1.15 and tv60 >= 1.05 and up >= 45:
        stage = "theme_flow_seed"
    elif tv20 >= 1.20 and top >= 65:
        stage = "leader_only"
    elif ret <= -1.0 and up < 40:
        stage = "washout_rebound_watch"
    else:
        stage = "watch_only"
    return stage, score


def first_trade_after(trade_dates: pd.Series, date: str) -> pd.Timestamp | None:
    dates = pd.Series(pd.to_datetime(trade_dates).sort_values().unique())
    idx = dates.searchsorted(pd.Timestamp(date), side="right")
    if idx >= len(dates):
        return None
    return pd.Timestamp(dates.iloc[idx])


def code_names(meta: pd.DataFrame, codes: set[str]) -> str:
    if not codes:
        return ""
    m = meta[meta["code"].isin(codes)].sort_values("code")
    return ", ".join(f"{r.code}:{r.name}" for r in m.itertuples())


def pick_laggard(px: pd.DataFrame, flow_group: str, date: pd.Timestamp) -> tuple[str, str, float, float, float]:
    g = px[(px["flow_group"].eq(flow_group)) & (px["Date"].eq(date))].copy()
    if g.empty:
        return "", "", np.nan, np.nan, np.nan
    med5 = g["ret5"].median()
    cand = g[
        (g["ret20"].fillna(-999) > 0)
        & (g["ret5"].fillna(999) <= med5)
        & (g["va_ratio20"].fillna(0) >= 1.0)
        & (g["vs25"].fillna(999).between(-8, 18))
    ].copy()
    if cand.empty:
        cand = g.sort_values(["ret5", "va_ratio20"], ascending=[True, False]).head(1).copy()
    cand = cand.sort_values(["va_ratio20", "ret1"], ascending=[False, False]).head(1)
    r = cand.iloc[0]
    return str(r["Code"]), str(r["name"]), float(r["ret5"]), float(r["vs25"]), float(r["va_ratio20"])


def partial_group_return(px: pd.DataFrame, flow_group: str, date: pd.Timestamp, codes: set[str] | None = None) -> tuple[float, int]:
    latest_date = pd.Timestamp(px["Date"].max())
    if latest_date <= date:
        return np.nan, 0
    start = px[(px["flow_group"].eq(flow_group)) & (px["Date"].eq(date))].copy()
    if codes is not None:
        start = start[start["Code"].isin(codes)]
    if start.empty:
        return np.nan, 0
    latest = px[(px["Date"].eq(latest_date)) & (px["Code"].isin(set(start["Code"])))][["Code", "AdjC"]].rename(
        columns={"AdjC": "latest_close"}
    )
    merged = start[["Code", "next_open"]].merge(latest, on="Code", how="inner")
    merged = merged.dropna(subset=["next_open", "latest_close"])
    if merged.empty:
        return np.nan, 0
    trade_days = px[(px["Date"] > date) & (px["Date"] <= latest_date)]["Date"].drop_duplicates().nunique()
    ret = (merged["latest_close"] / merged["next_open"] - 1.0) * 100.0
    return float(ret.mean()), int(trade_days)


def build_leads(memos: list[dict], meta: pd.DataFrame, px: pd.DataFrame, flow_daily: pd.DataFrame) -> pd.DataFrame:
    trade_dates = px["Date"].drop_duplicates().sort_values()
    rows = []
    meta_by_code = meta.set_index("code")
    semicon_codes = set(meta["code"])

    for memo in memos:
        signal_date = first_trade_after(trade_dates, memo["date"])
        if signal_date is None:
            continue

        direct = {str(c).replace(".T", "") for c in memo.get("related_codes", [])} & semicon_codes
        expanded = expanded_codes(memo, meta)
        direct_groups = set(meta[meta["code"].isin(direct)]["flow_group"])
        expanded_groups = set(meta[meta["code"].isin(expanded)]["flow_group"])
        groups = sorted(direct_groups | expanded_groups)
        if not groups:
            continue

        for flow_group in groups:
            fr = flow_daily[(flow_daily["flow_group"].eq(flow_group)) & (flow_daily["Date"].eq(signal_date))]
            if fr.empty:
                continue
            fr = fr.iloc[0]
            direct_codes = {c for c in direct if meta_by_code.loc[c, "flow_group"] == flow_group}
            expanded_codes_in_group = {c for c in expanded if meta_by_code.loc[c, "flow_group"] == flow_group}
            stage, score = classify(fr, len(direct_codes))
            lag_code, lag_name, lag_ret5, lag_vs25, lag_va20 = pick_laggard(px, flow_group, signal_date)

            direct_px = px[(px["Date"].eq(signal_date)) & (px["Code"].isin(direct_codes))]
            expanded_px = px[(px["Date"].eq(signal_date)) & (px["Code"].isin(expanded_codes_in_group))]
            flow_partial_ret, flow_partial_days = partial_group_return(px, flow_group, signal_date)
            direct_partial_ret, _ = partial_group_return(px, flow_group, signal_date, direct_codes)
            rows.append(
                {
                    "memo_date": memo["date"],
                    "signal_date": signal_date.date().isoformat(),
                    "issue": memo.get("issue", ""),
                    "flow_group": flow_group,
                    "stage": stage,
                    "lead_score": score,
                    "theme_hits": ",".join(sorted(memo_theme_hits(memo))),
                    "direct_count": len(direct_codes),
                    "expanded_count": len(expanded_codes_in_group),
                    "direct_names": code_names(meta, direct_codes),
                    "flow_turnover_oku": float(fr["flow_va"]) / 100_000_000.0,
                    "flow_tv5": float(fr["flow_tv5"]) if pd.notna(fr["flow_tv5"]) else np.nan,
                    "flow_tv20": float(fr["flow_tv20"]) if pd.notna(fr["flow_tv20"]) else np.nan,
                    "flow_tv60": float(fr["flow_tv60"]) if pd.notna(fr["flow_tv60"]) else np.nan,
                    "flow_ret1": float(fr["flow_ret1"]) if pd.notna(fr["flow_ret1"]) else np.nan,
                    "flow_ret5": float(fr["flow_ret5"]) if pd.notna(fr["flow_ret5"]) else np.nan,
                    "flow_up_ratio": float(fr["flow_up_ratio"]) if pd.notna(fr["flow_up_ratio"]) else np.nan,
                    "top_code": fr.get("top_code", ""),
                    "top_name": fr.get("top_name", ""),
                    "top_share": float(fr["top_share"]) if pd.notna(fr["top_share"]) else np.nan,
                    "laggard_code": lag_code,
                    "laggard_name": lag_name,
                    "laggard_ret5": lag_ret5,
                    "laggard_vs25": lag_vs25,
                    "laggard_va20": lag_va20,
                    "flow_next_ret1": float(fr["flow_next_ret1"]) if pd.notna(fr["flow_next_ret1"]) else np.nan,
                    "flow_next_ret5": float(fr["flow_next_ret5"]) if pd.notna(fr["flow_next_ret5"]) else np.nan,
                    "flow_partial_days": flow_partial_days,
                    "flow_partial_ret": flow_partial_ret,
                    "direct_next_ret5": float(direct_px["next_ret5"].mean()) if not direct_px.empty else np.nan,
                    "direct_partial_ret": direct_partial_ret,
                    "expanded_next_ret5": float(expanded_px["next_ret5"].mean()) if not expanded_px.empty else np.nan,
                    "my_thesis": memo.get("my_thesis", ""),
                }
            )
    leads = pd.DataFrame(rows)
    if leads.empty:
        return leads
    return leads.sort_values(["signal_date", "lead_score", "flow_turnover_oku"], ascending=[True, False, False])


def summarize(leads: pd.DataFrame) -> pd.DataFrame:
    if leads.empty:
        return leads
    return (
        leads.groupby(["stage", "flow_group"], as_index=False)
        .agg(
            n=("flow_group", "size"),
            avg_score=("lead_score", "mean"),
            win_next5=("flow_next_ret5", lambda x: float((x > 0).mean() * 100.0)),
            avg_next5=("flow_next_ret5", "mean"),
            med_next5=("flow_next_ret5", "median"),
            avg_tv20=("flow_tv20", "mean"),
            avg_up=("flow_up_ratio", "mean"),
            avg_top_share=("top_share", "mean"),
        )
        .sort_values(["stage", "avg_next5"], ascending=[True, False])
    )


def html_table(df: pd.DataFrame, cols: list[tuple[str, str]], limit: int | None = None) -> str:
    if limit is not None:
        df = df.head(limit)
    head = "".join(f"<th>{escape(label)}</th>" for _, label in cols)
    rows = []
    for _, r in df.iterrows():
        tds = []
        for col, _ in cols:
            v = r.get(col)
            if isinstance(v, float):
                if col.startswith("flow_tv") or col.endswith("va20"):
                    text, klass = fmt_x(v), ""
                elif "turnover" in col or "score" in col or col in {"n", "direct_count", "expanded_count"}:
                    text, klass = f"{v:,.2f}", ""
                else:
                    text, klass = fmt_pct(v), cls(v)
                tds.append(f"<td class='r {klass}'>{text}</td>")
            else:
                tds.append(f"<td>{escape(str(v))}</td>")
        rows.append("<tr>" + "".join(tds) + "</tr>")
    return "<div class='table-wrap'><table><thead><tr>" + head + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>"


def build_report(leads: pd.DataFrame, summary: pd.DataFrame, memos: list[dict]) -> str:
    latest = leads["signal_date"].max() if not leads.empty else "-"
    latest_rows = leads[leads["signal_date"].eq(latest)].sort_values(["lead_score", "flow_turnover_oku"], ascending=False)
    strong = leads[leads["stage"].isin(["active_rotation", "theme_flow_seed"])].sort_values(
        ["signal_date", "lead_score"], ascending=[False, False]
    )
    css = """
    body{margin:0;background:#0b0d12;color:#e8ecf3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;line-height:1.55}
    main{max-width:1440px;margin:0 auto;padding:28px}
    h1{font-size:25px;margin:0 0 8px} h2{font-size:18px;margin:28px 0 10px}
    .lead,.note{color:#a9b2c3}.cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
    .card{background:#141922;border:1px solid #263044;border-radius:8px;padding:14px}.k{color:#8d98aa;font-size:12px}.v{font-size:22px;font-weight:700}
    .table-wrap{overflow:auto;border:1px solid #263044;border-radius:8px;background:#10151d;margin-bottom:14px}
    table{border-collapse:collapse;width:100%;min-width:1100px}th,td{padding:8px 9px;border-bottom:1px solid #222b3a;vertical-align:top}th{font-size:12px;color:#a9b2c3;text-align:left;background:#151c27;position:sticky;top:0}td{font-size:13px}.r{text-align:right;font-variant-numeric:tabular-nums}.pos{color:#66d18f}.neg{color:#ff7b7b}
    """
    cards = [
        ("ソースメモ", f"{len(memos)}週", "headline-derived local memos"),
        ("lead行", f"{len(leads):,}", "memo x flow_group"),
        ("最新signal", latest, "memo翌営業日"),
        ("端緒候補", f"{len(strong):,}", "active_rotation + theme_flow_seed"),
    ]
    card_html = "".join(f"<div class='card'><div class='k'>{escape(k)}</div><div class='v'>{escape(v)}</div><div class='note'>{escape(n)}</div></div>" for k, v, n in cards)
    lead_cols = [
        ("signal_date", "signal"),
        ("memo_date", "memo"),
        ("stage", "stage"),
        ("lead_score", "score"),
        ("flow_group", "flow"),
        ("direct_count", "direct"),
        ("flow_turnover_oku", "売買代金億"),
        ("flow_tv5", "5日比"),
        ("flow_tv20", "20日比"),
        ("flow_tv60", "60日比"),
        ("flow_ret1", "flow1日"),
        ("flow_up_ratio", "上昇率"),
        ("top_name", "top"),
        ("top_share", "top占有"),
        ("laggard_name", "次候補"),
        ("laggard_ret5", "次候補5日"),
        ("laggard_vs25", "25日線比"),
        ("flow_next_ret5", "検証next5"),
        ("flow_partial_ret", "暫定ret"),
        ("flow_partial_days", "暫定日数"),
        ("direct_names", "direct names"),
    ]
    summary_cols = [
        ("stage", "stage"),
        ("flow_group", "flow"),
        ("n", "n"),
        ("avg_score", "score"),
        ("win_next5", "勝率next5"),
        ("avg_next5", "平均next5"),
        ("med_next5", "中央値"),
        ("avg_tv20", "20日比"),
        ("avg_up", "上昇率"),
        ("avg_top_share", "top占有"),
    ]
    return f"""<!doctype html><html lang="ja"><head><meta charset="utf-8"><title>Semicon Theme Flow Leads</title><style>{css}</style></head><body><main>
    <h1>半導体テーマ端緒検出: 週次ヘッドライン × 売買代金フロー</h1>
    <p class="lead">ヘッドラインは「どのテーマを見るか」だけに使い、売買判断には使わない。memo翌営業日のflow_group売買代金、breadth、top集中、次候補を同じ表に置く。</p>
    <div class="cards">{card_html}</div>
    <h2>最新日の端緒候補</h2>
    {html_table(latest_rows, lead_cols)}
    <h2>直近の強い端緒</h2>
    {html_table(strong, lead_cols, 40)}
    <h2>stage × flow 検証</h2>
    {html_table(summary, summary_cols)}
    <p class="note">閾値は最適化していない。active_rotation/theme_flow_seed は調査優先度であり、単独の買いシグナルではない。next5は後検証用で、判定には使っていない。</p>
    </main></body></html>"""


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    memos = json.loads(MEMO_PATH.read_text(encoding="utf-8"))["memos"]
    meta = build_meta()
    px = load_stock_history(meta)
    px, flow_daily = add_flow_history(px)
    leads = build_leads(memos, meta, px, flow_daily)
    summary = summarize(leads)
    leads.to_csv(LEADS_PATH, index=False)
    summary.to_csv(SUMMARY_PATH, index=False)
    REPORT_PATH.write_text(build_report(leads, summary, memos), encoding="utf-8")
    print(f"[OK] leads: {LEADS_PATH} rows={len(leads)}")
    print(f"[OK] summary: {SUMMARY_PATH} rows={len(summary)}")
    print(f"[OK] report: {REPORT_PATH}")
    if not summary.empty:
        print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
