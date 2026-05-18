from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "data" / "reports"

TOPIX_ORDER = {
    "TOPIX Core30": 0,
    "TOPIX Large70": 1,
    "TOPIX Mid400": 2,
    "TOPIX Small 1": 3,
    "TOPIX Small 2": 4,
}


@dataclass(frozen=True)
class Leg:
    code: str
    direction: str | None = None
    qty: int | None = None
    entry: float | None = None
    exit: float | None = None
    pl: int | None = None
    name: str | None = None
    source_date: str | None = None
    note: str = ""


CONFIG: dict[str, dict[str, Any]] = {
    "2026-05-11": {
        "title": "Trade Review — 2026/05/11（月）",
        "subtitle": "戦略: SQ+1 / weekday / pair ／ 日足: prices_max_1d ／ 5/7テンプレート準拠",
        "summary_total": -19100,
        "summary_count": 12,
        "summary_sub": "SQ+1 -4,550 / weekday -5,500 / pair -9,050",
        "title_summary": "-19,100円 12件・SQ+1 5件(-4,550) / weekday 2件(-5,500) / pair 3ペア(-9,050)",
        "summary_wins": 5,
        "summary_losses": 7,
        "pairs": [
            {"label": "長瀬産業×因幡電機産業 ペア", "sector": "卸売業", "legs": [Leg("8012"), Leg("9934")]},
            {"label": "大豊工業×日本精工 ペア", "sector": "機械", "legs": [Leg("6470"), Leg("6471")]},
            {"label": "北日本銀行×南都銀行 銀行ペア", "sector": "銀行業", "legs": [Leg("8551", "LONG", 100, 4815, 4810, -537, "北日本銀行", note="未決済"), Leg("8367")]},
        ],
        "singles": [
            {"kind": "SQ+1", "label": "SQ+1 銘柄（前日上昇Top10 SHORT）", "codes": ["2371", "3774", "2432", "3436", "6526"]},
            {"kind": "weekday", "label": "weekday 銘柄（月曜SHORT）", "codes": ["4204", "1605"]},
        ],
    },
    "2026-05-12": {
        "title": "Trade Review — 2026/05/12（火）",
        "subtitle": "戦略: grok / weekday / pair ／ 日足: prices_max_1d ／ 5/7テンプレート準拠",
        "summary_total": 5440,
        "summary_count": 9,
        "summary_sub": "grok +13,040 / weekday -12,600 / pair +5,000",
        "title_summary": "+5,440円 9件・grok 4件(+13,040) / weekday 2件(-12,600) / pair 2ペア(+5,000)",
        "summary_wins": 5,
        "summary_losses": 4,
        "pairs": [
            {"label": "中部飼料×フィード・ワン ペア", "sector": "食料品", "legs": [Leg("2053"), Leg("2060")]},
            {"label": "北日本銀行 5/11繰越精算", "sector": "銀行業", "legs": [Leg("8551")]},
        ],
        "singles": [
            {"kind": "grok", "label": "grok ショート", "codes": ["6526", "4222", "7089", "5121"]},
            {"kind": "weekday", "label": "weekday 銘柄（火曜SHORT）", "codes": ["5844", "5838"]},
        ],
    },
    "2026-05-13": {
        "title": "Trade Review — 2026/05/13（水）",
        "subtitle": "戦略: pair / grok ／ 日足: prices_max_1d ／ 5/7テンプレート準拠",
        "summary_total": -6760,
        "summary_count": 5,
        "summary_sub": "pair -9,850 / grok +3,090",
        "title_summary": "-6,760円 5件・pair 2ペア(-9,850) / grok 1件(+3,090)",
        "summary_wins": 3,
        "summary_losses": 2,
        "pairs": [
            {"label": "丸紅×豊田通商 ペア", "sector": "卸売業", "legs": [Leg("8002"), Leg("8015")]},
            {"label": "三井化学×UBE ペア", "sector": "化学", "legs": [Leg("4208"), Leg("4183")]},
        ],
        "singles": [
            {"kind": "grok", "label": "grok ショート", "codes": ["2693"]},
        ],
    },
    "2026-05-14": {
        "title": "Trade Review — 2026/05/14（木）",
        "subtitle": "戦略: pair / grok / calendar ／ 日足: prices_max_1d ／ 5/7テンプレート準拠",
        "summary_total": -5580,
        "summary_count": 6,
        "summary_sub": "pair -7,200 / grok +5,070 / calendar -3,450",
        "title_summary": "-5,580円 6件・pair 2ペア(-7,200) / grok 2件(+5,070) / calendar 1件(-3,450)",
        "summary_wins": 2,
        "summary_losses": 4,
        "pairs": [
            {"label": "DIC×旭化成 ペア", "sector": "化学", "legs": [Leg("4631"), Leg("3407", "SHORT", 200, 1750, 1729, 4200, "旭化成", note="建玉保有中")]},
            {"label": "三井化学×UBE ペア", "sector": "化学", "legs": [Leg("4208"), Leg("4183")]},
        ],
        "singles": [
            {"kind": "grok", "label": "grok ショート", "codes": ["5597", "6356"]},
            {"kind": "calendar", "label": "calendar 銘柄", "codes": ["8303"]},
        ],
    },
    "2026-05-15": {
        "title": "Trade Review — 2026/05/15（金）",
        "subtitle": "戦略: pair / weekday B4 LONG ／ 日足: prices_max_1d ／ 5/7テンプレート準拠",
        "summary_total": -34850,
        "summary_count": 9,
        "summary_sub": "pair -16,100 / weekday -18,750",
        "title_summary": "-34,850円 9件・pair 3ペア(-16,100) / weekday 4件(-18,750)",
        "summary_wins": 4,
        "summary_losses": 5,
        "pairs": [
            {"label": "旭化成×DIC ペア", "sector": "化学", "display_pl": 6972, "legs": [Leg("4631", source_date="2026-05-14"), Leg("3407")]},
            {"label": "東京建物×スターツコーポレーション ペア", "sector": "不動産業", "display_pl": -13200, "legs": [Leg("8804"), Leg("8850")]},
            {"label": "岩手銀行×四国銀行 ペア", "sector": "銀行業", "display_pl": -6500, "legs": [Leg("8345"), Leg("8387")]},
        ],
        "singles": [
            {"kind": "weekday", "label": "weekday B4 LONG 銘柄", "display_pl": -18750, "codes": ["5929", "1951", "8377", "5406"]},
        ],
    },
    "2026-05-18": {
        "title": "Trade Review — 2026/05/18（月）",
        "subtitle": "戦略: mistake / pair / calendar ／ 日足: prices_max_1d ／ 5/7テンプレート準拠",
        "summary_total": -36450,
        "summary_count": 8,
        "summary_sub": "取引ミス -12,000 / pair -23,287（岩手銀行含み益込み） / calendar +550",
        "title_summary": "-36,450円 8件・取引ミス 1件(-12,000) / pair 3ペア(-23,287) / calendar 1件(+550)",
        "summary_wins": 4,
        "summary_losses": 4,
        "pairs": [
            {"label": "東京建物×スターツコーポレーション ペア", "sector": "不動産業", "display_pl": -19550, "legs": [Leg("8804"), Leg("8850", "SHORT", 200, 4815.75, 4690, 25150, "スターツコーポレーション")]},
            {
                "label": "岩手銀行×四国銀行 ペア",
                "sector": "銀行業",
                "display_pl": -4287,
                "legs": [Leg("8345", "LONG", 300, 1882, 1888, 1713, "岩手銀行", note="未決済"), Leg("8387")],
                "order_note": "order.csv照合: 岩手銀行の返済注文は本日中・未約定で大引不成になっていない。四国銀行の返済注文は大引不成・未約定。岩手銀行 +1,713円は四国銀行とのペア未決済脚としてpair損益に含める。",
            },
            {"label": "東亞合成×日本パーカライジング ペア", "sector": "化学", "legs": [Leg("4045"), Leg("4095")]},
        ],
        "singles": [
            {"kind": "取引ミス", "label": "取引ミス", "codes": [], "legs": [Leg("8850", "LONG", 200, 4860, 4800, -12000, "スターツコーポレーション", note="本来SHORT脚を買建した誤発注の返済。戦略損益から分離。")]},
            {"kind": "calendar", "label": "calendar 銘柄", "codes": ["9433"]},
        ],
    },
}


CSS = """:root{--bg:#09090b;--card:#18181b;--card-border:#27272a;--text:#fafafa;--text-muted:#a1a1aa;
--up:#34d399;--up-bg:rgba(52,211,153,0.1);--down:#fb7185;--down-bg:rgba(251,113,133,0.1);
--amber:#fbbf24;--amber-bg:rgba(251,191,36,0.15);--blue:#60a5fa;--blue-bg:rgba(96,165,250,0.1);
--purple:#a78bfa;--purple-bg:rgba(167,139,250,0.1);}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:"Helvetica Neue",-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif;
font-feature-settings:"tnum" 1,"lnum" 1;-webkit-font-smoothing:antialiased;line-height:1.6;padding:24px;max-width:1200px;margin:0 auto}
h1{font-size:1.5rem;font-weight:700;margin-bottom:6px}.subtitle{font-size:.875rem;color:var(--text-muted);margin-bottom:20px}
.section{background:var(--card);border:1px solid var(--card-border);border-radius:12px;padding:24px;margin-bottom:20px}
.section h2{font-size:1.1rem;font-weight:700;margin-bottom:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.section h3{font-size:.95rem;color:var(--text-muted);margin:16px 0 8px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}
.stat-card{background:rgba(255,255,255,0.02);border:1px solid var(--card-border);border-radius:8px;padding:16px;text-align:center}
.stat-card .label{color:var(--text-muted);font-size:.75rem;margin-bottom:4px}.stat-card .value{font-size:1.35rem;font-weight:700;font-variant-numeric:tabular-nums}.stat-card .sub{color:var(--text-muted);font-size:.7rem;margin-top:2px}
table{width:100%;border-collapse:collapse;font-size:.85rem;margin:8px 0}th{text-align:left;padding:8px 12px;background:rgba(255,255,255,0.03);color:var(--text-muted);font-weight:600;border-bottom:1px solid var(--card-border)}
td{padding:8px 12px;border-bottom:1px solid rgba(255,255,255,0.05)}td.r,th.r{text-align:right;font-variant-numeric:tabular-nums}
.num-pos{color:var(--up);font-weight:600}.num-neg{color:var(--down);font-weight:600}.num-neutral{color:var(--text-muted)}
tr.hl td{background:rgba(251,191,36,0.08)}.badge{display:inline-block;padding:2px 10px;border-radius:9999px;font-size:.72rem;font-weight:600}
.badge-long{background:var(--up-bg);color:var(--up);border:1px solid rgba(52,211,153,0.3)}.badge-short{background:var(--down-bg);color:var(--down);border:1px solid rgba(251,113,133,0.3)}
.badge-grok{background:var(--amber-bg);color:var(--amber);border:1px solid rgba(251,191,36,0.3)}.badge-pair{background:var(--purple-bg);color:var(--purple);border:1px solid rgba(167,139,250,0.3)}
.badge-weekday,.badge-calendar,.badge-sq1{background:var(--blue-bg);color:var(--blue);border:1px solid rgba(96,165,250,0.3)}
.badge-mistake{background:var(--amber-bg);color:var(--amber);border:1px solid rgba(251,191,36,0.3)}
.verdict{background:rgba(255,255,255,0.02);border-left:3px solid var(--blue);padding:10px 14px;margin:10px 0;font-size:.88rem}.verdict.ok{border-left-color:var(--up)}.verdict.bad{border-left-color:var(--down)}
.memo{background:rgba(255,255,255,0.02);border-left:3px solid var(--amber);padding:12px 16px;margin-top:12px;font-size:.88rem;color:var(--text-muted);white-space:pre-wrap}
"""


def norm_code(v: Any) -> str:
    s = str(v).strip()
    try:
        return str(int(float(s))).zfill(4)
    except ValueError:
        return s.zfill(4)


def yen(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    sign = "+" if float(v) > 0 else ""
    return f"{sign}{int(round(float(v))):,} 円"


def fmt_price(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    f = float(v)
    return f"{int(f):,}" if f == int(f) else f"{f:,.1f}"


def cls(v: float | int) -> str:
    if v > 0:
        return "num-pos"
    if v < 0:
        return "num-neg"
    return "num-neutral"


def badge_direction(direction: str) -> str:
    return "badge-long" if direction == "LONG" else "badge-short"


def badge_kind(kind: str) -> str:
    k = kind.lower()
    if k == "sq+1":
        return "badge-sq1"
    if kind == "取引ミス":
        return "badge-mistake"
    return f"badge-{k}"


def load_trades() -> dict[tuple[str, str], list[dict[str, Any]]]:
    df = pd.read_parquet(ROOT / "data" / "parquet" / "stock_results.parquet")
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for _, r in df.iterrows():
        date = pd.to_datetime(r["約定日"]).strftime("%Y-%m-%d")
        code = norm_code(r["コード"])
        direction = "LONG" if str(r["売買"]) == "ロング" else "SHORT"
        rec = {
            "date": date,
            "code": code,
            "name": str(r["銘柄名"]),
            "direction": direction,
            "qty": int(r["数量"]),
            "entry": float(r["平均取得価額"]),
            "exit": float(r["平均単価"]),
            "pl": int(round(float(r["実現損益"]))),
        }
        out.setdefault((date, code), []).append(rec)
    return out


def load_prices() -> pd.DataFrame:
    df = pd.read_parquet(ROOT / "data" / "parquet" / "prices_max_1d.parquet")
    df["date"] = pd.to_datetime(df["date"])
    oc = pd.read_parquet(ROOT / "data" / "parquet" / "prices_topix500_oc.parquet")
    oc["date"] = pd.to_datetime(oc["Date"])
    oc["ticker"] = oc["Code"].map(lambda x: f"{norm_code(str(x)[:-1])}.T")
    oc["Open"] = oc["AdjO"].astype(float)
    oc["Close"] = oc["AdjC"].astype(float)
    oc["High"] = oc[["Open", "Close"]].max(axis=1)
    oc["Low"] = oc[["Open", "Close"]].min(axis=1)
    oc["Volume"] = 0.0
    oc = oc[["date", "Open", "High", "Low", "Close", "Volume", "ticker"]]
    existing = set(df["ticker"].dropna().unique())
    fallback = oc[~oc["ticker"].isin(existing)]
    df = pd.concat([df, fallback], ignore_index=True)
    return df


def load_meta() -> pd.DataFrame:
    df = pd.read_parquet(ROOT / "data" / "parquet" / "meta_jquants.parquet")
    df["code"] = df["code"].map(norm_code)
    df["_tier"] = df["topixnewindexseries"].map(TOPIX_ORDER).fillna(99)
    return df


def find_trade(trades: dict[tuple[str, str], list[dict[str, Any]]], date: str, leg: Leg) -> dict[str, Any]:
    source_date = leg.source_date or date
    candidates = trades.get((source_date, leg.code), [])
    if leg.direction:
        candidates = [x for x in candidates if x["direction"] == leg.direction] or candidates
    if candidates:
        t = candidates[0].copy()
    else:
        t = {"date": source_date, "code": leg.code, "name": leg.name or leg.code, "direction": leg.direction or "-", "qty": leg.qty or 0, "entry": leg.entry, "exit": leg.exit, "pl": leg.pl or 0}
    for key in ["direction", "qty", "entry", "exit", "pl", "name"]:
        v = getattr(leg, key)
        if v is not None:
            t[key] = v
    t["note"] = leg.note
    return t


def day_bar(prices: pd.DataFrame, code: str, date: str) -> dict[str, float] | None:
    d = prices[(prices["ticker"] == f"{code}.T") & (prices["date"] == pd.Timestamp(date))].dropna(subset=["Close"])
    if d.empty:
        return None
    r = d.iloc[-1]
    return {k: float(r[k]) for k in ["Open", "High", "Low", "Close", "Volume"]}


def ohlc_table(prices: pd.DataFrame, code: str, date: str, fallback: dict[str, Any] | None = None) -> str:
    d = prices[(prices["ticker"] == f"{code}.T") & (prices["date"] <= pd.Timestamp(date))].dropna(subset=["Close"]).tail(10)
    if d.empty and fallback is not None and fallback.get("entry") and fallback.get("exit"):
        o = float(fallback["entry"])
        c = float(fallback["exit"])
        d = pd.DataFrame([{
            "date": pd.Timestamp(date),
            "Open": o,
            "High": max(o, c),
            "Low": min(o, c),
            "Close": c,
            "Volume": 0,
        }])
    rows = []
    prev = None
    for _, r in d.iterrows():
        o, h, l, c = float(r["Open"]), float(r["High"]), float(r["Low"]), float(r["Close"])
        oc = (c - o) / o * 100 if o else 0
        pc = None if prev is None else (c - prev) / prev * 100
        rng = (h - l) / l * 100 if l else 0
        tr_cls = ' class="hl"' if r["date"].strftime("%Y-%m-%d") == date else ""
        rows.append(
            f'<tr{tr_cls}><td>{r["date"].strftime("%m/%d")}</td><td class="r">{fmt_price(o)}</td><td class="r">{fmt_price(h)}</td><td class="r">{fmt_price(l)}</td><td class="r">{fmt_price(c)}</td>'
            f'<td class="r">{int(r["Volume"]):,}</td><td class="r {cls(oc)}">{oc:+.2f}%</td><td class="r {cls(pc or 0)}">{"-" if pc is None else f"{pc:+.2f}%"}</td><td class="r">{rng:.2f}%</td></tr>'
        )
        prev = c
    return '<table><tr><th>日付</th><th class="r">始値</th><th class="r">高値</th><th class="r">安値</th><th class="r">終値</th><th class="r">出来高</th><th class="r">寄り引け</th><th class="r">前日比</th><th class="r">レンジ</th></tr>' + "".join(rows) + "</table>"


def execution_table(prices: pd.DataFrame, t: dict[str, Any], date: str) -> str:
    bar = day_bar(prices, t["code"], date)
    if not bar or not t.get("entry") or not t.get("exit") or not t.get("qty"):
        return '<p style="color:var(--text-muted);font-size:.88rem">執行品質は算出対象外</p>'
    lo, hi = bar["Low"], bar["High"]
    rng = hi - lo
    if rng <= 0:
        return '<p style="color:var(--text-muted);font-size:.88rem">執行品質は算出対象外</p>'
    entry, exit_, qty = float(t["entry"]), float(t["exit"]), int(t["qty"])
    if t["direction"] == "SHORT":
        entry_edge = (hi - entry) / rng * 100
        exit_edge = (exit_ - lo) / rng * 100
        moc = int((entry - bar["Close"]) * qty)
    else:
        entry_edge = (entry - lo) / rng * 100
        exit_edge = (hi - exit_) / rng * 100
        moc = int((bar["Close"] - entry) * qty)
    capture = 100 - (entry_edge + exit_edge) / 2
    diff = int(t["pl"]) - moc
    return (
        '<table><tr><th>指標</th><th class="r">値</th><th>所見</th></tr>'
        f'<tr><td>建玉位置</td><td class="r">{entry_edge:.1f}%</td><td>0%=理想 / 100%=最悪</td></tr>'
        f'<tr><td>決済位置</td><td class="r">{exit_edge:.1f}%</td><td>0%=理想 / 100%=最悪</td></tr>'
        f'<tr><td>取り幅達成度</td><td class="r {cls(capture)}">{capture:+.1f}%</td><td>+100=完璧 / 0=無駄 / −=逆走</td></tr>'
        f'<tr><td>MOC保持時損益</td><td class="r {cls(moc)}">{yen(moc)}</td><td>建値→大引けまで保持した場合</td></tr>'
        f'<tr><td>実現 vs MOC差</td><td class="r {cls(diff)}">{yen(diff)}</td><td>実現損益と大引け保持の差</td></tr></table>'
    )


def pair_memo(trades: list[dict[str, Any]], total: int, verdict: str, order_note: str = "") -> str:
    long_legs = [t for t in trades if t["direction"] == "LONG"]
    short_legs = [t for t in trades if t["direction"] == "SHORT"]
    long_pl = sum(int(t["pl"]) for t in long_legs)
    short_pl = sum(int(t["pl"]) for t in short_legs)
    notes = [t.get("note", "") for t in trades if t.get("note")]
    lines = [
        f"ペア合計は {total:+,}円でスプレッドは{verdict}。",
        f"LONG脚 {long_pl:+,}円 / SHORT脚 {short_pl:+,}円。pairは片脚の勝敗ではなく両脚合計で評価する。",
    ]
    if notes:
        lines.append("建玉状態: " + " / ".join(notes))
    if order_note:
        lines.append(order_note)
    if total < 0:
        lines.append("次回確認: 入口のabs zだけでなく、PF・同業内の相対強弱・決済条件を同時確認する。")
    else:
        lines.append("次回確認: 収束した要因がセクター全体の反転か、個別脚の一時要因かを分けて記録する。")
    return "\n".join(lines)


def single_memo(t: dict[str, Any], kind: str) -> str:
    pl = int(t["pl"])
    base = f"{kind}として {t['code']} {t['name']} を{t['direction']}で処理。実現損益は {pl:+,}円。"
    lines = [base]
    if t.get("note"):
        lines.append(t["note"])
    if kind == "取引ミス":
        lines.append("この損益は戦略評価から分離し、発注プロセスの確認項目として扱う。")
    elif pl < 0:
        lines.append("損失日は、方向性の誤りか、建値/決済位置の悪さか、地合い連動かを分解して次回の入口条件へ戻す。")
    else:
        lines.append("利益日は、戦略条件が効いたのか、地合いに助けられたのかを分けて次回も同じ条件で再現できるかを見る。")
    return "\n".join(lines)


def sector_table(meta: pd.DataFrame, prices: pd.DataFrame, sector: str, codes: set[str], date: str) -> str:
    peers = meta[meta["sectors"] == sector].sort_values(["_tier", "code"]).head(8)
    rows = []
    vals = []
    for _, m in peers.iterrows():
        code = m["code"]
        d = prices[(prices["ticker"] == f"{code}.T") & (prices["date"] <= pd.Timestamp(date))].dropna(subset=["Close"]).tail(2)
        if len(d) < 2:
            continue
        prev, cur = float(d.iloc[0]["Close"]), float(d.iloc[1]["Close"])
        pct = (cur - prev) / prev * 100 if prev else 0
        vals.append(pct)
        tr = ' class="hl"' if code in codes else ""
        tag = ' <span class="badge badge-pair" style="font-size:.65rem">本日脚</span>' if code in codes else ""
        rows.append(f'<tr{tr}><td>{code}</td><td>{escape(str(m["stock_name"]))}{tag}</td><td class="r">{fmt_price(prev)}</td><td class="r">{fmt_price(cur)}</td><td class="r {cls(pct)}">{pct:+.2f}%</td></tr>')
    avg = sum(vals) / len(vals) if vals else 0
    return (
        f'<h3>セクター内騰落（{escape(sector)} n={len(vals)}, 前日比）</h3><table><tr><th>コード</th><th>銘柄</th><th class="r">前日終値</th><th class="r">当日終値</th><th class="r">前日比</th></tr>'
        + "".join(rows)
        + f'<tr style="border-top:2px solid var(--card-border)"><td colspan="4" class="r"><b>セクター単純平均</b></td><td class="r {cls(avg)}"><b>{avg:+.2f}%</b></td></tr></table>'
    )


def render_pair(meta: pd.DataFrame, prices: pd.DataFrame, trades: list[dict[str, Any]], label: str, sector: str, display_pl: int | None, date: str, order_note: str = "") -> str:
    total = display_pl if display_pl is not None else sum(int(t["pl"]) for t in trades)
    long = next((t for t in trades if t["direction"] == "LONG"), trades[0])
    short = next((t for t in trades if t["direction"] == "SHORT"), trades[-1])
    rows = []
    for t in trades:
        bar = day_bar(prices, t["code"], date)
        close = bar["Close"] if bar else t["exit"]
        rows.append(f'<tr><td>{t["code"]}</td><td>{escape(t["name"])}</td><td><span class="badge {badge_direction(t["direction"])}">{t["direction"]}</span></td><td class="r">{fmt_price(t["entry"])}</td><td class="r">{fmt_price(t["exit"])}</td><td class="r">{fmt_price(close)}</td><td class="r"><span class="{cls(t["pl"])}">{yen(t["pl"])}</span></td></tr>')
    verdict = "収束" if total > 0 else "発散"
    memo = pair_memo(trades, int(total), verdict, order_note)
    return f"""
    <div class="section">
      <h2>{escape(label)} <span class="badge badge-pair">pair</span></h2>
      <div class="grid">
        <div class="stat-card"><div class="label">ペア合計</div><div class="value"><span class="{cls(total)}">{yen(total)}</span></div><div class="sub">スプレッド {verdict}</div></div>
        <div class="stat-card"><div class="label">SHORT脚</div><div class="value" style="font-size:.95rem">{short["code"]}</div><div class="sub"><span class="{cls(short["pl"])}">{yen(short["pl"])}</span></div></div>
        <div class="stat-card"><div class="label">LONG脚</div><div class="value" style="font-size:.95rem">{long["code"]}</div><div class="sub"><span class="{cls(long["pl"])}">{yen(long["pl"])}</span></div></div>
        <div class="stat-card"><div class="label">セクター</div><div class="value" style="font-size:.95rem">{escape(sector)}</div><div class="sub">両脚同セクター</div></div>
      </div>
      <h3>両脚の約定</h3>
      <table><tr><th>コード</th><th>銘柄</th><th>方向</th><th class="r">建値</th><th class="r">決済</th><th class="r">大引け</th><th class="r">損益</th></tr>{''.join(rows)}</table>
      {sector_table(meta, prices, sector, {t["code"] for t in trades}, date)}
      <h3>セクター観察（{escape(sector)}）</h3>
      <p style="color:var(--text-muted);font-size:.88rem;line-height:1.7">両脚とも同セクター。個別材料ではなくセクター全体の相対強弱で動く。<br>ペアP&Lは <b>両脚スプレッドの収束/発散</b> で評価する。片脚単体の勝敗は意味なし。</p>
      <div class="verdict {'ok' if total > 0 else 'bad'}">ペア: スプレッド{verdict} / 合計 {total:+,}円</div>
      {'<h3>注文確認</h3><div class="memo">' + escape(order_note) + '</div>' if order_note else ''}
      <h3>メモ</h3><div class="memo">{escape(memo)}</div>
    </div>"""


def render_single(prices: pd.DataFrame, t: dict[str, Any], kind: str, date: str) -> str:
    bar = day_bar(prices, t["code"], date)
    close = bar["Close"] if bar else None
    low = bar["Low"] if bar else None
    high = bar["High"] if bar else None
    pct = int(t["pl"]) / (float(t["entry"]) * int(t["qty"])) * 100 if t.get("entry") and t.get("qty") else 0
    note = f'<div class="verdict bad">{escape(t["note"])}</div>' if t.get("note") else ""
    memo = single_memo(t, kind)
    return f"""
    <div class="section">
      <h2>{t["code"]} {escape(t["name"])} <span class="badge {badge_direction(t["direction"])}">{t["direction"]}</span> <span class="badge {badge_kind(kind)}">{escape(kind)}</span></h2>
      <div class="grid">
        <div class="stat-card"><div class="label">建値 → 決済</div><div class="value">{fmt_price(t["entry"])} → {fmt_price(t["exit"])}</div><div class="sub">{t["qty"]}株</div></div>
        <div class="stat-card"><div class="label">実現損益</div><div class="value"><span class="{cls(t["pl"])}">{yen(t["pl"])}</span></div><div class="sub">{pct:+.2f}%</div></div>
        <div class="stat-card"><div class="label">大引け</div><div class="value">{fmt_price(close)}</div><div class="sub">終値</div></div>
        <div class="stat-card"><div class="label">日中レンジ</div><div class="value" style="font-size:.95rem">{fmt_price(low)} 〜 {fmt_price(high)}</div><div class="sub">L 〜 H</div></div>
      </div>
      <h3>日足 OHLC（直近10営業日, prices_max_1d）</h3>
      {ohlc_table(prices, t["code"], date, t)}
      <h3>執行品質（0%=理想 / 100%=最悪）</h3>
      {execution_table(prices, t, date)}
      {note}
      <h3>メモ</h3><div class="memo">{escape(memo)}</div>
    </div>"""


def data_quality_section(date: str, cfg: dict[str, Any]) -> str:
    return f"""
<div class="section">
  <h2>データ照合</h2>
  <table>
    <tr><th>項目</th><th>確認元</th><th>扱い</th></tr>
    <tr><td>実現損益・約定</td><td>stock_results.parquet / stock_results__today.csv</td><td>戦略別・銘柄別損益の主ソース</td></tr>
    <tr><td>日足・大引け・執行品質</td><td>prices_max_1d.parquet / prices_topix500_oc.parquet</td><td>OHLCとMOC比較に使用</td></tr>
    <tr><td>セクター・銘柄名</td><td>meta_jquants.parquet</td><td>pair同業比較に使用</td></tr>
    <tr><td>注文・保有</td><td>order.csv / hold_stocks.csv</td><td>必要日のみ注文条件・未決済を本文に反映</td></tr>
  </table>
  <div class="memo">基準: trade_review_20260507.html の粒度。対象日 {escape(date)} / {escape(cfg['summary_sub'])}</div>
</div>"""


def render_report(date: str, cfg: dict[str, Any], trades_by_key: dict[tuple[str, str], list[dict[str, Any]]], prices: pd.DataFrame, meta: pd.DataFrame) -> str:
    pair_sections = []
    single_sections_by_kind: dict[str, list[str]] = {}
    pair_count = len(cfg["pairs"])
    for p in cfg["pairs"]:
        legs = [find_trade(trades_by_key, date, leg) for leg in p["legs"]]
        pair_sections.append(render_pair(meta, prices, legs, p["label"], p["sector"], p.get("display_pl"), date, p.get("order_note", "")))
    for group in cfg["singles"]:
        kind = group["kind"]
        items = [find_trade(trades_by_key, date, leg) for leg in group.get("legs", [])]
        items += [find_trade(trades_by_key, date, Leg(code)) for code in group.get("codes", [])]
        single_sections_by_kind.setdefault(kind, [])
        for t in items:
            single_sections_by_kind[kind].append(render_single(prices, t, kind, date))
    total = int(cfg["summary_total"])
    wins, losses = int(cfg["summary_wins"]), int(cfg["summary_losses"])
    all_trades = []
    for p in cfg["pairs"]:
        all_trades += [find_trade(trades_by_key, date, leg) for leg in p["legs"]]
    for g in cfg["singles"]:
        all_trades += [find_trade(trades_by_key, date, leg) for leg in g.get("legs", [])]
        all_trades += [find_trade(trades_by_key, date, Leg(code)) for code in g.get("codes", [])]
    max_loss = min(all_trades, key=lambda x: x["pl"])
    max_win = max(all_trades, key=lambda x: x["pl"])
    single_sub = " / ".join(f"{kind} {len(sections)}件" for kind, sections in single_sections_by_kind.items())
    top_sub = f"{single_sub} / pair {pair_count * 2}件" if pair_count else single_sub
    body = [data_quality_section(date, cfg), f'<h2 style="margin:24px 0 12px">pair 銘柄（{pair_count}ペア）</h2>', *pair_sections]
    for kind, sections in single_sections_by_kind.items():
        body.append(f'<h2 style="margin:24px 0 12px">{escape(kind)} 銘柄（{len(sections)}件）</h2>')
        body.extend(sections)
    title_line = f"{cfg['title']} - {cfg['title_summary']}"
    return f"""<!doctype html>
<html lang="ja"><head><meta charset="utf-8"><title>{escape(title_line)}</title><style>{CSS}</style></head>
<body>
<h1>{escape(title_line)}</h1><div class="subtitle">{escape(cfg['subtitle'])}</div>
<div class="section"><div class="grid">
<div class="stat-card"><div class="label">約定件数</div><div class="value">{cfg['summary_count']}</div><div class="sub">{escape(top_sub)}</div></div>
<div class="stat-card"><div class="label">合計損益</div><div class="value"><span class="{cls(total)}">{yen(total)}</span></div><div class="sub">{escape(cfg['summary_sub'])}</div></div>
<div class="stat-card"><div class="label">勝ち / 負け</div><div class="value">{wins} / {losses}</div><div class="sub">勝率 {wins/(wins+losses)*100:.1f}%</div></div>
<div class="stat-card"><div class="label">最大損失 / 最大利益</div><div class="value" style="font-size:.95rem">{int(max_loss['pl']):+,} / {int(max_win['pl']):+,}</div><div class="sub">{max_loss['code']} / {max_win['code']}</div></div>
</div></div>
{''.join(body)}
</body></html>"""


def main() -> None:
    trades = load_trades()
    prices = load_prices()
    meta = load_meta()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for date, cfg in CONFIG.items():
        path = OUT_DIR / f"trade_review_{date.replace('-', '')}.html"
        path.write_text(render_report(date, cfg, trades, prices, meta), encoding="utf-8")
        print(path, path.stat().st_size)


if __name__ == "__main__":
    main()
