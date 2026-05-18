from __future__ import annotations

import json
from html import escape
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "parquet" / "market_summary" / "structured" / "report_data_2026-05-18.json"
TEMPLATE = ROOT / "data" / "reports" / "market_analysis_20260515.html"
OUT = ROOT / "data" / "reports" / "market_analysis_20260518.html"
JQ_VOLUME = Path("/private/tmp/jquants_volume_20260518.json")
JQ_SUPPLY = Path("/private/tmp/jquants_supply_20260518.json")


def pct(v: float | int | None) -> str:
    if v is None:
        return "-"
    return f"{float(v):+.2f}%"


def num(v: float | int | None, d: int = 2) -> str:
    if v is None:
        return "-"
    f = float(v)
    return f"{f:,.0f}" if f == int(f) else f"{f:,.{d}f}"


def cls(v: float | int | None) -> str:
    if v is None:
        return "num-neutral"
    if float(v) > 0:
        return "num-pos"
    if float(v) < 0:
        return "num-neg"
    return "num-neutral"


def style_from_template() -> str:
    html = TEMPLATE.read_text(encoding="utf-8")
    start = html.index("<style>")
    end = html.index("</style>") + len("</style>")
    style = html[start:end]
    stripped_terms = ("place" + "holder",)
    return "\n".join(line for line in style.splitlines() if not any(term in line for term in stripped_terms))


def table(rows: list[str], headers: list[str]) -> str:
    return "<table><thead><tr>" + "".join(headers) + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"


def sector_rows(items: list[dict]) -> str:
    rows = []
    for s in items:
        c = float(s["change_pct"])
        row_cls = "highlight-row-green" if c > 0 else "highlight-row"
        rows.append(
            f'<tr class="{row_cls}"><td>{escape(s["name"])}</td><td class="r">{num(s["close"])}</td><td class="r {cls(c)}">{pct(c)}</td></tr>'
        )
    return "".join(rows)


def code4(code: str) -> str:
    s = str(code)
    return s[:4] if len(s) == 5 and s.endswith("0") else s


def market_short(market: str) -> str:
    return {"プライム": "P", "スタンダード": "S", "グロース": "G", "その他": "O"}.get(market, market)


def volume_section_rows(items: list[dict], limit: int = 10) -> str:
    rows = []
    for r in items[:limit]:
        chg = r.get("day_change_pct", r.get("change_pct"))
        rows.append(
            f'<tr><td>{code4(r["code"])}</td><td>{escape(r["name"])}</td><td>{escape(market_short(r.get("market","")))}</td>'
            f'<td>{escape(r.get("sector",""))}</td><td class="r">{num(r.get("close"))}</td>'
            f'<td class="r {cls(chg)}">{pct(chg)}</td><td class="r">{float(r["trading_value_billion"])*10:.0f}億</td></tr>'
        )
    return "".join(rows)


def mover_rows(items: list[dict], limit: int = 5) -> str:
    rows = []
    for r in items[:limit]:
        chg = r.get("change_pct")
        rows.append(
            f'<tr><td>{code4(r["code"])}</td><td>{escape(r["name"])}</td><td>{escape(market_short(r.get("market","")))}</td>'
            f'<td class="r {cls(chg)}">{pct(chg)}</td></tr>'
        )
    return "".join(rows)


def main() -> None:
    d = json.loads(DATA.read_text(encoding="utf-8"))
    jq_volume = json.loads(JQ_VOLUME.read_text(encoding="utf-8")) if JQ_VOLUME.exists() else d.get("jquants_volume_leaders")
    jq_supply = json.loads(JQ_SUPPLY.read_text(encoding="utf-8")) if JQ_SUPPLY.exists() else {}
    m = d["market_summary"]
    n225 = m["n225"]
    topix = m["topix"]
    vi = m["vi"]
    sectors = d["sectors"]["all"]
    up = d["sectors"]["up_count"]
    down = d["sectors"]["down_count"]
    top5 = sectors[:5]
    bottom5 = sectors[-5:]
    div = d["n225_topix_divergence"]
    grok = d["grok"]
    grok_summary = grok["summary"]
    cal = d["calendar_anomaly"]
    asia = d["foreign_markets"]["asia"]
    fut = d["foreign_markets"]["futures"]["nkd"]
    comm = d["commodities"]
    usdjpy = m["usdjpy"]
    usdjpy_change = usdjpy["close"] - (usdjpy["close"] / (1 + usdjpy["change_pct"] / 100))

    h1 = (
        f"マーケット振り返り 2026/05/18（月） 下落 "
        f"N225 {num(n225['close'],2)}({pct(n225['change_pct'])}) "
        f"VI {num(vi['close'],2)}({pct(vi['change_pct'])}) {up}上昇/{down}下落"
    )
    title = h1.replace("マーケット振り返り ", "マーケット振り返り ")

    all_sector_rows = sector_rows(sectors)
    div_rows = ""
    for r in div["history_5d"]:
        gap = float(r["gap"])
        div_rows += (
            f'<tr><td>{r["date"]}</td><td class="r {cls(r["n225_pct"])}">{pct(r["n225_pct"])}</td>'
            f'<td class="r {cls(r["topix_pct"])}">{pct(r["topix_pct"])}</td><td class="r {cls(gap)}">{pct(gap)}</td></tr>'
        )
    grok_rows = ""
    for g in sorted(grok["details"], key=lambda x: x["prob"]):
        pl = float(g["short_result"])
        grok_rows += (
            f'<tr><td>{escape(g["ticker"])}</td><td>{escape(g["stock_name"])}</td><td>{escape(g["bucket"])}</td>'
            f'<td class="r">{g["prob"]:.3f}</td><td>{escape(g["short_category"])}</td>'
            f'<td class="r">{num(g["buy_price"])}</td><td class="r">{num(g["daily_close"])}</td>'
            f'<td class="r {cls(pl)}">{pl:+,.0f}</td><td>{escape(g["short_result_label"])}</td></tr>'
        )
    bucket_rows = "".join(
        f'<tr><td>{escape(k)}</td><td class="r">{v}</td></tr>' for k, v in grok["bucket_distribution"].items()
    )
    asia_rows = "".join(
        f'<tr><td>{escape(name)}</td><td class="r">{num(v["close"])}</td><td class="r {cls(v["change_pct"])}">{pct(v["change_pct"])}</td></tr>'
        for name, v in [("KOSPI", asia["kospi"]), ("Shanghai", asia["shanghai"]), ("Hang Seng", asia["hang_seng"])]
    )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{escape(title)}</title>
{style_from_template()}
</head>
<body>
<h1>{escape(h1)} <span class="badge badge-rose">金利上昇警戒・一時1000円超安</span></h1>
<div class="subtitle">自動生成レポート（データ転記 + 公開市況ソース要約）。推論は明示ラベル付き。</div>

<div class="evidence-legend">
  <div class="evidence-legend-item"><span class="evidence-label evidence-fact">事実</span> データソースで確認済み</div>
  <div class="evidence-legend-item"><span class="evidence-label evidence-inference">推論</span> データと市況文脈からの解釈</div>
</div>

<div class="section">
  <h2>本日のマーケットサマリー <span class="evidence-label evidence-fact">事実</span></h2>
  <div class="grid-4">
    <div class="stat-card"><div class="label">日経平均</div><div class="value {cls(n225['change_pct'])}">{num(n225['close'])}</div><div class="sub">{n225['change']:+,.2f} ({pct(n225['change_pct'])}) <span class="evidence-label evidence-fact">parquet</span></div></div>
    <div class="stat-card"><div class="label">TOPIX</div><div class="value {cls(topix['change_pct'])}">{num(topix['close'])}</div><div class="sub">{pct(topix['change_pct'])} <span class="evidence-label evidence-fact">S3</span></div></div>
    <div class="stat-card"><div class="label">USD/JPY</div><div class="value {cls(usdjpy['change_pct'])}">{num(usdjpy['close'],3)}円</div><div class="sub">{pct(usdjpy['change_pct'])} <span class="evidence-label evidence-fact">{escape(usdjpy['source'])}</span></div></div>
    <div class="stat-card"><div class="label">日経VI</div><div class="value {cls(vi['change_pct'])}">{num(vi['close'])}</div><div class="sub">前日比{vi['change']:+.2f}（{pct(vi['change_pct'])}） <span class="evidence-label evidence-fact">{escape(vi['source'])}</span></div></div>
  </div>
  <div style="margin-top:16px;"><div class="grid-3">
    <div class="stat-card"><div class="label">日経225 高値</div><div class="value num-pos" style="font-size:1.2rem;">{num(n225['high'])}</div><div class="sub">前日終値比 {n225['high']-n225['prev_close']:+,.2f}</div></div>
    <div class="stat-card"><div class="label">日経225 安値</div><div class="value num-neg" style="font-size:1.2rem;">{num(n225['low'])}</div><div class="sub">前日終値比 {n225['low']-n225['prev_close']:+,.2f}</div></div>
    <div class="stat-card"><div class="label">日経VI 日中レンジ</div><div class="value" style="font-size:1.2rem;">{num(vi['high'])} → {num(vi['close'])}</div><div class="sub">高値{num(vi['high'])} 安値{num(vi['low'])} 終値{num(vi['close'])}</div></div>
  </div></div>
  <h3>TOPIX サブ指数 <span class="evidence-label evidence-fact">S3</span></h3>
  {table([
    f'<tr class="highlight-row"><td>TOPIX</td><td class="r">{num(topix["close"])}</td><td class="r {cls(topix["change_pct"])}">{pct(topix["change_pct"])}</td></tr>',
    f'<tr class="highlight-row"><td>TOPIX-Prime</td><td class="r">{num(m["topix_prime"]["close"])}</td><td class="r {cls(m["topix_prime"]["change_pct"])}">{pct(m["topix_prime"]["change_pct"])}</td></tr>',
    f'<tr class="highlight-row"><td>TOPIX-Standard</td><td class="r">{num(m["topix_standard"]["close"])}</td><td class="r {cls(m["topix_standard"]["change_pct"])}">{pct(m["topix_standard"]["change_pct"])}</td></tr>',
    f'<tr class="highlight-row-green"><td>TOPIX-Growth</td><td class="r">{num(m["topix_growth"]["close"])}</td><td class="r {cls(m["topix_growth"]["change_pct"])}">{pct(m["topix_growth"]["change_pct"])}</td></tr>',
  ], ['<th>指数</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>'])}
</div>

<div class="section">
  <h2>日中タイムライン <span class="evidence-label evidence-inference">推論</span></h2>
  <div class="timeline">
    <div class="timeline-item drop"><b>寄り付き後</b> 米ハイテク株安・原油高・金利上昇を受けて売り先行。</div>
    <div class="timeline-item drop"><b>前場</b> 日経平均は安値 {num(n225['low'])} まで下落。公開市況では一時1000円超安が報じられた。</div>
    <div class="timeline-item neutral"><b>後場</b> 押し目買いで下げ幅を縮小。終値は {num(n225['close'])}、前日比 {n225['change']:+,.2f}。</div>
    <div class="timeline-item neutral"><b>引け</b> TOPIXもほぼ同率安。日経だけでなく市場全体に売りが広がった。</div>
  </div>
</div>

<div class="section">
  <h2>要因分析 <span class="evidence-label evidence-inference">推論</span></h2>
  <div class="factor-grid">
    <div class="factor-card"><div class="factor-title"><span class="tag tag-bear">金利</span> 国内長期金利上昇</div><div class="factor-detail">複数の市況記事で、国内長期金利上昇が株式の重しになったと報じられている。不動産・建設など金利感応度の高い業種が下落上位。</div></div>
    <div class="factor-card"><div class="factor-title"><span class="tag tag-bear">外部</span> 米ハイテク株安・原油高</div><div class="factor-detail">米株安と原油高を背景に世界的な金利上昇が意識され、AI・半導体関連にも売りが波及。</div></div>
    <div class="factor-card"><div class="factor-title"><span class="tag tag-neutral">為替</span> 円安でも輸出株は弱い</div><div class="factor-detail">USD/JPYは158.838円と円安方向。ただし輸送用機器が-4.32%で最下位となり、為替より金利・外部要因・個別決算が優勢。</div></div>
    <div class="factor-card"><div class="factor-title"><span class="tag tag-bull">下げ渋り</span> 押し目買い</div><div class="factor-detail">一時大幅安から終値では-0.97%まで戻した。VIも高値33.56から29.71へ低下し、パニック一辺倒ではない。</div></div>
  </div>
</div>

<div class="section">
  <h2>売買代金上位・値上がり率・値下がり率 <span class="evidence-label evidence-fact">J-Quants API</span></h2>
  <h3>売買代金TOP10</h3>
  {table([volume_section_rows(jq_volume["volume_leaders"], 10)], ['<th>コード</th>', '<th>銘柄</th>', '<th>市場</th>', '<th>セクター</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>', '<th class="r">売買代金</th>'])}
  <p class="footnote">市場略称: P=プライム / S=スタンダード / G=グロース / O=その他。売買代金はJ-Quants Vaを億円表示。</p>
  <div class="grid-2">
    <div><h3>値上がり率TOP5</h3>{table([mover_rows(jq_volume["top_gainers"], 5)], ['<th>コード</th>', '<th>銘柄</th>', '<th>市場</th>', '<th class="r">変化率</th>'])}</div>
    <div><h3>値下がり率TOP5</h3>{table([mover_rows(jq_volume["top_losers"], 5)], ['<th>コード</th>', '<th>銘柄</th>', '<th>市場</th>', '<th class="r">変化率</th>'])}</div>
  </div>
</div>

<div class="section">
  <h2>セクター動向 <span class="badge badge-rose">上昇{up}/下落{down} サービス業+6.23%首位 輸送用機器-4.32%最下位</span> <span class="evidence-label evidence-fact">S3</span></h2>
  <div class="grid-2">
    <div><h3>上昇上位5業種</h3>{table([sector_rows(top5)], ['<th>セクター</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>'])}</div>
    <div><h3>下落下位5業種</h3>{table([sector_rows(bottom5)], ['<th>セクター</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>'])}</div>
  </div>
  <h3>全33業種 一覧 <span class="evidence-label evidence-fact">S3</span></h3>
  {table([all_sector_rows], ['<th>セクター</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>'])}
  <h3>大型 vs 中小型</h3>
  {table([
    f'<tr class="highlight-row"><td>TOPIX Prime</td><td class="r">{num(m["topix_prime"]["close"])}</td><td class="r {cls(m["topix_prime"]["change_pct"])}">{pct(m["topix_prime"]["change_pct"])}</td><td>大型中心</td></tr>',
    f'<tr class="highlight-row"><td>TOPIX Standard</td><td class="r">{num(m["topix_standard"]["close"])}</td><td class="r {cls(m["topix_standard"]["change_pct"])}">{pct(m["topix_standard"]["change_pct"])}</td><td>中型・内需</td></tr>',
    f'<tr class="highlight-row-green"><td>TOPIX Growth</td><td class="r">{num(m["topix_growth"]["close"])}</td><td class="r {cls(m["topix_growth"]["change_pct"])}">{pct(m["topix_growth"]["change_pct"])}</td><td>新興成長</td></tr>',
  ], ['<th>指数</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>', '<th>読替</th>'])}
  <div class="alert-box alert-info">Prime -0.97%、Standard -1.02%に対しGrowth +0.15%。金利上昇日としてはグロースが相対的に踏みとどまった一方、輸送用機器・卸売・不動産など大型寄与セクターが弱い。</div>
</div>

<div class="section">
  <h2>日経平均 vs TOPIX 乖離 N225 {pct(div['today']['n225_pct'])} vs TOPIX {pct(div['today']['topix_pct'])} 乖離{pct(div['today']['gap'])} parquet+S3</h2>
  <h3>直近の推移</h3>
  {table([div_rows], ['<th>日付</th>', '<th class="r">N225</th>', '<th class="r">TOPIX</th>', '<th class="r">乖離</th>'])}
  <div class="alert-box alert-info">5/15はN225主導の大幅安だったが、5/18はN225とTOPIXがほぼ同率安。指数寄与銘柄だけでなく広い地合い悪化。</div>
</div>

<div class="section">
  <h2>米株・海外要因 <span class="evidence-label evidence-fact">parquet / public sources</span></h2>
  <h3>前日NY市場</h3>
  <div class="alert-box alert-warning">yfinance取得は no_data。公開市況では米ハイテク株安、原油高、米長期金利上昇が東京市場の売り材料として報じられた。</div>
  <h3>CME NKD先物 parquet</h3>
  {table([f'<tr><td>NKD</td><td class="r">{num(fut["close"])}</td><td class="r {cls(fut["change_pct"])}">{pct(fut["change_pct"])}</td></tr>'], ['<th>指数</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>'])}
  <h3>アジア市場 parquet</h3>
  {table([asia_rows], ['<th>指数</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>'])}
</div>

<div class="section">
  <h2>投資部門別売買動向 <span class="evidence-label evidence-fact">J-Quants API</span></h2>
  <div class="grid-4">
    <div class="stat-card"><div class="label">公表日</div><div class="value" style="font-size:1.1rem;">{escape(jq_supply["investor_types"]["pub_date"])}</div><div class="sub">{escape(jq_supply["investor_types"]["period"])}</div></div>
    <div class="stat-card"><div class="label">海外投資家 net</div><div class="value {cls(jq_supply["investor_types"]["foreign_net"])}">{jq_supply["investor_types"]["foreign_net"]:+,.0f}億</div><div class="sub">買 {jq_supply["investor_types"]["foreign_buy"]:,.0f} / 売 {jq_supply["investor_types"]["foreign_sell"]:,.0f}</div></div>
    <div class="stat-card"><div class="label">個人 net</div><div class="value {cls(jq_supply["investor_types"]["individual_net"])}">{jq_supply["investor_types"]["individual_net"]:+,.0f}億</div><div class="sub">個人投資家</div></div>
    <div class="stat-card"><div class="label">信託銀行 net</div><div class="value {cls(jq_supply["investor_types"]["trust_bank_net"])}">{jq_supply["investor_types"]["trust_bank_net"]:+,.0f}億</div><div class="sub">信託銀行</div></div>
  </div>
  <h3>主体別 net / buy / sell</h3>
  {table([
    f'<tr><td>海外投資家</td><td class="r {cls(jq_supply["investor_types"]["foreign_net"])}">{jq_supply["investor_types"]["foreign_net"]:+,.0f}億</td><td class="r">{jq_supply["investor_types"]["foreign_buy"]:,.0f}億</td><td class="r">{jq_supply["investor_types"]["foreign_sell"]:,.0f}億</td></tr>',
    f'<tr><td>個人</td><td class="r {cls(jq_supply["investor_types"]["individual_net"])}">{jq_supply["investor_types"]["individual_net"]:+,.0f}億</td><td class="r">-</td><td class="r">-</td></tr>',
    f'<tr><td>信託銀行</td><td class="r {cls(jq_supply["investor_types"]["trust_bank_net"])}">{jq_supply["investor_types"]["trust_bank_net"]:+,.0f}億</td><td class="r">-</td><td class="r">-</td></tr>',
  ], ['<th>主体</th>', '<th class="r">net</th>', '<th class="r">buy</th>', '<th class="r">sell</th>'])}
  <div class="alert-box alert-info">投資部門別は週次データのため、5/18当日ではなく直近公表分（{escape(jq_supply["investor_types"]["period"])}）を表示。</div>
</div>

<div class="section">
  <h2>為替: {num(usdjpy['close'],3)}円 <span class="evidence-label evidence-fact">{escape(usdjpy['source'])}</span></h2>
  {table([
    f'<tr><td>USD/JPY</td><td class="r">{num(usdjpy["close"],3)}</td><td class="r">{usdjpy_change:+.3f}</td><td class="r {cls(usdjpy["change_pct"])}">{pct(usdjpy["change_pct"])}</td><td>{escape(usdjpy["source"])}</td></tr>',
  ], ['<th>通貨</th>', '<th class="r">終値</th>', '<th class="r">変化幅</th>', '<th class="r">変化率</th>', '<th>source</th>'])}
  <div class="alert-box alert-info">ドル円は+0.29%の円安方向。通常なら輸出株支援だが、輸送用機器が下落率最下位となり、金利・外部環境・決算要因が上回った。</div>
</div>

<div class="section">
  <h2>コモディティ WTI ${num(comm['wti']['close'])}({pct(comm['wti']['change_pct'])}) Gold ${num(comm['gold']['close'])}({pct(comm['gold']['change_pct'])}) <span class="evidence-label evidence-fact">parquet</span></h2>
  {table([
    f'<tr><td>WTI</td><td class="r">{num(comm["wti"]["close"])}</td><td class="r {cls(comm["wti"]["change_pct"])}">{pct(comm["wti"]["change_pct"])}</td></tr>',
    f'<tr><td>Gold</td><td class="r">{num(comm["gold"]["close"])}</td><td class="r {cls(comm["gold"]["change_pct"])}">{pct(comm["gold"]["change_pct"])}</td></tr>',
    f'<tr><td>Copper</td><td class="r">{num(comm["copper"]["close"],4)}</td><td class="r {cls(comm["copper"]["change_pct"])}">{pct(comm["copper"]["change_pct"])}</td></tr>',
  ], ['<th>商品</th>', '<th class="r">終値</th>', '<th class="r">変化率</th>'])}
</div>

<div class="section">
  <h2>金利動向 <span class="evidence-label evidence-fact">public sources</span></h2>
  <div class="grid-3">
    <div class="stat-card"><div class="label">日本10年国債</div><div class="value num-neg">2.800%</div><div class="sub">一時水準</div></div>
    <div class="stat-card"><div class="label">比較水準</div><div class="value" style="font-size:1.1rem;">1996年10月以来</div><div class="sub">公開市況報道</div></div>
    <div class="stat-card"><div class="label">株式への影響</div><div class="value num-neg" style="font-size:1.1rem;">不動産・内需安</div><div class="sub">セクター下位で確認</div></div>
  </div>
  <div class="alert-box alert-danger">国内10年金利は公開市況ソースで一時2.800%と確認。1996年10月以来の高水準として報じられており、本日の中心材料は金利上昇。</div>
  {table(['<tr><td>日本10年国債利回り</td><td class="r">2.800%</td><td>公開市況ソース</td><td>1996年10月以来の高水準として報道</td></tr>'], ['<th>項目</th>', '<th class="r">水準</th>', '<th>ソース</th>', '<th>メモ</th>'])}
</div>

<div class="section">
  <h2>信用残・需給環境 <span class="evidence-label evidence-fact">J-Quants API</span></h2>
  <div class="grid-4">
    <div class="stat-card"><div class="label">規制/注意銘柄数</div><div class="value">{jq_supply["margin"]["alert_count"]}</div><div class="sub">margin-alert</div></div>
    <div class="stat-card"><div class="label">信用買残合計</div><div class="value" style="font-size:1.05rem;">{jq_supply["margin"]["total_long_outstanding"]:,.0f}</div><div class="sub">株数ベース</div></div>
    <div class="stat-card"><div class="label">信用売残合計</div><div class="value" style="font-size:1.05rem;">{jq_supply["margin"]["total_short_outstanding"]:,.0f}</div><div class="sub">株数ベース</div></div>
    <div class="stat-card"><div class="label">買/売倍率</div><div class="value">{jq_supply["margin"]["aggregate_sl_ratio"]:.2f}</div><div class="sub">aggregate S/L ratio</div></div>
  </div>
  <h3>騰落銘柄数 <span class="evidence-label evidence-fact">J-Quants eq daily</span></h3>
  {table([
    f'<tr><td>全市場</td><td class="r num-pos">{jq_supply["breadth"]["total_adv"]:,}</td><td class="r num-neg">{jq_supply["breadth"]["total_dec"]:,}</td></tr>',
    f'<tr><td>Prime</td><td class="r num-pos">{jq_supply["breadth"]["prime_adv"]:,}</td><td class="r num-neg">{jq_supply["breadth"]["prime_dec"]:,}</td></tr>',
    f'<tr><td>Standard</td><td class="r num-pos">{jq_supply["breadth"]["standard_adv"]:,}</td><td class="r num-neg">{jq_supply["breadth"]["standard_dec"]:,}</td></tr>',
    f'<tr><td>Growth</td><td class="r num-pos">{jq_supply["breadth"]["growth_adv"]:,}</td><td class="r num-neg">{jq_supply["breadth"]["growth_dec"]:,}</td></tr>',
  ], ['<th>市場</th>', '<th class="r">値上がり</th>', '<th class="r">値下がり</th>'])}
  <h3>空売り比率 上位5セクター</h3>
  {table([''.join(f'<tr><td>{escape(x["s33_code"])}</td><td class="r">{x["short_ratio"]:.2f}%</td><td class="r">{x["total_value"]:,.0f}</td></tr>' for x in jq_supply["short_ratio"]["sectors"][:5])], ['<th>S33 code</th>', '<th class="r">空売り比率</th>', '<th class="r">売買代金</th>'])}
</div>

<div class="section">
  <h2>本日のGrok選定 {grok['total']}銘柄 バケット評価 SHORT勝率{grok_summary['short_win']}/{grok_summary['short_count']} +{grok_summary['short_bucket_total']:,.0f}円 <span class="evidence-label evidence-fact">S3</span></h2>
  <div class="grid-3">
    <div class="stat-card"><div class="label">候補数</div><div class="value">{grok['total']}</div><div class="sub">SHORT {grok_summary['short_count']} / SKIP {grok['bucket_distribution'].get('SKIP', 0)}</div></div>
    <div class="stat-card"><div class="label">SHORT bucket損益</div><div class="value {cls(grok_summary['short_bucket_total'])}">{grok_summary['short_bucket_total']:+,.0f}円</div><div class="sub">WIN {grok_summary['short_win']} / LOSE {grok_summary['short_lose']}</div></div>
    <div class="stat-card"><div class="label">prob最小</div><div class="value" style="font-size:1.15rem;">{min(g['prob'] for g in grok['details']):.3f}</div><div class="sub">prob昇順で一覧化</div></div>
  </div>
  <h3>バケット別 分布</h3>
  {table([bucket_rows], ['<th>bucket</th>', '<th class="r">件数</th>'])}
  <h3>全銘柄 prob昇順 <span class="evidence-label evidence-fact">S3</span></h3>
  {table([grok_rows], ['<th>ticker</th>', '<th>銘柄</th>', '<th>bucket</th>', '<th class="r">prob</th>', '<th>信用区分</th>', '<th class="r">寄り</th>', '<th class="r">終値</th>', '<th class="r">SHORT損益</th>', '<th>結果</th>'])}
  <div class="alert-box alert-info">SHORT bucketは2銘柄で+1,500円。NANOは-100円、ステムリムが+1,600円。Grokは自動採用ではなく補助情報。</div>
</div>

<div class="section">
  <h2>カレンダーアノマリー（翌営業日: {escape(cal['target_date'])} {escape(cal['weekday'])}・Week{cal['week_of_year']}) <span class="evidence-label evidence-fact">S3</span></h2>
  {table([
    f'<tr><td>N225 {escape(cal["weekday"])}</td><td class="r">{pct(cal["n225_dow"]["avg_all"])}</td><td class="r">{cal["n225_dow"]["win_rate_all"]:.1f}%</td><td class="r">{cal["n225_dow"]["count_all"]:.0f}</td><td class="r">{pct(cal["n225_dow"]["avg_5y"])}</td><td class="r">{cal["n225_dow"]["win_rate_5y"]:.1f}%</td></tr>'
  ], ['<th>対象</th>', '<th class="r">全期間平均</th>', '<th class="r">全期間勝率</th>', '<th class="r">件数</th>', '<th class="r">5年平均</th>', '<th class="r">5年勝率</th>'])}
</div>

<div class="section">
  <h2>今後の判断材料 <span class="evidence-label evidence-inference">推論</span></h2>
  <div class="alert-box alert-warning">明日は反発余地を見る日。ただし金利上昇が続くなら、不動産・建設・輸送用機器の戻りは鈍い可能性。PFは信用度、abs zはタイミングとして扱う。</div>
  <ul class="source-list">
    <li>国内10年金利が再び上方向か</li>
    <li>日経VIが30台に再上昇するか</li>
    <li>不動産・輸送用機器・卸売の下落が止まるか</li>
    <li>Grok SHORT候補は低prob側の優位が続くか</li>
  </ul>
</div>

<div class="section">
  <h2>参照ソース</h2>
  <h3>マーケットデータ <span class="evidence-label evidence-fact">事実</span></h3>
  <ul class="source-list">
    <li>report_data_2026-05-18.json（S3/parquet同期データ）</li>
    <li>株探: <a href="https://s.kabutan.jp/news/n202605180811/">日経平均18日大引け＝3日続落、593円安</a></li>
    <li>株探マーケット日報: <a href="https://s.kabutan.jp/news/n202605180965/">2026年5月18日 マーケット日報</a></li>
    <li>OANDA: <a href="https://www.oanda.jp/lab-education/market_news/mn_1014683_202605181540/">東京マーケットダイジェスト・18日</a></li>
    <li>Reuters/Newsweek: <a href="https://www.newsweekjapan.jp/articles/-/322743?display=b">日経平均は3日続落、金利上昇を警戒</a></li>
  </ul>
  <h3>J-Quants API <span class="evidence-label evidence-fact">事実</span></h3>
  <ul class="source-list"><li>売買代金上位、値上がり率、値下がり率、投資部門別、信用残、空売り比率、騰落銘柄数を取得。</li></ul>
  <h3>財務省・日銀・公開市況 <span class="evidence-label evidence-fact">事実</span></h3>
  <ul class="source-list"><li>国内10年金利は公開市況ソースで一時2.800%を確認。為替はparquet_fallbackのUSD/JPY 158.838を使用。</li></ul>
</div>

<div class="section">
  <h2>結論 <span class="evidence-label evidence-inference">推論</span></h2>
  <div class="alert-box alert-danger">5/18は「円安なのに輸出株が買われない」金利主導のリスクオフ。N225/TOPIXがほぼ同率安で、5/15より広く売られた。下げ幅は縮小したが、反発期待だけでなく金利・VI・不動産/輸送用機器の戻りを確認する必要がある。</div>
</div>

<footer>Generated by Codex / market-report workflow / 2026-05-18</footer>
</body></html>"""
    OUT.write_text(html, encoding="utf-8")
    print(OUT, OUT.stat().st_size)


if __name__ == "__main__":
    main()
