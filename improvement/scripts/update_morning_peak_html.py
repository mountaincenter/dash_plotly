"""
高値崩れ常習犯リスト - 差分更新スクリプト

機能:
- CSVのチェック状態をマスターとして保持
- 新規銘柄は「NEW」マーク付きで追加
- 条件外になった銘柄は「除外候補」表示
- detected_at（初回検出日）、last_seen_at（最終検出日）で鮮度管理
"""

import pandas as pd
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

PARQUET_PATH = DATA_DIR / "morning_peak_watchlist.parquet"
MASTER_CSV_PATH = OUTPUT_DIR / "morning_peak_shortable.csv"
HTML_PATH = OUTPUT_DIR / "morning_peak_watchlist.html"


def load_master_csv():
    """マスターCSV読み込み（なければ空DataFrame）"""
    if MASTER_CSV_PATH.exists():
        df = pd.read_csv(MASTER_CSV_PATH)
        # 日付カラムがなければ追加
        if 'detected_at' not in df.columns:
            df['detected_at'] = date.today().isoformat()
        if 'last_seen_at' not in df.columns:
            df['last_seen_at'] = date.today().isoformat()
        if 'status' not in df.columns:
            df['status'] = 'active'
        return df
    return pd.DataFrame()


def merge_data(df_master: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    """マスターと新データをマージ"""
    today = date.today().isoformat()

    master_tickers = set(df_master['ticker'].tolist()) if len(df_master) > 0 else set()
    new_tickers = set(df_new['ticker'].tolist())

    # 新規銘柄
    added = new_tickers - master_tickers
    # 継続銘柄
    continued = new_tickers & master_tickers
    # 除外候補（マスターにあって新データにない）
    removed = master_tickers - new_tickers

    print(f"  継続: {len(continued)}件")
    print(f"  新規: {len(added)}件")
    print(f"  除外候補: {len(removed)}件")

    results = []

    # 継続銘柄: チェック状態保持 + last_seen_at更新
    for ticker in continued:
        master_row = df_master[df_master['ticker'] == ticker].iloc[0].to_dict()
        new_row = df_new[df_new['ticker'] == ticker].iloc[0]

        master_row.update({
            'stock_name': new_row['stock_name'],
            'market': new_row['market'],
            'morning_peak_count': new_row['morning_peak_count'],
            'am_peak_count': new_row['am_peak_count'],
            'pm_peak_count': new_row['pm_peak_count'],
            'am_peak_ratio': new_row['am_peak_ratio'],
            'avg_drop': new_row['avg_drop'],
            'latest_close': new_row['latest_close'],
            'recovery_rate': new_row.get('recovery_rate', 0),
            'last_seen_at': today,
            'status': 'active'
        })
        results.append(master_row)

    # 新規銘柄
    for ticker in added:
        new_row = df_new[df_new['ticker'] == ticker].iloc[0]
        results.append({
            'ticker': ticker,
            'stock_name': new_row['stock_name'],
            'market': new_row['market'],
            'morning_peak_count': new_row['morning_peak_count'],
            'am_peak_count': new_row['am_peak_count'],
            'pm_peak_count': new_row['pm_peak_count'],
            'am_peak_ratio': new_row['am_peak_ratio'],
            'avg_drop': new_row['avg_drop'],
            'latest_close': new_row['latest_close'],
            'recovery_rate': new_row.get('recovery_rate', 0),
            'shortable': False,
            'day_trade': False,
            'ng': False,
            'detected_at': today,
            'last_seen_at': today,
            'status': 'new'
        })

    # 除外候補: チェック状態保持 + status変更
    for ticker in removed:
        master_row = df_master[df_master['ticker'] == ticker].iloc[0].to_dict()
        master_row['status'] = 'removed'
        results.append(master_row)

    return pd.DataFrame(results)


def generate_html(df: pd.DataFrame):
    """HTML生成"""
    today = date.today().isoformat()

    # ソート: active優先、その中でmorning_peak_count降順
    status_order = {'new': 0, 'active': 1, 'removed': 2}
    df['_sort'] = df['status'].map(status_order)
    df = df.sort_values(['_sort', 'morning_peak_count'], ascending=[True, False])
    df = df.drop('_sort', axis=1)

    total = len(df)
    active_count = len(df[df['status'].isin(['active', 'new'])])
    new_count = len(df[df['status'] == 'new'])
    removed_count = len(df[df['status'] == 'removed'])

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>高値崩れ常習犯リスト v4</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }}
        h1 {{ color: #00d4ff; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #333; padding: 8px 12px; text-align: left; }}
        th {{ background: #16213e; color: #00d4ff; position: sticky; top: 0; cursor: pointer; }}
        th:hover {{ background: #1e3a5f; }}
        tr:nth-child(even) {{ background: #1f1f3d; }}
        tr:hover {{ background: #2a2a5a; }}
        tr.ng-row {{ opacity: 0.5; }}
        tr.new-row {{ background: #1a3a1a !important; }}
        tr.removed-row {{ background: #3a1a1a !important; opacity: 0.7; }}
        .high {{ color: #ff6b6b; }}
        .mid {{ color: #ffd93d; }}
        .low {{ color: #6bcb77; }}
        .safe {{ color: #6bcb77; font-weight: bold; }}
        .danger {{ color: #ff6b6b; font-weight: bold; }}
        .new-badge {{ background: #22c55e; color: #000; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; font-weight: bold; }}
        .removed-badge {{ background: #ef4444; color: #fff; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; }}
        .summary {{ background: #16213e; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .filter {{ margin: 10px 0; }}
        input[type="text"], select {{ padding: 8px; margin-right: 10px; background: #2a2a5a; border: 1px solid #444; color: #eee; border-radius: 4px; }}
        input[type="checkbox"] {{ width: 18px; height: 18px; cursor: pointer; }}
        .check-col {{ text-align: center; }}
        .num-col {{ text-align: right; }}
        .btn {{ padding: 8px 16px; background: #00d4ff; color: #000; border: none; border-radius: 4px; cursor: pointer; margin-right: 10px; }}
        .btn-export {{ background: #6bcb77; }}
        .btn-save {{ background: #f97316; }}
        .count-box {{ background: #2a2a5a; padding: 10px; border-radius: 4px; margin-top: 10px; }}
        .progress {{ background: #16213e; padding: 10px; border-radius: 4px; margin-top: 10px; }}
        .progress-bar {{ background: #333; height: 20px; border-radius: 10px; overflow: hidden; }}
        .progress-fill {{ background: linear-gradient(90deg, #00d4ff, #6bcb77); height: 100%; }}
        .under100 {{ color: #888; }}
        .legend {{ background: #16213e; padding: 10px; border-radius: 4px; margin-top: 10px; font-size: 0.9em; }}
        .vol-high {{ color: #ff6b6b; font-weight: bold; }}
        .vol-mid {{ color: #ffd93d; }}
        .vol-low {{ color: #888; }}
        .date-col {{ font-size: 0.85em; color: #888; }}
    </style>
</head>
<body>
    <h1>高値崩れ常習犯リスト v4</h1>
    <div class="summary">
        <strong>総数:</strong> {active_count}銘柄（<span class="new-badge">NEW {new_count}</span> / <span class="removed-badge">除外候補 {removed_count}</span>）|
        <strong>条件:</strong> 日中高値 → 終値-5%以上 |
        <strong>最終更新:</strong> {today}
    </div>
    <div class="legend">
        <strong>AM率:</strong>
        <span class="low">70%以上 = 前場型</span> |
        <span class="mid">50-70% = 混合型</span> |
        <span class="high">50%以下 = 後場型</span>
        <br>
        <strong>後場回復:</strong>
        <span class="safe">30%以下 = 安全</span> |
        <span class="mid">31-50%</span> |
        <span class="danger">51%以上 = 踏み上げ注意</span>
        <br>
        <strong>ステータス:</strong>
        <span class="new-badge">NEW</span> = 今回新規検出 |
        <span class="removed-badge">除外候補</span> = 条件外（チェック状態保持）
    </div>
    <div class="filter">
        <input type="text" id="search" placeholder="銘柄名/コードで検索" onkeyup="filterTable()">
        <select id="marketFilter" onchange="filterTable()"><option value="">全市場</option><option value="グロース">グロース</option><option value="スタンダード">スタンダード</option></select>
        <select id="statusFilter" onchange="filterTable()"><option value="">全ステータス</option><option value="new">NEW</option><option value="active">継続</option><option value="removed">除外候補</option><option value="shortable">制度信用可</option><option value="day_trade">いちにち信用売可</option><option value="ng">不可</option><option value="unchecked">未チェック</option></select>
        <select id="recoveryFilter" onchange="filterTable()"><option value="">全回復率</option><option value="safe">安全（30%以下）</option><option value="mid">中程度（31-50%）</option><option value="danger">踏み上げ注意（51%以上）</option></select>
        <button class="btn btn-export" onclick="exportChecked()">CSVエクスポート</button>
    </div>
    <div class="count-box">
        <span>制度信用可: <strong id="shortCount">0</strong></span> |
        <span>いちにち信用売可: <strong id="dayCount">0</strong></span> |
        <span>不可: <strong id="ngCount">0</strong></span>
    </div>
    <div class="progress">
        <strong>進捗:</strong> <span id="progressText">0</span> / {active_count} (<span id="progressPct">0</span>%)
        <div class="progress-bar"><div class="progress-fill" id="progressFill" style="width: 0%"></div></div>
    </div>
    <table id="stockTable">
        <thead>
            <tr>
                <th>状態</th>
                <th>コード</th>
                <th>銘柄名</th>
                <th>市場</th>
                <th>回数</th>
                <th>AM/PM</th>
                <th>AM率</th>
                <th>後場回復</th>
                <th>平均崩れ</th>
                <th>株価</th>
                <th>初回検出</th>
                <th class="check-col">制度</th>
                <th class="check-col">いちにち</th>
                <th class="check-col">不可</th>
            </tr>
        </thead>
        <tbody>
"""

    for idx, row in df.iterrows():
        ticker = row['ticker']
        code = ticker.replace('.T', '')
        name = row['stock_name']
        market = row['market']
        count = int(row['morning_peak_count'])
        am_count = int(row.get('am_peak_count', 0))
        pm_count = int(row.get('pm_peak_count', 0))
        am_ratio = row.get('am_peak_ratio', 0)
        avg_drop = row.get('avg_drop', 0)
        price = row.get('latest_close', 0)
        recovery = row.get('recovery_rate', 0)
        shortable = row.get('shortable', False)
        day_trade = row.get('day_trade', False)
        ng = row.get('ng', False)
        detected_at = row.get('detected_at', today)
        status = row.get('status', 'active')

        # 行のクラス
        row_class = ''
        if ng:
            row_class = 'ng-row'
        elif status == 'new':
            row_class = 'new-row'
        elif status == 'removed':
            row_class = 'removed-row'

        # ステータスバッジ
        status_badge = ''
        if status == 'new':
            status_badge = '<span class="new-badge">NEW</span>'
        elif status == 'removed':
            status_badge = '<span class="removed-badge">除外</span>'
        else:
            status_badge = '-'

        # 回数のクラス
        count_class = 'high' if count >= 20 else 'mid' if count >= 10 else ''

        # AM率のクラス
        am_class = 'low' if am_ratio >= 70 else 'mid' if am_ratio >= 50 else 'high'

        # 回復率のクラス
        recovery_class = 'safe' if recovery <= 30 else 'mid' if recovery <= 50 else 'danger'

        # 平均崩れのクラス
        drop_class = 'high' if avg_drop and avg_drop <= -10 else 'mid' if avg_drop and avg_drop <= -8 else ''

        # 株価のクラス
        price_class = 'under100' if price and price < 100 else ''

        # チェック状態
        short_checked = 'checked' if shortable else ''
        day_checked = 'checked' if day_trade else ''
        ng_checked = 'checked' if ng else ''

        # 株価表示
        price_str = f"{price:,.0f}円" if price else "-"

        # 平均崩れ表示
        drop_str = f"{avg_drop:.1f}%" if avg_drop else "-"

        html += f"""<tr class="{row_class}" data-market="{market}" data-price="{price}" data-ticker="{ticker}" data-name="{name}" data-recovery="{recovery}" data-status="{status}">
<td>{status_badge}</td>
<td>{code}</td>
<td>{name}</td>
<td>{market}</td>
<td class="num-col {count_class}">{count}</td>
<td class="num-col">{am_count}/{pm_count}</td>
<td class="num-col {am_class}">{am_ratio:.0f}%</td>
<td class="num-col {recovery_class}">{recovery:.0f}%</td>
<td class="num-col {drop_class}">{drop_str}</td>
<td class="num-col {price_class}">{price_str}</td>
<td class="date-col">{detected_at}</td>
<td class="check-col"><input type="checkbox" class="shortable" onchange="updateCounts(); updateRowStyle(this);" {short_checked}></td>
<td class="check-col"><input type="checkbox" class="day-trade" onchange="updateCounts(); updateRowStyle(this);" {day_checked}></td>
<td class="check-col"><input type="checkbox" class="ng" onchange="updateCounts(); updateRowStyle(this);" {ng_checked}></td>
</tr>
"""

    html += f"""        </tbody>
    </table>
    <script>
        const ACTIVE_COUNT = {active_count};

        function updateCounts() {{
            document.getElementById('shortCount').textContent = document.querySelectorAll('.shortable:checked').length;
            document.getElementById('dayCount').textContent = document.querySelectorAll('.day-trade:checked').length;
            document.getElementById('ngCount').textContent = document.querySelectorAll('.ng:checked').length;

            let total = 0;
            document.querySelectorAll('#stockTable tbody tr').forEach(row => {{
                const status = row.dataset.status;
                if (status === 'removed') return;
                const s = row.querySelector('.shortable').checked;
                const d = row.querySelector('.day-trade').checked;
                const n = row.querySelector('.ng').checked;
                if (s || d || n) total++;
            }});

            const pct = Math.round(total / ACTIVE_COUNT * 100);
            document.getElementById('progressText').textContent = total;
            document.getElementById('progressPct').textContent = pct;
            document.getElementById('progressFill').style.width = pct + '%';
        }}

        function updateRowStyle(cb) {{
            const row = cb.closest('tr');
            if (cb.classList.contains('ng') && cb.checked) {{
                row.classList.add('ng-row');
            }} else if (cb.classList.contains('ng') && !cb.checked) {{
                row.classList.remove('ng-row');
            }}
        }}

        function filterTable() {{
            const search = document.getElementById('search').value.toLowerCase();
            const market = document.getElementById('marketFilter').value;
            const statusFilter = document.getElementById('statusFilter').value;
            const recoveryFilter = document.getElementById('recoveryFilter').value;

            document.querySelectorAll('#stockTable tbody tr').forEach(row => {{
                const name = row.dataset.name.toLowerCase();
                const ticker = row.dataset.ticker.toLowerCase();
                const rowMarket = row.dataset.market;
                const recovery = parseFloat(row.dataset.recovery) || 0;
                const status = row.dataset.status;
                const shortable = row.querySelector('.shortable').checked;
                const dayTrade = row.querySelector('.day-trade').checked;
                const ng = row.querySelector('.ng').checked;

                let show = true;
                if (search && !name.includes(search) && !ticker.includes(search)) show = false;
                if (market && rowMarket !== market) show = false;

                if (statusFilter === 'new' && status !== 'new') show = false;
                if (statusFilter === 'active' && status !== 'active') show = false;
                if (statusFilter === 'removed' && status !== 'removed') show = false;
                if (statusFilter === 'shortable' && !shortable) show = false;
                if (statusFilter === 'day_trade' && !dayTrade) show = false;
                if (statusFilter === 'ng' && !ng) show = false;
                if (statusFilter === 'unchecked' && (shortable || dayTrade || ng)) show = false;

                if (recoveryFilter === 'safe' && recovery > 30) show = false;
                if (recoveryFilter === 'mid' && (recovery <= 30 || recovery > 50)) show = false;
                if (recoveryFilter === 'danger' && recovery <= 50) show = false;

                row.style.display = show ? '' : 'none';
            }});
        }}

        function exportChecked() {{
            let csv = 'ticker,stock_name,market,morning_peak_count,avg_drop,latest_close,shortable,day_trade,ng,detected_at,last_seen_at,status\\n';
            document.querySelectorAll('#stockTable tbody tr').forEach(row => {{
                const cells = row.querySelectorAll('td');
                csv += row.dataset.ticker + ',' +
                       '"' + row.dataset.name + '",' +
                       row.dataset.market + ',' +
                       cells[4].textContent.trim() + ',' +
                       cells[8].textContent.trim() + ',' +
                       row.dataset.price + ',' +
                       row.querySelector('.shortable').checked + ',' +
                       row.querySelector('.day-trade').checked + ',' +
                       row.querySelector('.ng').checked + ',' +
                       cells[10].textContent.trim() + ',' +
                       '{today},' +
                       row.dataset.status + '\\n';
            }});
            const blob = new Blob([csv], {{type: 'text/csv'}});
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = 'morning_peak_shortable.csv';
            a.click();
        }}

        // 初期化
        updateCounts();
        document.querySelectorAll('.ng:checked').forEach(cb => {{
            cb.closest('tr').classList.add('ng-row');
        }});
    </script>
</body>
</html>
"""

    return html


def main():
    print("=== 高値崩れ常習犯リスト - 差分更新 ===\n")

    # 1. データ読み込み
    print("1. データ読み込み...")
    df_new = pd.read_parquet(PARQUET_PATH)
    df_master = load_master_csv()
    print(f"   新データ: {len(df_new)}件")
    print(f"   マスター: {len(df_master)}件")

    # 2. マージ
    print("\n2. データマージ...")
    df_merged = merge_data(df_master, df_new)
    print(f"   マージ後: {len(df_merged)}件")

    # 3. マスターCSV保存
    print("\n3. マスターCSV保存...")
    df_merged.to_csv(MASTER_CSV_PATH, index=False)
    print(f"   -> {MASTER_CSV_PATH}")

    # 4. HTML生成
    print("\n4. HTML生成...")
    html = generate_html(df_merged)
    HTML_PATH.write_text(html, encoding='utf-8')
    print(f"   -> {HTML_PATH}")

    # サマリー
    print("\n=== サマリー ===")
    print(f"総数: {len(df_merged)}件")
    print(f"  - NEW: {len(df_merged[df_merged['status'] == 'new'])}件")
    print(f"  - 継続: {len(df_merged[df_merged['status'] == 'active'])}件")
    print(f"  - 除外候補: {len(df_merged[df_merged['status'] == 'removed'])}件")


if __name__ == '__main__':
    main()
