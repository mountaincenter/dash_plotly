"""
stock_results.csv から V2_PAIRS 該当のペアトレード実績を抽出する。

検出ロジック:
- 取得日/建日（エントリー日）でグルーピングし、V2_PAIRS 該当ペア・逆方向を検出
- 片脚先精算でも取得日が同じなら同一ペアとして紐付く
- 買埋 = ショート決済、売埋 = ロング決済
"""
from pathlib import Path
import sys

import pandas as pd

BASE = Path("/Users/hiroyukiyamanaka/dev/python_stock_rebuild/dash_plotly")
PARQUET = BASE / "data" / "parquet" / "stock_results.parquet"

sys.path.insert(0, str(BASE / "scripts" / "pipeline"))
from generate_pairs_signals import V2_PAIRS  # noqa: E402

PAIRS_START = pd.Timestamp("2026-04-10")
PAIR_SET = {frozenset([a.replace(".T", ""), b.replace(".T", "")])
            for a, b, *_ in V2_PAIRS}


def load():
    df = pd.read_parquet(PARQUET)
    df["コード"] = df["コード"].astype(str)
    df["約定日"] = pd.to_datetime(df["約定日"])
    df["取得日"] = pd.to_datetime(df["取得日"])
    df["実現損益"] = pd.to_numeric(df["実現損益"], errors="coerce").fillna(0).astype(int)
    return df[df["取得日"] >= PAIRS_START].copy()


def detect(df):
    rows = []
    for entry_date, grp in df.groupby("取得日"):
        consumed = set()
        longs = set(grp[grp["売買"] == "ロング"]["コード"])
        shorts = set(grp[grp["売買"] == "ショート"]["コード"])
        matched = set()
        for pair in PAIR_SET:
            a, b = tuple(pair)
            if (a in longs and b in shorts) or (a in shorts and b in longs):
                matched.add((a, b)) if a < b else matched.add((b, a))
        for a, b in matched:
            a_avail = grp[(grp["コード"] == a) & (~grp.index.isin(consumed))]
            b_avail = grp[(grp["コード"] == b) & (~grp.index.isin(consumed))]
            for i in range(min(len(a_avail), len(b_avail))):
                ar, br = a_avail.iloc[i], b_avail.iloc[i]
                consumed.update([ar.name, br.name])
                for tk, r, partner in [(a, ar, b), (b, br, a)]:
                    rows.append({
                        "取得日": entry_date.date(),
                        "約定日": r["約定日"].date(),
                        "コード": tk,
                        "銘柄": r["銘柄名"],
                        "売買": r["売買"],
                        "単価": r["平均単価"],
                        "数量": r["数量"],
                        "損益": r["実現損益"],
                        "ペア相手": partner,
                    })
    return pd.DataFrame(rows).sort_values(["取得日", "コード"]).reset_index(drop=True)


def main():
    df = load()
    out = detect(df)
    if out.empty:
        print(f"ペアトレード検出なし (対象期間: {PAIRS_START.date()}~)")
        return
    print(out.to_string(index=False))
    total = out["損益"].sum()
    days = out["取得日"].nunique()
    pairs = len(out) // 2
    win = (out.groupby("取得日")["損益"].sum() > 0).sum()
    print(f"\n合計: {total:+,}円  /  {pairs}ペア  /  {days}日  /  勝ち {win}日")


if __name__ == "__main__":
    main()
