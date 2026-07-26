from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd


DASH_ROOT = Path(__file__).resolve().parents[4]
SOURCE_WATCH_HTML = (
    DASH_ROOT
    / "data/research/grok_session_handoffs_20260718/02_grok_entry_exit/output"
    / "daytrade_watch_universe.html"
)
DAILY_PATH = DASH_ROOT / "data/parquet/prices_max_1d.parquet"
MINUTE_ROOT = DASH_ROOT / "data/research/jquants_all_market/minute"
TICK_ROOT = DASH_ROOT / "data/research/jquants_tick/filtered"
MINUTE_MANIFEST_PATH = (
    DASH_ROOT / "data/research/jquants_all_market/fetch_manifest.parquet"
)
TICK_MANIFEST_PATH = DASH_ROOT / "data/research/jquants_tick/fetch_manifest.parquet"
OUTPUT_ROOT = (
    DASH_ROOT
    / "data/research/grok_session_handoffs_20260718/04_technical_entry_training/output"
)
OUTPUT_HTML = OUTPUT_ROOT / "technical_entry_training.html"
OUTPUT_RESULTS = OUTPUT_ROOT / "technical_entry_training_results.js"
DEFAULT_CUTOFF = "09:30"


CASE_SPECS: list[dict[str, str]] = [
    {
        "id": "8306_20260723_0930",
        "ticker": "8306.T",
        "date": "2026-07-23",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "buy",
        "intradayGuide": "buy",
        "pattern": "上昇トレンド継続候補",
        "lesson": "日足上昇基調 × GD吸収 × VWAP支持",
        "rationale": (
            "日足は主要SMAの上で推移。ザラ場はGDを吸収し、高値・安値を"
            "切り上げながらVWAP上を維持した場面。"
        ),
        "invalidation": "VWAPと直近押し安値を割り、回復できない。",
        "warning": "上昇率だけで追わず、押し安値とVWAPの支持を確認する。",
    },
    {
        "id": "6723_20260724_0930",
        "ticker": "6723.T",
        "date": "2026-07-24",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "sell",
        "intradayGuide": "sell",
        "pattern": "戻り売り候補",
        "lesson": "日足弱含み × GD × VWAP回復失敗",
        "rationale": (
            "日足は25SMAを下回り短期モメンタムが弱い。ザラ場も戻り高値を"
            "切り下げ、VWAPを回復できない場面。"
        ),
        "invalidation": "VWAPと直近戻り高値を回復し、その上で定着する。",
        "warning": "GDそのものではなく、戻りが失敗した順序を根拠にする。",
    },
    {
        "id": "8035_20260723_0930",
        "ticker": "8035.T",
        "date": "2026-07-23",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "wait",
        "intradayGuide": "buy",
        "pattern": "GU上昇継続候補",
        "lesson": "日足方向混在 × GU上昇 × VWAP上維持",
        "rationale": (
            "日足だけでは主要SMAの並びと短期下落が混在するため静観。"
            "09:30時点では全30分をVWAP上で推移し、高値圏を維持しているため"
            "買いへ変わる例。"
        ),
        "invalidation": "VWAPと直近押し安値を割り、回復できない。",
        "warning": "GUの上昇だけを追わず、VWAP上の維持と押し安値を基準にする。",
    },
    {
        "id": "6857_20260721_0930",
        "ticker": "6857.T",
        "date": "2026-07-21",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "wait",
        "intradayGuide": "wait",
        "pattern": "方向確認待ち",
        "lesson": "日足調整中 × VWAP交錯 × 09:30静観",
        "rationale": (
            "日足は調整中で方向を決めにくい。09:30時点のザラ場もVWAPの"
            "上下を交錯し、5SMAは20SMAを下回るため方向確定まで静観する例。"
        ),
        "invalidation": "朝高値・朝安値のどちらかを抜け、VWAPの片側へ定着したら再評価する。",
        "warning": "その後の上昇を知って、09:30時点の交錯を買いへ書き換えない。",
    },
    {
        "id": "5803_20260722_0930",
        "ticker": "5803.T",
        "date": "2026-07-22",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "sell",
        "intradayGuide": "wait",
        "pattern": "遅い初約定後の方向確認",
        "lesson": "日足下降調整 × 09:12初約定 × 09:30静観",
        "rationale": (
            "日足は25・75SMAを下回る調整局面。当日は09:12まで約定がなく、"
            "09:30時点では初約定後の安値からVWAP上へ戻しているため、"
            "戻り失敗が確定するまで静観する例。"
        ),
        "invalidation": "朝高値を上抜くか、VWAPと直近押し安値を割った後に戻れなければ再評価する。",
        "warning": "09:00から09:12は約定なし。18本だけで20SMAを推定しない。",
    },
    {
        "id": "3382_20260724_0930",
        "ticker": "3382.T",
        "date": "2026-07-24",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "buy",
        "intradayGuide": "buy",
        "pattern": "前日線回復候補",
        "lesson": "日足回復基調 × 前日終値回復 × VWAP支持",
        "rationale": (
            "日足は主要線を回復しつつある。ザラ場は小幅GDを吸収して"
            "前日終値を回復し、VWAP上で高値・安値を切り上げた場面。"
        ),
        "invalidation": "前日終値とVWAPを連続して割り、回復できない。",
        "warning": "前日終値を一度超えただけでなく、上で保てるかを見る。",
    },
    {
        "id": "7182_20260724_0930",
        "ticker": "7182.T",
        "date": "2026-07-24",
        "cutoff": DEFAULT_CUTOFF,
        "dailyGuide": "buy",
        "intradayGuide": "wait",
        "pattern": "朝高値後の方向確認",
        "lesson": "日足上昇基調 × VWAP割れ直後 × 09:30静観",
        "rationale": (
            "日足は上昇基調でも、ザラ場は朝高値から下げて09:30直前に"
            "VWAPと5・20SMAを下回った。買い継続か失速かの確認まで静観する例。"
        ),
        "invalidation": "VWAPを回復して朝高値へ向かうか、直近安値を割って戻れなければ再評価する。",
        "warning": "日足の買い目線と、09:30時点で今すぐ買える状態を分ける。",
    },
]


def safe_number(value: Any, digits: int = 6) -> float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    if not math.isfinite(number):
        return None
    return round(number, digits)


def iso_or_none(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def add_indicators(frame: pd.DataFrame, *, intraday: bool) -> pd.DataFrame:
    result = frame.sort_values("datetime" if intraday else "trading_date").copy()
    close = result["close"].astype(float)
    if intraday:
        result["sma5"] = close.rolling(5, min_periods=5).mean()
        result["sma20"] = close.rolling(20, min_periods=20).mean()
        result["cum_volume"] = result["volume"].cumsum()
        result["cum_value"] = result["value"].cumsum()
        result["vwap"] = result["cum_value"] / result["cum_volume"].replace(0, pd.NA)
    else:
        for window in (25, 75, 200):
            result[f"sma{window}"] = close.rolling(
                window, min_periods=window
            ).mean()

    bb_mid = close.rolling(20, min_periods=20).mean()
    bb_std = close.rolling(20, min_periods=20).std(ddof=0)
    result["bb_mid"] = bb_mid
    result["bb_upper"] = bb_mid + 2 * bb_std
    result["bb_lower"] = bb_mid - 2 * bb_std

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    average_gain = gain.ewm(
        alpha=1 / 14, adjust=False, min_periods=14
    ).mean()
    average_loss = loss.ewm(
        alpha=1 / 14, adjust=False, min_periods=14
    ).mean()
    relative_strength = average_gain / average_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + relative_strength))
    rsi = rsi.mask((average_loss == 0) & (average_gain > 0), 100)
    rsi = rsi.mask((average_loss == 0) & (average_gain == 0), 50)
    result["rsi14"] = rsi

    ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    result["macd"] = ema12 - ema26
    result["macd_signal"] = result["macd"].ewm(
        span=9, adjust=False, min_periods=9
    ).mean()
    result["macd_hist"] = result["macd"] - result["macd_signal"]
    return result


def load_stock_names() -> dict[str, str]:
    text = SOURCE_WATCH_HTML.read_text(encoding="utf-8")
    match = re.search(r"const stocks = (\[.*?\]);\n\s*const", text, re.S)
    if match is None:
        raise RuntimeError(f"銘柄定義を取得できません: {SOURCE_WATCH_HTML}")
    rows = json.loads(match.group(1))
    return {str(row["ticker"]): str(row["name"]) for row in rows}


def load_daily_data(tickers: list[str], max_date: str) -> pd.DataFrame:
    daily = pd.read_parquet(
        DAILY_PATH,
        filters=[("ticker", "in", tickers)],
    ).rename(
        columns={
            "date": "trading_date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    daily["trading_date"] = pd.to_datetime(daily["trading_date"])
    daily = daily.dropna(subset=["open", "high", "low", "close", "volume"])

    latest = daily["trading_date"].max()
    additions: list[dict[str, Any]] = []
    for path in sorted(MINUTE_ROOT.glob("trading_date=*/part-000.parquet")):
        trading_date = pd.Timestamp(path.parent.name.split("=", 1)[1])
        if trading_date <= latest or trading_date > pd.Timestamp(max_date):
            continue
        minute = pd.read_parquet(
            path,
            columns=[
                "ticker",
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ],
        )
        minute = minute[minute["ticker"].isin(tickers)].sort_values(
            ["ticker", "datetime"]
        )
        for ticker, group in minute.groupby("ticker", sort=False):
            additions.append(
                {
                    "trading_date": trading_date,
                    "open": float(group["open"].iloc[0]),
                    "high": float(group["high"].max()),
                    "low": float(group["low"].min()),
                    "close": float(group["close"].iloc[-1]),
                    "volume": float(group["volume"].sum()),
                    "ticker": str(ticker),
                }
            )

    if additions:
        daily = pd.concat([daily, pd.DataFrame(additions)], ignore_index=True)
    return (
        daily.drop_duplicates(["ticker", "trading_date"], keep="last")
        .sort_values(["ticker", "trading_date"])
        .reset_index(drop=True)
    )


def load_minute_day(trading_date: str) -> pd.DataFrame:
    path = MINUTE_ROOT / f"trading_date={trading_date}" / "part-000.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(
        path,
        columns=[
            "ticker",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
        ],
    )


def load_tick_day(trading_date: str) -> pd.DataFrame:
    path = TICK_ROOT / f"trading_date={trading_date}" / "daytrade_watch_universe.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(
        path,
        columns=["ticker", "datetime", "price", "trading_volume", "turnover"],
    )


def to_tick_tempo(ticks: pd.DataFrame) -> pd.DataFrame:
    source = ticks.sort_values("datetime").set_index("datetime")
    tempo = source.resample("30s").agg(
        price=("price", "last"),
        trades=("price", "size"),
        volume=("trading_volume", "sum"),
        turnover=("turnover", "sum"),
    )
    tempo = tempo[tempo["trades"] > 0].reset_index()
    tempo["bin_end"] = tempo["datetime"] + pd.Timedelta(seconds=30)
    return tempo


def detect_recent_gaps(history: pd.DataFrame) -> list[dict[str, Any]]:
    rows = history.tail(61).reset_index(drop=True)
    gaps: list[dict[str, Any]] = []
    for index in range(1, len(rows)):
        previous = rows.iloc[index - 1]
        current = rows.iloc[index]
        if float(current["low"]) > float(previous["high"]):
            gaps.append(
                {
                    "date": current["trading_date"].strftime("%Y-%m-%d"),
                    "direction": "up",
                    "lower": safe_number(previous["high"]),
                    "upper": safe_number(current["low"]),
                }
            )
        elif float(current["high"]) < float(previous["low"]):
            gaps.append(
                {
                    "date": current["trading_date"].strftime("%Y-%m-%d"),
                    "direction": "down",
                    "lower": safe_number(current["high"]),
                    "upper": safe_number(previous["low"]),
                }
            )
    return gaps[-3:]


def daily_payload(history: pd.DataFrame) -> list[dict[str, Any]]:
    fields = [
        "sma25",
        "sma75",
        "sma200",
        "bb_mid",
        "bb_upper",
        "bb_lower",
        "rsi14",
        "macd",
        "macd_signal",
        "macd_hist",
    ]
    payload: list[dict[str, Any]] = []
    for row in history.itertuples(index=False):
        item = {
            "date": row.trading_date.strftime("%Y-%m-%d"),
            "open": safe_number(row.open),
            "high": safe_number(row.high),
            "low": safe_number(row.low),
            "close": safe_number(row.close),
            "volume": int(row.volume),
        }
        for field in fields:
            item[field] = safe_number(getattr(row, field))
        payload.append(item)
    return payload


def minute_payload(bars: pd.DataFrame) -> list[dict[str, Any]]:
    fields = [
        "vwap",
        "sma5",
        "sma20",
        "bb_mid",
        "bb_upper",
        "bb_lower",
        "rsi14",
        "macd",
        "macd_signal",
        "macd_hist",
    ]
    payload: list[dict[str, Any]] = []
    for row in bars.itertuples(index=False):
        item = {
            "datetime": row.datetime.isoformat(),
            "time": row.datetime.strftime("%H:%M"),
            "open": safe_number(row.open),
            "high": safe_number(row.high),
            "low": safe_number(row.low),
            "close": safe_number(row.close),
            "volume": int(row.volume),
        }
        for field in fields:
            item[field] = safe_number(getattr(row, field))
        payload.append(item)
    return payload


def tempo_payload(tempo: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "datetime": row.datetime.isoformat(),
            "end": row.bin_end.isoformat(),
            "time": row.datetime.strftime("%H:%M:%S"),
            "price": safe_number(row.price),
            "trades": int(row.trades),
            "volume": int(row.volume),
            "turnover": int(row.turnover),
        }
        for row in tempo.itertuples(index=False)
    ]


def indicator_snapshot(row: pd.Series, *, intraday: bool) -> dict[str, Any]:
    fields = [
        "bb_mid",
        "bb_upper",
        "bb_lower",
        "rsi14",
        "macd",
        "macd_signal",
        "macd_hist",
    ]
    if intraday:
        fields = ["vwap", "sma5", "sma20", *fields]
    else:
        fields = ["sma25", "sma75", "sma200", *fields]
    return {field: safe_number(row[field]) for field in fields}


def profit_intervals(
    ticks: pd.DataFrame, entry_price: float, multiplier: int
) -> tuple[list[dict[str, str]], int]:
    minute_prices = (
        ticks.set_index("datetime")["price"].resample("1min").last().dropna()
    )
    positive_minutes = minute_prices[
        (minute_prices.astype(float) - entry_price) * 100 * multiplier > 0
    ].index
    if len(positive_minutes) == 0:
        return [], 0

    intervals: list[dict[str, str]] = []
    start = pd.Timestamp(positive_minutes[0])
    previous = start
    for value in positive_minutes[1:]:
        current = pd.Timestamp(value)
        if current - previous != pd.Timedelta(minutes=1):
            intervals.append(
                {
                    "start": start.isoformat(),
                    "end": (previous + pd.Timedelta(minutes=1)).isoformat(),
                }
            )
            start = current
        previous = current
    intervals.append(
        {
            "start": start.isoformat(),
            "end": (previous + pd.Timedelta(minutes=1)).isoformat(),
        }
    )
    return intervals, len(positive_minutes)


def build_side_outcome(
    ticks: pd.DataFrame, entry_price: float, *, side: str
) -> dict[str, Any]:
    multiplier = 1 if side == "buy" else -1
    pnl = (ticks["price"].astype(float) - entry_price) * 100 * multiplier
    mfe_index = pnl.idxmax()
    mae_index = pnl.idxmin()
    positive = pnl[pnl > 0]
    plus_5000 = pnl[pnl >= 5000]
    stop = pnl[pnl <= -5000]
    first_profit_index = positive.index[0] if not positive.empty else None
    plus_index = plus_5000.index[0] if not plus_5000.empty else None
    stop_index = stop.index[0] if not stop.empty else None
    plus_time = ticks.loc[plus_index, "datetime"] if plus_index is not None else None
    stop_time = ticks.loc[stop_index, "datetime"] if stop_index is not None else None
    if plus_time is None and stop_time is None:
        order = "neither"
    elif stop_time is None:
        order = "plus-first"
    elif plus_time is None:
        order = "stop-first"
    elif plus_time < stop_time:
        order = "plus-first"
    elif stop_time < plus_time:
        order = "stop-first"
    else:
        order = "same-time"

    intervals, positive_minutes = profit_intervals(ticks, entry_price, multiplier)
    stop_level = entry_price - 50 if side == "buy" else entry_price + 50
    return {
        "side": side,
        "entryPrice": safe_number(entry_price),
        "entryTime": iso_or_none(ticks.iloc[0]["datetime"]),
        "stopLevel": safe_number(stop_level),
        "stopReached": stop_index is not None,
        "stopTime": iso_or_none(stop_time),
        "stopArrivalPrice": (
            safe_number(ticks.loc[stop_index, "price"])
            if stop_index is not None
            else None
        ),
        "closePrice": safe_number(ticks.iloc[-1]["price"]),
        "closeTime": iso_or_none(ticks.iloc[-1]["datetime"]),
        "closePnl": safe_number(pnl.iloc[-1], 2),
        "mfe": safe_number(pnl.loc[mfe_index], 2),
        "mfeTime": iso_or_none(ticks.loc[mfe_index, "datetime"]),
        "mae": safe_number(pnl.loc[mae_index], 2),
        "maeTime": iso_or_none(ticks.loc[mae_index, "datetime"]),
        "firstProfitTime": (
            iso_or_none(ticks.loc[first_profit_index, "datetime"])
            if first_profit_index is not None
            else None
        ),
        "plus5000Time": iso_or_none(plus_time),
        "stopVsPlusOrder": order,
        "profitIntervals": intervals,
        "positiveMinutes": positive_minutes,
    }


def build_wait_outcome(ticks: pd.DataFrame, entry_price: float) -> dict[str, Any]:
    change = ticks["price"].astype(float) - entry_price
    high_index = change.idxmax()
    low_index = change.idxmin()
    return {
        "referencePrice": safe_number(entry_price),
        "referenceTime": iso_or_none(ticks.iloc[0]["datetime"]),
        "maxUpPerShare": safe_number(change.loc[high_index]),
        "maxUp100Shares": safe_number(change.loc[high_index] * 100, 2),
        "maxUpTime": iso_or_none(ticks.loc[high_index, "datetime"]),
        "maxDownPerShare": safe_number(change.loc[low_index]),
        "maxDown100Shares": safe_number(change.loc[low_index] * 100, 2),
        "maxDownTime": iso_or_none(ticks.loc[low_index, "datetime"]),
        "closeChangePerShare": safe_number(change.iloc[-1]),
        "closeChange100Shares": safe_number(change.iloc[-1] * 100, 2),
        "closePrice": safe_number(ticks.iloc[-1]["price"]),
        "closeTime": iso_or_none(ticks.iloc[-1]["datetime"]),
    }


def build_case(
    spec: dict[str, str],
    name: str,
    daily: pd.DataFrame,
    minute_day: pd.DataFrame,
    tick_day: pd.DataFrame,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    ticker = spec["ticker"]
    target_date = pd.Timestamp(spec["date"])
    cutoff = pd.Timestamp(f"{spec['date']} {spec['cutoff']}")

    history_source = daily[
        (daily["ticker"] == ticker) & (daily["trading_date"] < target_date)
    ].copy()
    if len(history_source) < 200:
        raise RuntimeError(f"{ticker} の日足が200本未満です: {len(history_source)}")
    history = add_indicators(history_source, intraday=False).tail(240)

    minute_source = (
        minute_day[minute_day["ticker"] == ticker]
        .copy()
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    tick_source = (
        tick_day[tick_day["ticker"] == ticker]
        .copy()
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    if minute_source.empty or tick_source.empty:
        raise RuntimeError(f"{spec['date']} {ticker} の分足またはtickがありません")
    required_minute = ["open", "high", "low", "close", "volume", "value"]
    required_tick = ["price", "trading_volume", "turnover"]
    if minute_source[required_minute].isna().any().any():
        raise RuntimeError(f"{spec['id']} の分足に欠損があります")
    if tick_source[required_tick].isna().any().any():
        raise RuntimeError(f"{spec['id']} のtickに欠損があります")

    visible_minute_source = minute_source[minute_source["datetime"] < cutoff].copy()
    visible_tick_source = tick_source[tick_source["datetime"] <= cutoff].copy()
    future_ticks = (
        tick_source[tick_source["datetime"] > cutoff]
        .copy()
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    if visible_minute_source.empty or visible_tick_source.empty or future_ticks.empty:
        raise RuntimeError(f"{spec['id']} のcutoff前後データが不足しています")

    visible_minute = add_indicators(visible_minute_source, intraday=True)
    full_minute = add_indicators(minute_source, intraday=True)
    visible_tempo = to_tick_tempo(visible_tick_source)
    visible_tempo = visible_tempo[visible_tempo["bin_end"] <= cutoff]
    full_tempo = to_tick_tempo(tick_source)

    previous = history.iloc[-1]
    five_days_before = history.iloc[-6]
    recent_20 = history.tail(20)
    first_minute = minute_source.iloc[0]
    last_visible = visible_minute.iloc[-1]
    recent_visible_tempo = visible_tempo.tail(10)
    late_open = pd.Timestamp(first_minute["datetime"]).time() > pd.Timestamp(
        f"{spec['date']} 09:00"
    ).time()

    daily_context = {
        "previousDate": previous["trading_date"].strftime("%Y-%m-%d"),
        "previousClose": safe_number(previous["close"]),
        "previousHigh": safe_number(previous["high"]),
        "previousLow": safe_number(previous["low"]),
        "previousVolume": int(previous["volume"]),
        "averageVolume20": safe_number(recent_20["volume"].mean()),
        "recentHigh20": safe_number(recent_20["high"].max()),
        "recentLow20": safe_number(recent_20["low"].min()),
        "sma25Slope5": safe_number(previous["sma25"] - five_days_before["sma25"]),
        "sma75Slope5": safe_number(previous["sma75"] - five_days_before["sma75"]),
        "sma200Slope5": safe_number(
            previous["sma200"] - five_days_before["sma200"]
        ),
        "sma25DistancePct": safe_number(
            (previous["close"] / previous["sma25"] - 1) * 100
        ),
        "sma75DistancePct": safe_number(
            (previous["close"] / previous["sma75"] - 1) * 100
        ),
        "sma200DistancePct": safe_number(
            (previous["close"] / previous["sma200"] - 1) * 100
        ),
        "recentGaps": detect_recent_gaps(history),
        "indicators": indicator_snapshot(previous, intraday=False),
    }
    intraday_context = {
        "open": safe_number(first_minute["open"]),
        "firstTradeTime": iso_or_none(tick_source.iloc[0]["datetime"]),
        "lateOpen": late_open,
        "lastVisiblePrice": safe_number(last_visible["close"]),
        "visibleHigh": safe_number(visible_minute["high"].max()),
        "visibleLow": safe_number(visible_minute["low"].min()),
        "visibleVolume": int(visible_minute["volume"].sum()),
        "tickTrades30sAverage": safe_number(
            recent_visible_tempo["trades"].mean()
            if not recent_visible_tempo.empty
            else None
        ),
        "indicators": indicator_snapshot(last_visible, intraday=True),
        "marketSectorStatus": "市場・セクター相対データはこの教材には未収録",
    }
    public_case = {
        "id": spec["id"],
        "ticker": ticker,
        "name": name,
        "date": spec["date"],
        "cutoff": spec["cutoff"],
        "daily": daily_payload(history),
        "intraday": minute_payload(visible_minute),
        "tickTempo": tempo_payload(visible_tempo),
        "dailyContext": daily_context,
        "intradayContext": intraday_context,
    }

    entry_price = float(future_ticks.iloc[0]["price"])
    buy_outcome = build_side_outcome(future_ticks, entry_price, side="buy")
    sell_outcome = build_side_outcome(future_ticks, entry_price, side="sell")
    result_case = {
        "id": spec["id"],
        "guidance": {
            "daily": spec["dailyGuide"],
            "intraday": spec["intradayGuide"],
            "pattern": spec["pattern"],
            "lesson": spec["lesson"],
            "rationale": spec["rationale"],
            "invalidation": spec["invalidation"],
            "warning": spec["warning"],
        },
        "fullIntraday": minute_payload(full_minute),
        "fullTickTempo": tempo_payload(full_tempo),
        "outcomes": {
            "buy": buy_outcome,
            "sell": sell_outcome,
            "wait": build_wait_outcome(future_ticks, entry_price),
        },
    }
    audit = {
        "id": spec["id"],
        "dailyLast": history.iloc[-1]["trading_date"].isoformat(),
        "visibleMinuteLast": visible_minute.iloc[-1]["datetime"].isoformat(),
        "visibleTickLast": visible_tick_source.iloc[-1]["datetime"].isoformat(),
        "entryTime": future_ticks.iloc[0]["datetime"].isoformat(),
        "entryPrice": entry_price,
        "minuteRows": len(minute_source),
        "tickRows": len(tick_source),
        "lateOpen": late_open,
        "buyStopReached": buy_outcome["stopReached"],
        "sellStopReached": sell_outcome["stopReached"],
    }
    return public_case, result_case, audit


def validate_manifests(dates: list[str]) -> None:
    minute_manifest = pd.read_parquet(MINUTE_MANIFEST_PATH)
    tick_manifest = pd.read_parquet(TICK_MANIFEST_PATH)
    for trading_date in dates:
        minute_rows = minute_manifest[
            minute_manifest["trading_date"].astype(str) == trading_date
        ]
        tick_rows = tick_manifest[
            tick_manifest["trading_date"].astype(str) == trading_date
        ]
        if minute_rows.empty or minute_rows.iloc[-1]["status"] != "complete":
            raise RuntimeError(f"分足manifestがcompleteではありません: {trading_date}")
        if tick_rows.empty or (
            tick_rows.iloc[-1]["validation"] != "tick_minute_ohlcv_value_match"
        ):
            raise RuntimeError(f"tick manifestの検証状態が不正です: {trading_date}")


def validate_payloads(
    public_cases: list[dict[str, Any]],
    result_cases: list[dict[str, Any]],
) -> None:
    forbidden_public_keys = {
        "guidance",
        "outcomes",
        "fullIntraday",
        "fullTickTempo",
        "lesson",
        "rationale",
        "invalidation",
        "warning",
    }
    long_stop_seen = False
    short_stop_seen = False
    direction_change_seen = False
    result_by_id = {item["id"]: item for item in result_cases}
    spec_by_id = {item["id"]: item for item in CASE_SPECS}

    for public_case in public_cases:
        case_id = public_case["id"]
        spec = spec_by_id[case_id]
        result = result_by_id[case_id]
        target_date = pd.Timestamp(public_case["date"])
        cutoff = pd.Timestamp(f"{public_case['date']} {public_case['cutoff']}")
        if pd.Timestamp(public_case["daily"][-1]["date"]) >= target_date:
            raise AssertionError(f"{case_id}: 日足に対象日が混入")
        if max(
            pd.Timestamp(row["datetime"]) for row in public_case["intraday"]
        ) >= cutoff:
            raise AssertionError(f"{case_id}: 分足にcutoff以後が混入")
        if max(pd.Timestamp(row["end"]) for row in public_case["tickTempo"]) > cutoff:
            raise AssertionError(f"{case_id}: tick tempoにcutoff後が混入")
        if forbidden_public_keys.intersection(public_case):
            raise AssertionError(f"{case_id}: 公開payloadに結果キーが混入")

        buy = result["outcomes"]["buy"]
        sell = result["outcomes"]["sell"]
        if pd.Timestamp(buy["entryTime"]) <= cutoff:
            raise AssertionError(f"{case_id}: entryがcutoff後ではありません")
        if not math.isclose(buy["stopLevel"], buy["entryPrice"] - 50):
            raise AssertionError(f"{case_id}: ロングSL水準が不正")
        if not math.isclose(sell["stopLevel"], sell["entryPrice"] + 50):
            raise AssertionError(f"{case_id}: ショートSL水準が不正")
        if not math.isclose(buy["closePnl"], -sell["closePnl"]):
            raise AssertionError(f"{case_id}: 大引け損益の符号が不整合")
        if not math.isclose(buy["mfe"], -sell["mae"]):
            raise AssertionError(f"{case_id}: MFE/MAEが不整合")
        if not math.isclose(buy["mae"], -sell["mfe"]):
            raise AssertionError(f"{case_id}: MAE/MFEが不整合")
        long_stop_seen = long_stop_seen or buy["stopReached"]
        short_stop_seen = short_stop_seen or sell["stopReached"]
        direction_change_seen = direction_change_seen or (
            spec["dailyGuide"] != spec["intradayGuide"]
        )

    if not long_stop_seen or not short_stop_seen:
        raise AssertionError("ロング・ショート双方のSL到達例がありません")
    if not direction_change_seen:
        raise AssertionError("日足とザラ場で判断が変わるケースがありません")


def build_training_data() -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    invalid_cutoffs = sorted(
        {
            str(spec["cutoff"])
            for spec in CASE_SPECS
            if spec["cutoff"] != DEFAULT_CUTOFF
        }
    )
    if invalid_cutoffs:
        raise RuntimeError(
            f"判断時刻がデフォルト{DEFAULT_CUTOFF}と不一致です: {invalid_cutoffs}"
        )
    names = load_stock_names()
    tickers = sorted({spec["ticker"] for spec in CASE_SPECS})
    missing_names = sorted(set(tickers) - set(names))
    if missing_names:
        raise RuntimeError(f"監視母集団にないticker: {missing_names}")

    dates = sorted({spec["date"] for spec in CASE_SPECS})
    validate_manifests(dates)
    daily = load_daily_data(tickers, max(dates))
    minute_cache: dict[str, pd.DataFrame] = {}
    tick_cache: dict[str, pd.DataFrame] = {}
    public_cases: list[dict[str, Any]] = []
    result_cases: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []

    for spec in CASE_SPECS:
        trading_date = spec["date"]
        if trading_date not in minute_cache:
            minute_cache[trading_date] = load_minute_day(trading_date)
        if trading_date not in tick_cache:
            tick_cache[trading_date] = load_tick_day(trading_date)
        public_case, result_case, audit = build_case(
            spec,
            names[spec["ticker"]],
            daily,
            minute_cache[trading_date],
            tick_cache[trading_date],
        )
        public_cases.append(public_case)
        result_cases.append(result_case)
        audits.append(audit)

    validate_payloads(public_cases, result_cases)
    public_data = {
        "generatedAt": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(),
        "caseCount": len(public_cases),
        "sources": {
            "daily": str(DAILY_PATH.relative_to(DASH_ROOT)),
            "minute": str(MINUTE_ROOT.relative_to(DASH_ROOT)),
            "tick": str(TICK_ROOT.relative_to(DASH_ROOT)),
            "minuteManifest": str(MINUTE_MANIFEST_PATH.relative_to(DASH_ROOT)),
            "tickManifest": str(TICK_MANIFEST_PATH.relative_to(DASH_ROOT)),
        },
        "cases": public_cases,
    }
    result_data = {
        "generatedAt": public_data["generatedAt"],
        "caseCount": len(result_cases),
        "cases": result_cases,
    }
    return public_data, result_data, audits


HTML_TEMPLATE = r"""<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>日足→ザラ場 エントリー判断学習</title>
  <style>
    :root {
      color-scheme: dark;
      --bg:#071016; --panel:#0f1a22; --panel2:#15242e; --line:#2a3d49;
      --text:#edf5f7; --muted:#91a6af; --green:#41d39d; --red:#ff7780;
      --amber:#f4bd55; --blue:#69b7ff; --purple:#bc91ff; --cyan:#65d8e8;
    }
    * { box-sizing:border-box; }
    body {
      margin:0; color:var(--text); background:
      radial-gradient(circle at 10% 0%,#153342 0,transparent 28%),
      linear-gradient(160deg,#071016,#0b151d 48%,#071016);
      font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif;
    }
    button,input,textarea { font:inherit; }
    button { cursor:pointer; }
    .shell { max-width:1540px; margin:0 auto; padding:18px; }
    .topbar {
      display:flex; gap:16px; justify-content:space-between; align-items:flex-start;
      padding:18px 20px; border:1px solid var(--line); border-radius:18px;
      background:rgba(15,26,34,.9); box-shadow:0 20px 60px rgba(0,0,0,.22);
    }
    h1 { margin:0 0 6px; font-size:clamp(22px,3vw,34px); letter-spacing:.02em; }
    h2,h3 { margin:0; }
    .subtitle,.muted { color:var(--muted); line-height:1.6; }
    .actions { display:flex; flex-wrap:wrap; gap:8px; justify-content:flex-end; }
    .button {
      border:1px solid var(--line); color:var(--text); background:#14242e;
      border-radius:10px; padding:9px 13px;
    }
    .button:hover { border-color:#527183; }
    .button.primary { background:#174b59; border-color:#2b8296; }
    .button.danger { color:#ffb6ba; }
    .layout {
      display:grid; grid-template-columns:260px minmax(0,1fr); gap:16px; margin-top:16px;
    }
    .sidebar,.panel {
      border:1px solid var(--line); border-radius:16px; background:rgba(15,26,34,.94);
    }
    .sidebar { padding:12px; height:fit-content; position:sticky; top:12px; }
    .sidebar-title { padding:5px 5px 11px; color:var(--muted); font-size:13px; }
    .case-list { display:grid; gap:8px; }
    .case-button {
      text-align:left; color:var(--text); background:#111f28; border:1px solid #253946;
      border-radius:12px; padding:11px;
    }
    .case-button.active { border-color:var(--cyan); background:#15303a; }
    .case-top { display:flex; justify-content:space-between; gap:8px; }
    .case-name { color:var(--muted); font-size:13px; margin-top:3px; }
    .case-state { display:inline-block; margin-top:7px; padding:2px 7px; border-radius:999px;
      background:#23343e; color:var(--muted); font-size:11px; }
    .case-state.done { color:#8ce7c4; background:#164232; }
    .workspace { min-width:0; display:grid; gap:14px; }
    .panel { padding:16px; }
    .instrument { display:flex; gap:12px; align-items:flex-start; justify-content:space-between; }
    .instrument h2 { font-size:23px; }
    .badges { display:flex; flex-wrap:wrap; gap:7px; margin-top:8px; }
    .badge { border:1px solid #365260; border-radius:999px; padding:4px 9px; font-size:12px; color:#bad0d9; }
    .flow { display:grid; grid-template-columns:repeat(4,1fr); gap:7px; margin-top:13px; }
    .flow-step { border:1px solid var(--line); border-radius:10px; padding:8px; text-align:center; color:var(--muted); font-size:12px; }
    .flow-step.active { color:var(--text); border-color:var(--cyan); background:#15313a; }
    .flow-step.done { color:#8ce7c4; border-color:#27684f; }
    .section-head { display:flex; align-items:flex-end; justify-content:space-between; gap:12px; margin-bottom:12px; }
    .section-head small { color:var(--muted); }
    .chart-card { border:1px solid #243945; border-radius:13px; background:#0a141b; padding:10px; }
    canvas { display:block; width:100%; height:360px; }
    .tempo canvas { height:130px; }
    .facts { display:grid; grid-template-columns:repeat(auto-fit,minmax(145px,1fr)); gap:8px; margin-top:10px; }
    .fact { border:1px solid #253b47; background:#101e27; border-radius:10px; padding:9px; min-width:0; }
    .fact span { display:block; color:var(--muted); font-size:11px; margin-bottom:4px; }
    .fact strong { font-size:14px; overflow-wrap:anywhere; }
    details { margin-top:10px; border:1px solid #263b47; border-radius:10px; background:#0d1921; }
    summary { cursor:pointer; color:#b8cbd3; padding:10px 12px; }
    .indicator-grid { padding:0 12px 12px; display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:7px; }
    .decision-card { margin-top:14px; border-top:1px solid var(--line); padding-top:14px; }
    .decision-card h3 { font-size:17px; }
    .decision-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:9px; margin-top:10px; }
    .decision {
      min-height:50px; border:1px solid #38505c; border-radius:11px; color:var(--text); background:#13232c;
      font-weight:700;
    }
    .decision[data-value="buy"].selected { color:var(--green); border-color:var(--green); background:rgba(65,211,157,.1); }
    .decision[data-value="sell"].selected { color:var(--red); border-color:var(--red); background:rgba(255,119,128,.1); }
    .decision[data-value="wait"].selected { color:var(--amber); border-color:var(--amber); background:rgba(244,189,85,.1); }
    .decision:disabled { cursor:default; opacity:.72; }
    .memo { margin-top:12px; }
    textarea {
      width:100%; min-height:72px; resize:vertical; color:var(--text); background:#09141b;
      border:1px solid #304651; border-radius:10px; padding:10px;
    }
    .lock-row { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-top:11px; }
    .error { color:#ff9ea4; min-height:1.4em; font-size:13px; }
    .locked-summary { display:flex; flex-wrap:wrap; gap:8px; margin-top:10px; }
    .locked-chip { border:1px solid #35505d; border-radius:999px; padding:5px 9px; color:#c6d8df; font-size:12px; }
    .notice { border-left:3px solid var(--amber); padding:8px 11px; background:rgba(244,189,85,.07); color:#d9c89e; margin:10px 0; line-height:1.55; }
    .result-panel { display:grid; gap:12px; }
    .guide { border:1px solid #2d5260; border-radius:13px; background:#10242d; padding:14px; line-height:1.65; }
    .result-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:8px; }
    .metric { border:1px solid #2b414c; border-radius:11px; background:#0d1a22; padding:11px; }
    .metric span { color:var(--muted); display:block; font-size:12px; margin-bottom:5px; }
    .metric strong { font-size:18px; }
    .positive { color:var(--green); } .negative { color:var(--red); }
    .intervals { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
    .interval { border:1px solid #315044; background:#102b22; color:#9be4c7; border-radius:999px; padding:4px 8px; font-size:12px; }
    .source { color:var(--muted); font-size:12px; line-height:1.65; overflow-wrap:anywhere; }
    @media (max-width:900px) {
      .layout { grid-template-columns:1fr; }
      .sidebar { position:static; }
      .case-list { grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); }
      .topbar,.instrument { flex-direction:column; }
      .actions { justify-content:flex-start; }
      canvas { height:310px; }
    }
    @media (max-width:560px) {
      .shell { padding:9px; }
      .flow { grid-template-columns:repeat(2,1fr); }
      .decision-grid { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header class="topbar">
      <div>
        <h1>日足 → ザラ場 エントリー判断学習</h1>
        <div class="subtitle">未来を隠した2段階判断。必須回答は各段階の「買い・売り・静観」だけです。</div>
      </div>
      <div class="actions">
        <button class="button" id="exportButton">回答履歴をJSON保存</button>
        <button class="button danger" id="resetButton">履歴を全消去</button>
      </div>
    </header>

    <div class="layout">
      <aside class="sidebar">
        <div class="sidebar-title">学習ケース <span id="progressText"></span></div>
        <div class="case-list" id="caseList"></div>
      </aside>

      <section class="workspace">
        <section class="panel">
          <div class="instrument">
            <div>
              <h2 id="instrumentTitle"></h2>
              <div class="muted" id="instrumentMeta"></div>
              <div class="badges" id="badges"></div>
            </div>
            <button class="button" id="newAttemptButton" hidden>新しい回答としてやり直す</button>
          </div>
          <div class="flow" id="flow"></div>
          <div class="locked-summary" id="lockedSummary"></div>
        </section>

        <section class="panel" id="dailyStage">
          <div class="section-head">
            <div><h2>1. 日足判断</h2><small>対象日前日まで。対象日の日足・寄り付きは未使用。</small></div>
          </div>
          <div class="chart-card"><canvas id="dailyChart"></canvas></div>
          <div class="facts" id="dailyFacts"></div>
          <details>
            <summary>補助指標を見る（BB20・RSI14・MACD）</summary>
            <div class="indicator-grid" id="dailyIndicators"></div>
          </details>
          <div class="decision-card" id="dailyDecisionCard">
            <h3>日足だけならどうする？</h3>
            <div class="decision-grid" data-stage="daily">
              <button class="decision" data-value="buy">買い</button>
              <button class="decision" data-value="sell">売り</button>
              <button class="decision" data-value="wait">静観</button>
            </div>
            <div class="lock-row">
              <button class="button primary" id="lockDailyButton">日足判断を固定してザラ場へ</button>
              <span class="error" id="dailyError"></span>
            </div>
          </div>
        </section>

        <section class="panel" id="intradayStage" hidden>
          <div class="section-head">
            <div><h2>2. ザラ場判断</h2><small id="intradayCutoffNote"></small></div>
          </div>
          <div id="lateOpenNotice"></div>
          <div class="chart-card"><canvas id="intradayChart"></canvas></div>
          <div class="facts" id="intradayFacts"></div>
          <details>
            <summary>補助指標を見る（BB20・RSI14・MACD）</summary>
            <div class="indicator-grid" id="intradayIndicators"></div>
          </details>
          <div class="chart-card tempo" style="margin-top:10px">
            <div class="muted" style="font-size:12px;margin-bottom:4px">30秒約定テンポ（棒の高さ＝約定件数）</div>
            <canvas id="tickChart"></canvas>
          </div>
          <div class="notice" id="marketSectorNotice"></div>
          <div class="decision-card" id="intradayDecisionCard">
            <h3>この時点でエントリーする？</h3>
            <div class="decision-grid" data-stage="intraday">
              <button class="decision" data-value="buy">買い</button>
              <button class="decision" data-value="sell">売り</button>
              <button class="decision" data-value="wait">静観</button>
            </div>
            <div class="memo">
              <label for="memoInput" class="muted">任意メモ（1欄のみ）</label>
              <textarea id="memoInput" placeholder="判断理由を短く残す場合だけ記入"></textarea>
            </div>
            <div class="lock-row">
              <button class="button primary" id="lockIntradayButton">ザラ場判断を固定</button>
              <span class="error" id="intradayError"></span>
            </div>
          </div>
          <div class="lock-row" id="revealRow" hidden>
            <button class="button primary" id="revealButton">結果を開く</button>
            <span class="muted">2つの判断は固定済みです。</span>
          </div>
        </section>

        <section class="panel" id="resultStage" hidden>
          <div class="section-head">
            <div><h2>3. 結果</h2><small>エントリー判断の答え合わせ。途中利確の唯一解は示しません。</small></div>
          </div>
          <div class="result-panel" id="resultPanel"></div>
        </section>

        <section class="panel source" id="sourceNote"></section>
      </section>
    </div>
  </main>

  <script>
    const TRAINING = __PUBLIC_DATA__;
    const STORAGE_KEY = "technical-entry-training-v2";
    const RESULT_SCRIPT = "./technical_entry_training_results.js";
    const LABELS = {buy:"買い",sell:"売り",wait:"静観"};
    const COLORS = {
      up:"#41d39d",down:"#ff7780",grid:"#21333d",text:"#a8bbc3",
      amber:"#f4bd55",blue:"#69b7ff",purple:"#bc91ff",cyan:"#65d8e8",muted:"#718992"
    };

    let state = loadState();
    let currentCase = TRAINING.cases[0];
    let dailyDraft = "";
    let intradayDraft = "";
    const resultCache = new Map();
    const resultPromises = new Map();

    function loadState() {
      try {
        const parsed=JSON.parse(localStorage.getItem(STORAGE_KEY)||"{}");
        if(parsed&&parsed.version===2&&parsed.cases)return parsed;
      } catch(error) { console.warn(error); }
      return {version:2,cases:{}};
    }
    function persistState(){localStorage.setItem(STORAGE_KEY,JSON.stringify(state));}
    function attemptsFor(caseId=currentCase.id){
      if(!Array.isArray(state.cases[caseId]))state.cases[caseId]=[];
      return state.cases[caseId];
    }
    function currentAttempt(){
      const attempts=attemptsFor();
      if(!attempts.length){
        attempts.push({attemptId:`${currentCase.id}-${Date.now()}`,createdAt:new Date().toISOString(),dailyDecision:"",intradayDecision:"",memo:""});
        persistState();
      }
      return attempts.at(-1);
    }
    function stageOf(attempt=currentAttempt()){
      if(!attempt.dailyDecision)return "daily";
      if(!attempt.intradayDecision)return "intraday";
      if(!attempt.revealedAt)return "ready";
      return "result";
    }
    function completed(caseId){
      return attemptsFor(caseId).some(item=>item.revealedAt);
    }
    function escapeHtml(value){
      return String(value??"").replace(/[&<>"']/g,char=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#039;"}[char]));
    }
    function number(value,digits=1){
      return value===null||value===undefined||Number.isNaN(Number(value))?"—":Number(value).toLocaleString("ja-JP",{maximumFractionDigits:digits});
    }
    function money(value){
      if(value===null||value===undefined)return "—";
      const n=Number(value); return `${n>=0?"+":""}${Math.round(n).toLocaleString("ja-JP")}円`;
    }
    function signed(value,suffix="",digits=1){
      if(value===null||value===undefined)return "—";
      const n=Number(value); return `${n>=0?"+":""}${number(n,digits)}${suffix}`;
    }
    function timeOnly(value,seconds=true){
      if(!value)return "—";
      const date=new Date(value);
      return date.toLocaleTimeString("ja-JP",{hour:"2-digit",minute:"2-digit",second:seconds?"2-digit":undefined,hour12:false});
    }
    function fact(label,value){return `<div class="fact"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;}
    function metric(label,value,tone=""){return `<div class="metric"><span>${escapeHtml(label)}</span><strong class="${tone}">${escapeHtml(value)}</strong></div>`;}
    function setupCanvas(canvas){
      const ratio=window.devicePixelRatio||1, rect=canvas.getBoundingClientRect();
      const width=Math.max(320,Math.round(rect.width)),height=Math.max(120,Math.round(rect.height));
      canvas.width=Math.round(width*ratio);canvas.height=Math.round(height*ratio);
      const ctx=canvas.getContext("2d");ctx.setTransform(ratio,0,0,ratio,0,0);
      return {ctx,width,height};
    }
    function drawCandleChart(canvas,rows,options={}){
      if(!canvas||!rows?.length)return;
      const {ctx,width,height}=setupCanvas(canvas);ctx.clearRect(0,0,width,height);
      const plot={left:58,top:15,width:Math.max(40,width-76),height:Math.max(80,height-82)};
      const volume={top:plot.top+plot.height+10,height:35};
      const extra=(options.horizontal||[]).map(item=>item.value).filter(value=>Number.isFinite(value));
      const low=Math.min(...rows.map(row=>row.low),...extra),high=Math.max(...rows.map(row=>row.high),...extra);
      const pad=Math.max((high-low)*.05,1),yLow=low-pad,yHigh=high+pad;
      const yFor=value=>plot.top+(yHigh-value)/(yHigh-yLow)*plot.height;
      const xFor=index=>plot.left+plot.width*(index+.5)/rows.length;
      ctx.strokeStyle=COLORS.grid;ctx.lineWidth=1;ctx.font="11px -apple-system,sans-serif";
      for(let i=0;i<5;i++){
        const y=plot.top+plot.height*i/4,value=yHigh-(yHigh-yLow)*i/4;
        ctx.beginPath();ctx.moveTo(plot.left,y);ctx.lineTo(plot.left+plot.width,y);ctx.stroke();
        ctx.fillStyle=COLORS.text;ctx.textAlign="right";ctx.fillText(number(value,1),plot.left-6,y+4);
      }
      const maxVolume=Math.max(...rows.map(row=>row.volume||0),1);
      const candleWidth=Math.max(1.2,Math.min(7,plot.width/rows.length*.64));
      rows.forEach((row,index)=>{
        const x=xFor(index),up=row.close>=row.open,color=up?COLORS.up:COLORS.down;
        ctx.strokeStyle=color;ctx.fillStyle=color;
        ctx.beginPath();ctx.moveTo(x,yFor(row.high));ctx.lineTo(x,yFor(row.low));ctx.stroke();
        const yo=yFor(row.open),yc=yFor(row.close);
        ctx.fillRect(x-candleWidth/2,Math.min(yo,yc),candleWidth,Math.max(1,Math.abs(yo-yc)));
        const vh=(row.volume||0)/maxVolume*volume.height;
        ctx.globalAlpha=.45;ctx.fillRect(x-candleWidth/2,volume.top+volume.height-vh,candleWidth,vh);ctx.globalAlpha=1;
      });
      (options.lines||[]).forEach(line=>{
        ctx.strokeStyle=line.color;ctx.lineWidth=line.width||1.4;ctx.beginPath();let started=false;
        rows.forEach((row,index)=>{
          const value=row[line.key];if(value===null||value===undefined)return;
          const x=xFor(index),y=yFor(value);if(!started){ctx.moveTo(x,y);started=true;}else ctx.lineTo(x,y);
        });ctx.stroke();
      });
      (options.horizontal||[]).forEach(item=>{
        if(!Number.isFinite(item.value))return;
        const y=yFor(item.value);ctx.strokeStyle=item.color||COLORS.muted;ctx.setLineDash(item.dash||[5,4]);
        ctx.beginPath();ctx.moveTo(plot.left,y);ctx.lineTo(plot.left+plot.width,y);ctx.stroke();ctx.setLineDash([]);
        ctx.fillStyle=item.color||COLORS.muted;ctx.textAlign="left";ctx.fillText(item.label||"",plot.left+4,y-3);
      });
      if(options.cutoff){
        const cutoffMs=new Date(options.cutoff).getTime();
        let index=rows.findIndex(row=>new Date(row.datetime).getTime()>=cutoffMs);
        if(index<0)index=rows.length-1;
        const x=xFor(index);ctx.strokeStyle=COLORS.amber;ctx.setLineDash([6,4]);ctx.beginPath();ctx.moveTo(x,plot.top);ctx.lineTo(x,volume.top+volume.height);ctx.stroke();ctx.setLineDash([]);
        ctx.fillStyle=COLORS.amber;ctx.textAlign="left";ctx.fillText("判断時刻",x+4,plot.top+12);
      }
      ctx.fillStyle=COLORS.text;ctx.textAlign="center";
      const labels=Math.min(6,rows.length);
      for(let i=0;i<labels;i++){
        const index=Math.round((rows.length-1)*i/Math.max(1,labels-1));
        const label=options.daily?rows[index].date.slice(5):rows[index].time;
        ctx.fillText(label,xFor(index),volume.top+volume.height+17);
      }
    }
    function drawTempo(canvas,rows){
      if(!canvas||!rows?.length)return;
      const {ctx,width,height}=setupCanvas(canvas);ctx.clearRect(0,0,width,height);
      const plot={left:48,top:12,width:Math.max(30,width-64),height:Math.max(40,height-34)};
      const sorted=rows.map(row=>row.trades).sort((a,b)=>a-b);
      const cap=sorted[Math.floor((sorted.length-1)*.96)]||1;
      const barWidth=Math.max(1,plot.width/rows.length*.82);
      rows.forEach((row,index)=>{
        const x=plot.left+plot.width*(index+.5)/rows.length;
        const h=Math.min(1,row.trades/cap)*plot.height;
        ctx.fillStyle=COLORS.blue;ctx.globalAlpha=.72;ctx.fillRect(x-barWidth/2,plot.top+plot.height-h,barWidth,h);ctx.globalAlpha=1;
      });
      ctx.strokeStyle=COLORS.grid;ctx.beginPath();ctx.moveTo(plot.left,plot.top+plot.height);ctx.lineTo(plot.left+plot.width,plot.top+plot.height);ctx.stroke();
      ctx.fillStyle=COLORS.text;ctx.font="11px -apple-system,sans-serif";ctx.textAlign="left";
      ctx.fillText(rows[0].time.slice(0,5),plot.left,plot.top+plot.height+16);
      ctx.textAlign="right";ctx.fillText(rows.at(-1).time.slice(0,5),plot.left+plot.width,plot.top+plot.height+16);
    }
    function renderCaseList(){
      const root=document.getElementById("caseList");
      root.innerHTML=TRAINING.cases.map(item=>`<button class="case-button ${item.id===currentCase.id?"active":""}" data-id="${item.id}">
        <div class="case-top"><strong>${escapeHtml(item.ticker)}</strong><span>${escapeHtml(item.date.slice(5))}</span></div>
        <div class="case-name">${escapeHtml(item.name)}</div>
        <span class="case-state ${completed(item.id)?"done":""}">${completed(item.id)?"結果確認済み":"未完了"}</span>
      </button>`).join("");
      root.querySelectorAll("button").forEach(button=>button.addEventListener("click",()=>{
        saveMemo();currentCase=TRAINING.cases.find(item=>item.id===button.dataset.id);
        dailyDraft="";intradayDraft="";renderAll();
      }));
      const count=TRAINING.cases.filter(item=>completed(item.id)).length;
      document.getElementById("progressText").textContent=`${count}/${TRAINING.caseCount}`;
    }
    function renderHeader(){
      const attempt=currentAttempt(),stage=stageOf(attempt);
      document.getElementById("instrumentTitle").textContent=`${currentCase.name}  ${currentCase.ticker}`;
      document.getElementById("instrumentMeta").textContent=`${currentCase.date}｜判断時刻 ${currentCase.cutoff}`;
      document.getElementById("badges").innerHTML=`<span class="badge">100株</span><span class="badge">固定SL 5,000円</span><span class="badge">盲検リプレイ</span>`;
      const stages=["daily","intraday","ready","result"],names=["日足","ザラ場","判断固定","結果"];
      document.getElementById("flow").innerHTML=stages.map((value,index)=>{
        const currentIndex=stages.indexOf(stage);
        return `<div class="flow-step ${index<currentIndex?"done":index===currentIndex?"active":""}">${index+1}. ${names[index]}</div>`;
      }).join("");
      const chips=[];
      if(attempt.dailyDecision)chips.push(`<span class="locked-chip">日足：${LABELS[attempt.dailyDecision]}（固定）</span>`);
      if(attempt.intradayDecision)chips.push(`<span class="locked-chip">ザラ場：${LABELS[attempt.intradayDecision]}（固定）</span>`);
      document.getElementById("lockedSummary").innerHTML=chips.join("");
      document.getElementById("newAttemptButton").hidden=stage!=="result";
    }
    function renderDaily(){
      const attempt=currentAttempt(),locked=Boolean(attempt.dailyDecision);
      drawCandleChart(document.getElementById("dailyChart"),currentCase.daily,{
        daily:true,
        lines:[{key:"sma25",color:COLORS.amber},{key:"sma75",color:COLORS.blue},{key:"sma200",color:COLORS.purple,width:1.8}],
        horizontal:[
          {value:currentCase.dailyContext.previousHigh,label:"前日高",color:"#83a7b5"},
          {value:currentCase.dailyContext.previousLow,label:"前日安",color:"#83a7b5"},
          {value:currentCase.dailyContext.recentHigh20,label:"20日高",color:COLORS.red},
          {value:currentCase.dailyContext.recentLow20,label:"20日安",color:COLORS.green}
        ]
      });
      const c=currentCase.dailyContext,i=c.indicators;
      const gaps=c.recentGaps.length?c.recentGaps.map(g=>`${g.date.slice(5)} ${g.direction==="up"?"上窓":"下窓"} ${number(g.lower)}–${number(g.upper)}`).join(" / "):"直近60日に窓候補なし";
      document.getElementById("dailyFacts").innerHTML=[
        fact("前営業日終値",number(c.previousClose)),
        fact("25SMA / 距離",`${number(i.sma25)} / ${signed(c.sma25DistancePct,"%",1)}`),
        fact("75SMA / 距離",`${number(i.sma75)} / ${signed(c.sma75DistancePct,"%",1)}`),
        fact("200SMA / 距離",`${number(i.sma200)} / ${signed(c.sma200DistancePct,"%",1)}`),
        fact("25/75/200SMA 5日傾き",`${signed(c.sma25Slope5)} / ${signed(c.sma75Slope5)} / ${signed(c.sma200Slope5)}`),
        fact("直近20日 高値 / 安値",`${number(c.recentHigh20)} / ${number(c.recentLow20)}`),
        fact("前日出来高 / 20日平均",`${number(c.previousVolume,0)} / ${number(c.averageVolume20,0)}`),
        fact("直近の窓候補",gaps)
      ].join("");
      document.getElementById("dailyIndicators").innerHTML=[
        fact("BB20 上 / 中 / 下",`${number(i.bb_upper)} / ${number(i.bb_mid)} / ${number(i.bb_lower)}`),
        fact("RSI14",number(i.rsi14)),
        fact("MACD / Signal",`${number(i.macd)} / ${number(i.macd_signal)}`),
        fact("Histogram",number(i.macd_hist))
      ].join("");
      const card=document.getElementById("dailyDecisionCard");
      card.querySelectorAll(".decision").forEach(button=>{
        const value=attempt.dailyDecision||dailyDraft;
        button.classList.toggle("selected",button.dataset.value===value);button.disabled=locked;
      });
      document.getElementById("lockDailyButton").hidden=locked;
      document.getElementById("dailyError").textContent="";
    }
    function renderIntraday(){
      const attempt=currentAttempt(),stage=stageOf(attempt),visible=stage!=="daily";
      document.getElementById("intradayStage").hidden=!visible;
      if(!visible)return;
      document.getElementById("intradayCutoffNote").textContent=`${currentCase.cutoff}より前に完了した1分足と、同時刻までのtickのみ。`;
      const c=currentCase.intradayContext,i=c.indicators;
      document.getElementById("lateOpenNotice").innerHTML=c.lateOpen?`<div class="notice">初約定は ${timeOnly(c.firstTradeTime)}。09:00から初約定までは欠損ではなく約定なしです。</div>`:"";
      drawCandleChart(document.getElementById("intradayChart"),currentCase.intraday,{
        lines:[{key:"sma5",color:COLORS.cyan},{key:"sma20",color:COLORS.blue},{key:"vwap",color:COLORS.amber,width:1.8}],
        horizontal:[
          {value:currentCase.dailyContext.previousClose,label:"前日終値",color:"#83a7b5"},
          {value:c.open,label:"寄付",color:COLORS.purple}
        ]
      });
      drawTempo(document.getElementById("tickChart"),currentCase.tickTempo);
      document.getElementById("intradayFacts").innerHTML=[
        fact("判断時点価格",number(c.lastVisiblePrice)),
        fact("VWAP",number(i.vwap)),
        fact("5SMA / 20SMA",`${number(i.sma5)} / ${number(i.sma20)}`),
        fact("前日終値",number(currentCase.dailyContext.previousClose)),
        fact("寄付",number(c.open)),
        fact("見えている高値 / 安値",`${number(c.visibleHigh)} / ${number(c.visibleLow)}`),
        fact("累積出来高",number(c.visibleVolume,0)),
        fact("直近30秒平均約定件数",number(c.tickTrades30sAverage))
      ].join("");
      document.getElementById("intradayIndicators").innerHTML=[
        fact("BB20 上 / 中 / 下",`${number(i.bb_upper)} / ${number(i.bb_mid)} / ${number(i.bb_lower)}`),
        fact("RSI14",number(i.rsi14)),
        fact("MACD / Signal",`${number(i.macd)} / ${number(i.macd_signal)}`),
        fact("Histogram",number(i.macd_hist))
      ].join("");
      document.getElementById("marketSectorNotice").textContent=c.marketSectorStatus;
      const locked=Boolean(attempt.intradayDecision);
      document.querySelectorAll('[data-stage="intraday"] .decision').forEach(button=>{
        const value=attempt.intradayDecision||intradayDraft;
        button.classList.toggle("selected",button.dataset.value===value);button.disabled=locked;
      });
      document.getElementById("memoInput").value=attempt.memo||"";
      document.getElementById("memoInput").disabled=locked;
      document.getElementById("lockIntradayButton").hidden=locked;
      document.getElementById("intradayError").textContent="";
      document.getElementById("revealRow").hidden=stage!=="ready";
    }
    function saveMemo(){
      const input=document.getElementById("memoInput");
      if(!input||input.disabled||document.getElementById("intradayStage").hidden)return;
      currentAttempt().memo=input.value.trim();persistState();
    }
    function loadResultFor(caseId){
      if(resultCache.has(caseId))return Promise.resolve(resultCache.get(caseId));
      if(resultPromises.has(caseId))return resultPromises.get(caseId);
      const promise=new Promise((resolve,reject)=>{
        const script=document.createElement("script");script.src=RESULT_SCRIPT;
        script.onload=()=>{
          const payload=window.__TECHNICAL_ENTRY_RESULTS__;
          const result=payload?.cases?.find(item=>item.id===caseId);
          delete window.__TECHNICAL_ENTRY_RESULTS__;
          script.remove();
          resultPromises.delete(caseId);
          if(!result){reject(new Error("対象ケースの結果データが空です"));return;}
          resultCache.set(caseId,result);resolve(result);
        };
        script.onerror=()=>{
          script.remove();resultPromises.delete(caseId);
          reject(new Error(`結果データを読み込めません: ${RESULT_SCRIPT}`));
        };
        document.body.appendChild(script);
      });
      resultPromises.set(caseId,promise);
      return promise;
    }
    function orderLabel(value){
      return {neither:"どちらも未到達","plus-first":"+5,000円が先","stop-first":"SLが先","same-time":"同時刻"}[value]||"—";
    }
    function renderResult(){
      const attempt=currentAttempt(),root=document.getElementById("resultPanel");
      document.getElementById("resultStage").hidden=stageOf(attempt)!=="result";
      if(stageOf(attempt)!=="result")return;
      root.innerHTML=`<div class="muted">結果データを読み込み中…</div>`;
      loadResultFor(currentCase.id).then(result=>{
        const guide=result.guidance,outcome=result.outcomes[attempt.intradayDecision];
        const guideHtml=`<div class="guide">
          <strong>記録した判断：</strong>日足 ${LABELS[attempt.dailyDecision]} → ザラ場 ${LABELS[attempt.intradayDecision]}<br>
          <strong>教材上の観察例：</strong>日足 ${LABELS[guide.daily]} → ザラ場 ${LABELS[guide.intraday]}｜${escapeHtml(guide.pattern)}<br>
          <strong>${escapeHtml(guide.lesson)}</strong><br>${escapeHtml(guide.rationale)}<br>
          <strong>否定条件：</strong>${escapeHtml(guide.invalidation)}<br>
          <span class="muted">これは唯一の正解ラベルではありません。静観も、見送った値幅として別評価します。</span>
        </div>`;
        let outcomeHtml="";
        if(attempt.intradayDecision==="wait"){
          outcomeHtml=`<div class="result-grid">
            ${metric("観測基準（次の実約定）",`${number(outcome.referencePrice)}円 ${timeOnly(outcome.referenceTime)}`)}
            ${metric("指定時刻後の最大上昇",`${signed(outcome.maxUpPerShare,"円")} / 100株 ${money(outcome.maxUp100Shares)}`,"positive")}
            ${metric("最大上昇時刻",timeOnly(outcome.maxUpTime))}
            ${metric("指定時刻後の最大下落",`${signed(outcome.maxDownPerShare,"円")} / 100株 ${money(outcome.maxDown100Shares)}`,"negative")}
            ${metric("最大下落時刻",timeOnly(outcome.maxDownTime))}
            ${metric("大引けまでの変化",`${signed(outcome.closeChangePerShare,"円")} / 100株 ${money(outcome.closeChange100Shares)}`,outcome.closeChange100Shares>=0?"positive":"negative")}
          </div>
          <div class="notice">静観では仮想ポジションを建てていません。上下値幅は機会損失の参考値であり、取得可能な利益ではありません。</div>`;
        } else {
          const stopText=outcome.stopReached?`到達 ${timeOnly(outcome.stopTime)}（最初の約定 ${number(outcome.stopArrivalPrice)}円）`:"未到達";
          const intervals=outcome.profitIntervals.length?outcome.profitIntervals.map(item=>`<span class="interval">${timeOnly(item.start,false)}–${timeOnly(item.end,false)}</span>`).join(""):`<span class="muted">含み益の分足区間なし</span>`;
          outcomeHtml=`<div class="result-grid">
            ${metric("仮定エントリー",`${number(outcome.entryPrice)}円 ${timeOnly(outcome.entryTime)}`)}
            ${metric("5,000円固定SL水準",`${number(outcome.stopLevel)}円`)}
            ${metric("SL水準到達",stopText,outcome.stopReached?"negative":"")}
            ${metric("大引け損益",money(outcome.closePnl),outcome.closePnl>=0?"positive":"negative")}
            ${metric("MFE / 時刻",`${money(outcome.mfe)} / ${timeOnly(outcome.mfeTime)}`,"positive")}
            ${metric("MAE / 時刻",`${money(outcome.mae)} / ${timeOnly(outcome.maeTime)}`,"negative")}
            ${metric("初めて含み益",timeOnly(outcome.firstProfitTime))}
            ${metric("初めて+5,000円",timeOnly(outcome.plus5000Time))}
            ${metric("SLと+5,000円の順序",orderLabel(outcome.stopVsPlusOrder))}
            ${metric("含み益だった分数",`${number(outcome.positiveMinutes,0)}分`)}
          </div>
          <div><strong>含み益だった時間帯</strong><div class="intervals">${intervals}</div></div>
          <div class="notice">SLは水準への到達判定です。飛び越えた場合に、その水準で実際に約定できたことを意味しません。MFE時刻も実運用上の最適利確時刻ではありません。</div>`;
        }
        root.innerHTML=guideHtml+outcomeHtml+`<div class="chart-card"><canvas id="resultChart"></canvas></div><div class="notice">${escapeHtml(guide.warning)}</div>`;
        const sideOutcome=attempt.intradayDecision==="wait"?null:outcome;
        drawCandleChart(document.getElementById("resultChart"),result.fullIntraday,{
          lines:[{key:"sma5",color:COLORS.cyan},{key:"sma20",color:COLORS.blue},{key:"vwap",color:COLORS.amber,width:1.8}],
          cutoff:`${currentCase.date}T${currentCase.cutoff}:00`,
          horizontal:sideOutcome?[
            {value:sideOutcome.entryPrice,label:"Entry",color:COLORS.cyan},
            {value:sideOutcome.stopLevel,label:"SL",color:COLORS.red}
          ]:[]
        });
      }).catch(error=>{root.innerHTML=`<div class="error">${escapeHtml(error.message)}</div>`;});
    }
    function buildExportPayload(){
      const records=[];
      Object.entries(state.cases).forEach(([caseId,attempts])=>{
        (attempts||[]).forEach(attempt=>{
          if(!attempt.dailyDecision&&!attempt.intradayDecision&&!attempt.memo)return;
          records.push({caseId,...attempt});
        });
      });
      return {exportedAt:new Date().toISOString(),version:2,records};
    }
    function renderSource(){
      document.getElementById("sourceNote").innerHTML=`生成：${escapeHtml(TRAINING.generatedAt)}<br>
        日足：${escapeHtml(TRAINING.sources.daily)}<br>分足：${escapeHtml(TRAINING.sources.minute)}<br>
        tick：約定データ ${escapeHtml(TRAINING.sources.tick)}<br>
        tickから板・注文取消・売買主体・成行方向は復元していません。正本grok_trending_archive.parquetは入力・出力・加工に使用していません。`;
    }
    function renderAll(){
      renderCaseList();renderHeader();renderDaily();renderIntraday();renderResult();renderSource();
    }
    document.querySelectorAll('[data-stage="daily"] .decision').forEach(button=>button.addEventListener("click",()=>{
      if(currentAttempt().dailyDecision)return;dailyDraft=button.dataset.value;renderDaily();
    }));
    document.querySelectorAll('[data-stage="intraday"] .decision').forEach(button=>button.addEventListener("click",()=>{
      if(currentAttempt().intradayDecision)return;intradayDraft=button.dataset.value;renderIntraday();
    }));
    document.getElementById("lockDailyButton").addEventListener("click",()=>{
      if(!dailyDraft){document.getElementById("dailyError").textContent="買い・売り・静観のいずれかを選んでください。";return;}
      const attempt=currentAttempt();attempt.dailyDecision=dailyDraft;attempt.dailyDecidedAt=new Date().toISOString();persistState();dailyDraft="";renderAll();
    });
    document.getElementById("lockIntradayButton").addEventListener("click",()=>{
      if(!intradayDraft){document.getElementById("intradayError").textContent="買い・売り・静観のいずれかを選んでください。";return;}
      const attempt=currentAttempt();attempt.intradayDecision=intradayDraft;attempt.intradayDecidedAt=new Date().toISOString();attempt.memo=document.getElementById("memoInput").value.trim();persistState();intradayDraft="";renderAll();
    });
    document.getElementById("memoInput").addEventListener("input",()=>saveMemo());
    document.getElementById("revealButton").addEventListener("click",()=>{
      const attempt=currentAttempt();if(!attempt.dailyDecision||!attempt.intradayDecision)return;
      attempt.revealedAt=new Date().toISOString();persistState();renderAll();
    });
    document.getElementById("newAttemptButton").addEventListener("click",()=>{
      attemptsFor().push({attemptId:`${currentCase.id}-${Date.now()}`,createdAt:new Date().toISOString(),dailyDecision:"",intradayDecision:"",memo:""});
      dailyDraft="";intradayDraft="";persistState();renderAll();
    });
    document.getElementById("exportButton").addEventListener("click",()=>{
      saveMemo();const payload=buildExportPayload();
      const blob=new Blob([JSON.stringify(payload,null,2)],{type:"application/json"});
      const link=document.createElement("a");link.href=URL.createObjectURL(blob);link.download="technical_entry_training_history.json";link.click();
      setTimeout(()=>URL.revokeObjectURL(link.href),1000);
    });
    document.getElementById("resetButton").addEventListener("click",()=>{
      if(!confirm("このブラウザに保存した学習履歴をすべて消去しますか？"))return;
      state={version:2,cases:{}};dailyDraft="";intradayDraft="";persistState();renderAll();
    });
    window.addEventListener("resize",()=>{
      renderDaily();renderIntraday();if(stageOf()==="result")renderResult();
    });
    window.__trainingTestApi={
      getState:()=>JSON.parse(JSON.stringify(state)),
      getStage:()=>stageOf(),
      buildExportPayload,
      isResultLoaded:()=>resultCache.has(currentCase.id),
      hasFullResultPayload:()=>Boolean(window.__TECHNICAL_ENTRY_RESULTS__)
    };
    renderAll();
  </script>
</body>
</html>
"""


def json_for_script(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace(
        "</", "<\\/"
    )


def main() -> None:
    public_data, result_data, audits = build_training_data()
    html = HTML_TEMPLATE.replace("__PUBLIC_DATA__", json_for_script(public_data))
    result_script = (
        "window.__TECHNICAL_ENTRY_RESULTS__="
        + json_for_script(result_data)
        + ';\nwindow.dispatchEvent(new Event("technical-entry-results-ready"));\n'
    )

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    OUTPUT_RESULTS.write_text(result_script, encoding="utf-8")

    print(f"generated: {OUTPUT_HTML}")
    print(f"generated: {OUTPUT_RESULTS}")
    print(f"cases: {public_data['caseCount']}")
    print(f"html_bytes: {OUTPUT_HTML.stat().st_size}")
    print(f"result_bytes: {OUTPUT_RESULTS.stat().st_size}")
    print("validation: manifests=ok, cutoff_boundaries=ok, long_short_sl=ok")
    for audit in audits:
        print(
            "case:"
            f" {audit['id']}"
            f" daily_last={audit['dailyLast'][:10]}"
            f" minute_last={audit['visibleMinuteLast'][11:19]}"
            f" tick_last={audit['visibleTickLast'][11:19]}"
            f" entry={audit['entryTime'][11:19]}@{audit['entryPrice']}"
            f" late_open={audit['lateOpen']}"
            f" buy_sl={audit['buyStopReached']}"
            f" sell_sl={audit['sellStopReached']}"
        )


if __name__ == "__main__":
    main()
