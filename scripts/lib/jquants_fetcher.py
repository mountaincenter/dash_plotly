#!/usr/bin/env python3
"""
J-Quants Data Fetcher (v2)
株価データ・銘柄情報の取得
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import List
import time

import pandas as pd

from scripts.lib.jquants_client import JQuantsClient


# v2 カラム名マッピング（v2短縮名 → v1互換名）
V2_COLUMN_MAP = {
    "O": "Open",
    "H": "High",
    "L": "Low",
    "C": "Close",
    "Vo": "Volume",
    "Va": "TurnoverValue",
    "AdjO": "AdjustmentOpen",
    "AdjH": "AdjustmentHigh",
    "AdjL": "AdjustmentLow",
    "AdjC": "AdjustmentClose",
    "AdjVo": "AdjustmentVolume",
}

# v2 銘柄マスターカラム名マッピング（v2短縮名 → v1互換名）
V2_MASTER_COLUMN_MAP = {
    "CoName": "CompanyName",
    "CoNameEn": "CompanyNameEnglish",
    "S17": "Sector17Code",
    "S17Nm": "Sector17CodeName",
    "S33": "Sector33Code",
    "S33Nm": "Sector33CodeName",
    "ScaleCat": "ScaleCategory",
    "Mkt": "MarketCode",
    "MktNm": "MarketCodeName",
    "Mrgn": "MarginCode",
    "MrgnNm": "MarginCodeName",
}


class JQuantsFetcher:
    """J-Quants API v2 からデータを取得するクラス"""

    def __init__(self, client: JQuantsClient | None = None):
        """
        Args:
            client: JQuantsClient インスタンス。Noneの場合は自動生成
        """
        self.client = client or JQuantsClient()

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """v2カラム名をv1互換形式に変換"""
        if df.empty:
            return df
        return df.rename(columns=V2_COLUMN_MAP)

    def get_listed_info(self) -> pd.DataFrame:
        """
        上場銘柄一覧を取得

        Returns:
            銘柄情報のDataFrame（v1互換カラム名）
        """
        print("[PROGRESS] Requesting /equities/master from J-Quants API v2...")
        data = self.client.request("/equities/master")
        info = data.get("data", [])

        if not info:
            print("[PROGRESS] No data received from J-Quants API")
            return pd.DataFrame()

        print(f"[PROGRESS] Received {len(info)} stocks from J-Quants API")
        df = pd.DataFrame(info)

        # v2カラム名をv1互換に変換
        df = df.rename(columns=V2_MASTER_COLUMN_MAP)

        print("[PROGRESS] Converted to DataFrame with v1-compatible columns")
        return df

    def get_prices_daily(
        self,
        code: str | None = None,
        from_date: str | date | None = None,
        to_date: str | date | None = None,
    ) -> pd.DataFrame:
        """
        日次株価データを取得

        Args:
            code: 銘柄コード（例: "7203"）。Noneの場合は全銘柄
            from_date: 取得開始日（YYYY-MM-DD）
            to_date: 取得終了日（YYYY-MM-DD）

        Returns:
            株価データのDataFrame
        """
        params = {}
        if code:
            params["code"] = code
        if from_date:
            params["from"] = str(from_date)
        if to_date:
            params["to"] = str(to_date)

        data = self.client.request("/equities/bars/daily", params=params)
        prices = data.get("data", [])

        if not prices:
            return pd.DataFrame()

        df = pd.DataFrame(prices)

        # v2カラム名をv1互換に変換
        df = self._normalize_columns(df)

        # 日付列を変換
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])

        # 数値列を変換
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "TurnoverValue"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def get_prices_daily_batch(
        self,
        codes: List[str],
        from_date: str | date | None = None,
        to_date: str | date | None = None,
        batch_delay: float = 1.0,
    ) -> pd.DataFrame:
        """
        複数銘柄の日次株価データを一括取得

        Args:
            codes: 銘柄コードのリスト
            from_date: 取得開始日
            to_date: 取得終了日
            batch_delay: 各リクエスト間の待機時間（秒）

        Returns:
            全銘柄の株価データを結合したDataFrame
        """
        frames = []
        total = len(codes)
        print(f"[PROGRESS] Fetching prices for {total} stocks from J-Quants API v2...")

        for i, code in enumerate(codes, 1):
            try:
                # 10銘柄ごとに進捗表示
                if i % 10 == 0 or i == total:
                    print(f"[PROGRESS] Processing stock {i}/{total} ({code})...", flush=True)

                df = self.get_prices_daily(code, from_date, to_date)
                if not df.empty:
                    frames.append(df)

                # レート制限対策
                if i < total and batch_delay > 0:
                    time.sleep(batch_delay)

            except Exception as e:
                print(f"[WARN] Failed to fetch prices for {code}: {e}")
                continue

        if not frames:
            print("[PROGRESS] No price data retrieved")
            return pd.DataFrame()

        # 空のDataFrameを除外してから結合（FutureWarning対策）
        non_empty_frames = [df for df in frames if not df.empty]

        if not non_empty_frames:
            print("[PROGRESS] No price data retrieved")
            return pd.DataFrame()

        print(f"[PROGRESS] Concatenating data from {len(non_empty_frames)} stocks...")
        # FutureWarning回避: dtypeを明示的に保持
        result = pd.concat(non_empty_frames, ignore_index=True, sort=False)
        print(f"[PROGRESS] Total rows: {len(result)}")
        return result

    def get_latest_trading_day(self, lookback_days: int = 30) -> str:
        """
        取引カレンダーAPIから直近の営業日を取得

        重要: 16時基準のロジック
        - JST 16:00未満: 前営業日を返す
        - JST 16:00以降（26:00含む）: 当日が営業日なら当日、非営業日なら前営業日を返す

        Args:
            lookback_days: 過去何日分のカレンダーを取得するか（デフォルト: 30）

        Returns:
            YYYY-MM-DD形式の日付文字列

        Raises:
            RuntimeError: 取引カレンダーの取得に失敗した場合、または営業日が見つからない場合
        """
        from datetime import timezone

        # GitHub Actions対応: UTC時刻を取得してJSTに変換
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        now_jst = now_utc + timedelta(hours=9)
        jst_hour = now_jst.hour
        jst_date = now_jst.date()

        # 過去lookback_days日分の取引カレンダーを取得
        to_date = jst_date
        from_date = to_date - timedelta(days=lookback_days)

        params = {
            "from": str(from_date),
            "to": str(to_date)
        }

        # v2: /markets/calendar（v1は/markets/trading_calendar）
        response = self.client.request("/markets/calendar", params=params)

        # v2: dataキー
        calendar_data = response.get("data", [])
        if not calendar_data:
            raise RuntimeError("Failed to fetch trading calendar from J-Quants")

        calendar = pd.DataFrame(calendar_data)

        # HolDiv が "1" (営業日) のレコードのみ
        # 0: 非営業日、1: 営業日、2: 半日立会、3: 祝日取引のある非営業日
        # Note: v2では HolidayDivision → HolDiv に変更
        trading_days = calendar[calendar["HolDiv"] == "1"].copy()

        if trading_days.empty:
            raise RuntimeError("No trading days found in the calendar")

        # 16時基準のロジック
        if jst_hour < 16:
            # 16時未満: 前営業日
            cutoff_date = jst_date - timedelta(days=1)
            cutoff_str = str(cutoff_date)
            trading_days = trading_days[trading_days["Date"] <= cutoff_str].copy()
        else:
            # 16時以降（26:00含む）: 当日を含む
            cutoff_str = str(jst_date)
            trading_days = trading_days[trading_days["Date"] <= cutoff_str].copy()

        if trading_days.empty:
            raise RuntimeError("No past trading days found in the calendar")

        # ソートして最新を取得（Date列は文字列だが YYYY-MM-DD 形式なので文字列ソートで正しい）
        trading_days = trading_days.sort_values("Date", ascending=False)

        # 最新の営業日を取得
        latest_trading_day = trading_days.iloc[0]["Date"]

        return latest_trading_day

    def get_previous_trading_day(self, target_date: str | date) -> str | None:
        """
        指定日の前営業日を取得

        Args:
            target_date: 基準日（YYYY-MM-DD形式の文字列またはdateオブジェクト）

        Returns:
            前営業日（YYYY-MM-DD形式）、見つからない場合はNone
        """
        target_date_str = str(target_date)[:10]  # YYYY-MM-DD形式に正規化

        # 取引カレンダーを取得（前後30日）
        from_date = (datetime.strptime(target_date_str, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        to_date = (datetime.strptime(target_date_str, "%Y-%m-%d") + timedelta(days=5)).strftime("%Y-%m-%d")

        try:
            # v2: /markets/calendar（v1は/markets/trading_calendar）
            response = self.client.request(
                "/markets/calendar",
                params={"from": from_date, "to": to_date}
            )

            # v2: dataキー
            calendar_data = response.get("data", [])
            if not calendar_data:
                return None

            calendar = pd.DataFrame(calendar_data)
            # 営業日のみ抽出（HolDiv == "1"）
            # Note: v2では HolidayDivision → HolDiv に変更
            trading_days = calendar[calendar["HolDiv"] == "1"]["Date"].tolist()

            if target_date_str in trading_days:
                idx = trading_days.index(target_date_str)
                if idx > 0:
                    return trading_days[idx - 1]
            else:
                # target_dateが営業日でない場合、直前の営業日を返す
                prev_days = [d for d in trading_days if d < target_date_str]
                if prev_days:
                    return max(prev_days)

            return None

        except Exception as e:
            print(f"[WARN] Failed to get previous trading day for {target_date_str}: {e}")
            return None

    def get_indices(
        self,
        code: str | None = None,
        from_date: str | date | None = None,
        to_date: str | date | None = None,
    ) -> pd.DataFrame:
        """
        指数データを取得（TOPIX, Prime, Standard, Growth, 33業種別など）

        Args:
            code: 指数コード（例: "0000"=TOPIX）。Noneの場合は全指数
            from_date: 取得開始日（YYYY-MM-DD）
            to_date: 取得終了日（YYYY-MM-DD）

        Returns:
            指数データのDataFrame
        """
        params = {}
        if code:
            params["code"] = code
        if from_date:
            params["from"] = str(from_date)
        if to_date:
            params["to"] = str(to_date)

        data = self.client.request("/indices/bars/daily", params=params)
        indices = data.get("data", [])

        if not indices:
            return pd.DataFrame()

        df = pd.DataFrame(indices)

        # v2カラム名をv1互換に変換
        df = self._normalize_columns(df)

        # 日付列を変換
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])

        # 数値列を変換
        numeric_cols = ["Open", "High", "Low", "Close"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def get_indices_topix(
        self,
        from_date: str | date | None = None,
        to_date: str | date | None = None,
    ) -> pd.DataFrame:
        """
        TOPIX指数データを取得

        Args:
            from_date: 取得開始日（YYYY-MM-DD）
            to_date: 取得終了日（YYYY-MM-DD）

        Returns:
            TOPIX指数データのDataFrame
        """
        # v2: /indices/bars/daily でcode=0000を指定
        return self.get_indices(code="0000", from_date=from_date, to_date=to_date)

    def get_index_options(
        self,
        date: str | date | None = None,
    ) -> pd.DataFrame:
        """
        日経225オプション四本値データを取得

        Args:
            date: 取得日（YYYY-MM-DD or YYYYMMDD）

        Returns:
            オプションデータのDataFrame
        """
        params = {}
        if date:
            # YYYY-MM-DD → YYYYMMDD
            params["date"] = str(date).replace("-", "")

        data = self.client.request("/derivatives/options/bars/daily", params=params)
        options = data.get("data", [])

        if not options:
            return pd.DataFrame()

        df = pd.DataFrame(options)

        # v2カラム名をv1互換に変換
        df = self._normalize_columns(df)

        # 日付列を変換
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])

        # 数値列を変換
        numeric_cols = [
            "WholeDayOpen", "WholeDayHigh", "WholeDayLow", "WholeDayClose",
            "NightSessionOpen", "NightSessionHigh", "NightSessionLow", "NightSessionClose",
            "DaySessionOpen", "DaySessionHigh", "DaySessionLow", "DaySessionClose",
            "Volume", "OpenInterest", "TurnoverValue",
            "StrikePrice", "SettlementPrice", "TheoreticalPrice",
            "UnderlyingPrice", "ImpliedVolatility", "BaseVolatility", "InterestRate"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def convert_to_yfinance_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        J-Quants形式のDataFrameをyfinance互換形式に変換

        Args:
            df: J-Quants APIから取得したDataFrame

        Returns:
            yfinance互換形式のDataFrame
        """
        if df.empty:
            return pd.DataFrame(
                columns=["date", "Open", "High", "Low", "Close", "Volume", "ticker"]
            )

        result = df.copy()

        # カラム名を統一
        rename_map = {
            "Date": "date",
            "Code": "ticker",
        }
        result = result.rename(columns=rename_map)

        # ティッカーシンボルを変換（yfinance互換）
        # J-Quantsの5桁コード -> 最後の1桁（チェックデジット）を削除して.Tを追加
        # 例: "27490" -> "2749.T"
        if "ticker" in result.columns:
            result["ticker"] = result["ticker"].astype(str).str[:-1] + ".T"

        # 必要なカラムのみ抽出
        required_cols = ["date", "Open", "High", "Low", "Close", "Volume", "ticker"]
        available_cols = [col for col in required_cols if col in result.columns]
        result = result[available_cols]

        # 日付でソート
        if "date" in result.columns and "ticker" in result.columns:
            result = result.sort_values(["ticker", "date"]).reset_index(drop=True)

        return result
