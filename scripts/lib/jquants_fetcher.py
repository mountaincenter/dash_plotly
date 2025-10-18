#!/usr/bin/env python3
"""
J-Quants Data Fetcher
株価データ・銘柄情報の取得
"""

from __future__ import annotations

from datetime import date, datetime
from typing import List
import time

import pandas as pd

from scripts.lib.jquants_client import JQuantsClient


class JQuantsFetcher:
    """J-Quants APIからデータを取得するクラス"""

    def __init__(self, client: JQuantsClient | None = None):
        """
        Args:
            client: JQuantsClient インスタンス。Noneの場合は自動生成
        """
        self.client = client or JQuantsClient()

    def get_listed_info(self) -> pd.DataFrame:
        """
        上場銘柄一覧を取得

        Returns:
            銘柄情報のDataFrame
        """
        print("[PROGRESS] Requesting /listed/info from J-Quants API...")
        data = self.client.request("/listed/info")
        info = data.get("info", [])

        if not info:
            print("[PROGRESS] No data received from J-Quants API")
            return pd.DataFrame()

        print(f"[PROGRESS] Received {len(info)} stocks from J-Quants API")
        df = pd.DataFrame(info)
        print("[PROGRESS] Converted to DataFrame")
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

        data = self.client.request("/prices/daily_quotes", params=params)
        prices = data.get("daily_quotes", [])

        if not prices:
            return pd.DataFrame()

        df = pd.DataFrame(prices)

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
        print(f"[PROGRESS] Fetching prices for {total} stocks from J-Quants API...")

        for i, code in enumerate(codes, 1):
            try:
                # 100銘柄ごとに進捗表示
                if i % 100 == 0 or i == total:
                    print(f"[PROGRESS] Processing stock {i}/{total} ({code})...")

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

        print(f"[PROGRESS] Concatenating data from {len(frames)} stocks...")
        result = pd.concat(frames, ignore_index=True)
        print(f"[PROGRESS] Total rows: {len(result)}")
        return result

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
