#!/usr/bin/env python3
"""
J-Quants API Client (v2)
APIキー認証によるシンプルなクライアント
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Any
import requests
from dotenv import load_dotenv


class JQuantsClient:
    """J-Quants API v2 認証クライアント"""

    def __init__(self, env_file: str | Path | None = None):
        """
        Args:
            env_file: .env.jquants のパス。Noneの場合は自動検出
        """
        # ローカル開発: .env.jquantsファイルを読む
        # GitHub Actions: 環境変数から直接読む（ファイル不要）
        if env_file is None:
            # scripts/lib/jquants_client.py から見てプロジェクトルート
            env_file = Path(__file__).resolve().parents[2] / ".env.jquants"

        # ファイルが存在する場合のみload_dotenv（GitHub Actionsでは存在しない）
        if Path(env_file).exists():
            load_dotenv(env_file)

        # v2: APIキー認証
        self.api_key = os.getenv("JQUANTS_API_KEY")

        # v1互換: 旧環境変数も確認（移行期間中）
        if not self.api_key:
            # v1の認証情報があれば警告
            if os.getenv("JQUANTS_REFRESH_TOKEN") or os.getenv("JQUANTS_MAIL_ADDRESS"):
                print("[WARN] v1認証情報（REFRESH_TOKEN/MAIL_ADDRESS）は非推奨です。")
                print("[WARN] JQUANTS_API_KEYを設定してください。")
                print("[WARN] https://jpx-jquants.com/ でAPIキーを発行できます。")
            raise ValueError(
                "JQUANTS_API_KEY is required. "
                "Please set it in .env.jquants or as an environment variable. "
                "Get your API key from: https://jpx-jquants.com/"
            )

        self.plan = os.getenv("JQUANTS_PLAN", "free")
        self.base_url = os.getenv("JQUANTS_API_BASE_URL", "https://api.jquants.com/v2")

    def get_headers(self) -> Dict[str, str]:
        """API呼び出し用のヘッダーを取得"""
        return {
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
        }

    def request(
        self,
        endpoint: str,
        method: str = "GET",
        params: Dict[str, Any] | None = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        J-Quants APIへのリクエスト

        Args:
            endpoint: APIエンドポイント（例: "/equities/master"）
            method: HTTPメソッド
            params: クエリパラメータ
            **kwargs: requests.requestに渡す追加引数

        Returns:
            APIレスポンスのJSON
        """
        url = f"{self.base_url}{endpoint}"
        headers = self.get_headers()

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            timeout=kwargs.pop("timeout", 30),
            **kwargs,
        )
        response.raise_for_status()

        return response.json()

    def request_with_pagination(
        self,
        endpoint: str,
        params: Dict[str, Any] | None = None,
        data_key: str = "data",
        max_pages: int = 100,
        **kwargs,
    ) -> list:
        """
        ページネーション対応のAPIリクエスト

        Args:
            endpoint: APIエンドポイント
            params: クエリパラメータ
            data_key: レスポンス内のデータキー
            max_pages: 最大ページ数（無限ループ防止）
            **kwargs: requests.requestに渡す追加引数

        Returns:
            全ページのデータを結合したリスト
        """
        all_data = []
        params = params or {}
        page = 0

        while page < max_pages:
            response = self.request(endpoint, params=params, **kwargs)

            # データを取得
            data = response.get(data_key, [])
            all_data.extend(data)

            # ページネーションキーを確認
            pagination_key = response.get("pagination_key")
            if not pagination_key:
                break

            # 次のページへ
            params["pagination_key"] = pagination_key
            page += 1

        return all_data
