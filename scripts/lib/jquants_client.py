#!/usr/bin/env python3
"""
J-Quants API Client
認証とトークン管理を行うクライアント
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Any
import requests
from dotenv import load_dotenv


class JQuantsClient:
    """J-Quants API認証クライアント"""

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

        self.refresh_token = os.getenv("JQUANTS_REFRESH_TOKEN")
        self.mail_address = os.getenv("JQUANTS_MAIL_ADDRESS")
        self.password = os.getenv("JQUANTS_PASSWORD")
        self.plan = os.getenv("JQUANTS_PLAN", "free")
        self.base_url = os.getenv("JQUANTS_API_BASE_URL", "https://api.jquants.com/v1")

        self._id_token: str | None = None

        if not self.refresh_token:
            raise ValueError(
                "JQUANTS_REFRESH_TOKEN not found. "
                "Please set it in .env.jquants"
            )

    def _get_id_token(self) -> str:
        """IDトークンを取得（キャッシュあり）"""
        if self._id_token:
            return self._id_token

        url = f"{self.base_url}/token/auth_refresh"
        params = {"refreshtoken": self.refresh_token}

        response = requests.post(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        self._id_token = data.get("idToken")

        if not self._id_token:
            raise RuntimeError("Failed to retrieve ID token from J-Quants API")

        return self._id_token

    def get_headers(self) -> Dict[str, str]:
        """API呼び出し用のヘッダーを取得"""
        id_token = self._get_id_token()
        return {
            "Authorization": f"Bearer {id_token}",
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
            endpoint: APIエンドポイント（例: "/listed/info"）
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
