"""
J-Quants API integration module

このモジュールは個人の私的利用に限定されています。
取得したデータの外部配信・再配布は利用規約により禁止されています。

Usage:
    - 内部的なスキャルピング銘柄選定のみに使用
    - 選定結果（ティッカーリストのみ）を外部公開
    - 具体的な株価データ（OHLCV）は公開しない
"""

from jquants.client import JQuantsClient
from jquants.fetcher import JQuantsFetcher
from jquants.screener import ScalpingScreener

__all__ = [
    "JQuantsClient",
    "JQuantsFetcher",
    "ScalpingScreener",
]
