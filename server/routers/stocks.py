from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from ..utils import load_all_stocks, merge_price_data_into_meta, enrich_stocks_with_all_data

router = APIRouter()


@router.get("/all")
def list_all_stocks():
    """全ての銘柄を一度に取得（all_stocks.parquetから統合版を返す）

    Returns:
        {
            "core": [...],            # 静的銘柄（TOPIX_CORE30 + 高市銘柄）
            "scalping_entry": [...],  # スキャルピングEntry銘柄
            "scalping_active": [...]  # スキャルピングActive銘柄
        }
    """
    # all_stocks.parquetから全銘柄を取得
    all_stocks = load_all_stocks(tag=None)

    # categoriesでフィルタリング
    core = [s for s in all_stocks if any(c in ["TOPIX_CORE30", "高市銘柄"] for c in (s.get("categories") or []))]
    scalping_entry_meta = [s for s in all_stocks if "SCALPING_ENTRY" in (s.get("categories") or [])]
    scalping_active_meta = [s for s in all_stocks if "SCALPING_ACTIVE" in (s.get("categories") or [])]

    # スキャルピング銘柄に価格データをマージ（フロントエンドがRow[]型を期待）
    scalping_entry = merge_price_data_into_meta(scalping_entry_meta)
    scalping_active = merge_price_data_into_meta(scalping_active_meta)

    return {
        "core": core if core else [],
        "scalping_entry": scalping_entry if scalping_entry else [],
        "scalping_active": scalping_active if scalping_active else [],
    }


@router.get("/enriched")
def list_stocks_enriched(tag: Optional[str] = Query(default=None, description="Filter by tag with full data (meta + price + perf + tech)")):
    """
    全データを統合して返す（メタ + 価格 + テクニカル）
    - tag指定時: そのタグの銘柄のみ
    - tag未指定: 全銘柄

    スキャルピング銘柄も対応:
    - tag=SCALPING_ENTRY or scalping_entry
    - tag=SCALPING_ACTIVE or scalping_active
    """
    # all_stocks.parquetからメタデータ取得
    meta = load_all_stocks(tag=tag)

    if not meta:
        return []

    # 全データを統合
    enriched = enrich_stocks_with_all_data(meta)
    return enriched


@router.get("")
def list_stocks(tag: Optional[str] = Query(default=None, description="Filter by primary tag (e.g., takaichi, TOPIX_CORE30)")):
    """全銘柄のメタデータを取得（all_stocks.parquetから）

    Args:
        tag: categoriesでフィルタリング（例: "TOPIX_CORE30", "高市銘柄", "SCALPING_ENTRY", "SCALPING_ACTIVE"）

    Returns:
        銘柄のメタデータリスト
    """
    data = load_all_stocks(tag=tag)
    return data if data is not None else []
