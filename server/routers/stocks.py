from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from ..utils import load_master_meta, load_scalping_meta, merge_price_data_into_meta, enrich_stocks_with_all_data

router = APIRouter()


@router.get("/all")
def list_all_stocks():
    """全ての銘柄を一度に取得（meta + scalping_entry + scalping_active）

    Returns:
        {
            "core": [...],            # meta.parquet (TOPIX_CORE30 + 高市銘柄) - メタデータのみ
            "scalping_entry": [...],  # scalping_entry.parquet - 価格データ込み
            "scalping_active": [...]  # scalping_active.parquet - 価格データ込み
        }
    """
    core = load_master_meta(tag=None)  # 全銘柄（フィルタなし）
    scalping_entry_meta = load_scalping_meta("entry")
    scalping_active_meta = load_scalping_meta("active")

    # スキャルピング銘柄に価格データをマージ（フロントエンドがRow[]型を期待）
    scalping_entry = merge_price_data_into_meta(scalping_entry_meta)
    scalping_active = merge_price_data_into_meta(scalping_active_meta)

    return {
        "core": core if core is not None else [],
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
    # メタデータ取得
    if tag:
        tag_lower = tag.lower()
        if "scalping_entry" in tag_lower or tag_lower == "scalping_entry":
            meta = load_scalping_meta("entry")
        elif "scalping_active" in tag_lower or tag_lower == "scalping_active":
            meta = load_scalping_meta("active")
        else:
            meta = load_master_meta(tag=tag)
    else:
        meta = load_master_meta(tag=None)

    if not meta:
        return []

    # 全データを統合
    enriched = enrich_stocks_with_all_data(meta)
    return enriched


@router.get("")
def list_stocks(tag: Optional[str] = Query(default=None, description="Filter by primary tag (e.g., takaichi, TOPIX_CORE30)")):
    data = load_master_meta(tag=tag)
    return data if data is not None else []
