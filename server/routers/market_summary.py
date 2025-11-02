# server/routers/market_summary.py
from fastapi import APIRouter, HTTPException
from typing import Dict, List, Optional
from pathlib import Path
import json
import os

router = APIRouter()


def get_market_summary_from_s3_or_local(date: str, file_type: str = "structured") -> Dict:
    """
    S3またはローカルから市場サマリーファイルを取得

    Args:
        date: YYYY-MM-DD形式の日付
        file_type: "structured" (JSON) or "raw" (Markdown)

    Returns:
        Dict: JSONデータ（structuredの場合）またはMarkdownテキストを含む辞書
    """
    import boto3
    from botocore.exceptions import ClientError

    # ファイル拡張子を決定
    ext = "json" if file_type == "structured" else "md"
    filename = f"market_summary/{file_type}/{date}.{ext}"

    # ローカルパス
    local_path = Path(__file__).resolve().parents[2] / "data" / "parquet" / filename

    # ローカルファイルが存在すればそれを使用
    if local_path.exists():
        if file_type == "structured":
            with open(local_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            with open(local_path, 'r', encoding='utf-8') as f:
                return {"content": f.read()}

    # S3から取得
    try:
        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        key = f"parquet/{filename}"
        region = os.getenv("AWS_REGION", "ap-northeast-1")

        s3_client = boto3.client("s3", region_name=region)

        # S3からダウンロード
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')

        if file_type == "structured":
            return json.loads(content)
        else:
            return {"content": content}

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise HTTPException(status_code=404, detail=f"Market summary for {date} not found")
        raise HTTPException(status_code=500, detail=f"S3 error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading market summary: {str(e)}")


def list_available_dates() -> List[str]:
    """
    利用可能な市場サマリーの日付リストを取得

    Returns:
        List[str]: YYYY-MM-DD形式の日付リスト（降順）
    """
    import boto3
    from botocore.exceptions import ClientError

    # ローカルから取得を試みる
    local_dir = Path(__file__).resolve().parents[2] / "data" / "parquet" / "market_summary" / "structured"

    dates = []

    if local_dir.exists():
        for json_file in local_dir.glob("*.json"):
            date_str = json_file.stem  # YYYY-MM-DD
            dates.append(date_str)

    # S3からも取得（ローカルにない場合の補完）
    try:
        bucket = os.getenv("S3_BUCKET", "stock-api-data")
        prefix = "parquet/market_summary/structured/"
        region = os.getenv("AWS_REGION", "ap-northeast-1")

        s3_client = boto3.client("s3", region_name=region)

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                if key.endswith(".json"):
                    # parquet/market_summary/structured/YYYY-MM-DD.json → YYYY-MM-DD
                    date_str = key.split("/")[-1].replace(".json", "")
                    if date_str not in dates:
                        dates.append(date_str)

    except Exception:
        pass  # S3エラーは無視（ローカルデータがあればOK）

    # 重複削除 & 降順ソート
    return sorted(list(set(dates)), reverse=True)


@router.get("/market-summary/latest")
async def get_latest_market_summary() -> Dict:
    """
    最新の市場サマリーを取得

    Returns:
        Dict: 市場サマリーデータ（構造化JSON）
    """
    dates = list_available_dates()

    if not dates:
        raise HTTPException(status_code=404, detail="No market summaries available")

    latest_date = dates[0]
    return get_market_summary_from_s3_or_local(latest_date, "structured")


@router.get("/market-summary/{date}")
async def get_market_summary_by_date(date: str, format: str = "structured") -> Dict:
    """
    指定日の市場サマリーを取得

    Args:
        date: YYYY-MM-DD形式の日付
        format: "structured" (デフォルト) または "markdown"

    Returns:
        Dict: 市場サマリーデータ
    """
    file_type = "raw" if format == "markdown" else "structured"
    return get_market_summary_from_s3_or_local(date, file_type)


@router.get("/market-summary")
async def list_market_summaries() -> Dict:
    """
    利用可能な市場サマリーの日付一覧を取得

    Returns:
        Dict: {"dates": ["2025-10-31", "2025-10-30", ...]}
    """
    dates = list_available_dates()
    return {"dates": dates}
