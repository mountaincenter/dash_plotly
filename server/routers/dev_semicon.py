"""AI/semiconductor trend-following prototype API.

This is a semi-discretionary signal surface, not an auto-trading engine.
It turns the existing semiconductor risk report artifacts into a compact
BUY/WATCH/AVOID dashboard for /dev/semicon.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

ROOT = Path(__file__).resolve().parents[2]
SEMICON_OUT = ROOT / "scripts" / "analysis" / "semiconductor" / "output"
ANALYSIS_DIR = ROOT / "data" / "analysis"
PRICES_PATH = SEMICON_OUT / "prices_raw.parquet"
FUNDAMENTALS_PATH = SEMICON_OUT / "yfinance_fundamentals_summary.csv"
REPORT_PATH = SEMICON_OUT / "ai_semiconductor_yf_entry_risk_report.html"
BACKTEST_SUMMARY_PATH = SEMICON_OUT / "semicon_trend_backtest_summary.csv"
BACKTEST_REPORT_PATH = SEMICON_OUT / "semicon_trend_backtest_report.html"
INTRADAY_GRID_PATH = SEMICON_OUT / "semicon_intraday_long_short_grid.csv"
HOLD_STOCKS_PATH = ROOT / "data" / "csv" / "hold_stocks.csv"
TOPIX_PRICES_PATH = ROOT / "data" / "parquet" / "granville" / "prices_topix.parquet"

SEMICON_ENTRY_JSON = ANALYSIS_DIR / "semicon_entry_decisions.json"
SEMICON_DOMESTIC_JSON = ANALYSIS_DIR / "semicon_domestic_candidates.json"

S3_BUCKET = os.getenv("S3_BUCKET") or os.getenv("DATA_BUCKET") or "stock-api-data"
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
APP_ENV = (os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or os.getenv("STAGE") or "local").strip().lower()
USE_S3_ARTIFACTS = APP_ENV in {"production", "prod"}

S3_ENTRY_KEY = "analysis/semicon_entry_decisions.json"
S3_DOMESTIC_KEY = "analysis/semicon_domestic_candidates.json"
S3_REPORT_KEY = "analysis/semicon_entry_risk_report.html"
S3_BACKTEST_REPORT_KEY = "analysis/semicon_trend_backtest_report.html"

router = APIRouter()


@dataclass(frozen=True)
class SemiconStock:
    code: str
    name: str
    label: str
    segment: str
    core_segment: str
    sub_segment: str
    theme_driver: str
    classification_basis: str
    market: str = "JP"


UNIVERSE = [
    SemiconStock("6857", "アドバンテスト", "A", "テスタ/検査", "半導体コア", "後工程/検査", "AI半導体検査需要", "経産省: 製造装置・後工程 / 半導体製造装置分類"),
    SemiconStock("6146", "ディスコ", "A", "後工程装置", "半導体コア", "後工程装置", "AI半導体後工程投資", "経産省: 後工程高度化 / 半導体製造装置分類"),
    SemiconStock("8035", "東京エレクトロン", "A", "エッチング/成膜/塗布", "半導体コア", "前工程装置", "ロジック/DRAM投資", "経産省: 製造装置 / 半導体製造装置分類"),
    SemiconStock("6920", "レーザーテック", "A", "EUVマスク検査", "半導体コア", "前工程検査装置", "EUV/先端ロジック投資", "経産省: 製造装置 / 先端半導体サプライチェーン"),
    SemiconStock("7735", "SCREEN", "A", "洗浄装置", "半導体コア", "前工程装置", "先端工程投資", "経産省: 製造装置 / 半導体製造装置分類"),
    SemiconStock("6525", "KOKUSAI ELECTRIC", "A", "成膜装置", "半導体コア", "前工程装置", "メモリ/ロジック成膜投資", "経産省: 製造装置 / 半導体製造装置分類"),
    SemiconStock("4062", "イビデン", "A", "パッケージ基板", "半導体コア", "後工程/パッケージ基板", "AI/HBM/高性能PKG", "経産省: 後工程高度化 / AIサーバー基板需要"),
    SemiconStock("4186", "東京応化工業", "A", "フォトレジスト", "半導体コア", "半導体材料", "先端露光材料", "経産省: 部素材 / 半導体材料分類"),
    SemiconStock("4004", "レゾナック", "A", "パッケージ/CMP材料", "半導体コア", "半導体材料/パッケージ材料", "AI半導体材料需要", "経産省: 部素材 / 後工程材料"),
    SemiconStock("3436", "SUMCO", "A", "シリコンウエハ", "半導体コア", "半導体材料", "ウエハ市況", "経産省: 部素材 / 半導体材料分類"),
    SemiconStock("4063", "信越化学工業", "A", "シリコンウエハ", "半導体コア", "半導体材料", "ウエハ/材料市況", "経産省: 部素材 / 半導体材料分類"),
    SemiconStock("6723", "ルネサス", "A", "マイコン/アナログ", "半導体コア", "半導体デバイス", "車載/産業/AI周辺制御", "経産省: 半導体デバイス / AIインフラ制御需要"),
    SemiconStock("285A", "キオクシア", "A", "NAND", "半導体コア", "メモリ", "NAND/SSD/データセンター", "経産省: メモリ / AIデータセンター・SSD需要"),
    SemiconStock("6963", "ローム", "A", "SiC/パワー半導体", "半導体コア", "SiC/パワー半導体", "電動化/電源効率", "経産省: パワー半導体 / SiC投資"),
    SemiconStock("6526", "ソシオネクスト", "A", "ASIC/SoC設計", "半導体コア", "ASIC/SoC設計", "AI/車載ASIC設計", "経産省: 半導体デバイス / ファブレス設計"),
    SemiconStock("2802", "味の素", "A/B", "ABF/パッケージ材料", "半導体コア", "ABF/パッケージ材料", "AI/HBM/高性能PKG材料", "経産省: 部素材 / 後工程材料"),
    SemiconStock("7912", "大日本印刷", "A/B", "フォトマスク/電子材料", "半導体コア", "フォトマスク/電子材料", "先端露光/電子材料", "経産省: 部素材 / フォトマスク"),
    SemiconStock("7911", "TOPPAN", "A/B", "半導体パッケージ/フォトマスク", "半導体コア", "フォトマスク/パッケージ材料", "先端露光/後工程材料", "経産省: 部素材 / フォトマスク・後工程材料"),
    SemiconStock("8088", "岩谷産業", "C", "産業ガス/水素", "半導体コア", "産業ガス/工場インフラ", "半導体工場ガス/水素", "経産省: 工場インフラ / 半導体材料周辺"),
    SemiconStock("6503", "三菱電機", "A", "パワー半導体/重電", "電力/冷却", "パワー半導体/重電", "DC電力・省エネ投資", "経産省: パワー半導体 / DC電力制約"),
    SemiconStock("6504", "富士電機", "A", "パワー半導体", "電力/冷却", "パワー半導体", "DC電力・電源効率", "経産省: パワー半導体 / DC電力制約"),
    SemiconStock("6501", "日立製作所", "A/B", "検査装置/電力", "電力/冷却", "電力/制御/検査", "DC電力・制御インフラ", "経産省: 電力/制御波及 / DC設備投資"),
    SemiconStock("5801", "古河電工", "B", "光通信/電力ケーブル/冷却", "光/通信", "光通信/電力ケーブル", "AIデータセンター接続・電力配線", "富士経済: 光通信市場 / DCケーブル需要"),
    SemiconStock("5803", "フジクラ", "B", "光通信/高密度配線", "光/通信", "光通信/高密度配線", "AIデータセンター接続", "富士経済: 光通信市場 / DC光接続需要"),
    SemiconStock("6971", "京セラ", "B", "パッケージ/電子部品", "AIサーバー部品", "パッケージ/電子部品", "AIサーバー周辺部品需要", "TrendForce: AI server MLCC/受動部品 / 電子部品"),
    SemiconStock("6981", "村田製作所", "B", "MLCC/電源部品/EMI", "AIサーバー部品", "MLCC/受動部品", "AIサーバーMLCC需給", "TrendForce: AI server MLCC逼迫 / 受動部品"),
    SemiconStock("6976", "太陽誘電", "B", "MLCC/受動部品", "AIサーバー部品", "MLCC/受動部品", "AIサーバーMLCC需給", "TrendForce: AI server MLCC逼迫 / 受動部品"),
    SemiconStock("6762", "TDK", "B", "電子部品/受動部品", "AIサーバー部品", "MLCC/受動部品", "AIサーバー受動部品需要", "TrendForce: AI server MLCC/受動部品 / 電子部品"),
    SemiconStock("6367", "ダイキン工業", "B", "冷却/空調", "電力/冷却", "冷却/空調", "AIデータセンター熱対策", "矢野経済: DC冷却/液浸冷却 / 高負荷計算需要"),
    SemiconStock("6368", "オルガノ", "C", "超純水/工場インフラ", "電力/冷却", "超純水/工場インフラ", "半導体工場/AIインフラ水処理", "経産省: 工場インフラ / 半導体製造周辺"),
    SemiconStock("5802", "住友電工", "C", "光通信/電線", "光/通信", "光通信/電線", "DC光・電力配線", "富士経済: 光通信市場 / DCケーブル需要"),
    SemiconStock("5805", "SWCC", "C", "電線/電力ケーブル", "光/通信", "電線/電力ケーブル", "DC電力配線", "DC電力制約 / 電線・ケーブル需要"),
    SemiconStock("6645", "オムロン", "C", "電源/制御", "AIサーバー部品", "電源/制御部品", "工場/電源制御", "AIインフラ周辺 / 制御部品"),
    SemiconStock("1963", "日揮HD", "C", "設備/EPC", "DC建設/設備", "設備/EPC", "DC/工場設備投資", "矢野経済: DC新設・増設 / 設備投資"),
    SemiconStock("1979", "大気社", "C", "空調/クリーンルーム", "DC建設/設備", "空調/クリーンルーム", "DC/半導体工場設備", "矢野経済: DC冷却 / クリーンルーム設備"),
    SemiconStock("1802", "大林組", "D", "建設", "DC建設/設備", "建設", "DC建設", "矢野経済: DC新設・増設 / 建設波及"),
    SemiconStock("1925", "大和ハウス", "D", "建設/不動産", "DC建設/設備", "建設/不動産", "DC建設/用地", "矢野経済: DC新設・増設 / 用地・建設"),
    SemiconStock("8801", "三井不動産", "D", "不動産/DC", "DC建設/設備", "不動産/DC", "DC用地・不動産", "矢野経済: DC新設・増設 / 用地制約"),
    SemiconStock("8802", "三菱地所", "D", "不動産/DC", "DC建設/設備", "不動産/DC", "DC用地・不動産", "矢野経済: DC新設・増設 / 用地制約"),
]

OVERSEAS = {
    "NVDA": "NVIDIA",
    "^SOX": "SOX",
    "AVGO": "Broadcom",
    "MU": "Micron",
    "TSM": "TSMC",
    "^IXIC": "NASDAQ",
    "NQ=F": "NASDAQ先物",
    "NKD=F": "日経先物CME",
    "JPY=X": "USDJPY",
}

OVERSEAS_INDICATOR_META = {
    "^SOX": {
        "role": "米半導体指数",
        "risk_note": "SOXが弱い日は半導体コア全体の寄り天を警戒。強い日は主力と周辺の維持を確認する。",
        "good_when": "up",
    },
    "NVDA": {
        "role": "AIサーバー本流",
        "risk_note": "NVIDIAが弱い日はAIインフラ順張りの前提が弱い。強くても国内は寄り後維持を確認する。",
        "good_when": "up",
    },
    "AVGO": {
        "role": "ASIC/ネットワーク",
        "risk_note": "Broadcomが強い日はASIC、光通信、パッケージ基板、MLCC周辺の物色継続を確認する。",
        "good_when": "up",
    },
    "MU": {
        "role": "メモリ/SSD",
        "risk_note": "Micronが強い日はNAND、メモリ、MLCC周辺に追い風。急騰後は高値掴みを避ける。",
        "good_when": "up",
    },
    "TSM": {
        "role": "ファウンドリ",
        "risk_note": "TSMCが強い日は前工程装置と材料に追い風。弱い日は装置ど真ん中の買いを抑える。",
        "good_when": "up",
    },
    "^IXIC": {
        "role": "米グロース地合い",
        "risk_note": "NASDAQが弱い日は半導体材料が良くてもリスク許容度を下げる。",
        "good_when": "up",
    },
    "NQ=F": {
        "role": "NASDAQ先物",
        "risk_note": "寄付前の米グロース先物。強くても日本の寄り後30分で維持できるかを見る。",
        "good_when": "up",
    },
    "NKD=F": {
        "role": "日経CME",
        "risk_note": "日本寄付前の指数地合い。強い日は寄り天、弱い日は押し目待ちを優先する。",
        "good_when": "up",
    },
    "JPY=X": {
        "role": "USDJPY",
        "risk_note": "円安は輸出株に追い風だが、原油高と同時なら日本株には質が悪い。",
        "good_when": "mixed",
    },
}

SEGMENT_GROUPS = {
    "半導体主力": {"NAND", "成膜装置", "テスタ/検査", "後工程装置", "エッチング/成膜/塗布", "EUVマスク検査", "洗浄装置"},
    "AIインフラ": {"光通信/高密度配線", "光通信/電力ケーブル/冷却", "MLCC/電源部品/EMI", "パッケージ基板", "マイコン/アナログ", "成膜装置"},
    "電力": {"検査装置/電力", "パワー半導体/重電", "パワー半導体", "光通信/電線", "電線/電力ケーブル"},
    "光通信": {"光通信/高密度配線", "光通信/電力ケーブル/冷却", "光通信/電線", "電線/電力ケーブル"},
    "冷却/空調": {"冷却/空調", "空調/クリーンルーム"},
    "材料/基板": {"パッケージ基板", "フォトレジスト", "パッケージ/CMP材料", "シリコンウエハ"},
    "DC建設/不動産": {"建設", "建設/不動産", "不動産/DC", "設備/EPC"},
}

INDICATOR_CODES = {"6857", "6146", "8035", "6920", "7735", "285A"}
HEAVY_WATCH_CODES = {"4062", "5801"}
UNIVERSE_BY_CODE = {stock.code: stock for stock in UNIVERSE}
HOLD_EXPOSURE_EXTRA_BY_CODE = {
    "6055": SemiconStock("6055", "ジャパンマテリアル", "B", "特殊ガス/半導体材料", "半導体コア", "半導体材料", "半導体工場材料", "経産省: 部素材 / 半導体材料分類"),
    "6323": SemiconStock("6323", "ローツェ", "A", "搬送装置/半導体装置", "半導体コア", "前工程/搬送装置", "半導体設備投資", "経産省: 製造装置 / 半導体製造装置分類"),
}

CLASSIFICATION_BASIS = [
    {
        "layer": "半導体コア",
        "basis": "経産省の半導体・デジタル産業戦略を骨格に、デバイス、製造装置、部素材、後工程/パッケージへ分解。",
        "use": "SOX/NVDA/MUに最も近い温度計。値嵩や左尾が大きい銘柄は指標扱いも多い。",
    },
    {
        "layer": "AIサーバー部品",
        "basis": "TrendForce等のMLCC/受動部品需給資料を根拠に、AIサーバーの部品逼迫を別枠化。",
        "use": "装置ど真ん中から資金が移る時の実弾候補。村田、太陽誘電、TDK、京セラ系の扱い。",
    },
    {
        "layer": "電力/冷却",
        "basis": "矢野経済のデータセンター/液浸冷却市場資料を根拠に、電力制約と発熱対策を別枠化。",
        "use": "原油・金利・地政学と絡む。半導体主力が高すぎる時の二段目候補。",
    },
    {
        "layer": "光/通信",
        "basis": "富士経済の光通信市場調査、DC向け光/ケーブル需要を根拠に分類。",
        "use": "AIデータセンター接続需要。急騰しやすく寄り天確認が必須。",
    },
    {
        "layer": "DC建設/設備",
        "basis": "矢野経済のDC新設・増設、用地・電力制約の整理を根拠に分類。",
        "use": "テーマ末端波及。短期では主役にしにくく、資金流入確認用。",
    },
]

THEME_LAYER_BY_CODE = {
    "285A": ("半導体本流", "メモリ/ストレージ"),
    "6857": ("半導体本流", "後工程/検査"),
    "6146": ("半導体本流", "後工程/検査"),
    "8035": ("半導体本流", "前工程装置"),
    "6920": ("半導体本流", "前工程装置"),
    "7735": ("半導体本流", "前工程装置"),
    "6525": ("半導体本流", "前工程装置"),
    "6723": ("半導体本流", "半導体デバイス/設計"),
    "6963": ("半導体本流", "半導体デバイス/設計"),
    "6526": ("半導体本流", "半導体デバイス/設計"),
    "4186": ("半導体本流", "半導体材料"),
    "3436": ("半導体本流", "半導体材料"),
    "4063": ("半導体本流", "半導体材料"),
    "7912": ("半導体本流", "半導体材料"),
    "7911": ("半導体本流", "半導体材料"),
    "8088": ("半導体本流", "半導体材料"),
    "4004": ("半導体本流", "パッケージ/基板"),
    "4062": ("半導体本流", "パッケージ/基板"),
    "2802": ("半導体本流", "パッケージ/基板"),
    "6971": ("AIサーバー周辺", "MLCC/電子部品"),
    "6981": ("AIサーバー周辺", "MLCC/電子部品"),
    "6976": ("AIサーバー周辺", "MLCC/電子部品"),
    "6762": ("AIサーバー周辺", "MLCC/電子部品"),
    "6645": ("AIサーバー周辺", "MLCC/電子部品"),
    "5801": ("AIサーバー周辺", "光通信/電線"),
    "5803": ("AIサーバー周辺", "光通信/電線"),
    "5802": ("AIサーバー周辺", "光通信/電線"),
    "5805": ("AIサーバー周辺", "光通信/電線"),
    "6503": ("AIサーバー周辺", "電力/冷却/工場インフラ"),
    "6504": ("AIサーバー周辺", "電力/冷却/工場インフラ"),
    "6501": ("AIサーバー周辺", "電力/冷却/工場インフラ"),
    "6367": ("AIサーバー周辺", "電力/冷却/工場インフラ"),
    "6368": ("AIサーバー周辺", "電力/冷却/工場インフラ"),
    "1963": ("DCインフラ周辺", "DC建設/設備"),
    "1979": ("DCインフラ周辺", "DC建設/設備"),
    "1802": ("DCインフラ周辺", "DC建設/設備"),
    "1925": ("DCインフラ周辺", "DC建設/設備"),
    "8801": ("DCインフラ周辺", "DC建設/設備"),
    "8802": ("DCインフラ周辺", "DC建設/設備"),
    "6055": ("半導体本流", "半導体材料"),
    "6323": ("半導体本流", "前工程装置"),
}


def _theme_flow_for_code(code: str, stock: SemiconStock | None = None) -> tuple[str, str]:
    key = str(code).replace(".T", "")
    if key in THEME_LAYER_BY_CODE:
        return THEME_LAYER_BY_CODE[key]
    if stock is None:
        return "未分類", "未分類"
    if stock.core_segment == "DC建設/設備":
        return "DCインフラ周辺", "DC建設/設備"
    if stock.core_segment in {"AIサーバー部品", "光/通信", "電力/冷却"}:
        return "AIサーバー周辺", stock.sub_segment
    return "半導体本流", stock.sub_segment


def _action_hint_from_flow(hint: str) -> str:
    if hint in {"資金流入候補", "広がりあり"}:
        return "乗る候補"
    if hint in {"長期異常値+資金流入", "リーダー集中", "短期反応"}:
        return "待つ/押し目"
    if hint == "売買活発だが利確/逆風":
        return "触らない"
    return "温度計/監視"


def _classification_meta(stock: SemiconStock) -> dict[str, str]:
    theme_layer, flow_group = _theme_flow_for_code(stock.code, stock)
    return {
        "core_segment": stock.core_segment,
        "sub_segment": stock.sub_segment,
        "theme_layer": theme_layer,
        "flow_group": flow_group,
        "theme_driver": stock.theme_driver,
        "classification_basis": stock.classification_basis,
    }


def _attach_classification_meta(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for signal in signals:
        row = dict(signal)
        code = str(row.get("code", "")).strip()
        stock = UNIVERSE_BY_CODE.get(code) or HOLD_EXPOSURE_EXTRA_BY_CODE.get(code)
        if stock is not None:
            row.update(_classification_meta(stock))
        else:
            row.setdefault("core_segment", "未分類")
            row.setdefault("sub_segment", str(row.get("segment", "")) or "未分類")
            row.setdefault("theme_layer", "未分類")
            row.setdefault("flow_group", "未分類")
            row.setdefault("theme_driver", "要確認")
            row.setdefault("classification_basis", "UNIVERSE未登録")
        rows.append(row)
    return rows


def _s3_client():
    import boto3

    return boto3.client("s3", region_name=AWS_REGION)


def _read_local_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def _read_s3_json(key: str) -> dict[str, Any] | None:
    if not S3_BUCKET:
        return None
    try:
        obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _read_semicon_json(local_path: Path, s3_key: str) -> dict[str, Any] | None:
    if USE_S3_ARTIFACTS:
        return _read_s3_json(s3_key)
    return _read_local_json(local_path)


def _read_semicon_html(local_path: Path, s3_key: str) -> str | None:
    if USE_S3_ARTIFACTS:
        if not S3_BUCKET:
            return None
        try:
            obj = _s3_client().get_object(Bucket=S3_BUCKET, Key=s3_key)
            return obj["Body"].read().decode("utf-8")
        except Exception:
            return None
    if not local_path.exists():
        return None
    return local_path.read_text(encoding="utf-8")


def _safe_float(v: object) -> float | None:
    try:
        if v is None or pd.isna(v):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_percent(v: object) -> float | None:
    if v is None or pd.isna(v):
        return None
    text = str(v).replace("%", "").replace("+", "").replace(",", "").strip()
    if text in {"", "-", "nan"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_number(v: object) -> float | None:
    if v is None or pd.isna(v):
        return None
    text = str(v).replace(",", "").strip()
    if text in {"", "-", "nan"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _pct(now: float, before: float) -> float | None:
    if before == 0 or pd.isna(now) or pd.isna(before):
        return None
    return float((now / before - 1.0) * 100.0)


def _max_dd(series: pd.Series) -> float | None:
    s = series.dropna().astype(float)
    if s.empty:
        return None
    return float((s / s.cummax() - 1.0).min() * 100.0)


def _cvar05(series: pd.Series) -> float | None:
    ret = series.dropna().astype(float).pct_change().dropna()
    if ret.empty:
        return None
    cutoff = ret.quantile(0.05)
    tail = ret[ret <= cutoff]
    if tail.empty:
        return None
    return float(tail.mean() * 100.0)


def _metric_row(code: str, series: pd.Series) -> dict[str, float | str | None]:
    s = series.dropna().astype(float)
    latest_date = s.index[-1].date().isoformat() if not s.empty and hasattr(s.index[-1], "date") else ""
    if len(s) < 80:
        return {"code": code, "date": latest_date, "missing": True}
    close = float(s.iloc[-1])
    ma25 = s.rolling(25).mean()
    hi20 = float(s.tail(20).max())
    ret5 = _pct(close, float(s.iloc[-6])) if len(s) >= 6 else None
    ret20 = _pct(close, float(s.iloc[-21])) if len(s) >= 21 else None
    vs25 = _pct(close, float(ma25.iloc[-1]))
    dist20hi = _pct(close, hi20)
    cvar05 = _cvar05(s)
    return {
        "code": code,
        "date": latest_date,
        "close": close,
        "ret5": ret5,
        "ret20": ret20,
        "vs25": vs25,
        "dist20hi": dist20hi,
        "max_dd_60d": _max_dd(s.tail(60)),
        "cvar05": cvar05,
        "missing": False,
    }


def _market_regime(rows: list[dict[str, Any]]) -> dict[str, Any]:
    lookup = {r["ticker"]: r for r in rows}
    keys = ["^SOX", "NVDA", "AVGO", "MU", "TSM", "^IXIC", "NQ=F"]
    positives = sum(1 for k in keys if (lookup.get(k, {}).get("ret5") or 0) > 0)
    negatives_1d = sum(1 for k in ["^SOX", "NVDA", "MU", "^IXIC"] if (lookup.get(k, {}).get("ret1") or 0) < -1.5)
    sox_ret5 = lookup.get("^SOX", {}).get("ret5")
    nvda_ret5 = lookup.get("NVDA", {}).get("ret5")
    if negatives_1d >= 2:
        state = "RISK_OFF"
        label = "米半導体が短期逆風"
    elif positives >= 4 and (sox_ret5 or 0) > 0 and (nvda_ret5 or 0) > 0:
        state = "RISK_ON"
        label = "米半導体が追い風"
    elif positives >= 4 and (sox_ret5 or 0) > 0:
        state = "SELECTIVE_RISK_ON"
        label = "半導体周辺は追い風 / NVDA逆風"
    else:
        state = "NEUTRAL"
        label = "米地合いは中立"
    return {
        "state": state,
        "label": label,
        "positive_count": positives,
        "negative_1d_count": negatives_1d,
        "sox_ret5": sox_ret5,
        "nvda_ret5": nvda_ret5,
    }


def _market_indicators_from_overseas(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    indicators: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip()
        meta = OVERSEAS_INDICATOR_META.get(ticker)
        if not ticker or not meta:
            continue
        indicators.append(
            {
                "ticker": ticker,
                "name": row.get("name") or OVERSEAS.get(ticker, ticker),
                "role": meta["role"],
                "risk_note": meta["risk_note"],
                "good_when": meta["good_when"],
                "date": row.get("date"),
                "close": row.get("close"),
                "ret1": row.get("ret1"),
                "ret5": row.get("ret5"),
                "ret20": row.get("ret20"),
                "source": "overseas",
                "missing": False,
            }
        )
    return indicators


def _market_indicator_date(rows: list[dict[str, Any]]) -> str | None:
    dates = [str(row.get("date")) for row in rows if isinstance(row, dict) and row.get("date")]
    return max(dates) if dates else None


def _score_stock(stock: SemiconStock, metrics: dict[str, Any], market_state: str, fundamentals: dict[str, Any]) -> dict[str, Any]:
    score = {"A": 4.0, "A/B": 3.5, "B": 2.0, "C": 0.5, "D": 0.0}.get(stock.label, 0.0)
    reasons = []
    warnings = []
    ret5 = metrics.get("ret5")
    ret20 = metrics.get("ret20")
    vs25 = metrics.get("vs25")
    dist20hi = metrics.get("dist20hi")
    cvar05 = metrics.get("cvar05")
    max_dd_60d = metrics.get("max_dd_60d")
    revenue_growth = fundamentals.get("yf_revenue_growth")
    op_margin = fundamentals.get("yf_operating_margin")

    if market_state == "RISK_ON":
        score += 2
        reasons.append("米半導体追い風")
    elif market_state == "RISK_OFF":
        score -= 3
        warnings.append("米半導体逆風")

    if ret5 is not None and ret5 > 0:
        score += 1.5
        reasons.append("5日順張り")
    if ret20 is not None and ret20 > 0:
        score += 1.0
        reasons.append("20日順張り")
    if vs25 is not None and vs25 > 0:
        score += 1.0
        reasons.append("25日線上")
    if dist20hi is not None and -5 <= dist20hi <= 0:
        score += 1.0
        reasons.append("20日高値圏")
    if revenue_growth is not None and revenue_growth > 20:
        score += 1.0
        reasons.append("売上成長")
    if op_margin is not None and op_margin > 20:
        score += 0.5
        reasons.append("高OP率")

    if vs25 is not None and vs25 > 18:
        score -= 2.0
        warnings.append("25日線乖離過熱")
    elif vs25 is not None and vs25 > 12:
        score -= 1.0
        warnings.append("寄り高注意")
    if cvar05 is not None and cvar05 <= -6:
        score -= 2.0
        warnings.append("左尾深い")
    elif cvar05 is not None and cvar05 <= -4:
        score -= 1.0
        warnings.append("左尾注意")
    if max_dd_60d is not None and max_dd_60d <= -25:
        score -= 1.0
        warnings.append("60日DD深い")

    if stock.label in {"C", "D"} and score >= 8:
        decision = "WATCH"
    elif score >= 9 and market_state != "RISK_OFF" and "左尾深い" not in warnings:
        decision = "BUY_CANDIDATE"
    elif score >= 5:
        decision = "WATCH"
    else:
        decision = "AVOID"

    if decision == "BUY_CANDIDATE":
        entry_rule = "前日高値超えまたは寄り後VWAP上維持。寄りで飛びすぎなら待つ"
    elif decision == "WATCH":
        entry_rule = "地合いと寄付差を確認。高寄り・左尾悪化なら入らない"
    else:
        entry_rule = "翌営業日の順張り対象外"

    return {
        "code": stock.code,
        "ticker": f"{stock.code}.T",
        "name": stock.name,
        "label": stock.label,
        "segment": stock.segment,
        **_classification_meta(stock),
        "decision": decision,
        "score": round(score, 2),
        "reasons": reasons,
        "warnings": warnings,
        "entry_rule": entry_rule,
        **metrics,
        "revenue_growth": revenue_growth,
        "op_margin": op_margin,
    }


def _load_fundamentals() -> dict[str, dict[str, Any]]:
    if not FUNDAMENTALS_PATH.exists():
        return {}
    df = pd.read_csv(FUNDAMENTALS_PATH)
    rows = {}
    for _, row in df.iterrows():
        code = str(row.get("code", "")).strip()
        rows[code] = {
            "yf_revenue_growth": _safe_float(row.get("yf_revenue_growth")),
            "yf_operating_margin": _safe_float(row.get("yf_operating_margin")),
        }
    return rows


def _decision_from_report(value: object) -> str:
    text = str(value)
    if "見送り" in text:
        return "AVOID"
    return "WATCH"


def _entry_trigger_price(rule: object) -> float | None:
    text = str(rule)
    match = re.search(r"前日高値([0-9,]+(?:\.[0-9]+)?)超え", text)
    if match:
        return _parse_number(match.group(1))
    return None


def _segment_name(raw_segment: object) -> list[str]:
    segment = str(raw_segment)
    groups = [name for name, members in SEGMENT_GROUPS.items() if segment in members]
    return groups or ["その他"]


def _build_segment_strength(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expanded = []
    for signal in signals:
        for group in _segment_name(signal.get("segment")):
            expanded.append(
                {
                    "segment": group,
                    "code": signal.get("code"),
                    "name": signal.get("name"),
                    "ret5": signal.get("ret5"),
                    "ret20": signal.get("ret20"),
                    "vs25": signal.get("vs25"),
                    "score": signal.get("score"),
                    "decision": signal.get("decision"),
                }
            )
    if not expanded:
        return []

    df = pd.DataFrame(expanded)
    rows = []
    for segment, g in df.groupby("segment", sort=False):
        ret5 = pd.to_numeric(g["ret5"], errors="coerce")
        ret20 = pd.to_numeric(g["ret20"], errors="coerce")
        vs25 = pd.to_numeric(g["vs25"], errors="coerce")
        score = pd.to_numeric(g["score"], errors="coerce")
        leader_row = g.loc[score.fillna(-999).idxmax()] if not g.empty else None
        rows.append(
            {
                "segment": segment,
                "count": int(len(g)),
                "avg_ret5": _safe_float(ret5.mean()),
                "avg_ret20": _safe_float(ret20.mean()),
                "avg_vs25": _safe_float(vs25.mean()),
                "breadth5": _safe_float((ret5 > 0).mean() * 100.0),
                "breadth20": _safe_float((ret20 > 0).mean() * 100.0),
                "watch_count": int((g["decision"] != "AVOID").sum()),
                "leader_code": str(leader_row["code"]) if leader_row is not None else "",
                "leader_name": str(leader_row["name"]) if leader_row is not None else "",
                "leader_score": _safe_float(leader_row["score"]) if leader_row is not None else None,
            }
        )
    return sorted(rows, key=lambda r: ((r["avg_ret5"] or -999), (r["avg_ret20"] or -999)), reverse=True)


@lru_cache(maxsize=4)
def _load_turnover_prices(path_str: str, mtime_ns: int) -> pd.DataFrame:
    _ = mtime_ns
    df = pd.read_parquet(path_str, columns=["date", "Close", "Volume", "ticker"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["code_key"] = df["ticker"].astype(str).str.replace(".T", "", regex=False)
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
    df["turnover_yen"] = df["Close"] * df["Volume"]
    return df.dropna(subset=["date", "Close", "Volume", "turnover_yen"])


def _flow_hint(
    vs5: float | None,
    vs20: float | None,
    vs60: float | None,
    avg_ret1: float | None,
    up_ratio: float | None,
    top_share: float | None,
) -> str:
    vs5 = vs5 or 0.0
    vs20 = vs20 or 0.0
    vs60 = vs60 or 0.0
    avg_ret1 = avg_ret1 or 0.0
    up_ratio = up_ratio or 0.0
    top_share = top_share or 0.0
    if vs20 >= 1.5 and top_share >= 70 and avg_ret1 > 0:
        return "リーダー集中"
    if vs60 >= 3.0 and vs20 >= 1.5 and avg_ret1 > 0 and up_ratio >= 50:
        return "長期異常値+資金流入"
    if vs20 >= 1.5 and avg_ret1 > 0 and up_ratio >= 50:
        return "資金流入候補"
    if vs20 >= 1.5 and (avg_ret1 < 0 or up_ratio < 50):
        return "売買活発だが利確/逆風"
    if vs5 >= 1.5 and vs20 < 1.2:
        return "短期反応"
    if avg_ret1 > 0 and up_ratio >= 60:
        return "広がりあり"
    return "様子見"


def _build_flow_analysis(signals: list[dict[str, Any]]) -> dict[str, Any]:
    if not TOPIX_PRICES_PATH.exists() or not signals:
        return {"available": False, "reason": "prices_topix.parquet or signals missing"}

    try:
        prices = _load_turnover_prices(str(TOPIX_PRICES_PATH), TOPIX_PRICES_PATH.stat().st_mtime_ns)
    except Exception as exc:
        return {"available": False, "reason": f"failed_to_load_prices: {type(exc).__name__}"}

    if prices.empty:
        return {"available": False, "reason": "prices_empty"}

    latest = prices["date"].max()
    all_daily = prices.groupby("date", sort=True).agg(
        turnover_yen=("turnover_yen", "sum"),
        n=("ticker", "nunique"),
    )
    if latest not in all_daily.index:
        return {"available": False, "reason": "latest_market_missing"}

    sig_df = pd.DataFrame(signals)
    if "code" not in sig_df.columns:
        return {"available": False, "reason": "signal_code_missing"}
    sig_df["code_key"] = sig_df["code"].astype(str).str.replace(".T", "", regex=False)
    sig_df["core_segment"] = sig_df.get("core_segment", "未分類").fillna("未分類")
    sig_df["sub_segment"] = sig_df.get("sub_segment", sig_df.get("segment", "未分類")).fillna("未分類")
    sig_df["theme_layer"] = sig_df.get("theme_layer", "未分類").fillna("未分類")
    sig_df["flow_group"] = sig_df.get("flow_group", sig_df["sub_segment"]).fillna(sig_df["sub_segment"])
    sig_df["name"] = sig_df.get("name", sig_df["code_key"]).fillna(sig_df["code_key"])
    sig_df["segment"] = sig_df.get("segment", sig_df["sub_segment"]).fillna(sig_df["sub_segment"])

    hist = prices[prices["code_key"].isin(sig_df["code_key"])].merge(
        sig_df[["code_key", "code", "name", "segment", "core_segment", "sub_segment", "theme_layer", "flow_group"]],
        on="code_key",
        how="left",
    )
    if hist.empty:
        return {"available": False, "reason": "flow_history_empty"}

    hist = hist.sort_values(["code_key", "date"])
    hist["ret1"] = hist.groupby("code_key")["Close"].pct_change() * 100.0
    latest_hist = hist[hist["date"].eq(latest)].copy()
    if latest_hist.empty:
        return {"available": False, "reason": "latest_signal_prices_missing"}

    sem_daily = hist.groupby("date", sort=True).agg(
        turnover_yen=("turnover_yen", "sum"),
        avg_ret1=("ret1", "mean"),
        up_ratio=("ret1", lambda x: float((x > 0).mean() * 100.0)),
        n=("code_key", "nunique"),
    )
    sem_latest = sem_daily.loc[latest]
    all_latest = all_daily.loc[latest]

    def ratio(latest_value: float, series: pd.Series, window: int) -> float | None:
        avg = series.tail(window).mean()
        if pd.isna(avg) or avg == 0:
            return None
        return _safe_float(latest_value / avg)

    def top1_ex(series_df: pd.DataFrame) -> tuple[float | None, float | None, str, str]:
        if series_df.empty:
            return None, None, "", ""
        top = series_df.sort_values("turnover_yen", ascending=False).iloc[0]
        total = float(series_df["turnover_yen"].sum())
        top_turnover = float(top["turnover_yen"])
        top_share = top_turnover / total * 100.0 if total else None
        return _safe_float((total - top_turnover) / 1e9), _safe_float(top_share), str(top.get("code") or top.get("code_key")), str(top.get("name") or "")

    universe_ex_top1_bil, universe_top1_share, universe_top_code, universe_top_name = top1_ex(latest_hist)

    def summarize(level: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for name, g in hist.groupby(level, sort=False):
            daily = g.groupby("date", sort=True).agg(
                turnover_yen=("turnover_yen", "sum"),
                avg_ret1=("ret1", "mean"),
                up_ratio=("ret1", lambda x: float((x > 0).mean() * 100.0)),
                n=("code_key", "nunique"),
            )
            if latest not in daily.index:
                continue
            current = daily.loc[latest]
            members = g[g["date"].eq(latest)].copy()
            ex_top1_bil, top_share, top_code, top_name = top1_ex(members)
            turnover_bil = float(current["turnover_yen"]) / 1e9
            vs5 = ratio(float(current["turnover_yen"]), daily["turnover_yen"], 5)
            vs20 = ratio(float(current["turnover_yen"]), daily["turnover_yen"], 20)
            vs60 = ratio(float(current["turnover_yen"]), daily["turnover_yen"], 60)
            avg_ret1 = _safe_float(current["avg_ret1"])
            up_ratio = _safe_float(current["up_ratio"])
            rows.append(
                {
                    "segment": str(name),
                    "n": int(current["n"]),
                    "turnover_bil": _safe_float(turnover_bil),
                    "turnover_ex_top1_bil": ex_top1_bil,
                    "turnover_vs5": vs5,
                    "turnover_vs20": vs20,
                    "turnover_vs60": vs60,
                    "avg_ret1": avg_ret1,
                    "up_ratio": up_ratio,
                    "top_code": top_code,
                    "top_name": top_name,
                    "top_share": top_share,
                    "hint": _flow_hint(vs5, vs20, vs60, avg_ret1, up_ratio, top_share),
                }
            )
        for row in rows:
            row["action_hint"] = _action_hint_from_flow(str(row.get("hint") or ""))
        return sorted(rows, key=lambda r: r.get("turnover_bil") or 0, reverse=True)

    latest_code = latest_hist.copy()
    rolling_avg = (
        hist.groupby(["code_key", "date"], as_index=False)
        .agg(turnover_yen=("turnover_yen", "sum"), ret1=("ret1", "last"))
        .sort_values(["code_key", "date"])
    )
    rolling_avg["turnover_avg20"] = rolling_avg.groupby("code_key")["turnover_yen"].transform(
        lambda s: s.rolling(20, min_periods=5).mean()
    )
    rolling_avg["turnover_avg5"] = rolling_avg.groupby("code_key")["turnover_yen"].transform(
        lambda s: s.rolling(5, min_periods=3).mean()
    )
    rolling_avg["turnover_avg60"] = rolling_avg.groupby("code_key")["turnover_yen"].transform(
        lambda s: s.rolling(60, min_periods=10).mean()
    )
    latest_avg = rolling_avg[rolling_avg["date"].eq(latest)][["code_key", "turnover_avg5", "turnover_avg20", "turnover_avg60"]]
    latest_code = latest_code.merge(latest_avg, on="code_key", how="left")
    flow_avg = latest_hist.groupby("flow_group")["ret1"].mean().to_dict()
    latest_code["flow_avg_ret1"] = latest_code["flow_group"].map(flow_avg)

    individuals = []
    for _, row in latest_code.sort_values("turnover_yen", ascending=False).head(20).iterrows():
        turnover = _safe_float(row.get("turnover_yen"))
        avg5 = _safe_float(row.get("turnover_avg5"))
        avg20 = _safe_float(row.get("turnover_avg20"))
        avg60 = _safe_float(row.get("turnover_avg60"))
        ret1 = _safe_float(row.get("ret1"))
        group_ret = _safe_float(row.get("flow_avg_ret1"))
        flow_fit = "順行" if ret1 is not None and group_ret is not None and ((ret1 >= 0 and group_ret >= 0) or (ret1 < 0 and group_ret < 0)) else "逆行"
        individuals.append(
            {
                "code": str(row.get("code") or row.get("code_key")),
                "name": str(row.get("name") or ""),
                "core_segment": str(row.get("core_segment") or "未分類"),
                "sub_segment": str(row.get("sub_segment") or "未分類"),
                "theme_layer": str(row.get("theme_layer") or "未分類"),
                "flow_group": str(row.get("flow_group") or "未分類"),
                "ret1": ret1,
                "turnover_bil": _safe_float((turnover or 0) / 1e9),
                "turnover_vs5": _safe_float(turnover / avg5) if turnover is not None and avg5 not in (None, 0) else None,
                "turnover_vs20": _safe_float(turnover / avg20) if turnover is not None and avg20 not in (None, 0) else None,
                "turnover_vs60": _safe_float(turnover / avg60) if turnover is not None and avg60 not in (None, 0) else None,
                "flow_fit": flow_fit,
            }
        )

    return {
        "available": True,
        "date": latest.date().isoformat(),
        "market": {
            "turnover_bil": _safe_float(float(all_latest["turnover_yen"]) / 1e9),
            "turnover_vs5": ratio(float(all_latest["turnover_yen"]), all_daily["turnover_yen"], 5),
            "turnover_vs20": ratio(float(all_latest["turnover_yen"]), all_daily["turnover_yen"], 20),
            "turnover_vs60": ratio(float(all_latest["turnover_yen"]), all_daily["turnover_yen"], 60),
            "n": int(all_latest["n"]),
        },
        "universe": {
            "turnover_bil": _safe_float(float(sem_latest["turnover_yen"]) / 1e9),
            "turnover_ex_top1_bil": universe_ex_top1_bil,
            "turnover_vs5": ratio(float(sem_latest["turnover_yen"]), sem_daily["turnover_yen"], 5),
            "turnover_vs20": ratio(float(sem_latest["turnover_yen"]), sem_daily["turnover_yen"], 20),
            "turnover_vs60": ratio(float(sem_latest["turnover_yen"]), sem_daily["turnover_yen"], 60),
            "market_share": _safe_float(float(sem_latest["turnover_yen"]) / float(all_latest["turnover_yen"]) * 100.0),
            "avg_ret1": _safe_float(sem_latest["avg_ret1"]),
            "up_ratio": _safe_float(sem_latest["up_ratio"]),
            "n": int(sem_latest["n"]),
            "top_code": universe_top_code,
            "top_name": universe_top_name,
            "top_share": universe_top1_share,
        },
        "core_segments": summarize("core_segment"),
        "sub_segments": summarize("sub_segment"),
        "theme_layers": summarize("theme_layer"),
        "flow_groups": summarize("flow_group"),
        "individuals": individuals,
        "notes": [
            "売買代金は prices_topix.parquet の Close×Volume 概算。",
            "top1占有率が高い日は、テーマ全体ではなくリーダー集中として分離する。",
            "これは観測レイヤーであり、単独では売買シグナルにしない。",
        ],
    }


def _trade_bucket(signal: dict[str, Any]) -> tuple[str, list[str]]:
    code = str(signal.get("code", ""))
    decision = str(signal.get("decision", ""))
    close = _safe_float(signal.get("close"))
    ret5 = _safe_float(signal.get("ret5"))
    vs25 = _safe_float(signal.get("vs25"))
    cvar = _safe_float(signal.get("cvar05"))
    left_tail = str(signal.get("left_tail", ""))
    judgement = str(signal.get("judgement", ""))
    reasons: list[str] = []

    if code in INDICATOR_CODES:
        reasons.append("値嵩/主力の温度計")
        return "指標銘柄", reasons

    if decision == "AVOID" or "見送り" in judgement:
        reasons.append("判定が見送り")
        return "見送り", reasons

    if close is not None and close >= 20000:
        reasons.append("株価2万円以上")
        return "過熱注意", reasons

    if code in HEAVY_WATCH_CODES:
        reasons.append("値嵩寄りの監視銘柄")
        return "過熱注意", reasons

    if (vs25 is not None and vs25 >= 25) or (ret5 is not None and ret5 >= 18):
        reasons.append("短期過熱")
        return "過熱注意", reasons

    if left_tail == "高" or (cvar is not None and cvar <= -6):
        reasons.append("左尾高")
        return "過熱注意", reasons

    if "寄り後条件付き" in judgement or decision == "WATCH":
        reasons.append("寄り後条件付き")
        return "実弾候補", reasons

    reasons.append("条件未分類")
    return "見送り", reasons


def _attach_trade_buckets(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for signal in signals:
        bucket, reasons = _trade_bucket(signal)
        enriched = dict(signal)
        enriched["trade_bucket"] = bucket
        enriched["trade_bucket_reasons"] = reasons
        rows.append(enriched)
    return rows


def _bucket_summary(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = ["実弾候補", "指標銘柄", "過熱注意", "見送り"]
    rows = []
    for bucket in order:
        items = [s for s in signals if s.get("trade_bucket") == bucket]
        rows.append(
            {
                "bucket": bucket,
                "count": len(items),
                "leaders": [
                    {"code": s.get("code"), "name": s.get("name"), "score": s.get("score")}
                    for s in sorted(items, key=lambda x: _safe_float(x.get("score")) or 0, reverse=True)[:3]
                ],
            }
        )
    return rows


def _attach_entry_decisions(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    segment_lookup = {r["segment"]: r for r in _build_segment_strength(signals)}
    rows = []
    for signal in signals:
        bucket = str(signal.get("trade_bucket", ""))
        ret5 = _safe_float(signal.get("ret5"))
        ret20 = _safe_float(signal.get("ret20"))
        vs25 = _safe_float(signal.get("vs25"))
        score = _safe_float(signal.get("score")) or 0.0
        cvar = _safe_float(signal.get("cvar05"))
        left_tail = str(signal.get("left_tail", ""))
        groups = _segment_name(signal.get("segment"))
        group_stats = [segment_lookup[g] for g in groups if g in segment_lookup]
        segment_strong = any((g.get("avg_ret5") or 0) > 0 and (g.get("breadth5") or 0) >= 50 for g in group_stats)
        segment_hot = any((g.get("avg_ret5") or 0) >= 6 or (g.get("avg_ret20") or 0) >= 30 for g in group_stats)

        priority = score
        reasons: list[str] = []
        status = "WAIT"

        if bucket == "実弾候補":
            priority += 4
            reasons.append("実弾候補")
            status = "READY"
        elif bucket == "過熱注意":
            priority -= 1
            reasons.append("過熱注意")
            status = "WAIT"
        elif bucket == "指標銘柄":
            priority -= 2
            reasons.append("指標銘柄")
            status = "WAIT"
        else:
            priority -= 5
            reasons.append("見送り区分")
            status = "AVOID"

        if segment_strong:
            priority += 2
            reasons.append("セグメント強い")
        else:
            priority -= 2
            reasons.append("セグメント弱い")
            if status == "READY":
                status = "WAIT"

        if segment_hot:
            reasons.append("テーマ過熱")
            if bucket != "実弾候補":
                priority -= 1

        if ret5 is not None and ret5 > 0:
            priority += 1
            reasons.append("5日上昇")
        if ret20 is not None and ret20 > 0:
            priority += 0.5
            reasons.append("20日上昇")
        if vs25 is not None and vs25 > 0:
            priority += 0.5
            reasons.append("25日線上")

        if vs25 is not None and vs25 >= 25:
            priority -= 3
            reasons.append("25日線乖離過大")
            status = "WAIT" if status == "READY" else status
        if ret5 is not None and ret5 >= 18:
            priority -= 2
            reasons.append("5日急騰")
            status = "WAIT" if status == "READY" else status
        if left_tail == "高" or (cvar is not None and cvar <= -6):
            priority -= 2
            reasons.append("左尾注意")
            status = "WAIT" if status == "READY" else status

        enriched = dict(signal)
        enriched["entry_status"] = status
        enriched["entry_priority"] = round(priority, 2)
        enriched["entry_reasons"] = reasons
        rows.append(enriched)

    return sorted(rows, key=lambda r: (_safe_float(r.get("entry_priority")) or -999), reverse=True)


def _load_backtest_summary() -> dict[str, Any]:
    if not BACKTEST_SUMMARY_PATH.exists():
        return {
            "available": False,
            "rows": [],
            "report_url": None,
            "takeaway": "未検証",
        }

    df = pd.read_csv(BACKTEST_SUMMARY_PATH)
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rows.append(
            {
                "variant": str(row.get("variant", "")),
                "n": int(row.get("n", 0)),
                "days": int(row.get("days", 0)),
                "pf": _safe_float(row.get("pf")),
                "win_rate": _safe_float(row.get("win_rate")),
                "sum_pnl_100": _safe_float(row.get("sum_pnl_100")),
                "avg_pnl_100": _safe_float(row.get("avg_pnl_100")),
                "max_dd_100": _safe_float(row.get("max_dd_100")),
                "worst_trade_100": _safe_float(row.get("worst_trade_100")),
                "q05_100": _safe_float(row.get("q05_100")),
                "cvar05_100": _safe_float(row.get("cvar05_100")),
                "from": str(row.get("from", "")),
                "to": str(row.get("to", "")),
            }
        )

    by_variant = {r["variant"]: r for r in rows}
    top1 = by_variant.get("market_momentum_guard_top1") or by_variant.get("market_momentum_top1")
    top3 = by_variant.get("market_momentum_top3")
    if top1 and top3:
        takeaway = "市場モメンタム通過時もTop3は弱い。実運用候補はTop1限定"
    elif top1:
        takeaway = "実運用候補は市場モメンタム通過時のTop1限定"
    else:
        takeaway = "検証サマリー要確認"

    return {
        "available": True,
        "rows": rows,
        "report_url": "/api/dev/semicon/backtest-report" if BACKTEST_REPORT_PATH.exists() else None,
        "takeaway": takeaway,
    }


def _load_morning_pilot() -> dict[str, Any]:
    base = {
        "available": False,
        "label": "午前順張りパイロット",
        "entry_window": "09:20-09:25",
        "exit_window": "10:15-10:45",
        "max_positions": 1,
        "max_shares": 100,
        "rules": [
            "9:00-9:15は見るだけ。寄り直後の飛びつきを避ける",
            "9:20-9:25でセグメントと個別が崩れていない時だけ候補化",
            "10:15-10:45で利確または撤退。後場へ期待を持ち越さない",
            "10時以降の新規ロングは原則しない",
            "地合い・セグメント・個別のどれかが崩れたら見送り",
        ],
        "entry_checks": [
            "外部地合いが逆風優勢でない",
            "対象フロー層の上昇比率が崩れていない",
            "寄付差が過大でない",
            "9:20時点でVWAP上またはVWAP回復が見える",
            "前日高値または発火価格に近い形で失速していない",
        ],
        "exit_rules": [
            "10:15-10:45で利益を確定する",
            "VWAP割れ、指数失速、同一セグメント失速なら早めに撤退",
            "後場の再上昇期待で粘らない",
        ],
        "evidence": [],
    }
    if not INTRADAY_GRID_PATH.exists():
        base["reason"] = "semicon_intraday_long_short_grid.csv missing"
        return base

    try:
        df = pd.read_csv(INTRADAY_GRID_PATH)
    except Exception as exc:
        base["reason"] = f"failed_to_read_intraday_grid: {type(exc).__name__}"
        return base

    required = {"variant", "entry", "long_exit", "type", "n", "days", "pf", "win", "sum", "avg", "dd", "worst", "cvar05"}
    if not required.issubset(df.columns):
        base["reason"] = "intraday_grid_schema_mismatch"
        return base

    preferred = [
        ("avoid_overheat_noru", "09:25", "10:30", "本線: 過熱を避けて9:25→10:30"),
        ("avoid_overheat_noru", "09:25", "10:15", "早め撤退: 過熱を避けて9:25→10:15"),
        ("mlcc_flow_top1_day", "09:20", "10:45", "MLCC top1: 9:20→10:45"),
        ("action_noru_top1_day", "09:20", "10:15", "フロー上位top1: 9:20→10:15"),
    ]
    evidence = []
    for variant, entry, long_exit, label in preferred:
        hit = df[
            (df["type"].astype(str).eq("long_only"))
            & (df["variant"].astype(str).eq(variant))
            & (df["entry"].astype(str).eq(entry))
            & (df["long_exit"].astype(str).eq(long_exit))
        ]
        if hit.empty:
            continue
        row = hit.iloc[0]
        evidence.append(
            {
                "label": label,
                "variant": variant,
                "entry": entry,
                "exit": long_exit,
                "n": int(row.get("n", 0)),
                "days": int(row.get("days", 0)),
                "pf": _safe_float(row.get("pf")),
                "win_rate": _safe_float(row.get("win")),
                "sum_pnl_100": _safe_float(row.get("sum")),
                "avg_pnl_100": _safe_float(row.get("avg")),
                "max_dd_100": _safe_float(row.get("dd")),
                "worst_trade_100": _safe_float(row.get("worst")),
                "cvar05_100": _safe_float(row.get("cvar05")),
            }
        )

    base.update(
        {
            "available": bool(evidence),
            "source": INTRADAY_GRID_PATH.name,
            "takeaway": "寄り直後ではなく9:20-9:25まで待ち、10時台に閉じる。午後の期待で伸ばさない。",
            "evidence": evidence,
        }
    )
    if not evidence:
        base["reason"] = "preferred_morning_rows_missing"
    return base


def _load_hold_short_exposures() -> list[dict[str, Any]]:
    if not HOLD_STOCKS_PATH.exists():
        return []
    try:
        df = pd.read_csv(HOLD_STOCKS_PATH)
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        code = str(row.get("コード", "")).strip()
        side = str(row.get("売買", "")).strip()
        stock = UNIVERSE_BY_CODE.get(code) or HOLD_EXPOSURE_EXTRA_BY_CODE.get(code)
        if stock is None or side != "売建":
            continue

        pnl = _parse_number(row.get("評価損益額合計(円)"))
        pnl_pct = _parse_number(row.get("評価損益率(%)"))
        qty = _parse_number(row.get("建玉数量合計(株/口)"))
        current_price = _parse_number(row.get("時価(円)"))
        entry_value = _parse_number(row.get("建玉金額合計(円)"))
        risk_level = "高" if (pnl_pct is not None and pnl_pct <= -20) else "中" if (pnl_pct is not None and pnl_pct <= -10) else "低"
        note = "AI/半導体周辺テーマの売建。踏み上げ警戒" if risk_level in {"高", "中"} else "テーマ該当売建"
        rows.append(
            {
                "code": code,
                "ticker": f"{code}.T",
                "name": str(row.get("銘柄名", stock.name)).strip(),
                "segment": stock.segment,
                "label": stock.label,
                "side": side,
                "quantity": qty,
                "current_price": current_price,
                "entry_value": entry_value,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "risk_level": risk_level,
                "note": note,
            }
        )
    return sorted(rows, key=lambda r: (_safe_float(r.get("pnl")) or 0.0))


def _build_payload_from_report() -> dict[str, Any] | None:
    if not REPORT_PATH.exists():
        return None
    try:
        tables = pd.read_html(REPORT_PATH)
    except ValueError:
        return None
    if len(tables) < 3:
        return None

    coverage = tables[0]
    overseas_table = tables[1]
    ranking = tables[2]

    data_date = None
    if {"データ", "最新日"}.issubset(coverage.columns):
        daily = coverage[coverage["データ"].astype(str).str.contains("J-Quants 公式日足", na=False)]
        if not daily.empty:
            data_date = str(daily.iloc[0]["最新日"])
    if not data_date and "日足基準日" in ranking.columns and not ranking.empty:
        data_date = str(ranking["日足基準日"].max())

    overseas_rows = []
    for _, row in overseas_table.iterrows():
        ticker = str(row.get("指標", "")).strip()
        overseas_rows.append(
            {
                "ticker": ticker,
                "name": OVERSEAS.get(ticker, ticker),
                "date": str(row.get("日付", "")),
                "close": _parse_number(row.get("終値")),
                "ret1": _parse_percent(row.get("1日%")),
                "ret5": _parse_percent(row.get("5日%")),
                "ret20": _parse_percent(row.get("20日%")),
            }
        )
    market = _market_regime(overseas_rows)
    market_indicators = _market_indicators_from_overseas(overseas_rows)

    signals = []
    for _, row in ranking.iterrows():
        decision = _decision_from_report(row.get("判定"))
        entry_rule = str(row.get("翌日条件", ""))
        entry_trigger = _entry_trigger_price(entry_rule)
        reasons = []
        warnings = []
        label = str(row.get("根拠", ""))
        if str(row.get("判定", "")) == "寄り後条件付き":
            reasons.append("HTML統合ランキング: 寄り後条件付き")
        if str(row.get("判定", "")) == "押し目/回復待ち":
            reasons.append("HTML統合ランキング: 押し目/回復待ち")
        if str(row.get("左尾", "")) == "高":
            warnings.append("左尾高")
        signals.append(
            {
                "code": str(row.get("コード", "")),
                "ticker": f"{row.get('コード')}.T",
                "name": str(row.get("銘柄", "")),
                "label": label,
                "segment": str(row.get("分類", "")),
                "decision": decision,
                "score": _parse_number(row.get("統合点")) or 0,
                "reasons": reasons,
                "warnings": warnings,
                "entry_rule": entry_rule,
                "entry_trigger_price": entry_trigger,
                "date": str(row.get("日足基準日", "")),
                "close": _parse_number(row.get("公式終値")),
                "ret5": _parse_percent(row.get("5日")),
                "ret20": _parse_percent(row.get("20日")),
                "vs25": _parse_percent(row.get("25日線比")),
                "dist20hi": _parse_percent(row.get("20日高値比")),
                "max_dd_60d": None,
                "cvar05": _parse_percent(row.get("CVaR5")),
                "revenue_growth": _parse_percent(row.get("営業益YoY")),
                "op_margin": None,
                "left_tail": str(row.get("左尾", "")),
                "judgement": str(row.get("判定", "")),
            }
        )
    signals = _attach_entry_decisions(_attach_trade_buckets(_attach_classification_meta(signals)))

    parsed_date = pd.to_datetime(data_date, errors="coerce")
    stale_days = (date.today() - parsed_date.date()).days if not pd.isna(parsed_date) else None
    return {
        "generated_at": pd.Timestamp.fromtimestamp(REPORT_PATH.stat().st_mtime, tz="Asia/Tokyo").isoformat(),
        "data_date": data_date,
        "data_stale": stale_days is not None and stale_days >= 3,
        "stale_days": stale_days,
        "market": market,
        "signals": signals,
        "classification_basis": CLASSIFICATION_BASIS,
        "segment_strength": _build_segment_strength(signals),
        "flow_analysis": _build_flow_analysis(signals),
        "bucket_summary": _bucket_summary(signals),
        "market_indicators": market_indicators,
        "market_indicator_date": _market_indicator_date(market_indicators),
        "market_indicator_source": "ai_semiconductor_yf_entry_risk_report.html:overseas",
        "overseas": overseas_rows,
        "report_available": True,
        "report_url": "/api/dev/semicon/report",
        "source": "ai_semiconductor_yf_entry_risk_report.html",
        "operation": {
            "headline": "無条件買いなし。条件付き監視",
            "primary_action": "寄り後条件を満たす銘柄だけ小さく候補化",
            "morning_checks": [
                "SOX/NVIDIA/Micron/TSMC/NASDAQ先物/CMEを確認",
                "寄付差が過大なら待つ",
                "前日高値超えまたはVWAP上維持を確認",
                "左尾高の銘柄はロットを落とす",
            ],
            "avoid_rules": [
                "SOX/NVIDIA/Micronが同時に崩れる",
                "寄りで飛びすぎて前日高値超え後にVWAPを割る",
                "決算・材料・地政学でボラが読みにくい",
                "左尾高なのに通常ロットで入る",
            ],
        },
        "morning_pilot": _load_morning_pilot(),
        "backtest": _load_backtest_summary(),
        "hold_short_exposures": _load_hold_short_exposures(),
        "counts": {
            "buy": sum(1 for s in signals if s["decision"] == "BUY_CANDIDATE"),
            "watch": sum(1 for s in signals if s["decision"] == "WATCH"),
            "avoid": sum(1 for s in signals if s["decision"] == "AVOID"),
            "total": len(signals),
        },
    }


def _normalize_semicon_payload(data: dict[str, Any], source: str, us_pending: bool = False) -> dict[str, Any]:
    signals = data.get("signals")
    if signals is None:
        signals = data.get("rows") or data.get("candidates") or []
    if not isinstance(signals, list):
        signals = []
    signals = _attach_entry_decisions(_attach_trade_buckets(_attach_classification_meta(signals)))

    market = data.get("market")
    if not isinstance(market, dict):
        market = {
            "state": "US_PENDING" if us_pending else "NO_DATA",
            "label": "米国判定待ち" if us_pending else "データ未取得",
        }
    overseas_rows = data.get("overseas") or []
    if not isinstance(overseas_rows, list):
        overseas_rows = []
    if overseas_rows and not us_pending:
        market = _market_regime(overseas_rows)
    market_indicators = data.get("market_indicators") or _market_indicators_from_overseas(overseas_rows)
    if not isinstance(market_indicators, list):
        market_indicators = []

    payload = dict(data)
    payload.update(
        {
            "generated_at": data.get("generated_at"),
            "data_date": data.get("data_date") or data.get("price_data_date") or data.get("as_of") or data.get("date"),
            "price_data_date": data.get("price_data_date"),
            "price_source": data.get("price_source"),
            "market": market,
            "signals": signals,
            "classification_basis": data.get("classification_basis") or CLASSIFICATION_BASIS,
            "segment_strength": _build_segment_strength(signals),
            "flow_analysis": _build_flow_analysis(signals),
            "bucket_summary": _bucket_summary(signals),
            "market_indicators": market_indicators,
            "market_indicator_date": data.get("market_indicator_date") or _market_indicator_date(market_indicators),
            "market_indicator_source": data.get("market_indicator_source") or ("overseas" if market_indicators else None),
            "overseas": overseas_rows,
            "report_available": bool(data.get("report_available")),
            "report_url": data.get("report_url"),
            "backtest": data.get("backtest") or _load_backtest_summary(),
            "morning_pilot": data.get("morning_pilot") or _load_morning_pilot(),
            "hold_short_exposures": data.get("hold_short_exposures") or _load_hold_short_exposures(),
            "source": source,
            "source_environment": APP_ENV,
            "source_data_mode": "s3" if USE_S3_ARTIFACTS else "local",
            "source_data_mode_reason": "APP_ENV",
            "source_data_mode_error": None,
            "us_pending": us_pending,
            "counts": {
                "buy": sum(1 for s in signals if isinstance(s, dict) and s.get("decision") == "BUY_CANDIDATE"),
                "watch": sum(1 for s in signals if isinstance(s, dict) and s.get("decision") == "WATCH"),
                "avoid": sum(1 for s in signals if isinstance(s, dict) and s.get("decision") == "AVOID"),
                "total": len(signals),
            },
        }
    )
    return payload


def _build_payload_from_environment_json() -> dict[str, Any] | None:
    candidates: list[tuple[pd.Timestamp, pd.Timestamp, str, bool, dict[str, Any]]] = []

    entry = _read_semicon_json(SEMICON_ENTRY_JSON, S3_ENTRY_KEY)
    if entry is not None:
        source = S3_ENTRY_KEY if USE_S3_ARTIFACTS else SEMICON_ENTRY_JSON.name
        data_ts = pd.to_datetime(entry.get("price_data_date") or entry.get("data_date") or entry.get("as_of") or entry.get("date"), errors="coerce")
        gen_ts = pd.to_datetime(entry.get("artifact_generated_at") or entry.get("generated_at"), errors="coerce")
        candidates.append((data_ts, gen_ts, source, False, entry))

    domestic = _read_semicon_json(SEMICON_DOMESTIC_JSON, S3_DOMESTIC_KEY)
    if domestic is not None:
        source = S3_DOMESTIC_KEY if USE_S3_ARTIFACTS else SEMICON_DOMESTIC_JSON.name
        data_ts = pd.to_datetime(domestic.get("price_data_date") or domestic.get("data_date") or domestic.get("as_of") or domestic.get("date"), errors="coerce")
        gen_ts = pd.to_datetime(domestic.get("artifact_generated_at") or domestic.get("generated_at"), errors="coerce")
        candidates.append((data_ts, gen_ts, source, True, domestic))

    if not candidates:
        return None

    def sort_key(item: tuple[pd.Timestamp, pd.Timestamp, str, bool, dict[str, Any]]) -> tuple[pd.Timestamp, pd.Timestamp, int]:
        data_ts, gen_ts, _, us_pending, _ = item
        safe_data = data_ts if not pd.isna(data_ts) else pd.Timestamp.min
        safe_gen = gen_ts if not pd.isna(gen_ts) else pd.Timestamp.min
        entry_tie_breaker = 0 if us_pending else 1
        return safe_data, safe_gen, entry_tie_breaker

    _, _, source, us_pending, data = max(candidates, key=sort_key)
    return _normalize_semicon_payload(data, source, us_pending=us_pending)


def build_payload() -> dict[str, Any]:
    env_payload = _build_payload_from_environment_json()
    if env_payload is not None:
        return env_payload

    if USE_S3_ARTIFACTS:
        return {
            "generated_at": None,
            "data_date": None,
            "market": {"state": "NO_DATA", "label": "semicon S3 artifact not found"},
            "signals": [],
            "classification_basis": CLASSIFICATION_BASIS,
            "segment_strength": [],
            "flow_analysis": {"available": False, "reason": "s3_json_missing"},
            "bucket_summary": [],
            "overseas": [],
            "report_available": False,
            "morning_pilot": _load_morning_pilot(),
            "source": "s3_json_missing",
            "source_environment": APP_ENV,
            "source_data_mode": "s3",
            "source_data_mode_reason": "APP_ENV",
            "source_data_mode_error": None,
            "counts": {"buy": 0, "watch": 0, "avoid": 0, "total": 0},
        }

    report_payload = _build_payload_from_report()
    if report_payload is not None:
        report_payload["source_environment"] = APP_ENV
        report_payload["source_data_mode"] = "local"
        report_payload["source_data_mode_reason"] = "APP_ENV"
        report_payload["source_data_mode_error"] = None
        return report_payload

    if not PRICES_PATH.exists():
        return {
            "generated_at": None,
            "data_date": None,
            "market": {"state": "NO_DATA", "label": "データ未取得"},
            "signals": [],
            "classification_basis": CLASSIFICATION_BASIS,
            "flow_analysis": {"available": False, "reason": "prices_raw_missing"},
            "overseas": [],
            "morning_pilot": _load_morning_pilot(),
            "report_available": REPORT_PATH.exists(),
            "source_environment": APP_ENV,
            "source_data_mode": "local",
            "source_data_mode_reason": "APP_ENV",
            "source_data_mode_error": None,
        }

    prices = pd.read_parquet(PRICES_PATH).sort_index()
    fundamentals = _load_fundamentals()
    overseas_rows = []
    for ticker, name in OVERSEAS.items():
        if ticker not in prices.columns:
            continue
        metrics = _metric_row(ticker, prices[ticker])
        if metrics.get("missing"):
            continue
        overseas_rows.append(
            {
                "ticker": ticker,
                "name": name,
                "date": metrics.get("date"),
                "close": metrics.get("close"),
                "ret1": _pct(float(prices[ticker].dropna().iloc[-1]), float(prices[ticker].dropna().iloc[-2])) if len(prices[ticker].dropna()) >= 2 else None,
                "ret5": metrics.get("ret5"),
                "ret20": metrics.get("ret20"),
            }
        )
    market = _market_regime(overseas_rows)

    signals = []
    for stock in UNIVERSE:
        if stock.code not in prices.columns:
            continue
        metrics = _metric_row(stock.code, prices[stock.code])
        if metrics.get("missing"):
            continue
        signals.append(_score_stock(stock, metrics, market["state"], fundamentals.get(stock.code, {})))
    signals = _attach_entry_decisions(_attach_trade_buckets(_attach_classification_meta(signals)))
    data_date = str(prices.index.max().date()) if not prices.empty and hasattr(prices.index.max(), "date") else None
    stale_days = (date.today() - prices.index.max().date()).days if data_date and hasattr(prices.index.max(), "date") else None
    return {
        "generated_at": pd.Timestamp.now(tz="Asia/Tokyo").isoformat(),
        "data_date": data_date,
        "data_stale": stale_days is not None and stale_days >= 3,
        "stale_days": stale_days,
        "market": market,
        "signals": signals,
        "classification_basis": CLASSIFICATION_BASIS,
        "segment_strength": _build_segment_strength(signals),
        "flow_analysis": _build_flow_analysis(signals),
        "bucket_summary": _bucket_summary(signals),
        "overseas": overseas_rows,
        "report_available": REPORT_PATH.exists(),
        "report_url": "/api/dev/semicon/report",
        "source": "prices_raw.parquet",
        "source_environment": APP_ENV,
        "source_data_mode": "local",
        "source_data_mode_reason": "APP_ENV",
        "source_data_mode_error": None,
        "backtest": _load_backtest_summary(),
        "morning_pilot": _load_morning_pilot(),
        "hold_short_exposures": _load_hold_short_exposures(),
        "counts": {
            "buy": sum(1 for s in signals if s["decision"] == "BUY_CANDIDATE"),
            "watch": sum(1 for s in signals if s["decision"] == "WATCH"),
            "avoid": sum(1 for s in signals if s["decision"] == "AVOID"),
            "total": len(signals),
        },
    }


@router.get("/api/dev/semicon/signals")
async def get_semicon_signals():
    return build_payload()


@router.get("/api/dev/semicon/report")
async def get_semicon_report():
    html = _read_semicon_html(REPORT_PATH, S3_REPORT_KEY)
    if html is None:
        return JSONResponse(status_code=404, content={"detail": "semiconductor report not found"})
    return HTMLResponse(content=html, media_type="text/html")


@router.get("/api/dev/semicon/backtest-report")
async def get_semicon_backtest_report():
    html = _read_semicon_html(BACKTEST_REPORT_PATH, S3_BACKTEST_REPORT_KEY)
    if html is None:
        return JSONResponse(status_code=404, content={"detail": "semiconductor backtest report not found"})
    return HTMLResponse(content=html, media_type="text/html")
