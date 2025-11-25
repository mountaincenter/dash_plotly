#!/usr/bin/env python3
"""
J-Quants APIから財務データを取得
"""
import os
import requests
import json
from pathlib import Path
from datetime import datetime
import time

# 環境変数読み込み
env_file = Path(__file__).parent.parent / ".env.jquants"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()

BASE_URL = os.getenv("JQUANTS_API_BASE_URL", "https://api.jquants.com/v1")
MAIL = os.getenv("JQUANTS_MAIL_ADDRESS")
PASSWORD = os.getenv("JQUANTS_PASSWORD")

def get_refresh_token():
    """リフレッシュトークン取得（直接取得）"""
    url = f"{BASE_URL}/token/auth_user"
    data = {"mailaddress": MAIL, "password": PASSWORD}
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=json.dumps(data), headers=headers)

    if response.status_code != 200:
        print(f"[ERROR] Auth failed: {response.status_code} {response.text}")
        raise Exception(f"Authentication failed: {response.text}")

    result = response.json()
    if "refreshToken" not in result:
        print(f"[ERROR] No refreshToken in response: {result}")
        raise Exception(f"No refreshToken in response: {result}")

    return result["refreshToken"]

def get_fundamentals(ticker_code, refresh_token):
    """財務データ取得"""
    # ティッカーから数字のみ抽出（例: 2586.T -> 25860）
    code = ticker_code.replace('.T', '').replace('.', '')
    code = code.ljust(5, '0')  # 4桁の場合は5桁にパディング

    url = f"{BASE_URL}/fins/statements"
    headers = {"Authorization": f"Bearer {refresh_token}"}
    params = {"code": code}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        return None

    data = response.json()
    if not data.get("statements"):
        return None

    # 最新の決算データを取得
    latest = sorted(data["statements"], key=lambda x: x.get("DisclosedDate", ""), reverse=True)[0]

    return {
        "ticker": ticker_code,
        "disclosedDate": latest.get("DisclosedDate"),
        "fiscalYear": latest.get("FiscalYear"),
        "fiscalPeriod": latest.get("TypeOfCurrentPeriod"),
        "netSales": latest.get("NetSales"),  # 売上高
        "operatingProfit": latest.get("OperatingProfit"),  # 営業利益
        "ordinaryProfit": latest.get("OrdinaryProfit"),  # 経常利益
        "profit": latest.get("Profit"),  # 当期純利益
        "totalAssets": latest.get("TotalAssets"),  # 総資産
        "equity": latest.get("Equity"),  # 純資産
        "roe": latest.get("ROE"),  # ROE
        "roa": latest.get("ROA"),  # ROA
        "per": latest.get("PER"),  # PER
        "pbr": latest.get("PBR"),  # PBR
        "eps": latest.get("EPS"),  # EPS
        "bps": latest.get("BPS"),  # BPS
    }

def get_company_info(ticker_code, refresh_token):
    """企業情報取得"""
    code = ticker_code.replace('.T', '').replace('.', '')
    code = code.ljust(5, '0')

    url = f"{BASE_URL}/listed/info"
    headers = {"Authorization": f"Bearer {refresh_token}"}
    params = {"code": code}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        return None

    data = response.json()
    if not data.get("info"):
        return None

    info = data["info"][0]

    return {
        "ticker": ticker_code,
        "companyName": info.get("CompanyName"),
        "companyNameEnglish": info.get("CompanyNameEnglish"),
        "sector17": info.get("Sector17CodeName"),
        "sector33": info.get("Sector33CodeName"),
        "marketCode": info.get("MarketCodeName"),
        "marketCapitalization": info.get("MarketCapitalization"),  # 時価総額（百万円）
    }

def main(tickers):
    """メイン処理"""
    print("[INFO] Getting refresh token...")
    refresh_token = get_refresh_token()

    results = {}

    for ticker in tickers:
        print(f"[INFO] Fetching data for {ticker}...")

        # 財務データ取得
        fundamentals = get_fundamentals(ticker, refresh_token)
        # 企業情報取得
        company_info = get_company_info(ticker, refresh_token)

        results[ticker] = {
            "fundamentals": fundamentals,
            "companyInfo": company_info,
        }

        time.sleep(0.5)  # API制限を考慮

    return results

if __name__ == "__main__":
    import sys

    # コマンドライン引数からティッカーリストを取得
    if len(sys.argv) > 1:
        tickers = sys.argv[1].split(',')
    else:
        # デフォルト: 11銘柄
        tickers = ['2586.T', '6594.T', '5597.T', '5574.T', '302A.T', '9348.T', '6495.T', '2462.T', '265A.T', '2492.T', '6432.T']

    results = main(tickers)

    # JSON出力
    print(json.dumps(results, ensure_ascii=False, indent=2))
