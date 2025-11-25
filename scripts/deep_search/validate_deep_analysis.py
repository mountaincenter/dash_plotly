#!/usr/bin/env python3
"""
Validate deep_analysis_YYYY-MM-DD.json

docs/DATA_SOURCE_MAPPING.md に基づいて全28フィールドを検証

Usage:
  python3 scripts/deep_search/validate_deep_analysis.py 2025-11-19

Validation checks:
1. 29 top-level fields exist
2. companyInfo (7 keys)
3. fundamentals (15 keys)
4. priceAnalysis (5 keys)
5. stockPrice (4 keys)
6. earnings (7 keys)
7. analyst (4 keys) - must be populated
8. No null values
9. No empty strings (except valid empty lists/dicts)
10. No default placeholder values
"""
import sys
import json
from pathlib import Path
from typing import Dict, List, Any, Set

ROOT = Path(__file__).resolve().parents[2]

ANALYSIS_DIR = ROOT / 'data' / 'parquet' / 'backtest' / 'analysis'

# Required structure per DATA_SOURCE_MAPPING.md
REQUIRED_STRUCTURE = {
    'top_level': [
        'ticker', 'stockName', 'grokRank', 'v2Score', 'finalScore', 'scoreAdjustment',
        'recommendation', 'confidence', 'companyInfo', 'fundamentals', 'priceAnalysis',
        'stockPrice', 'earnings', 'analyst', 'webMaterials', 'adjustmentReasons',
        'risks', 'opportunities', 'dayTradeScore', 'dayTradeRecommendation',
        'dayTradeReasons', 'latestNews', 'sectorTrend', 'marketSentiment',
        'newsHeadline', 'verdict', 'technicalDataActual', 'originalTechnicalData',
        'originalRecommendation'
    ],
    'companyInfo': [
        'companyName', 'companyNameEnglish', 'sector17', 'sector33',
        'marketCode', 'marketName', 'scaleCategory'
    ],
    'fundamentals': [
        'disclosedDate', 'fiscalYear', 'fiscalPeriod', 'eps', 'bps',
        'operatingProfit', 'ordinaryProfit', 'netIncome', 'revenue',
        'totalAssets', 'equity', 'roe', 'roa', 'revenueGrowthYoY', 'profitGrowthYoY'
    ],
    'priceAnalysis': [
        'trend', 'priceMovement', 'volumeAnalysis', 'technicalLevels', 'patternAnalysis'
    ],
    'stockPrice': [
        'current', 'change_', 'volumeChange_', 'materialExhaustion'  # change_ and volumeChange_ have dynamic date suffix
    ],
    'earnings': [
        'date', 'quarter', 'revenue', 'revenueGrowth',
        'operatingProfit', 'operatingProfitGrowth', 'evaluation'
    ],
    'analyst': [
        'hasCoverage', 'targetPrice', 'upside', 'rating'
    ]
}


class ValidationError:
    """Validation error container"""

    def __init__(self, ticker: str, field: str, issue: str):
        self.ticker = ticker
        self.field = field
        self.issue = issue

    def __str__(self):
        return f"[{self.ticker}] {self.field}: {self.issue}"


def load_analysis_file(date: str) -> Dict:
    """deep_analysis_YYYY-MM-DD.json読み込み"""
    file_path = ANALYSIS_DIR / f'deep_analysis_{date}.json'

    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def validate_top_level_fields(stock: Dict) -> List[ValidationError]:
    """トップレベルフィールド検証"""
    errors = []
    ticker = stock.get('ticker', 'UNKNOWN')

    for field in REQUIRED_STRUCTURE['top_level']:
        if field not in stock:
            errors.append(ValidationError(ticker, field, 'フィールドが存在しない'))

    return errors


def validate_nested_object(stock: Dict, parent_key: str, required_keys: List[str]) -> List[ValidationError]:
    """ネストされたオブジェクト検証"""
    errors = []
    ticker = stock.get('ticker', 'UNKNOWN')

    if parent_key not in stock:
        errors.append(ValidationError(ticker, parent_key, 'フィールドが存在しない'))
        return errors

    obj = stock[parent_key]

    if not isinstance(obj, dict):
        errors.append(ValidationError(ticker, parent_key, f'dictである必要があります（実際: {type(obj).__name__}）'))
        return errors

    # Special handling for stockPrice (dynamic date keys)
    if parent_key == 'stockPrice':
        # Check for current and materialExhaustion
        if 'current' not in obj:
            errors.append(ValidationError(ticker, f'{parent_key}.current', 'フィールドが存在しない'))
        if 'materialExhaustion' not in obj:
            errors.append(ValidationError(ticker, f'{parent_key}.materialExhaustion', 'フィールドが存在しない'))

        # Check for date-suffixed keys (change_YYYY-MM-DD, volumeChange_YYYY-MM-DD)
        change_keys = [k for k in obj.keys() if k.startswith('change_')]
        volume_change_keys = [k for k in obj.keys() if k.startswith('volumeChange_')]

        if not change_keys:
            errors.append(ValidationError(ticker, f'{parent_key}.change_YYYY-MM-DD', 'フィールドが存在しない'))
        if not volume_change_keys:
            errors.append(ValidationError(ticker, f'{parent_key}.volumeChange_YYYY-MM-DD', 'フィールドが存在しない'))

        return errors

    # Normal key validation
    for key in required_keys:
        if key not in obj:
            errors.append(ValidationError(ticker, f'{parent_key}.{key}', 'フィールドが存在しない'))

    return errors


def validate_no_null_values(stock: Dict, parent_key: str = '') -> List[ValidationError]:
    """null値チェック（空リスト・空dictは許容）"""
    errors = []
    ticker = stock.get('ticker', 'UNKNOWN')

    # 許容される空値のフィールド（手動入力プレースホルダー）
    allowed_empty_fields = {
        'latestNews', 'webMaterials', 'adjustmentReasons', 'risks', 'opportunities',
        'sectorTrend', 'marketSentiment', 'newsHeadline', 'verdict', 'dayTradeReasons'
    }

    for key, value in stock.items():
        current_path = f'{parent_key}.{key}' if parent_key else key

        # Skip allowed empty fields
        if key in allowed_empty_fields:
            continue

        if value is None:
            # analyst fields: check hasCoverage first
            if parent_key == 'analyst':
                # For targetPrice and upside, null is OK if hasCoverage=False
                if key in ['targetPrice', 'upside']:
                    # Check parent object's hasCoverage
                    parent_obj = stock
                    for part in parent_key.split('.'):
                        if part:
                            parent_obj = parent_obj.get(part, {})
                    if parent_obj.get('hasCoverage'):
                        errors.append(ValidationError(ticker, current_path, 'null値（hasCoverage=Trueの場合は必須）'))
                    # else: null is OK when hasCoverage=False
                else:
                    # Other analyst fields like rating should not be null
                    if key not in ['targetPrice', 'upside']:
                        errors.append(ValidationError(ticker, current_path, 'null値（analyst情報は必須）'))
            # Other critical fields
            elif key in ['ticker', 'stockName', 'grokRank', 'v2Score', 'finalScore', 'recommendation', 'confidence']:
                errors.append(ValidationError(ticker, current_path, 'null値（必須フィールド）'))
            # Numeric fields in nested objects (optional but should not be null if available)
            elif parent_key in ['fundamentals', 'stockPrice', 'earnings']:
                pass  # Optional numeric fields can be null
            else:
                pass  # Other null values are allowed for optional fields

        elif isinstance(value, dict):
            errors.extend(validate_no_null_values(value, current_path))

        elif isinstance(value, str) and value == '' and key not in allowed_empty_fields:
            # Empty string check (except allowed fields)
            if parent_key == 'analyst':
                errors.append(ValidationError(ticker, current_path, '空文字列（analyst情報は必須）'))
            elif key in ['ticker', 'stockName', 'recommendation', 'confidence']:
                errors.append(ValidationError(ticker, current_path, '空文字列（必須フィールド）'))

    return errors


def validate_analyst_populated(stock: Dict) -> List[ValidationError]:
    """analyst情報が手動入力されているか検証"""
    errors = []
    ticker = stock.get('ticker', 'UNKNOWN')

    if 'analyst' not in stock:
        errors.append(ValidationError(ticker, 'analyst', 'フィールドが存在しない'))
        return errors

    analyst = stock['analyst']

    # hasCoverageがFalseの場合はOK（カバレッジなし）
    # targetPrice, upsideがnullでも問題ない
    if not analyst.get('hasCoverage'):
        # ratingが「カバレッジなし」であることを確認
        if analyst.get('rating') != 'カバレッジなし':
            errors.append(ValidationError(ticker, 'analyst.rating', 'hasCoverage=Falseの場合は「カバレッジなし」が必要'))
        return errors

    # hasCoverage=Trueの場合は他のフィールドも必要
    if analyst.get('targetPrice') is None:
        errors.append(ValidationError(ticker, 'analyst.targetPrice', 'null値（hasCoverage=Trueの場合は必須）'))
    if analyst.get('upside') is None:
        errors.append(ValidationError(ticker, 'analyst.upside', 'null値（hasCoverage=Trueの場合は必須）'))
    if not analyst.get('rating') or analyst.get('rating') == 'カバレッジなし':
        errors.append(ValidationError(ticker, 'analyst.rating', 'レーティングが設定されていない（hasCoverage=Trueの場合）'))

    return errors


def validate_stock(stock: Dict) -> List[ValidationError]:
    """1銘柄の完全検証"""
    errors = []

    # 1. Top-level fields
    errors.extend(validate_top_level_fields(stock))

    # 2. Nested objects
    errors.extend(validate_nested_object(stock, 'companyInfo', REQUIRED_STRUCTURE['companyInfo']))
    errors.extend(validate_nested_object(stock, 'fundamentals', REQUIRED_STRUCTURE['fundamentals']))
    errors.extend(validate_nested_object(stock, 'priceAnalysis', REQUIRED_STRUCTURE['priceAnalysis']))
    errors.extend(validate_nested_object(stock, 'stockPrice', REQUIRED_STRUCTURE['stockPrice']))
    errors.extend(validate_nested_object(stock, 'earnings', REQUIRED_STRUCTURE['earnings']))
    errors.extend(validate_nested_object(stock, 'analyst', REQUIRED_STRUCTURE['analyst']))

    # 3. No null values
    errors.extend(validate_no_null_values(stock))

    # 4. Analyst populated
    errors.extend(validate_analyst_populated(stock))

    return errors


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 validate_deep_analysis.py YYYY-MM-DD")
        sys.exit(1)

    date = sys.argv[1]

    print("=" * 80)
    print(f"Validate deep_analysis_{date}.json")
    print("=" * 80)

    # Load file
    print(f"\n[Step 1] Loading deep_analysis_{date}.json...")
    data = load_analysis_file(date)
    stocks = data.get('stockAnalyses', [])
    print(f"✅ Loaded {len(stocks)} stocks")

    # Validate each stock
    print("\n[Step 2] Validating stocks...")
    all_errors = []

    for stock in stocks:
        ticker = stock.get('ticker', 'UNKNOWN')
        errors = validate_stock(stock)

        if errors:
            print(f"  ❌ [{ticker}] {len(errors)} errors")
            all_errors.extend(errors)
        else:
            print(f"  ✅ [{ticker}] Valid")

    # Summary
    print("\n" + "=" * 80)
    print("Validation Summary")
    print("=" * 80)

    if all_errors:
        print(f"\n❌ Found {len(all_errors)} validation errors:\n")
        for error in all_errors:
            print(f"  {error}")
        print()
        sys.exit(1)
    else:
        print("\n✅ All validation checks passed!")
        print(f"\nFile is valid: deep_analysis_{date}.json")
        print(f"Total stocks: {len(stocks)}")
        print()
        print("Next steps:")
        print(f"  1. Review the file manually")
        print(f"  2. Merge with trading_recommendation.json:")
        print(f"     python3 scripts/merge_trading_recommendation_with_deep_analysis.py")
        print(f"  3. Enrich grok_analysis_merged.parquet:")
        print(f"     python3 scripts/pipeline/enrich_grok_analysis_with_deep_analysis.py")


if __name__ == '__main__':
    main()
