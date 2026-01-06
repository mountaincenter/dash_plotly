#!/usr/bin/env python3
"""
JPX 制限値幅計算モジュール

参照: https://www.jpx.co.jp/equities/trading/domestic/06.html
"""

from __future__ import annotations


# JPX制限値幅テーブル (基準値段上限, 制限値幅)
PRICE_LIMIT_TABLE: list[tuple[int, int]] = [
    (100, 30),
    (200, 50),
    (500, 80),
    (700, 100),
    (1_000, 150),
    (1_500, 300),
    (2_000, 400),
    (3_000, 500),
    (5_000, 700),
    (7_000, 1_000),
    (10_000, 1_500),
    (15_000, 3_000),
    (20_000, 4_000),
    (30_000, 5_000),
    (50_000, 7_000),
    (70_000, 10_000),
    (100_000, 15_000),
]

# 100,000円超の制限値幅
PRICE_LIMIT_OVER_100K = 30_000


def calc_price_limit(price: float | int) -> int:
    """
    基準値段から制限値幅を算出

    Args:
        price: 基準値段（前日終値）

    Returns:
        制限値幅（円）

    Examples:
        >>> calc_price_limit(500)
        80
        >>> calc_price_limit(1000)
        150
        >>> calc_price_limit(150000)
        30000
    """
    if price <= 0:
        return 0

    for threshold, limit in PRICE_LIMIT_TABLE:
        if price <= threshold:
            return limit

    return PRICE_LIMIT_OVER_100K


def calc_upper_limit_price(price: float | int) -> float:
    """
    制限値幅上限価格を算出

    Args:
        price: 基準値段（前日終値）

    Returns:
        制限値幅上限価格（基準値段 + 制限値幅）

    Examples:
        >>> calc_upper_limit_price(1000)
        1150.0
        >>> calc_upper_limit_price(5000)
        5700.0
    """
    return float(price) + calc_price_limit(price)


def calc_max_cost_100(price: float | int) -> int:
    """
    100株成行買いの最大必要資金を算出

    Args:
        price: 基準値段（前日終値）

    Returns:
        最大必要資金（円）= (基準値段 + 制限値幅) × 100

    Examples:
        >>> calc_max_cost_100(1000)
        115000
        >>> calc_max_cost_100(5000)
        570000
    """
    return int(calc_upper_limit_price(price) * 100)


if __name__ == "__main__":
    # テスト
    test_prices = [50, 100, 200, 500, 1000, 2000, 5000, 10000, 50000, 100000, 150000]
    print("基準値段 | 制限値幅 | 上限価格 | 100株最大")
    print("-" * 50)
    for p in test_prices:
        limit = calc_price_limit(p)
        upper = calc_upper_limit_price(p)
        max_cost = calc_max_cost_100(p)
        print(f"{p:>8,} | {limit:>8,} | {upper:>8,.0f} | {max_cost:>10,}")
