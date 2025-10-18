#!/usr/bin/env python3
"""
J-Quantsスキャルピング結果の情報を取得するスクリプト
"""
import sys
import json
import pandas as pd

def main():
    try:
        entry_df = pd.read_parquet('scalping_entry.parquet')
        active_df = pd.read_parquet('scalping_active.parquet')

        entry_count = len(entry_df)
        active_count = len(active_df)

        # ティッカーリスト取得（最大10件まで表示）
        entry_tickers = entry_df['ticker'].tolist()[:10] if entry_count > 0 else []
        active_tickers = active_df['ticker'].tolist()[:10] if active_count > 0 else []

        # 追加情報（10件以上ある場合）
        entry_more = f' (+{entry_count - 10} more)' if entry_count > 10 else ''
        active_more = f' (+{active_count - 10} more)' if active_count > 10 else ''

        # ティッカー文字列生成
        if entry_count == 0:
            entry_str = 'なし（0件）'
        elif entry_tickers:
            entry_str = ', '.join(entry_tickers) + entry_more
        else:
            entry_str = 'なし'

        if active_count == 0:
            active_str = 'なし（0件）'
        elif active_tickers:
            active_str = ', '.join(active_tickers) + active_more
        else:
            active_str = 'なし'

        result = {
            'entry_count': entry_count,
            'active_count': active_count,
            'entry_tickers': entry_str,
            'active_tickers': active_str
        }
        print(json.dumps(result, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({
            'entry_count': 0,
            'active_count': 0,
            'entry_tickers': f'エラー: {str(e)}',
            'active_tickers': f'エラー: {str(e)}'
        }, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
