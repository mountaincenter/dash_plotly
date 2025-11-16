#!/usr/bin/env python3
"""
grok_trending.parquet から trending_recommendation.json を生成
"""

import pandas as pd
import json
from datetime import datetime
from pathlib import Path

# パス設定
BASE_DIR = Path(__file__).parent.parent
PARQUET_PATH = BASE_DIR / "data/parquet/grok_trending.parquet"
META_PATH = BASE_DIR / "data/parquet/meta_jquants.parquet"
PRICES_PATH = BASE_DIR / "data/parquet/prices_max_1d.parquet"
OUTPUT_JSON_DIR = BASE_DIR / "data/parquet/backtest"
OUTPUT_JSON_DIR.mkdir(parents=True, exist_ok=True)

def load_trending_data() -> pd.DataFrame:
    """grok_trending.parquet を読み込み、meta_jquants.parquetからmarket/sectorsをマージ"""
    df = pd.read_parquet(PARQUET_PATH)
    print(f"[INFO] Loaded {len(df)} records from grok_trending.parquet")

    # meta_jquants.parquetからmarket/sectors情報を取得
    meta_df = pd.read_parquet(META_PATH)
    meta_df = meta_df[['ticker', 'market', 'sectors', 'series', 'topixnewindexseries']]

    # market/sectors列を削除してからマージ
    df = df.drop(columns=['market', 'sectors', 'series'], errors='ignore')
    df = df.merge(meta_df, on='ticker', how='left')
    print(f"[INFO] Merged market/sectors data from meta_jquants.parquet")

    return df

def safe_array_to_list(val):
    """numpy配列を安全にリストに変換（空の場合はNone）"""
    if hasattr(val, 'tolist'):
        lst = val.tolist()
        return lst if len(lst) > 0 else None
    elif isinstance(val, (list, tuple)):
        return list(val) if len(val) > 0 else None
    elif pd.isna(val):
        return None
    else:
        return val

def get_technical_data(ticker: str, target_date: str) -> dict:
    """株価データとテクニカル指標を取得"""
    try:
        # 株価データ読み込み
        prices_df = pd.read_parquet(PRICES_PATH)
        ticker_df = prices_df[prices_df['ticker'] == ticker].copy()

        if ticker_df.empty:
            return None

        # 日付でソート
        ticker_df['date'] = pd.to_datetime(ticker_df['date'])
        ticker_df = ticker_df.sort_values('date', ascending=False)

        # 対象日またはそれ以前の最新データを取得
        target_dt = pd.to_datetime(target_date)
        ticker_df = ticker_df[ticker_df['date'] <= target_dt]

        if ticker_df.empty:
            return None

        latest = ticker_df.iloc[0]

        # changePct計算（前日比）
        change_pct = None
        if len(ticker_df) > 1:
            prev_close = ticker_df.iloc[1]['Close']
            if prev_close and prev_close > 0:
                change_pct = ((latest['Close'] - prev_close) / prev_close) * 100

        # ATR計算（14日平均）
        atr14_pct = None
        if len(ticker_df) >= 14:
            recent_14 = ticker_df.iloc[:14]
            atr = (recent_14['High'] - recent_14['Low']).mean()
            if latest['Close'] and latest['Close'] > 0:
                atr14_pct = (atr / latest['Close']) * 100

        # RSI計算（14日）
        rsi14 = None
        if len(ticker_df) >= 15:
            recent_15 = ticker_df.iloc[:15].sort_values('date')
            closes = recent_15['Close'].values
            deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
            gains = [d if d > 0 else 0 for d in deltas]
            losses = [-d if d < 0 else 0 for d in deltas]
            avg_gain = sum(gains) / 14
            avg_loss = sum(losses) / 14
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi14 = 100 - (100 / (1 + rs))

        # 出来高比率計算（20日平均比）
        vol_ratio = None
        if len(ticker_df) >= 20:
            avg_vol_20 = ticker_df.iloc[:20]['Volume'].mean()
            if avg_vol_20 and avg_vol_20 > 0:
                vol_ratio = latest['Volume'] / avg_vol_20

        return {
            'close': float(latest['Close']) if pd.notna(latest['Close']) else None,
            'changePct': float(change_pct) if change_pct is not None else None,
            'volume': int(latest['Volume']) if pd.notna(latest['Volume']) else None,
            'volRatio': float(vol_ratio) if vol_ratio is not None else None,
            'atr14Pct': float(atr14_pct) if atr14_pct is not None else None,
            'rsi14': float(rsi14) if rsi14 is not None else None,
        }
    except Exception as e:
        print(f"[WARNING] Failed to get technical data for {ticker}: {e}")
        return None

def create_trending_json(df: pd.DataFrame) -> dict:
    """trending_recommendation.json の構造を生成"""

    # 最新の日付を取得
    latest_date = df['date'].max()
    df_latest = df[df['date'] == latest_date].copy()

    # stocks リストを生成
    stocks = []
    for _, row in df_latest.iterrows():
        # テクニカルデータ取得
        tech_data = get_technical_data(row['ticker'], latest_date)

        stock_data = {
            "ticker": row['ticker'],
            "code": row['code'],
            "stockName": row['stock_name'],
            "grokRank": int(row['grok_rank']),
            "selectionRank": int(row['selection_rank']),
            "marketInfo": {
                "market": row.get('market') if pd.notna(row.get('market')) else None,
                "sectors": row.get('sectors') if pd.notna(row.get('sectors')) else None,
                "series": row.get('series') if pd.notna(row.get('series')) else None,
                "topixNewIndexSeries": row.get('topixnewindexseries') if pd.notna(row.get('topixnewindexseries')) else None,
            },
            "trendingData": {
                "date": row['date'],
                "reason": row['reason'],
                "keySignal": row['key_signal'],
                "source": row['source'],
                "selectedTime": row['selected_time'],
                "updatedAt": row['updated_at'],
            },
            "socialMetrics": {
                "hasMention": bool(row['has_mention']),
                "mentionedBy": safe_array_to_list(row['mentioned_by']),
                "sentimentScore": float(row['sentiment_score']) if pd.notna(row['sentiment_score']) else None,
                "selectionScore": float(row['selection_score']) if pd.notna(row['selection_score']) else None,
            },
            "technicalData": {
                "close": tech_data['close'] if tech_data else None,
                "changePct": tech_data['changePct'] if tech_data else None,
                "volume": tech_data['volume'] if tech_data else None,
                "volRatio": tech_data['volRatio'] if tech_data else None,
                "atr14Pct": tech_data['atr14Pct'] if tech_data else None,
                "rsi14": tech_data['rsi14'] if tech_data else None,
                "score": row.get('score') if pd.notna(row.get('score')) else None,
            },
            "metadata": {
                "promptVersion": row['prompt_version'],
                "categories": safe_array_to_list(row['categories']),
                "tags": safe_array_to_list(row['tags']),
            }
        }
        stocks.append(stock_data)

    # 全体構造
    output = {
        "version": "1.0",
        "generatedAt": datetime.now().isoformat(),
        "dataSource": {
            "type": "grok_trending",
            "date": latest_date,
            "totalStocks": len(df_latest),
            "parquetFile": "grok_trending.parquet"
        },
        "summary": {
            "total": len(df_latest),
            "topRanked": len(df_latest[df_latest['grok_rank'] <= 5]),
            "withMention": int(df_latest['has_mention'].sum()),
            "avgSelectionScore": float(df_latest['selection_score'].mean()) if df_latest['selection_score'].notna().any() else None,
        },
        "rankedStocks": stocks
    }

    return output

def main():
    """メイン処理"""
    print("[INFO] Starting trending_recommendation.json generation...")

    # データ読み込み
    df = load_trending_data()

    # JSON生成
    trending_json = create_trending_json(df)

    # 出力ファイル名
    latest_date = df['date'].max()
    output_file = OUTPUT_JSON_DIR / "trading_recommendation.json"

    # JSON出力
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(trending_json, f, ensure_ascii=False, indent=2)

    print(f"[SUCCESS] Generated: {output_file}")
    print(f"[INFO] Total stocks: {trending_json['summary']['total']}")
    print(f"[INFO] Top ranked (<=5): {trending_json['summary']['topRanked']}")
    print(f"[INFO] With mention: {trending_json['summary']['withMention']}")

if __name__ == "__main__":
    main()
