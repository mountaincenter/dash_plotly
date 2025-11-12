"""
Grok推奨銘柄に対する売買判断レポート生成（改良版）

過去の分析結果を基に、複合的な判断基準で売買判定を行う：
1. Grokランク別勝率
2. 前日終値変化率（プラス/マイナス）
3. ボラティリティ（ATR）
4. カテゴリー分析結果
5. 複合条件パターン
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf
import json

# パス設定
BASE_DIR = Path(__file__).parent.parent
LATEST_GROK_PATH = BASE_DIR / 'data' / 'parquet' / 'grok_trending.parquet'
BACKTEST_DATA_PATH = BASE_DIR / 'test_output' / 'grok_analysis_base_latest.parquet'
OUTPUT_HTML_PATH = BASE_DIR / 'test_output' / 'trading_recommendation.html'
# 新パイプライン: S3同期対象
OUTPUT_JSON_PATH = BASE_DIR / 'data' / 'parquet' / 'backtest' / 'trading_recommendation.json'

# 動的スコアリングのための閾値
SCORING_THRESHOLDS = {
    'excellent': {'min_win_rate': 0.70, 'score': 50},  # 70%以上
    'good': {'min_win_rate': 0.60, 'score': 30},       # 60-70%
    'neutral': {'min_win_rate': 0.40, 'score': 10},    # 40-60%
    'poor': {'min_win_rate': 0.25, 'score': -10},      # 25-40%
    'bad': {'min_win_rate': 0.10, 'score': -30},       # 10-25%
    'terrible': {'min_win_rate': 0.0, 'score': -50}    # 10%未満
}


def fetch_previous_day_data(ticker):
    """yfinanceで前日データを取得"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period='5d')

        if len(hist) < 2:
            return None, None, None, None

        # 前日と前々日のデータ
        prev_close = hist['Close'].iloc[-2]
        prev_prev_close = hist['Close'].iloc[-3] if len(hist) >= 3 else prev_close
        prev_volume = hist['Volume'].iloc[-2]

        # 変化率
        change_pct = ((prev_close - prev_prev_close) / prev_prev_close * 100) if prev_prev_close > 0 else 0

        # ATR計算（簡易版: 直近5日の高値-安値の平均）
        if len(hist) >= 5:
            atr = (hist['High'].iloc[-5:] - hist['Low'].iloc[-5:]).mean()
            atr_pct = (atr / prev_close * 100) if prev_close > 0 else 0
        else:
            atr_pct = None

        return change_pct, atr_pct, prev_volume, prev_close

    except Exception as e:
        print(f"Warning: {ticker} のデータ取得失敗: {e}")
        return None, None, None, None


def calculate_score_from_win_rate(win_rate):
    """勝率からスコアを計算（動的スコアリング）"""
    for level, config in sorted(SCORING_THRESHOLDS.items(),
                                 key=lambda x: x[1]['min_win_rate'],
                                 reverse=True):
        if win_rate >= config['min_win_rate']:
            return config['score']
    return -50  # デフォルト（最低スコア）


def load_backtest_stats():
    """バックテストデータから統計情報を読み込み（動的計算）"""
    try:
        df = pd.read_parquet(BACKTEST_DATA_PATH)

        # ランク別統計（Phase2基準）
        rank_stats = df.groupby('grok_rank').agg({
            'phase2_win': ['sum', 'count', 'mean'],
            'phase2_return': 'mean'
        }).round(3)

        # ランク別の勝率とスコア
        rank_win_rates = {}
        rank_scores = {}
        rank_avg_returns = {}

        for rank in rank_stats.index:
            win_rate = rank_stats.loc[rank, ('phase2_win', 'mean')]
            avg_return = rank_stats.loc[rank, ('phase2_return', 'mean')]
            count = rank_stats.loc[rank, ('phase2_win', 'count')]

            rank_win_rates[rank] = win_rate * 100  # パーセント表示
            rank_avg_returns[rank] = avg_return * 100  # パーセント表示

            # 勝率ベースのスコア計算
            base_score = calculate_score_from_win_rate(win_rate)

            # 平均リターンでスコアを微調整（±10点）
            if avg_return > 0.03:  # 3%以上
                adjusted_score = base_score + 10
            elif avg_return < -0.02:  # -2%以下
                adjusted_score = base_score - 10
            else:
                adjusted_score = base_score

            # データ数が少ない場合はスコアを抑制（信頼性低下）
            if count < 5:
                adjusted_score = int(adjusted_score * 0.7)

            rank_scores[rank] = adjusted_score

        # カテゴリー別勝率
        cat_stats = df.groupby('category').agg({
            'phase2_win': lambda x: x.sum() / len(x) * 100,
            'phase2_return': 'mean'
        }).round(1)

        print(f"\n=== バックテスト統計（動的計算） ===")
        print(f"総データ数: {len(df)}件")
        print(f"\nランク別勝率とスコア:")
        for rank in sorted(rank_win_rates.keys()):
            print(f"  ランク{rank}: 勝率{rank_win_rates[rank]:.1f}%, "
                  f"平均リターン{rank_avg_returns[rank]:+.2f}%, "
                  f"スコア{rank_scores[rank]:+d}")

        return {
            'rank_win_rates': rank_win_rates,
            'rank_scores': rank_scores,
            'rank_avg_returns': rank_avg_returns,
            'category_win_rates': cat_stats['phase2_win'].to_dict()
        }
    except Exception as e:
        print(f"Warning: バックテストデータ読み込み失敗: {e}")
        import traceback
        traceback.print_exc()
        return {
            'rank_win_rates': {},
            'rank_scores': {},
            'rank_avg_returns': {},
            'category_win_rates': {}
        }


def determine_action_comprehensive(row, prev_change, atr_pct, backtest_stats):
    """複合的な判断基準で売買を決定（動的スコアリング版）"""

    ticker = row['ticker']
    grok_rank = row['grok_rank']

    # バックテスト統計から動的にスコアを取得
    rank_win_rate = backtest_stats['rank_win_rates'].get(grok_rank, 50.0)
    rank_score = backtest_stats['rank_scores'].get(grok_rank, -10)
    rank_avg_return = backtest_stats['rank_avg_returns'].get(grok_rank, 0.0)
    category_win_rates = backtest_stats['category_win_rates']

    # 深掘り分析の特記事項
    deep_analysis_notes = {
        # === 11/10 銘柄 ===
        '8746.T': {
            'note': '【本命】営業利益+805%、訴訟和解12億円は一過性、11/14に2Q決算',
            'fundamentals': {
                'operatingProfitGrowth': 805.1,
                'eps': -127.56,
                'epsNote': '訴訟和解12億円（一過性）',
                'nextEarningsDate': '2025-11-14'
            },
            'specialNotes': ['本業絶好調', '一過性損失を除けば優良']
        },
        '5189.T': {
            'note': '【除外】中間期赤字、出来高極小',
            'riskFactors': ['中間期赤字', '出来高極小', '流動性リスク'],
            'fundamentals': {'eps': -4.88, 'epsNote': '中間期赤字'}
        },
        '7937.T': {
            'note': '【除外】出来高8,270株',
            'riskFactors': ['出来高極小（8,270株）', '流動性リスク']
        },
        '3077.T': {
            'note': '【仕手株】11/7に-10.6%急落',
            'riskFactors': ['仕手株パターン', '急騰急落リスク'],
            'specialNotes': ['ストップ高2日連続後に急落']
        },
        '4598.T': {
            'note': '【ハイリスク】ストップ高頻発',
            'riskFactors': ['ストップ高頻発', '極端なボラティリティ']
        },
        '2334.T': {
            'note': '【急騰+55%】過熱感',
            'riskFactors': ['過熱感（+55%急騰）', '反落リスク']
        },
        '6927.T': {
            'note': '【急騰+8.9%】出来高161万株',
            'specialNotes': ['出来高急増（161万株）', '急騰+8.9%']
        },
        '3744.T': {
            'note': '【急騰+13.7%】黒字転換',
            'fundamentals': {'epsNote': '黒字転換'},
            'specialNotes': ['黒字転換', '急騰+13.7%']
        },
        # === 11/11 銘柄 ===
        '3895.T': {
            'note': '【要注意】今期経常を27％下方修正',
            'riskFactors': ['経常利益27％下方修正'],
            'fundamentals': {'epsNote': '中間経常177百万円'}
        },
        '9302.T': {
            'note': '【好材料】今期経常を3％上方修正',
            'specialNotes': ['業績上方修正'],
            'fundamentals': {'epsNote': '今期経常3％上方修正'}
        },
        '3103.T': {
            'note': '【決算日】11/11に決算発表予定',
            'riskFactors': ['決算発表直前（11/11）'],
            'specialNotes': ['社名変更: 日東紡績→ユニチカ']
        },
        '7014.T': {
            'note': '【減益】上期経常は22％減益',
            'riskFactors': ['上期経常22％減益'],
            'fundamentals': {'epsNote': '中間経常11,377百万円（22％減益）'}
        },
    }

    deep_analysis = deep_analysis_notes.get(ticker, {})
    special_note = deep_analysis.get('note', '')

    # カテゴリー情報
    categories = row.get('categories', [])
    if isinstance(categories, str):
        categories = eval(categories) if categories.startswith('[') else [categories]

    # 前日動向
    prev_direction = None
    if prev_change is not None:
        prev_direction = 'プラス' if prev_change >= 0 else 'マイナス'

    # ボラティリティレベル
    vol_level = None
    atr_level = None
    if atr_pct is not None:
        if atr_pct < 3.0:
            vol_level = '低ボラ'
            atr_level = 'low'
        elif atr_pct < 6.0:
            vol_level = '中ボラ'
            atr_level = 'medium'
        else:
            vol_level = '高ボラ'
            atr_level = 'high'

    # 判定
    action = '静観'
    reasons = []
    reasons_structured = []  # 構造化された理由（JSON用）
    confidence = '中'
    score = 0  # スコアリング（-100 ~ +100）

    # === ルール1: Grokランク基本スコア（動的計算） ===
    score += rank_score
    reason_text = f'Grokランク{grok_rank}は勝率{rank_win_rate:.1f}%'
    if rank_avg_return != 0:
        reason_text += f'（平均{rank_avg_return:+.2f}%）'
    reasons.append(reason_text)
    reasons_structured.append({
        'type': 'grok_rank',
        'description': reason_text,
        'impact': rank_score
    })

    # === ルール2: 前日動向との複合パターン ===
    if prev_direction == 'プラス' and grok_rank in [1, 2]:
        score -= 30
        reason_text = 'ランク1,2 × 前日プラス = 勝率0%パターン'
        reasons.append(reason_text)
        reasons_structured.append({
            'type': 'prev_day_change',
            'description': reason_text,
            'impact': -30
        })
        confidence = '高'

    if prev_direction == 'マイナス':
        score += 20
        reason_text = '前日マイナス（リバウンド効果）'
        reasons.append(reason_text)
        reasons_structured.append({
            'type': 'prev_day_change',
            'description': reason_text,
            'impact': 20
        })

    # === ルール3: ボラティリティ ===
    if vol_level == '低ボラ':
        score += 10
        reason_text = '低ボラ（安定）'
        reasons.append(reason_text)
        reasons_structured.append({
            'type': 'volatility',
            'description': reason_text,
            'impact': 10
        })
    elif vol_level == '高ボラ':
        score -= 10
        reason_text = '高ボラ（リスク大）'
        reasons.append(reason_text)
        reasons_structured.append({
            'type': 'volatility',
            'description': reason_text,
            'impact': -10
        })

    # === ルール4: カテゴリー勝率 ===
    for cat in categories:
        cat_name = cat.replace('[', '').replace(']', '').replace("'", "")
        if cat_name in category_win_rates:
            cat_wr = category_win_rates[cat_name]
            if cat_wr >= 50:
                score += 15
                reason_text = f'カテゴリー「{cat_name}」勝率{cat_wr:.0f}%'
                reasons.append(reason_text)
                reasons_structured.append({
                    'type': 'category',
                    'description': reason_text,
                    'impact': 15
                })
            elif cat_wr <= 25:
                score -= 15
                reason_text = f'カテゴリー「{cat_name}」勝率{cat_wr:.0f}%（低）'
                reasons.append(reason_text)
                reasons_structured.append({
                    'type': 'category',
                    'description': reason_text,
                    'impact': -15
                })

    # === スコアから行動決定 ===
    if score >= 30:
        action = '買い'
        if score >= 50:
            confidence = '高'
    elif score <= -30:
        action = '売り'
        if score <= -50:
            confidence = '高'
    else:
        action = '静観'

    # 推奨損切りライン（買いと売りで異なる設定）
    stop_loss_calculation = None
    if action == '売り':
        # 信用売り: ATRの120%、最小5%、最大10%（上昇リスク対策）
        if atr_pct:
            stop_loss = max(5.0, min(atr_pct * 1.2, 10.0))
            stop_loss_calculation = 'ATR × 1.2'
        else:
            stop_loss = 7.0  # デフォルト
            stop_loss_calculation = 'デフォルト'
    else:
        # 買い: ATRの80%、最小2%、最大5%
        if atr_pct:
            stop_loss = max(2.0, min(atr_pct * 0.8, 5.0))
            stop_loss_calculation = 'ATR × 0.8'
        else:
            stop_loss = 3.0
            stop_loss_calculation = 'デフォルト'

    # === ルール5: 深掘り分析による個別調整 ===
    if ticker == '8746.T':
        # 本業絶好調（営業利益+805%）、一過性損失
        score += 50
        confidence = '高'
    elif ticker in ['5189.T', '7937.T']:
        # 出来高極小（流動性リスク）
        score -= 30
        confidence = '低'
    elif ticker == '3077.T':
        # 仕手株パターン（急騰リスク大）
        score -= 20
        if action == '売り' and atr_pct:
            # 仕手株は損切りを最大10%に設定
            stop_loss = 10.0
            stop_loss_calculation = '仕手株対策（最大値）'
    elif ticker in ['2334.T', '6927.T', '3744.T']:
        # 急騰株（過熱感）
        score += 10  # やや買い優勢だが要注意

    # 深掘り分析の特記事項を追加
    if special_note:
        reasons.append(special_note)
        reasons_structured.append({
            'type': 'deep_analysis',
            'description': special_note,
            'impact': 0  # 深掘り分析は既にスコアに反映済み
        })

    return {
        'action': action,
        'reasons_text': ' / '.join(reasons),
        'reasons_structured': reasons_structured,
        'confidence': confidence,
        'score': score,
        'stop_loss': stop_loss,
        'stop_loss_calculation': stop_loss_calculation,
        'vol_level': vol_level,
        'atr_level': atr_level,
        'deep_analysis': deep_analysis if deep_analysis else None
    }


def generate_recommendation_report():
    """売買判断レポート生成"""

    # バックテストデータから統計読み込み（動的計算）
    print("バックテストデータから統計情報を読み込み中...")
    backtest_stats = load_backtest_stats()

    # バックテスト期間の取得
    try:
        backtest_df = pd.read_parquet(BACKTEST_DATA_PATH)
        backtest_period = {
            'start': backtest_df['backtest_date'].min().strftime('%Y-%m-%d'),
            'end': backtest_df['backtest_date'].max().strftime('%Y-%m-%d')
        }
        backtest_count = len(backtest_df)
    except:
        backtest_period = {'start': '2025-11-04', 'end': '2025-11-07'}
        backtest_count = 46

    # 最新Grokデータ読み込み
    print("最新Grok推奨銘柄を読み込み中...")
    df = pd.read_parquet(LATEST_GROK_PATH)

    # 前日データ取得
    print("各銘柄の前日データを取得中...")
    results = []
    json_stocks = []

    for _, row in df.iterrows():
        ticker = row['ticker']
        print(f"  {ticker} データ取得中...")

        prev_change, atr_pct, prev_volume, prev_close = fetch_previous_day_data(ticker)
        result = determine_action_comprehensive(
            row, prev_change, atr_pct, backtest_stats
        )

        # カテゴリー情報の取得
        categories = row.get('categories', [])
        if isinstance(categories, str):
            categories = eval(categories) if categories.startswith('[') else [categories]
        elif isinstance(categories, (list, np.ndarray)):
            categories = list(categories)  # numpy配列の場合はlistに変換
        else:
            categories = []

        # HTML用のデータ
        results.append({
            'ticker': ticker,
            'stock_name': row['stock_name'],
            'grok_rank': row['grok_rank'],
            'prev_change_pct': prev_change if prev_change is not None else 'N/A',
            'atr_pct': atr_pct if atr_pct is not None else 'N/A',
            'action': result['action'],
            'confidence': result['confidence'],
            'score': result['score'],
            'stop_loss': f"{result['stop_loss']:.1f}%",
            'reason': result['reasons_text'],
        })

        # JSON用の構造化データ
        action_map = {'買い': 'buy', '売り': 'sell', '静観': 'hold'}
        confidence_map = {'高': 'high', '中': 'medium', '低': 'low'}

        json_stock = {
            'ticker': ticker,
            'stockName': row['stock_name'],
            'grokRank': int(row['grok_rank']),
            'technicalData': {
                'prevClose': float(prev_close) if prev_close is not None else None,
                'prevDayChangePct': float(prev_change) if prev_change is not None else None,
                'atr': {
                    'value': float(atr_pct) if atr_pct is not None else None,
                    'level': result['atr_level'] if result['atr_level'] else 'medium'
                },
                'volume': int(prev_volume) if prev_volume is not None else None,
                'volatilityLevel': result['vol_level'] if result['vol_level'] else '中ボラ'
            },
            'recommendation': {
                'action': action_map[result['action']],
                'score': int(result['score']),
                'confidence': confidence_map[result['confidence']],
                'stopLoss': {
                    'percent': round(result['stop_loss'], 1),
                    'calculation': result['stop_loss_calculation'] or 'デフォルト'
                },
                'reasons': result['reasons_structured']
            },
            'categories': categories
        }

        # 深掘り分析データの追加
        if result['deep_analysis']:
            json_stock['deepAnalysis'] = {}
            if 'fundamentals' in result['deep_analysis']:
                json_stock['deepAnalysis']['fundamentals'] = result['deep_analysis']['fundamentals']
            if 'riskFactors' in result['deep_analysis']:
                json_stock['deepAnalysis']['riskFactors'] = result['deep_analysis']['riskFactors']
            if 'specialNotes' in result['deep_analysis']:
                json_stock['deepAnalysis']['specialNotes'] = result['deep_analysis']['specialNotes']

        json_stocks.append(json_stock)

    result_df = pd.DataFrame(results)

    # 買い・売り・静観で分類
    buy_stocks = result_df[result_df['action'] == '買い'].sort_values('score', ascending=False)
    sell_stocks = result_df[result_df['action'] == '売り'].sort_values('score')
    hold_stocks = result_df[result_df['action'] == '静観']

    # HTML生成
    html_content = f"""
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Grok推奨銘柄 総合売買判断レポート</title>
        <style>
            body {{
                font-family: 'Hiragino Sans', 'Hiragino Kaku Gothic ProN', 'YuGothic', sans-serif;
                max-width: 1600px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }}
            h1 {{
                color: #333;
                border-bottom: 3px solid #2196F3;
                padding-bottom: 10px;
            }}
            h2 {{
                color: #555;
                margin-top: 40px;
                border-left: 5px solid #FF9800;
                padding-left: 10px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                background-color: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
                font-size: 13px;
            }}
            td.num {{
                text-align: right;
            }}
            th {{
                background-color: #2196F3;
                color: white;
                font-weight: bold;
            }}
            tr:hover {{
                background-color: #f5f5f5;
            }}
            .buy {{
                background-color: #E8F5E9;
            }}
            .sell {{
                background-color: #FFEBEE;
            }}
            .hold {{
                background-color: #FFF3E0;
            }}
            .info-box {{
                background-color: #E3F2FD;
                border-left: 4px solid #2196F3;
                padding: 15px;
                margin: 20px 0;
            }}
            .warning-box {{
                background-color: #FFF3E0;
                border-left: 4px solid #FF9800;
                padding: 15px;
                margin: 20px 0;
            }}
            .summary {{
                display: flex;
                justify-content: space-around;
                margin: 30px 0;
            }}
            .summary-item {{
                text-align: center;
                padding: 20px;
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                flex: 1;
                margin: 0 10px;
            }}
            .summary-item h3 {{
                margin: 0;
                font-size: 24px;
            }}
            .summary-item p {{
                margin: 5px 0 0 0;
                color: #777;
            }}
        </style>
    </head>
    <body>
        <h1>📊 Grok推奨銘柄 総合売買判断レポート</h1>

        <div class="info-box">
            <strong>生成日時:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
            <strong>対象銘柄数:</strong> {len(df)}銘柄<br>
            <strong>分析基準:</strong> 過去{backtest_count}件のバックテスト（{backtest_period['start']}～{backtest_period['end']}） + 前日テクニカルデータ + カテゴリー分析
        </div>

        <div class="summary">
            <div class="summary-item" style="border-top: 4px solid #4CAF50;">
                <h3 style="color: #4CAF50;">{len(buy_stocks)}</h3>
                <p>買い候補</p>
            </div>
            <div class="summary-item" style="border-top: 4px solid #F44336;">
                <h3 style="color: #F44336;">{len(sell_stocks)}</h3>
                <p>売り候補</p>
            </div>
            <div class="summary-item" style="border-top: 4px solid #FF9800;">
                <h3 style="color: #FF9800;">{len(hold_stocks)}</h3>
                <p>静観</p>
            </div>
        </div>

        <div class="warning-box">
            <strong>⚠️ 重要な注意事項</strong><br>
            - 現状のバックテストデータは{backtest_count}件、統計的信頼性は限定的<br>
            - 必ず推奨損切りラインを設定すること<br>
            - スコアは複合判断の参考値（高いほど買い推奨、低いほど売り推奨）<br>
            - より多くのデータ（100件以上）で再検証が必要
        </div>

        <h2>🟢 買い候補（{len(buy_stocks)}銘柄）</h2>
        <p>複合判断でスコアがプラスのパターン。優先的に検討してください。</p>

        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th>ランク</th>
                    <th>前日変化率</th>
                    <th>ATR(%)</th>
                    <th>スコア</th>
                    <th>信頼度</th>
                    <th>推奨損切り</th>
                    <th>判断理由</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr class="buy">
                    <td>{row['ticker']}</td>
                    <td>{row['stock_name']}</td>
                    <td class="num">{row['grok_rank']}</td>
                    <td class="num">{row['prev_change_pct'] if row['prev_change_pct'] != 'N/A' else 'N/A'}</td>
                    <td class="num">{row['atr_pct'] if row['atr_pct'] != 'N/A' else 'N/A'}</td>
                    <td class="num" style="font-weight: bold; color: green;">+{row['score']}</td>
                    <td>{row['confidence']}</td>
                    <td class="num">{row['stop_loss']}</td>
                    <td style="font-size: 12px;">{row['reason']}</td>
                </tr>
                ''' for _, row in buy_stocks.iterrows()]) if len(buy_stocks) > 0 else '<tr><td colspan="9">該当なし</td></tr>'}
            </tbody>
        </table>

        <h2>🔴 売り候補（{len(sell_stocks)}銘柄）</h2>
        <p>複合判断でスコアがマイナスのパターン。信用売り、または見送りを推奨。</p>

        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th>ランク</th>
                    <th>前日変化率</th>
                    <th>ATR(%)</th>
                    <th>スコア</th>
                    <th>信頼度</th>
                    <th>推奨損切り</th>
                    <th>判断理由</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr class="sell">
                    <td>{row['ticker']}</td>
                    <td>{row['stock_name']}</td>
                    <td class="num">{row['grok_rank']}</td>
                    <td class="num">{row['prev_change_pct'] if row['prev_change_pct'] != 'N/A' else 'N/A'}</td>
                    <td class="num">{row['atr_pct'] if row['atr_pct'] != 'N/A' else 'N/A'}</td>
                    <td class="num" style="font-weight: bold; color: red;">{row['score']}</td>
                    <td>{row['confidence']}</td>
                    <td class="num">{row['stop_loss']}</td>
                    <td style="font-size: 12px;">{row['reason']}</td>
                </tr>
                ''' for _, row in sell_stocks.iterrows()]) if len(sell_stocks) > 0 else '<tr><td colspan="9">該当なし</td></tr>'}
            </tbody>
        </table>

        <h2>⚪ 静観（{len(hold_stocks)}銘柄）</h2>
        <p>複合判断で中立的なスコア。様子見を推奨。</p>

        <table>
            <thead>
                <tr>
                    <th>ティッカー</th>
                    <th>銘柄名</th>
                    <th>ランク</th>
                    <th>前日変化率</th>
                    <th>ATR(%)</th>
                    <th>スコア</th>
                    <th>信頼度</th>
                    <th>推奨損切り</th>
                    <th>判断理由</th>
                </tr>
            </thead>
            <tbody>
                {''.join([f'''
                <tr class="hold">
                    <td>{row['ticker']}</td>
                    <td>{row['stock_name']}</td>
                    <td class="num">{row['grok_rank']}</td>
                    <td class="num">{row['prev_change_pct'] if row['prev_change_pct'] != 'N/A' else 'N/A'}</td>
                    <td class="num">{row['atr_pct'] if row['atr_pct'] != 'N/A' else 'N/A'}</td>
                    <td class="num">{row['score']}</td>
                    <td>{row['confidence']}</td>
                    <td class="num">{row['stop_loss']}</td>
                    <td style="font-size: 12px;">{row['reason']}</td>
                </tr>
                ''' for _, row in hold_stocks.iterrows()]) if len(hold_stocks) > 0 else '<tr><td colspan="9">該当なし</td></tr>'}
            </tbody>
        </table>

        <h2>📋 判断基準（複合スコアリング）</h2>

        <div class="info-box">
            <h3>スコア計算ルール（バックテスト結果から動的計算）</h3>
            <ul>
                <li><strong>Grokランク:</strong>
                    {'、'.join([f"ランク{rank}={backtest_stats['rank_scores'].get(rank, 0):+d}点（勝率{backtest_stats['rank_win_rates'].get(rank, 0):.1f}%）"
                               for rank in sorted(backtest_stats['rank_win_rates'].keys())])}
                </li>
                <li><strong>前日動向:</strong> ランク1,2 × 前日プラス = -30点、前日マイナス = +20点</li>
                <li><strong>ボラティリティ:</strong> 低ボラ = +10点、高ボラ = -10点</li>
                <li><strong>カテゴリー:</strong> 勝率50%以上 = +15点、25%以下 = -15点</li>
            </ul>

            <h3>行動判定</h3>
            <ul>
                <li><strong>スコア +30以上:</strong> 買い候補</li>
                <li><strong>スコア -30以下:</strong> 売り候補</li>
                <li><strong>スコア -29 ~ +29:</strong> 静観</li>
            </ul>

            <h3>推奨損切りライン</h3>
            <ul>
                <li>ATRの80%、最小2%、最大5%で設定</li>
                <li>ボラティリティに応じて柔軟に調整</li>
            </ul>
        </div>

        <footer style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ddd; text-align: center; color: #777;">
            <p>生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p style="color: #F44336; font-weight: bold;">投資は自己責任で行ってください。このレポートは投資助言ではありません。</p>
        </footer>
    </body>
    </html>
    """

    # HTML保存
    OUTPUT_HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_HTML_PATH, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"\n売買判断レポート（HTML）を生成しました: {OUTPUT_HTML_PATH}")
    print(f"ファイルサイズ: {OUTPUT_HTML_PATH.stat().st_size / 1024:.1f} KB")

    # JSON生成
    json_data = {
        'version': '1.0',
        'generatedAt': datetime.now().isoformat(),
        'dataSource': {
            'backtestCount': backtest_count,
            'backtestPeriod': backtest_period,
            'technicalDataDate': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        },
        'summary': {
            'total': len(df),
            'buy': len([s for s in json_stocks if s['recommendation']['action'] == 'buy']),
            'sell': len([s for s in json_stocks if s['recommendation']['action'] == 'sell']),
            'hold': len([s for s in json_stocks if s['recommendation']['action'] == 'hold'])
        },
        'warnings': [
            f'現状のバックテストデータは{backtest_count}件、統計的信頼性は限定的',
            '必ず推奨損切りラインを設定すること',
            'より多くのデータ（100件以上）で再検証が必要'
        ],
        'stocks': json_stocks,
        'scoringRules': {
            'grokRank': {
                rank: {
                    'score': backtest_stats['rank_scores'].get(rank, -10),
                    'winRate': backtest_stats['rank_win_rates'].get(rank, 50.0),
                    'avgReturn': backtest_stats['rank_avg_returns'].get(rank, 0.0)
                }
                for rank in sorted(backtest_stats['rank_win_rates'].keys())
            },
            'prevDayChange': {
                'negative': {
                    'score': 20,
                    'reason': 'リバウンド効果'
                },
                'positiveWithLowRank': {
                    'score': -30,
                    'reason': '勝率0%パターン',
                    'condition': 'ランク1,2 × 前日プラス'
                }
            },
            'volatility': {
                'low': {
                    'score': 10,
                    'threshold': 3.0
                },
                'high': {
                    'score': -10,
                    'threshold': 6.0
                }
            },
            'actionThresholds': {
                'buy': 30,
                'sell': -30
            },
            'stopLoss': {
                'buy': {
                    'formula': 'ATR × 0.8',
                    'min': 2.0,
                    'max': 5.0
                },
                'sell': {
                    'formula': 'ATR × 1.2',
                    'min': 5.0,
                    'max': 10.0
                }
            }
        }
    }

    # JSON保存
    with open(OUTPUT_JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    print(f"売買判断レポート（JSON）を生成しました: {OUTPUT_JSON_PATH}")
    print(f"ファイルサイズ: {OUTPUT_JSON_PATH.stat().st_size / 1024:.1f} KB")

    print(f"\n=== サマリー ===")
    print(f"買い候補: {len(buy_stocks)}銘柄")
    print(f"売り候補: {len(sell_stocks)}銘柄")
    print(f"静観: {len(hold_stocks)}銘柄")

    if len(buy_stocks) > 0:
        print(f"\n【買い候補】")
        for _, row in buy_stocks.iterrows():
            print(f"  - {row['ticker']} {row['stock_name']} (スコア: +{row['score']}, 損切り: {row['stop_loss']})")

    if len(sell_stocks) > 0:
        print(f"\n【売り候補】")
        for _, row in sell_stocks.iterrows():
            print(f"  - {row['ticker']} {row['stock_name']} (スコア: {row['score']}, 損切り: {row['stop_loss']})")


if __name__ == '__main__':
    generate_recommendation_report()
