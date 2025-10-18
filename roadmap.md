# ロードマップ等

## 本アプリの目的

- このアプリでは TradingView,四季報,証券会社各種アプリを補足する役割を持つアプリ
- 銘柄の選定を機械的に抽出して個人の判断を排除し概ね 60%程度が妥当と思う銘柄をピックアップする
- 最終的には機械学習をもちいその抽出された株式の株価予測を行う
- これらを利用し株式投資で利益を得る
- 自身の利益確保が出来次第アプリ自体のマネタイズを図る

## データの範囲

- yfinance 及び jquants light プランの範囲のデータ
- jquants はデータ自身の公開が禁止されているので銘柄選定を扱う内部データに利用

## 対象となる銘柄及びその選定方法

- 静的銘柄(スイング・長期投資向け)
  - TOPIX,政策的に上昇が見込まれる銘柄(現状は高市銘柄と命名)
    - TOPIX:今フェーズでは CORE30 銘柄に絞って取り扱う
    - 高市銘柄:高市新総裁誕生に伴う同氏が掲げる政策に近しい銘柄をピックアップ
- 動的銘柄（デイ・スキャルピング向け）
  - 既存のテクニカル分析を用い前日の終値ベースで 買い、強い買いとなっている銘柄
  - 株価、時価総額及びボラティリィ等を鑑み 2 分類を作成
  - 現状は entry,active(名称は再検討)

## 銘柄算定手法等

- 静的銘柄
  - TOPIX_CORE30
    - 現状:./data/csv/topixwegith_j.csv から　 meta.parquet を策定
    - 今後:jquants を用いて一度だけ手動実行し./data/csv/topix_core30.csv を策定
  - 高市銘柄
    - 現状:./data/csv/takaichi_stock_final.csv meta.parquet を策定
    - 今後:現状維持、時事情報を把握し手動で ClaudeCode プロンプトにて takaichi_stock_latest.csv を策定し meta.parquet を修正
- 動的銘柄
  - スキャルピング Entry
    - 現状:大引け後 16 時及び 安全設計からトラフィックの少ないと思料される 26 時に cron 処理にてデータ更新し scalping_entry.parquet 更新
    - 今後:現状維持
  - スキャルピング Active
    - 現状:大引け後 16 時及び 安全設計からトラフィックの少ないと思料される 26 時に cron 処理にてデータ更新し scalping_active.parquet 更新
    - 今後:現状維持
- 実作業フロー

1. ClaudeCode にて プロンプトを入力し高市銘柄の選定を行い追加・削除を行う（頻度は最低月 1 で大きな時事情報がある都度）
2. jquants にて core30 銘柄を抽出し直接 meta.parquet へ保存または topix_core30.csv へ保存
3. ローカル python コマンドで jquants を用いスキャルピング銘柄を選定し parquet ファイルへ保存
4. 各ファイルのカラムが揃っていることを確認
5. yfinance を用いて全ての以下のデータを取得
   1. period:60d, inteval:5m
   2. period:60d, inteval:15m
   3. period:730d, interval:1h
   4. period:max, interval:1d
   5. period:max, interval:1mo
   6. period:max, interval:1h(データが 730d でエラーが出た銘柄のみ)
   7. ファイル名は prices*{period}*{interval}.parquetquet 6.データを S3 へアップロードし manifest.json で S3 が常に最新の状況する(5 と同時に行う) 7.ローカルでの挙動確認後 GitHubActions での cron 処理
      8.3 の銘柄選定以降を cron で処理 → 大引け後 16 時,更新されていない場合(manifest.json に 16 時やその日の手動更新)にトラフィックの少ない 26 時に実施
      9.S3 とローカルでデータ不整合を回避するためローカル python コマンドで S3 と./data/parquet データを同期
      注：.gitignore の通り parquet ファイルは github にあげない

## 現状の課題と改善点

- 後付けで実装しているので全体的な構造がスパゲッティとなっている
  - Core30 + 高市銘柄 + 動的と銘柄選定コードが入り組んでいる
  - 中途半端に実装してしまった predcition が残っている
  - yfinance + jquants にしてしまったためデータの取得先が統一されていない
  - cron 処理を後付けにしたため データの同期がしっかり定義さていない
- 技術的な知識がないのでまず全部を手動で扱ったあとに cron 処理を入れたいのですが最初に jquants でローカルでスキャルピング銘柄を選定した後 GitHubActions で更新されてしまうと非常に時間がかかり jquants の制限にかかるリスクがある
- ファイル構造をシンプルにしたいのですが静的銘柄は yfiance で完結するのに対し動的銘柄は jquants+yfinance なので jquants でエラーが生じた際に全部が更新できないリスクがあるので その障害耐性を有したい
- 基本的な考え方は DRY,SRP,MECE かつ可読性を担保した形で扱います
- 既存の ../stockfrontend との連携もあるので移行期には気を使って、移行後は旧部分は全て削除したいです
- 必要に応じて branch を作成して先祖返りできるようにする

  確認したい点

  1. meta.parquet の内容

  roadmap 31 行目: "topixwegith_j.csv から meta.parquet を策定"
  → meta.parquet には Core30 + 高市銘柄のみ
  を含める？（スキャルピング銘柄は別ファイル）

  → そもそもこの 3 ファイル構造及び名称がこれで良いか再検証しその上で - meta.parquet(静的銘柄全て:Core30 + 高市銘柄) - scalping_entry.parquet - scalping_active.parquet

  2. カラム構造

  roadmap 48 行目: "各ファイルのカラムが揃っていることを確認"
  → meta.parquet, scalping_entry.parquet, scalping_active.parquet
  は全て同じカラム構造（ticker, code, stock_name, market, sectors, series,
  topixnewindexseries, categories, tags）？

  → はい

  3. スキャルピング銘柄の価格データ

  roadmap 49 行目: "yfinance を用いて全ての以下のデータを取得"
  → 「全て」とは Core30 + 高市 + スキャルピング全銘柄？

  → はい

  4. 障害耐性の実装

  roadmap 69 行目: "jquants でエラーが生じた際に全部が更新できないリスク"
  → スキャルピング銘柄生成が失敗しても、既存の scalping\_\*.parquet を使って静的銘柄の
  価格更新は続行する？

  → 質問の意味がわかりません scalping\_\*.parquet を使って静的銘柄という表現がわかりません scalping の中に静的銘柄はありませんが

  確認・回答：

  1. meta.parquet の内容（行 82）

  「そもそもこの 3 ファイル構造及び名称がこれで良いか再検証」

  現在の構造:

  - meta.parquet: 静的銘柄（Core30 + 高市銘柄）
  - scalping_entry.parquet: 動的銘柄（エントリー向け）
  - scalping_active.parquet: 動的銘柄（アクティブ向け）

  評価: この 3 ファイル構造は妥当です。理由：

  - 静的/動的で更新頻度が異なる
  - フロントエンドで別々にフェッチできる（/stocks/all エンドポイント）
  - 障害耐性：J-Quants エラー時も meta.parquet は独立

  名称: 明確で良いと思います。代替案は特にありません。

  2. カラム構造（行 91）

  「はい」→ 了解しました。全 parquet ファイルで同じカラム構造。

  3. 価格データ（行 98）

  「はい」→ Core30 + 高市 + スキャルピング全銘柄を yfinance で取得。了解しました。

  4. 障害耐性（行 106）

  「質問の意味がわかりません」

  質問を修正します：
  J-Quants でスキャルピング銘柄生成が失敗した場合、どうすべきですか？

  - A. エラーで全処理を停止する
  - B. 警告を出して、既存の古い scalping\_\*.parquet
    を使い、yfinance 価格取得は続行する
  - C. スキャルピング銘柄なしで、meta.parquet のみ yfinance 価格取得する

  → 　 C の考え方ですが

  - 誤認の恐れがあるので一度 scalping\_\*.parquet ファイルをからにして meta.parquet だけ更新フロントエンドで選定データ更新処理ができませんでしたのようなエラーメッセージを表示
  - スキャルピング銘柄は分析結果を正しく表示するので決められた条件に合致しなければ 0 件という日もありますしエラーで表示できない場合もあると考えています

⏺ 実装計画

フェーズ 1: メタデータ生成の修正

1.1 scripts/generate_meta.py を作成

目的: CSV から静的銘柄の meta.parquet を生成

入力:

- data/raw/topixweight_j.csv または data/raw/topix_core30.csv
- data/raw/takaichi_stock_final.csv

出力:

- data/parquet/meta.parquet

処理:

1. CSV を読み込み
2. カラムを統一: ticker, code, stock_name, market, sectors, series,
   topixnewindexseries
3. categories を生成:

   - Core30: ["TOPIX_CORE30"]
   - 高市銘柄: ["高市銘柄"]

4. tags を生成:

   - Core30: []
   - 高市銘柄: 7 分類から抽出（例: ["半導体・先端技術"]）

5. parquet で保存

実行タイミング: 手動（CSV が更新された時のみ）

---

フェーズ 2: スキャルピング銘柄生成の修正

2.1 jquants/screener.py を修正

修正内容:

- generate_entry_list() と generate_active_list() の戻り値に追加:

  - categories カラム: ["SCALPING_ENTRY"] または ["SCALPING_ACTIVE"]
  - tags カラム: 既存の tags リスト（例: ["trend", "oversold"]）

    2.2 jquants/generate_scalping_final.py を修正

修正内容:

- meta.parquet と同じカラム構造を確保
- J-Quants の sectors, series, topixnewindexseries をマージ
- エラー時の処理:
  - 空の DataFrame を生成（カラム構造は維持）
  - scalping_entry.parquet と scalping_active.parquet を空で保存
  - エラーログを出力

実行タイミング: 手動（ローカル）、後で GitHub Actions に統合

---

フェーズ 3: 価格データ取得の修正

3.1 analyze/fetch_prices.py の load_universe() を修正

現状: スキャルピング銘柄をマージするコードが既にある（64-88 行目）

修正内容:

- そのまま維持（meta.parquet + scalping\_\*.parquet から銘柄リストを取得）
- 空の scalping\_\*.parquet でもエラーにならないよう処理

  3.2 scripts/gen_yfinance_parquet.py を修正

修正内容:

- \_generate_scalping_lists() 関数を削除（既に削除済み）
- スキャルピング銘柄の存在チェックを維持
- 空のスキャルピングファイルでも処理を続行

---

フェーズ 4: 既存データのクリーンアップ

4.1 現在の meta.parquet から旧スキャルピング銘柄を削除

方法:

# 一時スクリプト（scripts/cleanup_meta.py）

meta = pd.read_parquet('data/parquet/meta.parquet')

# tag1 が'SCALPING\_\*'の行を削除

meta_clean = meta[~meta['tag1'].isin(['SCALPING_ENTRY', 'SCALPING_ACTIVE'])]

# tag1/tag2/tag3 を categories+tags に変換

# 保存

実行タイミング: 一度だけ手動実行

---

実行順序

ローカルでの手動実行（初回）:

1. python scripts/cleanup_meta.py （旧データクリーンアップ）
2. python scripts/generate_meta.py （meta.parquet 生成）
3. python jquants/generate_scalping_final.py （スキャルピング銘柄生成、38 分）
4. python scripts/gen_yfinance_parquet.py （価格取得、S3 アップロード）

GitHub Actions での自動実行（毎日 16 時/2 時）:

1. python jquants/generate_scalping_final.py
2. python scripts/gen_yfinance_parquet.py

---

この実装計画で進めてよろしいですか？

meta*jquants.parquet は作成する
meta_jquants.parquet ファイルを S3 になければ作成あればそれを利用
次フェーズで Grok と連携したいわゆる株クラ銘柄リストにも利用
これを利用して scalping*\_.parquet を作成
scalping\_\_.parquet + meta.parquet → all_stocks.parquet

これを yfinance でデータ取得

これままでの ファイルを S3 で管理
all*stocks_parquet
meta.parqeut
scalping*{entry,active}.parqeut
prices*{period}*{interval}.parquet
tech_snapshot_1d.parqeut

manifest.json を作成して
常に S3 内をクリーンに保つ
この manifest.json 内に
update_flag_2025-10-18.txt この最新のものを入れて
update_flag が最新のものだけになるようにする

---

## 方針の評価（2025-10-18）

### ✅ 良い点

1. **`meta_jquants.parquet` のキャッシュ戦略**

   - S3 になければ作成、あれば再利用 → API 呼び出し削減、実行時間短縮
   - 銘柄マスターは頻繁に変わらないので合理的

2. **将来の拡張性（Grok 連携、株クラ銘柄）**

   - `meta_jquants.parquet` を再利用する設計で効率的

3. **all_stocks.parquet の導入**

   - `scalping_*.parquet + meta.parquet → all_stocks.parquet`
   - YFinance で一度に全銘柄取得できる（コードがシンプルに）

4. **manifest.json でのクリーン管理**
   - S3 内のファイル一覧を把握しやすい
   - `update_flag` の最新のみ保持で無駄なファイル削減

---

### ⚠️ 懸念点・確認事項

#### 1. `meta_jquants.parquet` の更新タイミング

- **質問:** 「S3 になければ作成」だと、一度作ったら二度と更新されない？
- **提案:**
  - 週 1 回強制更新（新規上場・廃止対応）
  - または有効期限を manifest.json に記録（例: 7 日間有効）

→ 提案を採用してください

#### 2. `all_stocks.parquet` のカラム構造

- **確認必要:**
  - `meta.parquet` と `scalping_*.parquet` のカラムが完全一致？
  - 重複除外は ticker ベース？code ベース？

→ data/parqeut 内にあるとおり meta.parqeut と scalping\_\*.parquet のカラムは違う
all_stocks.parqeut で全銘柄統一カラムになってますよね？そうしてください

#### 3. `update_flag` の管理

- **296-297 行目:** "update_flag が最新のものだけ"
- **提案:** manifest.json に記録する形式例:
  ```json
  {
    "generated_at": "2025-10-18T12:00:00Z",
    "latest_update_flag": "update_flag_2025-10-18.txt",
    "items": [...]
  }
  ```
  → 　要確認 結局 txt ファイルがあっても良いですが txt ファイルは直近更新したもののみ過去のものは毎回削除

#### 4. エラー時の障害耐性

- **J-Quants エラー時:**
  - `meta_jquants.parquet` の作成失敗 → 古い S3 版を使う？
  - `scalping_*.parquet` 生成失敗 → 空ファイル作成（既存方針通り）
- **YFinance エラー時:**
  - 一部銘柄のみ失敗した場合の処理は？

→ 現状の考え方では meta*jquants.parquet の作成失敗は想定していません
週次の強制更新ができない場合はそれまでに存在している meta_jquants.parquet を採用
scalping*\*.parquet が失敗した場合は 16 時,26 時ともに空ファイル
yfinance のエラーはできるものだけエラーが出た銘柄はそのままにしてください

---

### 📋 実装時のチェックリスト

#### GitHub Actions の処理フロー（毎日 16 時）:

```
1. meta_jquants.parquet をS3からダウンロード試行
   ├─ 成功 → そのまま使用
   └─ 失敗 → create_meta_jquants.py で新規作成 → S3アップロード

2. generate_scalping_final.py 実行
   ├─ 成功 → scalping_*.parquet 作成
   └─ 失敗 → 空の scalping_*.parquet 作成

3. all_stocks.parquet 作成
   - meta.parquet + scalping_*.parquet をマージ

4. gen_yfinance_parquet.py 実行
   - all_stocks.parquet の銘柄で価格取得
   - prices_*.parquet, tech_snapshot_1d.parquet 作成

5. manifest.json 更新
   - 全ファイルのリスト、ハッシュ、最新update_flag
   - 古いupdate_flag_*.txt を削除
```

---

### 🎯 総合評価: 8/10

**強み:**

- ファイル構造が明確
- キャッシュ戦略で効率的
- 拡張性が高い

**改善提案:**

- `meta_jquants.parquet` の更新ロジック明確化
- エラーハンドリングの詳細化
- manifest.json のスキーマ定義

**この方針で進めて問題ありません。** 実装時に上記の懸念点を解決していけば OK です。
