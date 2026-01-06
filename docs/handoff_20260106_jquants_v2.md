# 引き継ぎ: J-Quants v1→v2 移行

> **このファイルは引き継ぎ完了後に削除すること**

## 概要

J-Quants APIがv1からv2に移行。既存実装の更新が必要。

## 変更点

### 1. 認証方式

| 項目 | v1 | v2 |
|------|----|----|
| 方式 | メールアドレス+パスワード → RefreshToken → IDToken | **APIKey（ダッシュボードで発行）** |
| ヘッダー | `Authorization: Bearer {token}` | `x-api-key: {apikey}` |
| 有効期限 | 1週間（要更新） | **なし** |

### 2. エンドポイント

| 機能 | v1 | v2 |
|------|----|----|
| 株価 | `/v1/prices/daily_quotes` | `/v2/equities/bars/daily` |
| 前場株価 | `/v1/prices/prices_am` | `/v2/equities/bars/daily/am` |
| 銘柄一覧 | `/v1/listed/info` | `/v2/equities/master` |
| 決算予定 | `/v1/fins/announcement` | `/v2/equities/earnings-calendar` |
| 財務詳細 | `/v1/fins/fs_details` | `/v2/fins/details` |
| 指数 | `/v1/indices` | `/v2/indices/bars/daily` |

### 3. レスポンス形式

- データは `"data"` キー配下の配列
- ページネーション: `"pagination_key"` 対応

### 4. カラム名

```
Open → O
High → H
Low → L
Close → C
Volume → Vo
TurnoverValue → Va
```

### 5. レート制限

| プラン | 制限 |
|--------|------|
| Free | 5回/分 |
| Light | 60回/分 |
| Standard | 120回/分 |
| Premium | 500回/分 |

## 対象ファイル

| ファイル | 変更内容 |
|----------|----------|
| `scripts/lib/jquants_client.py` | 認証方式変更、トークン取得ロジック削除 |
| `scripts/lib/jquants_fetcher.py` | エンドポイント・カラム名変更 |
| `.github/workflows/*.yml` | 環境変数確認 |
| `.env.jquants` | APIKey追加 |

## 環境変数

```bash
# v1（削除可能）
JQUANTS_MAIL_ADDRESS=xxx
JQUANTS_PASSWORD=xxx
JQUANTS_REFRESH_TOKEN=xxx

# v2（新規追加）
JQUANTS_API_KEY=xxxxx  # ダッシュボードで発行
```

## 参考リンク

- 移行ガイド: https://jpx-jquants.com/ja/spec/migration-v1-v2
- APIスペック: https://jpx-jquants.com/ja/spec

## MCPサーバー

今回は導入しない。v2移行完了後に再検討。

---

**引き継ぎ完了後、このファイルを削除すること**
