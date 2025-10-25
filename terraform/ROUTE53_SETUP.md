# Route53 + ACM + App Runner セットアップガイド

## 概要

- **DNS管理**: お名前.com → Route53 に移行
- **SSL証明書**: ACM ワイルドカード証明書 (`*.ymnk.jp`)
- **カスタムドメイン**: `api.ymnk.jp` → App Runner

## セットアップ手順

### ステップ1: Terraform で Route53 とACM を作成

```bash
cd terraform
terraform plan
terraform apply
```

これで以下が作成されます:
- Route53 ホストゾーン (`ymnk.jp`)
- ACM ワイルドカード証明書 (`*.ymnk.jp`, `ymnk.jp`)
- DNS検証レコード（自動）
- App Runner カスタムドメイン設定

### ステップ2: Route53 のネームサーバーを確認

```bash
terraform output route53_name_servers
```

出力例:
```
[
  "ns-123.awsdns-12.com",
  "ns-456.awsdns-45.net",
  "ns-789.awsdns-78.org",
  "ns-012.awsdns-01.co.uk"
]
```

### ステップ3: お名前.com でネームサーバーを変更

1. **お名前.com にログイン**
   - https://www.onamae.com/

2. **ドメイン設定 → ネームサーバーの変更**
   - `ymnk.jp` を選択
   - 「他のネームサーバーを利用」を選択

3. **Route53 のネームサーバーを入力**
   - ステップ2で確認した4つのネームサーバーを入力
   - 例:
     ```
     プライマリネームサーバー: ns-123.awsdns-12.com
     セカンダリネームサーバー: ns-456.awsdns-45.net
     （以下同様に4つ全て入力）
     ```

4. **確認して設定を完了**

### ステップ4: DNS 浸透を待つ

- 通常: 数分〜数時間
- 最大: 24-48時間

**確認方法:**
```bash
# ネームサーバーが切り替わったか確認
dig ymnk.jp NS +short

# Route53のNSが返ってくればOK
# ns-123.awsdns-12.com.
# ns-456.awsdns-45.net.
# ...
```

### ステップ5: ACM 証明書の検証完了を確認

```bash
# 証明書のステータス確認
terraform output acm_certificate_status

# "ISSUED" と表示されればOK
```

### ステップ6: App Runner カスタムドメインの確認

```bash
# App Runner カスタムドメインのステータス確認
terraform output apprunner_custom_domain_status

# "active" と表示されればOK
```

### ステップ7: 動作確認

```bash
# api.ymnk.jp にアクセスできるか確認
curl https://api.ymnk.jp/health

# または
dig api.ymnk.jp +short
```

## Vercel の設定（フロントエンド）

現在お名前.com で設定されている Vercel 用のDNSレコードを Route53 に移行する必要があります。

**お名前.com の現在の設定を確認:**

1. お名前.com でDNS設定を開く
2. `ymnk.jp` と `www.ymnk.jp` のレコードを確認

**Route53 に追加（例）:**

```hcl
# terraform/vercel.tf を作成

# ymnk.jp → Vercel
resource "aws_route53_record" "root" {
  zone_id = aws_route53_zone.main.zone_id
  name    = "ymnk.jp"
  type    = "A"
  ttl     = 300
  records = ["76.76.21.21"]  # Vercel のIPアドレス（確認して修正）
}

# www.ymnk.jp → Vercel
resource "aws_route53_record" "www" {
  zone_id = aws_route53_zone.main.zone_id
  name    = "www.ymnk.jp"
  type    = "CNAME"
  ttl     = 300
  records = ["cname.vercel-dns.com"]  # Vercel のCNAME（確認して修正）
}
```

## トラブルシューティング

### ACM 証明書が ISSUED にならない

```bash
# DNS検証レコードが正しく設定されているか確認
aws route53 list-resource-record-sets \
  --hosted-zone-id $(terraform output -raw route53_zone_id) \
  --query "ResourceRecordSets[?Type=='CNAME']"
```

### App Runner カスタムドメインが active にならない

1. ACM証明書が ISSUED になっているか確認
2. DNS浸透が完了しているか確認
3. App Runner の検証レコードが正しく設定されているか確認

### ネームサーバー変更が反映されない

```bash
# キャッシュをクリアして確認
dig ymnk.jp NS @8.8.8.8 +short
```

## リソース削除

⚠️ **注意**: DNS設定を削除すると、ドメインにアクセスできなくなります。

```bash
# お名前.com でネームサーバーを元に戻してから実行
terraform destroy
```

## 費用

- **Route53 ホストゾーン**: $0.50/月
- **Route53 クエリ**: 100万クエリあたり $0.40
- **ACM証明書**: 無料
- **App Runner カスタムドメイン**: 無料

合計: 月額 $0.50 程度（クエリ数による）
