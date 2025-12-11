# =============================================================================
# AWS Cognito User Pool - FIDO2/Passkey Authentication
# =============================================================================

# Cognito User Pool
resource "aws_cognito_user_pool" "main" {
  name = "stock-app-user-pool"

  # パスワードポリシー
  password_policy {
    minimum_length    = 12
    require_lowercase = true
    require_numbers   = true
    require_symbols   = true
    require_uppercase = true
  }

  # サインインポリシー（パスワード + パスキー）
  sign_in_policy {
    allowed_first_auth_factors = ["PASSWORD", "WEB_AUTHN"]
  }

  # WebAuthn/パスキー設定
  web_authn_configuration {
    relying_party_id  = "stock.porque-and-because.work"
    user_verification = "preferred"  # "required" or "preferred"
  }

  # MFA設定 (FIDO2/WebAuthn)
  mfa_configuration = "OPTIONAL"

  # ソフトウェアトークンMFA（TOTP）- FIDO2の前提として必要
  software_token_mfa_configuration {
    enabled = true
  }

  # ユーザー属性
  schema {
    name                     = "email"
    attribute_data_type      = "String"
    mutable                  = true
    required                 = true
    developer_only_attribute = false

    string_attribute_constraints {
      min_length = 1
      max_length = 256
    }
  }

  # メール検証設定
  auto_verified_attributes = ["email"]

  # メール送信設定（Cognitoデフォルト）
  email_configuration {
    email_sending_account = "COGNITO_DEFAULT"
  }

  # アカウント復旧設定
  account_recovery_setting {
    recovery_mechanism {
      name     = "verified_email"
      priority = 1
    }
  }

  # ユーザー名設定（メールアドレスをユーザー名として使用）
  username_attributes = ["email"]

  # ユーザー名の大文字小文字を区別しない
  username_configuration {
    case_sensitive = false
  }

  # 管理者のみがユーザーを作成可能（セキュリティ強化）
  admin_create_user_config {
    allow_admin_create_user_only = true

    invite_message_template {
      email_subject = "Stock App - Your temporary password"
      email_message = "Your username is {username} and temporary password is {####}."
      sms_message   = "Your username is {username} and temporary password is {####}."
    }
  }

  # 削除保護
  deletion_protection = "ACTIVE"

  tags = {
    Name        = "stock-app-user-pool"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# Cognito User Pool Domain（Hosted UI用）
resource "aws_cognito_user_pool_domain" "main" {
  domain       = "stock-app-${data.aws_caller_identity.current.account_id}"
  user_pool_id = aws_cognito_user_pool.main.id
}

# Cognito User Pool Client（Next.js用）
resource "aws_cognito_user_pool_client" "nextjs" {
  name         = "stock-app-nextjs-client"
  user_pool_id = aws_cognito_user_pool.main.id

  # シークレット生成しない（SPA用）
  generate_secret = false

  # 明示的な認証フロー（パスキー対応）
  explicit_auth_flows = [
    "ALLOW_USER_SRP_AUTH",
    "ALLOW_REFRESH_TOKEN_AUTH",
    "ALLOW_USER_PASSWORD_AUTH",
    "ALLOW_USER_AUTH",  # パスキー/WebAuthn認証に必要
  ]

  # サポートするIDプロバイダー
  supported_identity_providers = ["COGNITO"]

  # コールバックURL（Next.jsアプリ）
  callback_urls = [
    "http://localhost:3000/dev/stock-results",
    "https://stock.porque-and-because.work/dev/stock-results",
  ]

  # ログアウトURL
  logout_urls = [
    "http://localhost:3000/dev",
    "https://stock.porque-and-because.work/dev",
  ]

  # OAuth設定
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_scopes                 = ["email", "openid", "profile", "aws.cognito.signin.user.admin"]

  # トークン有効期限
  access_token_validity  = 1   # 1時間
  id_token_validity      = 1   # 1時間
  refresh_token_validity = 30  # 30日

  token_validity_units {
    access_token  = "hours"
    id_token      = "hours"
    refresh_token = "days"
  }

  # トークン失効を有効化
  enable_token_revocation = true

  # ユーザー存在エラーを防止（セキュリティ）
  prevent_user_existence_errors = "ENABLED"
}

# Cognito User Pool Client（API/バックエンド用）
resource "aws_cognito_user_pool_client" "api" {
  name         = "stock-app-api-client"
  user_pool_id = aws_cognito_user_pool.main.id

  # シークレット生成（サーバーサイド用）
  generate_secret = true

  # 認証フロー
  explicit_auth_flows = [
    "ALLOW_ADMIN_USER_PASSWORD_AUTH",
    "ALLOW_REFRESH_TOKEN_AUTH",
  ]

  # トークン有効期限
  access_token_validity  = 1
  id_token_validity      = 1
  refresh_token_validity = 30

  token_validity_units {
    access_token  = "hours"
    id_token      = "hours"
    refresh_token = "days"
  }

  enable_token_revocation       = true
  prevent_user_existence_errors = "ENABLED"
}
