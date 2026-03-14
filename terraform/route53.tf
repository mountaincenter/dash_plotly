# Route 53 Hosted Zone for api.ymnk.jp
resource "aws_route53_zone" "api_subdomain" {
  name = "api.ymnk.jp"

  tags = {
    Name        = "api.ymnk.jp"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# CAA Records to allow AWS Certificate Manager
resource "aws_route53_record" "api_caa" {
  zone_id = aws_route53_zone.api_subdomain.zone_id
  name    = "api.ymnk.jp"
  type    = "CAA"
  ttl     = 300

  records = [
    "0 issue \"amazon.com\"",
    "0 issue \"amazontrust.com\"",
    "0 issue \"awstrust.com\"",
    "0 issue \"amazonaws.com\""
  ]
}

# CNAME record for stock.api.ymnk.jp pointing to App Runner
resource "aws_route53_record" "stock_api" {
  zone_id = aws_route53_zone.api_subdomain.zone_id
  name    = "stock.api.ymnk.jp"
  type    = "CNAME"
  ttl     = 300

  records = [aws_apprunner_service.stock_api.service_url]
}

# Certificate validation records (will be created by App Runner)
# These are managed dynamically based on App Runner's requirements

# Outputs
output "route53_zone_id" {
  description = "Route 53 hosted zone ID for api.ymnk.jp"
  value       = aws_route53_zone.api_subdomain.zone_id
}

output "route53_nameservers" {
  description = "Route 53 nameservers for api.ymnk.jp (add these to Vercel as NS records)"
  value       = aws_route53_zone.api_subdomain.name_servers
}

output "custom_domain_url" {
  description = "Custom domain URL for stock API"
  value       = "https://stock.api.ymnk.jp"
}
