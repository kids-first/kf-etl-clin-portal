resource "aws_secretsmanager_secret" "portal_etl_secret" {
  name        = "${var.project}/${var.environment}/secrets"
  description = "Portal ETL secrets"
}

resource "aws_secretsmanager_secret_version" "portal_etl_secret_version" {
  secret_id = aws_secretsmanager_secret.portal_etl_secret.id
  secret_string = jsonencode(
    {
      "slack_webhook" : ""
    }
  )
}