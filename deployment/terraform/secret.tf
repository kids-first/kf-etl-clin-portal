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

resource "aws_secretsmanager_secret" "fhir_secret" {
  name        = "${var.project}/${var.environment}/fhir_secret"
  description = "Portal ETL FHIR secrets"
}

resource "aws_secretsmanager_secret_version" "fhir_secret_version" {
  secret_id = aws_secretsmanager_secret.fhir_secret.id
  secret_string = jsonencode(
    {
      "keycloak_client_id" : "",
      "keycloak_client_secret" : "",
      "keycloak_url" : ""
    }
  )
}
