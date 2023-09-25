
# Lambda IAM Resources
#
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "lambda_service_role" {
  name               = "kf-lambda-portal-etl-${var.environment}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "emr_role_policy" {
  statement {
    effect = "Allow"

    actions = [
      "elasticmapreduce:ListClusters",
      "elasticmapreduce:DescribeCluster",
      "elasticmapreduce:RunJobFlow",
      "elasticmapreduce:TerminateJobFlows",
      "elasticmapreduce:AddJobFlowSteps",
      "elasticmapreduce:DescribeStep",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_role_policy" {
  name_prefix = "lambdaPortalEtlEMRReadPolicy"
  policy      = data.aws_iam_policy_document.emr_role_policy.json
  role        = aws_iam_role.lambda_service_role.name
}

data "aws_iam_policy_document" "describe_sg_role_policy" {
  statement {
    effect = "Allow"

    actions = [
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSecurityGroupRules",
      "ec2:DescribeTags",
    ]
    resources = ["*"]
  }

  statement {
    effect = "Allow"

    actions   = ["iam:PassRole"]
    resources = ["*"]
  }

}

resource "aws_iam_role_policy" "describe_sg_role_policy" {
  name_prefix = "lambdaPortalEtlSGReadPolicy"
  policy      = data.aws_iam_policy_document.describe_sg_role_policy.json
  role        = aws_iam_role.lambda_service_role.name
}

data "aws_iam_policy_document" "get_secret_policy" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.portal_etl_secret.arn]
  }
}

resource "aws_iam_role_policy" "get_secret_role_policy" {
  name_prefix = "lambdaPortalEtlGetSecretPolicy-${var.environment}"
  policy      = data.aws_iam_policy_document.get_secret_policy.json
  role        = aws_iam_role.lambda_service_role.name
}

resource "aws_iam_role_policy_attachment" "github_actions_managed_role_policies" {
  role       = aws_iam_role.lambda_service_role.name
  policy_arn = var.AWSLambdaBasicExecutionRoleManagedPolicy
}

#
# Create Lambda Functions
#
data "archive_file" "archive-initialize-portal-etl-emr-lambda" {
  type        = "zip"
  output_path = "../lambda_functions/initialize_portal_etl_emr_archive.zip"
  source_dir  = "../lambda_functions/initialize_portal_etl_emr/"
}

resource "aws_lambda_function" "initialize-portal-etl-emr-lambda" {
  function_name    = "PortalEtl-Initialize-EMR-${var.environment}"
  filename         = "../lambda_functions/initialize_portal_etl_emr_archive.zip"
  role             = aws_iam_role.lambda_service_role.arn
  handler          = "main.initialize_portal_etl_emr"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive-initialize-portal-etl-emr-lambda.output_base64sha256

  timeout = 30
}

data "archive_file" "archive-check-portal-etl-emr-step-status-lambda" {
  type        = "zip"
  output_path = "../lambda_functions/check_portal_etl_emr_step_status_archive.zip"
  source_dir  = "../lambda_functions/check_portal_etl_emr_step_status/"
}

resource "aws_lambda_function" "monitor-portal-etl-emr-lambda" {
  function_name    = "PortalEtl-Monitor-EMR-${var.environment}"
  filename         = "../lambda_functions/check_portal_etl_emr_step_status_archive.zip"
  role             = aws_iam_role.lambda_service_role.arn
  handler          = "main.check_portal_etl_emr_step_status"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive-check-portal-etl-emr-step-status-lambda.output_base64sha256
}

data "archive_file" "archive-add-portal-etl-emr-step-lambda" {
  type        = "zip"
  output_path = "../lambda_functions/add_portal_etl_emr_step_archive.zip"
  source_dir  = "../lambda_functions/add_portal_etl_emr_step/"
}

resource "aws_lambda_function" "add-portal-etl-emr-step-lambda" {
  function_name    = "PortalEtl-Submit-ETL-Step-${var.environment}"
  filename         = "../lambda_functions/add_portal_etl_emr_step_archive.zip"
  role             = aws_iam_role.lambda_service_role.arn
  handler          = "main.add_portal_etl_emr_step"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive-add-portal-etl-emr-step-lambda.output_base64sha256
}

data "archive_file" "archive-notify-portal-etl-emr-status-lambda" {
  type        = "zip"
  output_path = "../lambda_functions/notify_portal_etl_status_archive.zip"
  source_dir  = "../lambda_functions/notify_portal_etl_status/"
}

resource "aws_lambda_function" "notify-portal-etl-emr-status-lambda" {
  function_name = "PortalEtl-Notify-ETL-Status-${var.environment}"
  filename      = "../lambda_functions/notify_portal_etl_status_archive.zip"
  role          = aws_iam_role.lambda_service_role.arn
  handler       = "main.notify_portal_etl_status"
  environment {
    variables = {
      SECRET_NAME = aws_secretsmanager_secret.portal_etl_secret.name
    }
  }
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive-notify-portal-etl-emr-status-lambda.output_base64sha256
}

