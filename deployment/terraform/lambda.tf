#
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
  name_prefix = "lambdaVariantEtlServiceRole-"
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
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_role_policy" {
  name_prefix = "lambdaVariantEtlEMRReadPolicy"
  policy = data.aws_iam_policy_document.emr_role_policy.json
  role   = aws_iam_role.lambda_service_role.name
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

    actions = ["iam:PassRole"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "describe_sg_role_policy" {
  name_prefix = "lambdaVariantEtlSGReadPolicy"
  policy = data.aws_iam_policy_document.describe_sg_role_policy.json
  role   = aws_iam_role.lambda_service_role.name
}


resource "aws_iam_role_policy_attachment" "github_actions_managed_role_policies" {
  role       = aws_iam_role.lambda_service_role.name
  policy_arn = var.AWSLambdaBasicExecutionRoleManagedPolicy
}

#
# Create Lambda Functions
#
data "archive_file" "archive-initialize-variant-etl-emr-lambda" {
  type = "zip"
  output_path = "../lambda_functions/initialize_variant_etl_emr_archive.zip"
  source_dir = "../lambda_functions/initialize_variant_etl_emr/"
}

resource "aws_lambda_function" "initialize-variant-etl-emr-lambda" {
  function_name = "VariantEtl-Initialize-EMR"
  filename = "../lambda_functions/initialize_variant_etl_emr_archive.zip"
  role          = aws_iam_role.lambda_service_role.arn
  handler = "main.initialize_portal_etl_emr"
  runtime = "python3.9"
  source_code_hash = data.archive_file.archive-initialize-variant-etl-emr-lambda.output_base64sha256

  timeout = 30
}

data "archive_file" "archive-check-variant-etl-emr-step-status-lambda" {
  type = "zip"
  output_path = "../lambda_functions/check_variant_etl_emr_step_status_archive.zip"
  source_dir = "../lambda_functions/check_variant_etl_emr_step_status/"
}

resource "aws_lambda_function" "monitor-variant-etl-emr-lambda" {
  function_name = "VariantEtl-Monitor-EMR"
  filename = "../lambda_functions/check_variant_etl_emr_step_status_archive.zip"
  role          = aws_iam_role.lambda_service_role.arn
  handler = "main.check_variant_etl_emr_step_status"
  runtime = "python3.9"
  source_code_hash = data.archive_file.archive-check-variant-etl-emr-step-status-lambda.output_base64sha256
}

data "archive_file" "archive-add-variant-etl-emr-step-lambda" {
  type = "zip"
  output_path = "../lambda_functions/add_variant_etl_emr_step_archive.zip"
  source_dir = "../lambda_functions/add_variant_etl_emr_step/"
}

resource "aws_lambda_function" "add-variant-etl-emr-step-lambda" {
  function_name = "VariantEtl-Submit-ETL-Step"
  filename = "../lambda_functions/add_variant_etl_emr_step_archive.zip"
  role          = aws_iam_role.lambda_service_role.arn
  handler = "main.add_variant_etl_emr_step"
  runtime = "python3.9"
  source_code_hash = data.archive_file.archive-add-variant-etl-emr-step-lambda.output_base64sha256
}

data "archive_file" "archive-notify-variant-etl-emr-status-lambda" {
  type = "zip"
  output_path = "../lambda_functions/notify_variant_etl_status_archive.zip"
  source_dir = "../lambda_functions/notify_variant_etl_status/"
}

resource "aws_lambda_function" "notify-variant-etl-emr-status-lambda" {
  function_name = "VariantEtl-Notify-ETL-Status"
  filename = "../lambda_functions/notify_variant_etl_status_archive.zip"
  role          = aws_iam_role.lambda_service_role.arn
  handler = "main.notify_variant_etl_status"
  runtime = "python3.9"
  source_code_hash = data.archive_file.archive-notify-variant-etl-emr-status-lambda.output_base64sha256
}

