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
      "elasticmapreduce:ListSteps",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "emr_role_policy" {
  name_prefix = "lambdaPortalEtlEMRReadPolicy-"
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
  name_prefix = "lambdaPortalEtlSGReadPolicy-"
  policy      = data.aws_iam_policy_document.describe_sg_role_policy.json
  role        = aws_iam_role.lambda_service_role.name
}

data "aws_iam_policy_document" "get_secret_policy" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.portal_etl_secret.arn, aws_secretmanager_secret.fhir_secret.arn]
  }
}

resource "aws_iam_role_policy" "get_secret_role_policy" {
  name_prefix = "lambdaPortalEtlGetSecretPolicy-${var.environment}-"
  policy      = data.aws_iam_policy_document.get_secret_policy.json
  role        = aws_iam_role.lambda_service_role.name
}

data "aws_iam_policy_document" "start_step_fn_role_policy" {
  statement {
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.genomic_index_etl.arn]
  }
}

resource "aws_iam_role_policy" "start_step_fn_policy" {
  name_prefix = "lambdaPortalEtlStartStepFnPolicy-${var.environment}-"
  policy      = data.aws_iam_policy_document.start_step_fn_role_policy.json
  role        = aws_iam_role.lambda_service_role.name
}

resource "aws_iam_role_policy_attachment" "github_actions_managed_role_policies" {
  role       = aws_iam_role.lambda_service_role.name
  policy_arn = var.AWSLambdaBasicExecutionRoleManagedPolicy
}

#
# Create Lambda Functions
#
data "archive_file" "archive_initialize_portal_etl_emr_lambda" {
  type        = "zip"
  output_path = "${path.module}/../lambda_functions/initialize_portal_etl_emr_archive.zip"
  source_dir  = "${path.module}/../lambda_functions/initialize_portal_etl_emr/"
}

resource "aws_lambda_function" "initialize_portal_etl_emr_lambda" {
  function_name    = "PortalEtl-Initialize-EMR-${var.environment}"
  filename         = "${path.module}/../lambda_functions/initialize_portal_etl_emr_archive.zip"
  role             = aws_iam_role.lambda_service_role.arn
  handler          = "main.initialize_portal_etl_emr"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive_initialize_portal_etl_emr_lambda.output_base64sha256

  timeout = 30
}

data "archive_file" "archive_check_portal_etl_emr_step_status_lambda" {
  type        = "zip"
  output_path = "${path.module}/../lambda_functions/check_portal_etl_emr_step_status_archive.zip"
  source_dir  = "${path.module}/../lambda_functions/check_portal_etl_emr_step_status/"
}

resource "aws_lambda_function" "monitor_portal_etl_emr_lambda" {
  function_name    = "PortalEtl-Monitor-EMR-${var.environment}"
  filename         = "${path.module}/../lambda_functions/check_portal_etl_emr_step_status_archive.zip"
  role             = aws_iam_role.lambda_service_role.arn
  handler          = "main.check_portal_etl_emr_step_status"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive_check_portal_etl_emr_step_status_lambda.output_base64sha256
}

data "archive_file" "archive_add_portal_etl_emr_step_lambda" {
  type        = "zip"
  output_path = "${path.module}/../lambda_functions/add_portal_etl_emr_step_archive.zip"
  source_dir  = "${path.module}/../lambda_functions/add_portal_etl_emr_step/"
}

resource "aws_lambda_function" "add_portal_etl_emr_step_lambda" {
  function_name    = "PortalEtl-Submit-ETL-Step-${var.environment}"
  filename         = "${path.module}/../lambda_functions/add_portal_etl_emr_step_archive.zip"
  role             = aws_iam_role.lambda_service_role.arn
  handler          = "main.add_portal_etl_emr_step"
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive_add_portal_etl_emr_step_lambda.output_base64sha256
}

data "archive_file" "archive_notify_portal_etl_emr_status_lambda" {
  type        = "zip"
  output_path = "${path.module}/../lambda_functions/notify_portal_etl_status_archive.zip"
  source_dir  = "${path.module}/../lambda_functions/notify_portal_etl_status/"
}

resource "aws_lambda_function" "notify_portal_etl_emr_status_lambda" {
  function_name = "PortalEtl-Notify-ETL-Status-${var.environment}"
  filename      = "${path.module}/../lambda_functions/notify_portal_etl_status_archive.zip"
  role          = aws_iam_role.lambda_service_role.arn
  handler       = "main.notify_portal_etl_status"
  environment {
    variables = {
      SECRET_NAME = aws_secretsmanager_secret.portal_etl_secret.name
    }
  }
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive_notify_portal_etl_emr_status_lambda.output_base64sha256
}

data "archive_file" "archive_start_genomic_index_step_fn_lambda" {
  type        = "zip"
  output_path = "${path.module}/../lambda_functions/start_genomic_index_step_fn.zip"
  source_dir  = "${path.module}/../lambda_functions/start_genomic_index_step_fn/"
}

resource "aws_lambda_function" "start_genomic_index_step_fn_lambda" {
  function_name = "PortalEtl-Start-Genomic-Index-StepFn-${var.environment}"
  filename      = "${path.module}/../lambda_functions/start_genomic_index_step_fn.zip"
  role          = aws_iam_role.lambda_service_role.arn
  handler       = "main.start_genomic_index_step_fn"
  environment {
    variables = {
      GENOMIC_INDEX_STEP_FN_ARN = aws_sfn_state_machine.genomic_index_etl.arn
    }
  }
  runtime          = "python3.9"
  source_code_hash = data.archive_file.archive_start_genomic_index_step_fn_lambda.output_base64sha256
}

