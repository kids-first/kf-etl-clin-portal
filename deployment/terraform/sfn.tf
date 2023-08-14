
locals {
  short = "PortalEtl"
}

#
# Step Functions resources
#
resource "aws_sfn_state_machine" "default" {
  name     = "stateMachine${local.short}-${var.environment}"
  role_arn = aws_iam_role.step_functions_service_role.arn

  definition = templatefile("step-functions/etl.json.tmpl", {
    portal_etl_initialize_emr_arn    = aws_lambda_function.initialize-portal-etl-emr-lambda.arn,
    portal_etl_monitor_emr_arn       = aws_lambda_function.monitor-portal-etl-emr-lambda.arn,
    portal_etl_add_emr_step_arn      = aws_lambda_function.add-portal-etl-emr-step-lambda.arn,
    portal_etl_notify_emr_status_arn = aws_lambda_function.notify-portal-etl-emr-status-lambda.arn,
    environment                       = var.environment,
    portal_emr_ec2_subnet_id          = var.portal_emr_ec2_subnet_id
  })
}

#
# Step Functions IAM resources
#
data "aws_iam_policy_document" "step_functions_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "step_functions_service_role_policy" {
  statement {
    effect = "Allow"

    actions = [
      "lambda:InvokeFunction",
    ]
    resources = ["*"]
  }
  statement {
    effect = "Allow"

    actions = [
      "elasticmapreduce:DescribeCluster",
      "elasticmapreduce:TerminateJobFlows",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "step_functions_service_role" {
  name_prefix        = "sfn${local.short}ServiceRole"
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json
}

resource "aws_iam_role_policy" "step_functions_service_role_policy" {
  name_prefix = "sfn${local.short}ServiceRolePolicy"
  role        = aws_iam_role.step_functions_service_role.name
  policy      = data.aws_iam_policy_document.step_functions_service_role_policy.json
}