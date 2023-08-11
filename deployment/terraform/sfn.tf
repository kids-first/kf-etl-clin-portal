
locals {
  short = "VariantClinicalEtl"
}

#
# Step Functions resources
#
resource "aws_sfn_state_machine" "default" {
  name     = "stateMachine${local.short}"
  role_arn = aws_iam_role.step_functions_service_role.arn

  definition = templatefile("step-functions/etl.json.tmpl", {
    variant_etl_initialize_emr_arn    = aws_lambda_function.initialize-variant-etl-emr-lambda.arn,
    variant_etl_monitor_emr_arn       = aws_lambda_function.monitor-variant-etl-emr-lambda.arn,
    variant_etl_add_emr_step_arn      = aws_lambda_function.add-variant-etl-emr-step-lambda.arn,
    variant_etl_notify_emr_status_arn = aws_lambda_function.notify-variant-etl-emr-status-lambda.arn
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

    # Despite the "*" wildcard, only allow these actions for Batch jobs that were
    # started by Step Functions.
    # See: https://github.com/awsdocs/aws-step-functions-developer-guide/blob/master/doc_source/batch-iam.md
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