#
# Step Functions resources
#
resource "aws_sfn_state_machine" "default" {
  name     = "stateMachinePortalEtl-${var.environment}"
  role_arn = aws_iam_role.step_functions_service_role.arn

  definition = templatefile("step-functions/etl.json.tmpl", {
    portal_etl_initialize_emr_arn          = aws_lambda_function.initialize_portal_etl_emr_lambda.arn,
    portal_etl_monitor_emr_arn             = aws_lambda_function.monitor_portal_etl_emr_lambda.arn,
    portal_etl_add_emr_step_arn            = aws_lambda_function.add_portal_etl_emr_step_lambda.arn,
    portal_etl_notify_emr_status_arn       = aws_lambda_function.notify_portal_etl_emr_status_lambda.arn,
    environment                            = var.environment,
    portal_emr_ec2_subnet_id               = var.portal_emr_ec2_subnet_id,
    portal_etl_bucket                      = var.portal_etl_bucket,
    emr_instance_profile                   = var.emr_instance_profile,
    emr_service_role                       = var.emr_service_role,
    elastic_search_endpoint                = var.elastic_search_endpoint,
    account                                = var.account,
    run_genomic_index_etl                  = false,
    start_genomic_index_step_fn_lambda_arn = aws_lambda_function.start_genomic_index_step_fn_lambda.arn
  })
}

resource "aws_sfn_state_machine" "genomic_index_etl" {
  name     = "stateMachineGenomicIndexEtl-${var.environment}"
  role_arn = aws_iam_role.step_functions_service_role.arn

  definition = templatefile("step-functions/etl.json.tmpl", {
    portal_etl_initialize_emr_arn          = aws_lambda_function.initialize_portal_etl_emr_lambda.arn,
    portal_etl_monitor_emr_arn             = aws_lambda_function.monitor_portal_etl_emr_lambda.arn,
    portal_etl_add_emr_step_arn            = aws_lambda_function.add_portal_etl_emr_step_lambda.arn,
    portal_etl_notify_emr_status_arn       = aws_lambda_function.notify_portal_etl_emr_status_lambda.arn,
    environment                            = var.environment,
    portal_emr_ec2_subnet_id               = var.portal_emr_ec2_subnet_id,
    portal_etl_bucket                      = var.portal_etl_bucket,
    emr_instance_profile                   = var.emr_instance_profile,
    emr_service_role                       = var.emr_service_role,
    elastic_search_endpoint                = var.elastic_search_endpoint,
    account                                = var.account,
    run_genomic_index_etl                  = true,
    start_genomic_index_step_fn_lambda_arn = "",
    fhir_secret                            = aws_secretsmanager_secret.fhir_secret.id
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
  name               = "kf-sfn-portal-etl-${var.environment}-role"
  assume_role_policy = data.aws_iam_policy_document.step_functions_assume_role.json
}

resource "aws_iam_role_policy" "step_functions_service_role_policy" {
  name_prefix = "sfnPortalEtlServiceRolePolicy-"
  role        = aws_iam_role.step_functions_service_role.name
  policy      = data.aws_iam_policy_document.step_functions_service_role_policy.json
}
