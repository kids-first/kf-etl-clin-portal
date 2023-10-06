
locals {
  conf_location = "${path.module}/../lambda_functions/initialize_portal_etl_emr/conf"
}
resource "null_resource" "copy_emr_config_file" {
  triggers = {
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "cp -r ${path.module}/../../bin/conf/ ${local.conf_location}"
  }

  provisioner "local-exec" {
    command = "jq 'map(if .Properties[\"spark.hadoop.fs.s3a.assumed.role.arn\"] == \"arn:aws:iam::538745987955:role/kf-etl-server-prd-role\" then .Properties[\"spark.hadoop.fs.s3a.assumed.role.arn\"] = \"${var.spark_assumed_role}\" else . end)' ${local.conf_location}/spark-config-genomic.json > ${local.conf_location}/temp.json && mv ${local.conf_location}/temp.json ${local.conf_location}/spark-config-genomic.json"
  }
}