environment              = "qa"
project                  = "kf-etl-clin-portal"
account                  = "include"
portal_emr_ec2_subnet_id = "subnet-0f1161ac2ee2fba5b"
portal_etl_bucket        = "include-373997854230-datalake-qa"
emr_instance_profile     = "include-ec2-qa-profile"
emr_service_role         = "include-datalake-emr-qa-role"
elastic_search_endpoint  = "https://vpc-include-arranger-blue-es-qa-xf3ttht4hjmxjfoh5u5x4jnw34.us-east-1.es.amazonaws.com"
spark_assumed_role       = "arn:aws:iam::538745987955:role/kf-etl-server-qa-role"