#!/bin/bash

release_id=$1
study_id=$2
env=${3:-"qa"}
instance_type="m5.4xlarge"
instance_count="1"
if [ "$env" = "prd" ]
then
  subnet="subnet-0cdbe9ba6231146b5"
  es="https://vpc-arranger-es-service-ykxirqamjqxyiyfg2rruxusfg4.us-east-1.es.amazonaws.com"
else
  subnet="subnet-0f1161ac2ee2fba5b"
  es="https://vpc-include-arranger-blue-es-qa-xf3ttht4hjmxjfoh5u5x4jnw34.us-east-1.es.amazonaws.com"
fi

steps=$(cat <<EOF
[
   {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--class",
       "bio.ferlab.fhir.etl.IndexTask",
       "s3a://include-373997854230-datalake-${env}/jobs/index-task.jar",
       "${es}",
       "443",
       "${release_id}",
       "${study_id}",
       "study_centric",
       "config/${env}.conf"

     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Index Study"
   },
   {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--class",
       "bio.ferlab.fhir.etl.IndexTask",
       "s3a://include-373997854230-datalake-${env}/jobs/index-task.jar",
       "${es}",
       "443",
       "${release_id}",
       "${study_id}",
       "participant_centric",
       "config/${env}.conf"

     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Index Participant"
   },
   {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--class",
       "bio.ferlab.fhir.etl.IndexTask",
       "s3a://include-373997854230-datalake-${env}/jobs/index-task.jar",
       "${es}",
       "443",
       "${release_id}",
       "${study_id}",
       "file_centric",
       "config/${env}.conf"

     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Index File"
   },
   {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--class",
       "bio.ferlab.fhir.etl.IndexTask",
       "s3a://include-373997854230-datalake-${env}/jobs/index-task.jar",
       "${es}",
       "443",
       "${release_id}",
       "${study_id}",
       "biospecimen_centric",
       "config/${env}.conf"

     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Index Biospecimen"
   }



]
EOF
)


aws emr create-cluster \
  --applications Name=Hadoop Name=Spark \
  --ec2-attributes "{\"KeyName\":\"flintrock_include\",\"InstanceProfile\":\"include-ec2-${env}-profile\",\"SubnetId\":\"${subnet}\"}" \
  --service-role include-datalake-emr-$env-role \
  --enable-debugging \
  --release-label emr-6.5.0 \
  --bootstrap-actions Path="s3://include-373997854230-datalake-${env}/jobs/bootstrap-actions/enable-ssm.sh" Path="s3://include-373997854230-datalake-${env}/jobs/bootstrap-actions/install-java11.sh" \
  --steps "${steps}" \
  --log-uri "s3n://include-373997854230-datalake-${env}/jobs/elasticmapreduce/" \
  --name "Portal ETL - Index all - ${env} ${release_id} ${study_id}" \
  --instance-groups "[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]" \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --configurations file://./spark-config.json \
  --auto-terminate \
 --region us-east-1