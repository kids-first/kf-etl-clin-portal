#!/bin/bash

study_id=$1
env=${2:-"qa"}
instance_type="m5.4xlarge"
instance_count="1"
if [ "$env" = "prd" ]
then
  subnet="subnet-00aab84919d5a44e2"
else
  subnet="subnet-0f0c909ec60b377ce"
fi

steps=$(cat <<EOF
[
   {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--class",
       "bio.ferlab.etl.enrich.Enrich",
       "s3a://kf-strides-232196027141-datalake-${env}/jobs/enrich.jar",
       "config/${env}-kf-strides.conf",
       "default",
       "specimen",
       "${study_id}"

     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Enrich Specimen"
   }


]
EOF
)


aws emr create-cluster \
  --applications Name=Hadoop Name=Spark \
  --ec2-attributes "{\"KeyName\":\"flintrock\",\"InstanceProfile\":\"kf-variant-emr-ec2-${env}-profile\",\"SubnetId\":\"${subnet}\"}" \
  --service-role kf-variant-emr-$env-role \
  --enable-debugging \
  --release-label emr-6.11.0 \
  --bootstrap-actions Path="s3://kf-strides-232196027141-datalake-${env}/jobs/bootstrap-actions/enable-ssm.sh" Path="s3://kf-strides-232196027141-datalake-${env}/jobs/bootstrap-actions/install-java11.sh" \
  --steps "${steps}" \
  --log-uri "s3n://kf-strides-232196027141-datalake-${env}/jobs/elasticmapreduce/" \
  --name "Portal ETL - Enrich Specimen - ${env} ${release_id} ${study_id}" \
  --instance-groups "[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]" \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --configurations file://./spark-config.json \
  --auto-terminate \
  --region us-east-1