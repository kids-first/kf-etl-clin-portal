#!/bin/bash

job=$1
instance_count=${2:-"5"}
env=${3:-"prd"}
instance_type="r5.4xlarge"
if [ "$env" = "prd" ]; then
  subnet="subnet-00aab84919d5a44e2"
else
  subnet="subnet-0f0c909ec60b377ce"
fi

steps=$(
  cat <<EOF
[
   {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--class",
       "bio.ferlab.datalake.spark3.publictables.ImportPublicTable",
       "s3a://kf-strides-232196027141-datalake-${env}/jobs/etl.jar",
       "--config", "config/${env}-kf-strides.conf",
       "--steps", "default",
       "${job}"

     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Import Public table ${job}"
   }


]
EOF
)

aws emr create-cluster \
  --applications Name=Hadoop Name=Spark \
  --ec2-attributes "{\"KeyName\":\"flintrock\",\"InstanceProfile\":\"kf-variant-emr-ec2-${env}-profile\",\"SubnetId\":\"${subnet}\"}" \
  --service-role kf-variant-emr-$env-role \
  --enable-debugging \
  --release-label emr-7.0.0 \
  --bootstrap-actions Path="s3://kf-strides-232196027141-datalake-${env}/jobs/bootstrap-actions/enable-ssm.sh" \
  --steps "${steps}" \
  --log-uri "s3n://kf-strides-232196027141-datalake-${env}/jobs/elasticmapreduce/" \
  --name "Portal ETL - ImportPublicTable ${job} - ${env}" \
  --instance-groups "[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":150,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":8}]},\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":128,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.4xlarge\",\"Name\":\"Master - 1\"}]" \
  --configurations file://./conf/spark-config-genomic.json \
  --region us-east-1 \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --auto-terminate
