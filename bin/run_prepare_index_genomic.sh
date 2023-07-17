#!/bin/bash

job=${1:-"variant_centric"}
env=${2:-"prd"}
instance_type="r5.4xlarge"
instance_count="20"
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
       "bio.ferlab.etl.prepared.genomic.RunPrepareGenomic",
       "s3a://kf-strides-232196027141-datalake-${env}/jobs/etl.jar",
       "config/${env}-kf-strides.conf",
       "default",
       "${job}"

     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Prepare Index"
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
  --bootstrap-actions Path="s3://kf-strides-232196027141-datalake-${env}/jobs/bootstrap-actions/enable-ssm.sh" Path="s3://kf-strides-232196027141-datalake-${env}/jobs/bootstrap-actions/download_human_reference_genome.sh" \
  --steps "${steps}" \
  --log-uri "s3n://kf-strides-232196027141-datalake-${env}/jobs/elasticmapreduce/" \
  --name "Portal ETL - PrepareIndex ${job} - ${env} ${release_id} ${study_id}" \
  --instance-groups "[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":150,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":8}]},\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":128,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.4xlarge\",\"Name\":\"Master - 1\"}]" \
  --configurations file://./conf/spark-config-genomic.json \
  --region us-east-1 \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --auto-terminate
