#!/bin/bash
#set -x

release_id=$1
chromosome=${2:-"all"}
env=${3:-"qa"}
job=${4:-"variant_centric"}
sample=${5:-"all"}
input=${6:-"s3a://kf-strides-232196027141-datalake-$env/es_index"}
instance_type="m5.xlarge"
instance_count="1"
if [ "$env" = "prd" ]; then
  subnet="subnet-00aab84919d5a44e2"
  es="https://vpc-kf-arranger-blue-es-prd-4gbc2zkvm5uttysiqkcbzwxqeu.us-east-1.es.amazonaws.com"
else
  subnet="subnet-0f0c909ec60b377ce"
  es="https://vpc-kf-arranger-blue-es-service-exwupkrf4dyupg24dnfmvzcwri.us-east-1.es.amazonaws.com"
fi

steps=$(cat <<EOF
[
   {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--packages",
       "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.12",
       "--class",
       "bio.ferlab.etl.indexed.genomic.RunIndexGenomic",
       "s3a://kf-strides-232196027141-datalake-${env}/jobs/etl.jar",
       "${es}",
       "443",
       "${release_id}",
       "${job}",
       "config/${env}-kf-strides.conf",
       "${input}/${job}",
       "${chromosome}",
       "${sample}"
     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "CONTINUE",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Index ${job} - ${release_id} - ${chromosome}"
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
  --name "Portal ETL - Index Variant - ${env} ${release_id} ${job} ${chromosome}" \
  --instance-groups "[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]" \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --configurations file://./conf/spark-config-clinical.json \
  --auto-terminate \
 --region us-east-1