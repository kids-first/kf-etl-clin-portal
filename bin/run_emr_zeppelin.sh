#!/bin/bash

source "$(dirname "$0")/utils.sh"

ENV="qa"
PROJECT="kf-strides"
BUCKET="kf-strides-232196027141-datalake-qa"
INSTANCE_TYPE="m5.4xlarge"
INSTANCE_COUNT="1"
INSTANCE_PROFILE="kf-variant-emr-ec2-qa-profile"
SERVICE_ROLE="kf-variant-emr-qa-role"

SG_SERVICE=$(aws ec2 describe-security-groups --filters Name=group-name,Values=ElasticMapReduce-ServiceAccess-"${ENV}"-* --query "SecurityGroups[*].{Name:GroupName,ID:GroupId}" | jq -r '.[0].ID')
SG_MASTER=$(aws ec2 describe-security-groups --filters Name=group-name,Values=ElasticMapReduce-Master-Private-"${ENV}"-* --query "SecurityGroups[*].{Name:GroupName,ID:GroupId}" | jq -r '.[0].ID')
SG_SLAVE=$(aws ec2 describe-security-groups --filters Name=group-name,Values=ElasticMapReduce-Slave-Private-"${ENV}"-* --query "SecurityGroups[*].{Name:GroupName,ID:GroupId}" | jq -r '.[0].ID')

STEPS=$(
  cat <<EOF
[
  {
    "Type":"CUSTOM_JAR",
    "Name":"Cleanup jars",
    "ActionOnFailure":"TERMINATE_CLUSTER",
    "Jar":"command-runner.jar",
    "Args":[
      "bash","-c",
      "sudo rm -f /usr/lib/spark/jars/spark-avro.jar"
    ]
  }

]
EOF
)

declare -A PROJECT_TO_KEYNAME
PROJECT_TO_KEYNAME["kf-strides"]="flintrock"
PROJECT_TO_KEYNAME["include"]="flintrock_include"

SUBNET=$(net_conf_extractor "subnet" "${PROJECT}" "${ENV}")

aws emr create-cluster \
  --applications Name=Hadoop Name=Spark Name=Zeppelin \
  --ec2-attributes "{\"KeyName\":\"${PROJECT_TO_KEYNAME["${PROJECT}"]}\",\"InstanceProfile\":\"${INSTANCE_PROFILE}\",\"SubnetId\":\"${SUBNET}\", \"ServiceAccessSecurityGroup\":\"${SG_SERVICE}\", \"EmrManagedMasterSecurityGroup\":\"${SG_MASTER}\", \"EmrManagedSlaveSecurityGroup\":\"${SG_SLAVE}\"}" \
  --service-role "${SERVICE_ROLE}" \
  --enable-debugging \
  --release-label emr-6.5.0 \
  --bootstrap-actions Path="s3://${BUCKET}/jobs/bootstrap-actions/enable-ssm.sh" Path="s3://${BUCKET}/jobs/bootstrap-actions/install-java11.sh" \
  --steps "${STEPS}" \
  --log-uri "s3n://${BUCKET}/jobs/elasticmapreduce/" \
  --name "Zeppelin - ${ENV}" \
  --instance-groups "[{\"InstanceCount\":${INSTANCE_COUNT},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${INSTANCE_TYPE}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]" \
  --configurations file://./spark-config.json \
  --region us-east-1