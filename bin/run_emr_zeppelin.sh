#!/bin/bash

source "$(dirname "$0")/utils.sh"

usage() {
  echo "Usage: $0 [arguments]"
  echo "Run ec2 cluster with zeppelin notebook"
  echo
  echo "--project, project name (kf-strides or include)"
  echo "--bucket, bucket name"
  echo "--environment    environment"
  echo "--instance-type    instance type"
  echo "--instance-count    instance count"
  echo "--instance-profile    instance profile"
  echo "--service-role    aws service role"
  echo "--help    display usage"
  echo
  echo "Example(s):"
  echo "run_emr_zepplin --project include --environment qa --instance-type m5.8xlarge --instance-count 1 --bucket include-373997854230-datalake-qa --instance-profile include-ec2-qa-profile --service-role include-datalake-emr-qa-role"
  echo "Or"
  echo "run_emr_zeppelin.sh --project kf-strides --environment qa --instance-type m5.8xlarge --instance-count 1 --bucket kf-strides-232196027141-datalake-qa --instance-profile kf-variant-emr-ec2-qa-profile --service-role kf-variant-emr-qa-role"
  exit 1
}

PARSED_ARGUMENTS=$(getopt -a -n run_emr_zepplin -o '' --long project:,bucket:,environment:,instance-type:,instance-count:,instance-profile:,service-role:,help -- "$@")
VALID_ARGUMENTS=$?
if [ "$VALID_ARGUMENTS" != "0" ]; then
  usage
fi
eval set -- "$PARSED_ARGUMENTS"

ENV="qa"
PROJECT=$(unset)
BUCKET=$(unset)
INSTANCE_TYPE="m5.4xlarge"
INSTANCE_COUNT="1"
INSTANCE_PROFILE=$(unset)
SERVICE_ROLE=$(unset)

while :; do
  case "$1" in
  --project)
    PROJECT="$2"
    shift 2
    ;;
  --bucket)
    BUCKET=$2
    shift 2
    ;;
  --environment)
    ENV="$2"
    shift 2
    ;;
  --instance-type)
    INSTANCE_TYPE="$2"
    shift 2
    ;;
  --instance-count)
    INSTANCE_COUNT="$2"
    shift 2
    ;;
  --instance-profile)
    INSTANCE_PROFILE="$2"
    shift 2
    ;;
  --service-role)
    SERVICE_ROLE="$2"
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *)
    echo "Unexpected argument: $1"
    usage
    ;;
  esac
done

IS_PROJECT_NAME_OK=$(check_project "$PROJECT")
if [ "$IS_PROJECT_NAME_OK" -eq 1 ]; then
  echo "Project name must be equal to 'kf-strides' or 'include' but got: '${PROJECT}'"
  exit 1
fi

#if [ "${ENV}" == "prd" ] && [ "${PROJECT}" == "kf-strides" ]
#then
#  echo "This script does not support project ${PROJECT} in ${ENV}. Exiting"
#  exit 1
#fi

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

declare -A PROJECT_TO_KEYNAME=( ["kf-strides"]="flintrock" ["include"]="flintrock_include")

echo ${PROJECT_TO_KEYNAME[$PROJECT]}

SUBNET=$(net_conf_extractor "subnet" "${PROJECT}" "${ENV}")

aws emr create-cluster \
  --applications Name=Hadoop Name=Spark Name=Zeppelin \
  --ec2-attributes "{\"KeyName\":\"${PROJECT_TO_KEYNAME["${PROJECT}"]}\",\"InstanceProfile\":\"${INSTANCE_PROFILE}\",\"SubnetId\":\"${SUBNET}\", \"ServiceAccessSecurityGroup\":\"${SG_SERVICE}\", \"EmrManagedMasterSecurityGroup\":\"${SG_MASTER}\", \"EmrManagedSlaveSecurityGroup\":\"${SG_SLAVE}\"}" \
  --service-role "${SERVICE_ROLE}" \
  --enable-debugging \
  --release-label emr-6.9.0 \
  --bootstrap-actions Path="s3://${BUCKET}/jobs/bootstrap-actions/enable-ssm.sh" Path="s3://${BUCKET}/jobs/bootstrap-actions/install-java11.sh" \
  --steps "${STEPS}" \
  --log-uri "s3n://${BUCKET}/jobs/elasticmapreduce/" \
  --name "Zeppelin - ${ENV}" \
  --instance-groups "[{\"InstanceCount\":${INSTANCE_COUNT},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${INSTANCE_TYPE}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]" \
  --configurations file://./spark-config.json \
  --region us-east-1
