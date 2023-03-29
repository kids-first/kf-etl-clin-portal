#!/bin/bash

source "$(dirname "$0")/utils.sh"

truncate_emr_name_if_needed() {
  local name=$1
  MAX_EMR_NAME_LENGTH=256
  if [ ${#name} -ge "$MAX_EMR_NAME_LENGTH" ]; then
    DOTS='...'
    DOTS_LENGTH="${#DOTS}"
    echo | awk -v name="$name" -v end=$((MAX_EMR_NAME_LENGTH-DOTS_LENGTH)) -v dots="$DOTS" '{print substr(name, 1, end) dots}'
  else
    echo "${name}"
  fi
}

build_fhavro_file_arg_suffix() {
  # needed to accommodate file conventions in fhavro-export resources.
  # (kf-strides vs kfdrc)
  local suffix=$1
  echo "${suffix/"kf-strides"/"kfdrc"}"
}

filter_steps() {
  local allSteps=$1
  # Comma-separated list
  local stepsToExcludes="${2:-''}"
  echo "$allSteps" | jq --arg blacklist "$stepsToExcludes" '[.[] | select(.Name as $name | $blacklist | index($name) | not)]'
}

usage() {
  echo "Usage: $0 [arguments]"
  echo "Run ETL for a given project (Kids-First or Include)"
  echo
  echo "-p, --project    project name (kf-strides or include)"
  echo "-r, --release    release id"
  echo "-s, --studies    study ids separated by a comma"
  echo "-b, --bucket    bucket name"
  echo "-e, --environment    environment"
  echo "--instance-type    instance type"
  echo "--instance-count    instance count"
  echo "--instance-profile    instance profile"
  echo "--service-role    aws service role"
  echo "--skip-steps    'Download and Run Fhavro-export,Import Task,Enrich,Prepare Index,Index File'"
  echo "-h, --help    display usage"
  echo
  echo "Example(s):"
  echo "run_etl -p include -r re_061 -s DS-COG-ALL,DS-PCGC,DS360-CHD,HTP,DSC -e qa --instance-type m5.8xlarge --instance-count 1 -b include-373997854230-datalake-qa --instance-profile include-ec2-qa-profile --service-role include-datalake-emr-qa-role"
  exit 1
}

PARSED_ARGUMENTS=$(getopt -a -n run_etl -o p:r:s:b:e:h --long project:,release:,studies:,bucket:,environment:,instance-type:,instance-count:,instance-profile:,service-role:,help,skip-steps: -- "$@")
VALID_ARGUMENTS=$?
if [ "$VALID_ARGUMENTS" != "0" ]; then
  usage
fi
eval set -- "$PARSED_ARGUMENTS"

RELEASE_ID=$(unset)
BUCKET=$(unset)
ENV=$(unset)
INSTANCE_TYPE=$(unset)
INSTANCE_COUNT=$(unset)
PROJECT=$(unset)
STUDIES=$(unset)
INSTANCE_PROFILE=$(unset)
SERVICE_ROLE=$(unset)
SKIP_STEPS=$(unset)

while :; do
  case "$1" in
  -p | --project)
    PROJECT="$2"
    shift 2
    ;;
  -s | --studies)
    STUDIES="$2"
    shift 2
    ;;
  -r | --release)
    RELEASE_ID=$2
    shift 2
    ;;
  -b | --bucket)
    BUCKET=$2
    shift 2
    ;;
  -e | --environment)
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
  --skip-steps)
    SKIP_STEPS="$2"
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

SUBNET=$(net_conf_extractor "subnet" "${PROJECT}" "${ENV}")
ES_ENDPOINT=$(net_conf_extractor "es" "${PROJECT}" "${ENV}")

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
  },
  {
    "Type":"CUSTOM_JAR",
    "Name":"Download and Run Fhavro-export",
    "ActionOnFailure":"TERMINATE_CLUSTER",
    "Jar":"command-runner.jar",
    "Args":[
      "bash","-c",
      "aws s3 cp s3://${BUCKET}/jobs/fhavro-export.jar /home/hadoop; cd /home/hadoop; /usr/lib/jvm/java-11-amazon-corretto.x86_64/bin/java -jar fhavro-export.jar ${RELEASE_ID} ${STUDIES} $(build_fhavro_file_arg_suffix "${PROJECT}"-"${ENV}")"
    ]
  },
 {
    "Args": [
      "spark-submit",
      "--deploy-mode",
      "client",
      "--class",
      "bio.ferlab.dataservice.etl.DataserviceExportApp",
      "s3a://${BUCKET}/jobs/dataservice-export.jar",
      "config/${ENV}-${PROJECT}.conf",
      "default",
      "${RELEASE_ID}",
      "${STUDIES}"
    ],
    "Type": "CUSTOM_JAR",
    "ActionOnFailure": "TERMINATE_CLUSTER",
    "Jar": "command-runner.jar",
    "Properties": "",
    "Name": "Export Dataservice"
  },
  {
    "Args": [
      "spark-submit",
      "--deploy-mode",
      "client",
      "--class",
      "bio.ferlab.fhir.etl.ImportTask",
      "s3a://${BUCKET}/jobs/import-task.jar",
      "config/${ENV}-${PROJECT}.conf",
      "default",
      "${RELEASE_ID}",
      "${STUDIES}"
    ],
    "Type": "CUSTOM_JAR",
    "ActionOnFailure": "TERMINATE_CLUSTER",
    "Jar": "command-runner.jar",
    "Properties": "",
    "Name": "Import Task"
  },
  {
       "Args": [
         "spark-submit",
         "--deploy-mode",
         "client",
         "--class",
         "bio.ferlab.etl.enrich.Enrich",
         "s3a://${BUCKET}/jobs/enrich.jar",
          "config/${ENV}-${PROJECT}.conf",
         "default",
         "histology",
         "${STUDIES}"
       ],
       "Type": "CUSTOM_JAR",
       "ActionOnFailure": "TERMINATE_CLUSTER",
       "Jar": "command-runner.jar",
       "Properties": "",
       "Name": "Enrich"
 },
 {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--class",
       "bio.ferlab.fhir.etl.PrepareIndex",
       "s3a://${BUCKET}/jobs/prepare-index.jar",
        "config/${ENV}-${PROJECT}.conf",
       "default",
       "all",
       "${STUDIES}"
     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "TERMINATE_CLUSTER",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Prepare Index"
   },
   {
     "Args": [
       "spark-submit",
       "--deploy-mode",
       "client",
       "--class",
       "bio.ferlab.fhir.etl.IndexTask",
       "s3a://${BUCKET}/jobs/index-task.jar",
       "${ES_ENDPOINT}",
       "443",
       "${RELEASE_ID}",
       "${STUDIES}",
       "study_centric",
       "config/${ENV}-${PROJECT}.conf"
     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "TERMINATE_CLUSTER",
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
       "s3a://${BUCKET}/jobs/index-task.jar",
       "${ES_ENDPOINT}",
       "443",
       "${RELEASE_ID}",
       "${STUDIES}",
       "participant_centric",
        "config/${ENV}-${PROJECT}.conf"
     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "TERMINATE_CLUSTER",
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
       "s3a://${BUCKET}/jobs/index-task.jar",
       "${ES_ENDPOINT}",
       "443",
       "${RELEASE_ID}",
       "${STUDIES}",
       "file_centric",
       "config/${ENV}-${PROJECT}.conf"
     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "TERMINATE_CLUSTER",
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
       "s3a://${BUCKET}/jobs/index-task.jar",
       "${ES_ENDPOINT}",
       "443",
       "${RELEASE_ID}",
       "${STUDIES}",
       "biospecimen_centric",
        "config/${ENV}-${PROJECT}.conf"
     ],
     "Type": "CUSTOM_JAR",
     "ActionOnFailure": "TERMINATE_CLUSTER",
     "Jar": "command-runner.jar",
     "Properties": "",
     "Name": "Index Biospecimen"
   }
EOF
)

if [ "${PROJECT}" = 'kf-strides' ]; then
STEPS=$(
  cat <<EOF
  $STEPS
]
EOF
)
else #include
STEPS=$(
  cat <<EOF
   $STEPS,
   {
     "Type":"CUSTOM_JAR",
     "Name":"Publish",
     "ActionOnFailure":"TERMINATE_CLUSTER",
     "Jar":"command-runner.jar",
     "Args":[
       "bash","-c",
       "aws s3 cp s3://${BUCKET}/jobs/publish-task.jar /home/hadoop; cd /home/hadoop; /usr/lib/jvm/java-11-amazon-corretto.x86_64/bin/java -jar publish-task.jar ${ES_ENDPOINT} 443 ${RELEASE_ID} ${STUDIES} all"
     ]
   }
]
EOF
)
STEPS_TO_AVOID_WHEN_INCLUDE="Export Dataservice"
STEPS="$(filter_steps "$STEPS" "$STEPS_TO_AVOID_WHEN_INCLUDE")"
fi

# Remove all steps before $SKIP_STEPS if it exists - Allows to skip tests if needed.
if [ -n "$SKIP_STEPS" ]; then
  STEPS="$(filter_steps "$STEPS" "$SKIP_STEPS")"
fi

SG_SERVICE=$(aws ec2 describe-security-groups --filters Name=group-name,Values=ElasticMapReduce-ServiceAccess-"${ENV}"-* --query "SecurityGroups[*].{Name:GroupName,ID:GroupId}" | jq -r '.[0].ID')
SG_MASTER=$(aws ec2 describe-security-groups --filters Name=group-name,Values=ElasticMapReduce-Master-Private-"${ENV}"-* --query "SecurityGroups[*].{Name:GroupName,ID:GroupId}" | jq -r '.[0].ID')
SG_SLAVE=$(aws ec2 describe-security-groups --filters Name=group-name,Values=ElasticMapReduce-Slave-Private-"${ENV}"-* --query "SecurityGroups[*].{Name:GroupName,ID:GroupId}" | jq -r '.[0].ID')


#Once the emr cluster is successfully created, if one needs to access the cluster then:
#  - grab the id instance (for example, in the aws console);
#  - run this command: aws ssm start-session --target <INSTANCE ID>;
EMR_NAME=$(truncate_emr_name_if_needed "Portal ETL - All Steps - ${ENV} - ${RELEASE_ID} - ${STUDIES}")
aws emr create-cluster \
  --applications Name=Hadoop Name=Spark \
  --ec2-attributes "{\"InstanceProfile\":\"${INSTANCE_PROFILE}\",\"SubnetId\":\"${SUBNET}\", \"ServiceAccessSecurityGroup\":\"${SG_SERVICE}\", \"EmrManagedMasterSecurityGroup\":\"${SG_MASTER}\", \"EmrManagedSlaveSecurityGroup\":\"${SG_SLAVE}\"}" \
  --service-role "${SERVICE_ROLE}" \
  --enable-debugging \
  --release-label emr-6.9.0 \
  --bootstrap-actions Path="s3://${BUCKET}/jobs/bootstrap-actions/enable-ssm.sh" Path="s3://${BUCKET}/jobs/bootstrap-actions/install-java11.sh" \
  --steps "${STEPS}" \
  --log-uri "s3n://${BUCKET}/jobs/elasticmapreduce/" \
  --name "${EMR_NAME}" \
  --instance-groups "[{\"InstanceCount\":${INSTANCE_COUNT},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"${INSTANCE_TYPE}\",\"Name\":\"Master - 1\"}]" \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --configurations file://./spark-config.json \
  --auto-terminate \
  --region us-east-1