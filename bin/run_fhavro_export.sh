#!/bin/bash

release_id=$1
study_id=$2
env=${3:-"qa"}
instance_type="m5.4xlarge"
instance_count="1"
if [ "$env" = "prd" ]
then
  subnet="subnet-0cdbe9ba6231146b5"
  es=""
else
  subnet="subnet-0f1161ac2ee2fba5b"
  es="https://vpc-include-arranger-blue-es-qa-xf3ttht4hjmxjfoh5u5x4jnw34.us-east-1.es.amazonaws.com"
fi

steps=$(cat <<EOF
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
      "aws s3 cp s3://include-373997854230-datalake-${env}/jobs/fhavro-export.jar /home/hadoop; cd /home/hadoop; /usr/lib/jvm/java-11-amazon-corretto.x86_64/bin/java -jar fhavro-export.jar ${release_id} ${study_id} include-${env}"
    ]
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
  --name "Portal ETL - Fhavro Export - ${env} ${release_id} ${study_id}" \
  --instance-groups "[{\"InstanceCount\":${instance_count},\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"${instance_type}\",\"Name\":\"Core - 2\"},{\"InstanceCount\":1,\"EbsConfiguration\":{\"EbsBlockDeviceConfigs\":[{\"VolumeSpecification\":{\"SizeInGB\":32,\"VolumeType\":\"gp2\"},\"VolumesPerInstance\":2}]},\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"m5.xlarge\",\"Name\":\"Master - 1\"}]" \
  --scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
  --configurations file://./spark-config.json \
  --auto-terminate \
  --region us-east-1