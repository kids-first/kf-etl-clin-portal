#!/bin/bash
set -e

if [ -z "${AWS_ACCOUNT_NAME}" ] || [ -z "${AWS_ACCOUNT_ID}" ]
then
    echo "Environment variables 'AWS_ACCOUNT_NAME' and 'AWS_ACCOUNT_ID' must both be defined to construct bucket name. Exiting."
    exit 1
fi

set +x
aws ecr get-login-password --region us-east-1 | docker login -u AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com
set -x

env=${1:-'qa'}
bucket="${AWS_ACCOUNT_NAME}-${AWS_ACCOUNT_ID}-datalake-${env}"
job_dest="s3://${bucket}/jobs/"
template_dest="s3://${bucket}/templates/"

echo "Copy fhavro-export.jar ..."
aws s3 cp fhavro-export/target/scala-2.13/fhavro-export.jar $job_dest

echo "Copy etl.jar ..."
aws s3 cp etl/target/scala-2.12/etl.jar $job_dest

echo "Copy templates ..."
aws s3 cp --recursive etl/target/scala-2.12/classes/templates/ $template_dest

