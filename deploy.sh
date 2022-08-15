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

echo "Copy import-task.jar ..."
aws s3 cp import-task/target/scala-2.12/import-task.jar $job_dest

echo "Copy prepare-index.jar ..."
aws s3 cp prepare-index/target/scala-2.12/prepare-index.jar $job_dest

echo "Copy index-task.jar ..."
aws s3 cp index-task/target/scala-2.12/index-task.jar $job_dest

echo "Copy publish-task.jar ..."
aws s3 cp publish-task/target/scala-2.12/publish-task.jar $job_dest

echo "Copy templates ..."
aws s3 cp --recursive index-task/target/scala-2.12/classes/templates/ $template_dest

#FIXME echo "Publish docker images"
#FIXME docker push "$AWS_ACCOUNT_ID".dkr.ecr.us-east-1.amazonaws.com/etl-fhavro-export:latest
#FIXME docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/etl-fhavro-export:$GIT_COMMIT

#FIXME docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/etl-publish-task:latest
#FIXME docker push $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/etl-publish-task:$GIT_COMMIT

