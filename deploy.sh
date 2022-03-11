#!/bin/bash
set -e

env=$1
bucket="s3://include-373997854230-datalake-${env}"
job_dest="${bucket}/jobs/"
template_dest="${bucket}/templates/"

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