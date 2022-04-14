#!/bin/bash
set -e
set -x

mkdir -p ~/.ivy2 ~/.sbt ~/.m2 ~/.sbt_cache

echo "Build docker image SBT+Docker use for running tests ..."
docker build -t sbt-docker buildtools

echo "Cleanup project ..."
docker run --net host --rm -v $(pwd):/app/project \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/project \
    sbt-docker \
    sbt -Duser.home=/app clean

echo "Running tests ..."
docker run --net host --rm -v $(pwd):/app/project \
    -v /var/run/docker.sock:/var/run/docker.sock \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/project \
    sbt-docker \
    sbt -Duser.home=/app test

echo "Generate config ..."
docker run --net host --rm -v $(pwd):/app/project \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/project \
    sbt-docker \
    sbt -Duser.home=/app config/run

echo "Build jars ..."
docker run --net host --rm -v $(pwd):/app/project \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/project \
    sbt-docker \
    sbt -Duser.home=/app fhavro_export/assembly import_task/assembly prepare_index/assembly index_task/assembly publish_task/assembly

echo "Build docker image fhavro-export:$git_commit"
docker build -t $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/etl-fhavro-export:latest -t $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/etl-fhavro-export:$GIT_COMMIT  fhavro-export

echo "Build docker image publish-task:$git_commit"
docker build -t $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/etl-publish-task:latest -t $AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/etl-publish-task:$GIT_COMMIT publish-task
