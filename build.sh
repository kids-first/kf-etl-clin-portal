#!/bin/bash
set -e
set -x
mkdir -p ~/.ivy2 ~/.sbt ~/.m2 ~/.sbt_cache

echo "Cleanup project ..."
docker run --net host --rm -v $(pwd):/app/project \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/project \
    hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 \
    sbt -Duser.home=/app clean

#echo "Running tests ..."
#docker run --net host --rm -v $(pwd):/app/project \
#    -v /var/run/docker.sock:/var/run/docker.sock \
#    --user $(id -u):$(id -g) \
#    -v ~/.m2:/app/.m2 \
#    -v ~/.ivy2:/app/.ivy2 \
#    -v ~/.sbt:/app/.sbt \
#    -v ~/.sbt_cache:/app/.cache \
#    -w /app/project \
#    hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 \
#    sbt -Duser.home=/app test

echo "Generate config ..."
docker run --net host --rm -v $(pwd):/app/project \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/project \
    hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 \
    sbt -Duser.home=/app config/run

echo "Build jars ..."
docker run --net host --rm -v $(pwd):/app/project \
    --user $(id -u):$(id -g) \
    -v ~/.m2:/app/.m2 \
    -v ~/.ivy2:/app/.ivy2 \
    -v ~/.sbt:/app/.sbt \
    -v ~/.sbt_cache:/app/.cache \
    -w /app/project \
    hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 \
    sbt -Duser.home=/app fhavro_export/assembly import_task/assembly prepare_index/assembly index_task/assembly publish_task/assembly

echo "Build docker image for fhavro-export task"
docker build -t fhavro-export fhavro-export

echo "Build docker image for publish task"
docker build -t publish-task publish-task
