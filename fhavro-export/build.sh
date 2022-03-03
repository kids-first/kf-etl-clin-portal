#!/bin/bash
set -e

# sbt assembly
docker run -ti --rm -v $(pwd):/app/fhavro-export-etl \
    -v ~/.m2:/root/.m2 \
    -v ~/.ivy2:/root/.ivy2 \
    -v ~/.sbt:/root/.sbt \
    -w /app/fhavro-export-etl hseeberger/scala-sbt:11.0.13_1.6.1_2.13.7 \
    sbt clean assembly

docker build -t fhavro-export .