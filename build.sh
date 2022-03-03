#!/bin/bash
set -e

sbt clean
sbt config/run
sbt fhavro_export/assembly import_task/assembly prepare_index/assembly index_task/assembly publish_task/assembly

