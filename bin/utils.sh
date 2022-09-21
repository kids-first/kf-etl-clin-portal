#!/usr/local/bin/bash

set -e
set -x
declare -A PROJECT_TO_NET_CONF
PROJECT_TO_NET_CONF["subnet_kf-strides_qa"]="subnet-0f0c909ec60b377ce"
PROJECT_TO_NET_CONF["es_kf-strides_qa"]="https://vpc-kf-arranger-blue-es-service-exwupkrf4dyupg24dnfmvzcwri.us-east-1.es.amazonaws.com"
PROJECT_TO_NET_CONF["subnet_include_qa"]="subnet-0f1161ac2ee2fba5b"
PROJECT_TO_NET_CONF["es_include_qa"]="https://vpc-include-arranger-blue-es-qa-xf3ttht4hjmxjfoh5u5x4jnw34.us-east-1.es.amazonaws.com"
PROJECT_TO_NET_CONF["subnet_include_prd"]="subnet-0cdbe9ba6231146b5"
PROJECT_TO_NET_CONF["es_include_prd"]="https://vpc-include-arranger-blue-es-qa-xf3ttht4hjmxjfoh5u5x4jnw34.us-east-1.es.amazonaws.com"

check_project() {
  case $1 in
  kf-strides | include) echo 0 ;;
  *)
    echo 1
    ;;
  esac
}

net_conf_extractor() {
  local ITEM=$1
  local PROJECT=$2
  local ENV=$3
  echo "${PROJECT_TO_NET_CONF["${ITEM}_${PROJECT}_${ENV}"]}"
}