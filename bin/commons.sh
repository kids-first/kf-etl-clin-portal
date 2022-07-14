declare -A PROJECT_TO_NET_CONF
PROJECT_TO_NET_CONF["subnet_include_prd"]="subnet-0cdbe9ba6231146b5"
PROJECT_TO_NET_CONF["es_include_prd"]="https://vpc-arranger-es-service-ykxirqamjqxyiyfg2rruxusfg4.us-east-1.es.amazonaws.com"
PROJECT_TO_NET_CONF["subnet_include_qa"]="subnet-0f1161ac2ee2fba5b"
PROJECT_TO_NET_CONF["es_include_qa"]="https://vpc-include-arranger-blue-es-qa-xf3ttht4hjmxjfoh5u5x4jnw34.us-east-1.es.amazonaws.com"

PROJECT_TO_NET_CONF["subnet_kf_prd"]="subnet-todo"
PROJECT_TO_NET_CONF["es_kf_prd"]="https://vpc-arranger-es-service-todo.us-east-1.es.amazonaws.com"
PROJECT_TO_NET_CONF["subnet_kf_qa"]="subnet-todo"
PROJECT_TO_NET_CONF["es_kf_qa"]="https://vpc-include-arranger-blue-es-qa-todo.us-east-1.es.amazonaws.com"

net_conf_extractor() {
  local ITEM=$1
  local PROJECT=$2
  local ENV=$3
  echo "${PROJECT_TO_NET_CONF["${ITEM}_${PROJECT}_${ENV}"]}"
}
