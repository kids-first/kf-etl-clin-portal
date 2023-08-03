declare -A PROJECT_TO_NET_CONF=(
  ["subnet_kf-strides_qa"]="subnet-0f0c909ec60b377ce"
  ["es_kf-strides_qa"]="https://vpc-kf-arranger-blue-es-service-exwupkrf4dyupg24dnfmvzcwri.us-east-1.es.amazonaws.com"
  ["subnet_kf-strides_prd"]="subnet-00aab84919d5a44e2"
  ["es_kf-strides_prd"]="https://vpc-kf-arranger-blue-es-prd-4gbc2zkvm5uttysiqkcbzwxqeu.us-east-1.es.amazonaws.com"
  ["subnet_include_qa"]="subnet-0f1161ac2ee2fba5b"
  ["es_include_qa"]="https://vpc-include-arranger-blue-es-qa-xf3ttht4hjmxjfoh5u5x4jnw34.us-east-1.es.amazonaws.com"
  ["subnet_include_prd"]="subnet-0cdbe9ba6231146b5"
  ["es_include_prd"]="https://vpc-arranger-es-service-ykxirqamjqxyiyfg2rruxusfg4.us-east-1.es.amazonaws.com"
)

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

SUBNET=$(net_conf_extractor "subnet" "kf-strides" "qa")