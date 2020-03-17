#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please specify project name as a 1st arg"
  exit 2
fi
if [ -z "$API_URL" ]
then
  API_URL="http://127.0.0.1:8080/api/v1"
fi
project="${1}"
range="${2}"
metric="${3}"
if [ -z "$range" ]
then
  range='Last decade'
fi
if [ -z "$metric" ]
then
  metric='Contributions'
fi
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"CompaniesTable\",\"payload\":{\"project\":\"${project}\",\"range\":\"${range}\",\"metric\":\"${metric}\"}}" 2>/dev/null | jq
