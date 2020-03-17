#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please specify project name as a 1st arg"
  exit 1
fi
if [ -z "$API_URL" ]
then
  API_URL="http://127.0.0.1:8080/api/v1"
fi
project="${1}"
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"Companies\",\"payload\":{\"project\":\"${project}\"}}" 2>/dev/null | jq
