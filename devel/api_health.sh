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
curl http://127.0.0.1:8080/api/v1 -d"xyz" 2>/dev/null | jq
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"health\",\"payload\":{\"project\":\"${project}\"}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"Health\",\"payloada\":{\"project\":\"${project}\"}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"Health\",\"payload\":{\"projecta\":\"${project}\"}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"Health\",\"payload\":{\"project\":{\"obj\":\"val\"}}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"Health\",\"payload\":{\"project\":\"${project}xx\"}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"Health\",\"payload\":{\"project\":\"${project}\"}}" 2>/dev/null | jq
