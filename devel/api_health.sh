#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please specify project name as a 1st arg"
  exit 2
fi
project="${1}"
curl http://127.0.0.1:8080/api/v1 -d"xyz" 2>/dev/null | jq
curl -H "Content-Type: application/json" http://127.0.0.1:8080/api/v1 -d"{\"api\":\"health\",\"payloada\":{\"project\":\"${project}\"}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" http://127.0.0.1:8080/api/v1 -d"{\"api\":\"health\",\"payload\":{\"projecta\":\"${project}\"}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" http://127.0.0.1:8080/api/v1 -d"{\"api\":\"health\",\"payload\":{\"project\":{\"obj\":\"val\"}}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" http://127.0.0.1:8080/api/v1 -d"{\"api\":\"health\",\"payload\":{\"project\":\"${project}xx\"}}" 2>/dev/null | jq
curl -H "Content-Type: application/json" http://127.0.0.1:8080/api/v1 -d"{\"api\":\"health\",\"payload\":{\"project\":\"${project}\"}}" 2>/dev/null | jq
