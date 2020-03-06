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
raw=''
if [ ! -z "$2" ]
then
  raw="${2}"
fi
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"Countries\",\"payload\":{\"project\":\"${project}\",\"raw\":\"${raw}\"}}" 2>/dev/null | jq
