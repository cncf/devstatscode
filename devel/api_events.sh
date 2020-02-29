#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please specify project name as a 1st arg"
  exit 1
fi
if [ -z "$2" ]
then
  echo "$0: please specify timestamp from as a 2nd arg"
  exit 2
fi
if [ -z "$3" ]
then
  echo "$0: please specify timestamp to as a 3rd arg"
  exit 3
fi
if [ -z "$API_URL" ]
then
  API_URL="http://127.0.0.1:8080/api/v1"
fi
project="${1}"
from="${2}"
to="${3}"
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"Events\",\"payload\":{\"project\":\"${project}\",\"from\":\"${from}\",\"to\":\"${to}\"}}" 2>/dev/null | jq
