#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please specify project name as a 1st arg"
  exit 1
fi
if [ -z "$API_URL" ]
then
  export API_URL="http://127.0.0.1:8080/api/v1"
fi
if [ -z "$ORIGIN" ]
then
  export ORIGIN='https://teststats.cncf.io'
fi
project="${1}"
from="${2}"
to="${3}"
if [ -z "$DEBUG" ]
then
  curl -s -H "Origin: ${ORIGIN}" -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"SiteStats\",\"payload\":{\"project\":\"${project}\"}}" | jq
else
  echo curl -i -s -H "Origin: ${ORIGIN}" -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"SiteStats\",\"payload\":{\"project\":\"${project}\"}}"
  curl -i -s -H "Origin: ${ORIGIN}" -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"SiteStats\",\"payload\":{\"project\":\"${project}\"}}"
fi
