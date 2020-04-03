#!/bin/bash
if [ -z "$API_URL" ]
then
  API_URL="http://127.0.0.1:8080/api/v1"
fi
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
project="${1}"
from="${2}"
to="${3}"
period="${4}"
metric="${5}"
repository_group="${6}"
companies="${7}"
if [ -z "$period" ]
then
  period='7 Days MA'
fi
if [ -z "$metric" ]
then
  metric='Contributions'
fi
if [ -z "$repository_group" ]
then
  repository_group='All'
fi
if [ -z "$companies" ]
then
  companies='["All"]'
fi
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"ComStatsRepoGrp\",\"payload\":{\"project\":\"${project}\",\"from\":\"${from}\",\"to\":\"${to}\",\"period\":\"${period}\",\"metric\":\"${metric}\",\"repository_group\":\"${repository_group}\",\"companies\":${companies}}}" 2>/dev/null | jq
