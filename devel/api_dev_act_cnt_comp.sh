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
repository_group="${4}"
country="${5}"
companies="${6}"
github_id="${7}"
if [ -z "$range" ]
then
  range='Last decade'
fi
if [ -z "$metric" ]
then
  metric='Contributions'
fi
if [ -z "$repository_group" ]
then
  repository_group='All'
fi
if [ -z "$country" ]
then
  country='All'
fi
if [ -z "$companies" ]
then
  companies='["All"]'
fi
if [ -z "$github_id" ]
then
  github_id=''
fi
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"DevActCntComp\",\"payload\":{\"project\":\"${project}\",\"range\":\"${range}\",\"metric\":\"${metric}\",\"repository_group\":\"${repository_group}\",\"country\":\"${country}\",\"companies\":${companies},\"github_id\":\"${github_id}\",\"bg\":\"${BG}\"}}" 2>/dev/null | jq -rS .
