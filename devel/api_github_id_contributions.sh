#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: please specify Github login a 1st arg"
  exit 2
fi
if [ -z "$API_URL" ]
then
  API_URL="http://127.0.0.1:8080/api/v1"
fi
github_id="${1}"
if [ ! -z "$DEBUG" ]
then
  echo "curl -H 'Content-Type: application/json' '${API_URL}' -d'{\"api\":\"GithubIDContributions\",\"payload\":{\"github_id\":\"${github_id}\"}}'"
fi
curl -H "Content-Type: application/json" "${API_URL}" -d"{\"api\":\"GithubIDContributions\",\"payload\":{\"github_id\":\"${github_id}\"}}" 2>/dev/null | jq -rS .
