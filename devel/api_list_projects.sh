#!/bin/bash
if [ -z "$API_URL" ]
then
  API_URL="http://127.0.0.1:8080/api/v1"
fi
project="${1}"
if [ ! -z "$RAW" ]
then
  curl -H "Content-Type: application/json" "${API_URL}" -d'{"api":"ListProjects"}'
else
  curl -H "Content-Type: application/json" "${API_URL}" -d'{"api":"ListProjects"}' 2>/dev/null | jq
fi
