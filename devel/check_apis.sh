#!/bin/bash
curl -s -H 'Content-Type: application/json' https://devstats.cncf.io/api/v1 -d'{"api": "DevActCnt", "payload": {"project": "all", "range": "Last century", "metric": "contributions", "repository_group": "All", "country": "All", "github_id": "lukaszgryglicki"}}' | jq -r '.'
curl -s -H 'Content-Type: application/json' 'https://devstats.cncf.io/api/v1' -d'{"api":"GithubIDContributions","payload":{"github_id":"lukaszgryglicki"}}' | jq -r '.'
curl -s -H 'Content-Type: application/json' https://devstats.cncf.io/api/v1 -d'{"api":"CumulativeCounts","payload":{"project":"all","metric":"contributors"}}' | jq -r '.'
curl -s -H 'Content-Type: application/json' https://devstats.cncf.io/api/v1 -d'{"api":"CumulativeCounts","payload":{"project":"all","metric":"organizations"}}' | jq -r '.'
