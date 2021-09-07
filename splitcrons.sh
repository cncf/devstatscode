#!/bin/bash
# KUBERNETES_HOURS=18 (reserve this amount of time for Kubernetesproject sync).
# ALL_HOURS=18 (reserve this amount of time for All CNCF project sync).
# GHA_OFFSET=5 (start at HH:05, to ensure GHA archives are already saved).
# SYNC_HOURS=2 (ensure syncing projects every 2 hours)
./splitcrons ../devstats-helm/devstats-helm/values.yaml new-crons.sh
