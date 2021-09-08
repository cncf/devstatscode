#!/bin/bash
# KUBERNETES_HOURS=15 (reserve this amount of time for Kubernetes project sync [3,30]).
# ALL_HOURS=12 (reserve this amount of time for All CNCF project sync [3,30]).
# GHA_OFFSET=4 (start at HH:04, to ensure GHA archives are already saved [2,10]).
# SYNC_HOURS=2 (ensure syncing projects every 2 hours, only 1, 2 and 3 values are supported)
# OFFSET_HOURS=-4 (we assume half of weekend is Sun 3 AM, and assume USA tz -7 (3-7=-4), [-84,84])
# ALWAYS_PATCH=1 (skip checking for difference and always call kubectl patch)
# NEVER_PATCH=1 (do not execute kubectl patch - preview/dry mode)
# ./splitcrons ../devstats-helm/devstats-helm/values.yaml new-values.yaml
./splitcrons values.yaml new-values.yaml
