#!/bin/bash
# value a|b means a in weekly mode, b in monthly mode
# MONTHLY=1 (use 4-weeks - 28 days schedule instead of weekly one).
# KUBERNETES_HOURS=24|36 (reserve this amount of time for Kubernetes project sync [3,30|48]).
# ALL_HOURS=20|36 (reserve this amount of time for All CNCF project sync [3,30|48]).
# GHA_OFFSET=4 (start at HH:04, to ensure GHA archives are already saved [2,10]).
# SYNC_HOURS=2 (ensure syncing projects every 2 hours, only 1, 2 and 3 values are supported)
# OFFSET_HOURS=-4 (we assume half of weekend is Sun 3 AM, and assume USA tz -7 (3-7=-4), [-84,84])
# ALWAYS_PATCH=1 (skip checking for difference and always call kubectl patch)
# NEVER_PATCH=1 (do not execute kubectl patch - preview/dry mode)
# ONLY_ENV=1 (only patch CJs env variables)
# SKIP_AFFS_ENV=1 (skip patching env for affiliations cron jobs)
# SKIP_SYNC_ENV=1 (skip patching env for affiliations cron jobs)
# PATCH_ENV='AffSkipTemp,MaxHist,SkipAffsLock,AffsLockDB,NoDurable,DurablePQ,MaxRunDuration,SkipGHAPI,SkipGetRepos,NCPUs'
# ONLY_SUSPEND=1 (only process suspend data)
# SUSPEND_ALL=1 (suspend all cronjobs)
# NO_SUSPEND_H=1 (do not process (un)suspend for hourly sync crons
# NO_SUSPEND_A=1 (do not process (un)suspend for affiliations crons
# DEBUG=1 - more verbose output
# Examples:
# ./splitcrons ../devstats-helm/devstats-helm/values.yaml new-values.yaml
# MONTHLY=1 DEBUG=1 ./splitcrons devstats-helm/values.yaml new-values.yaml
# ALWAYS_PATCH=1 MONTHLY=1 DEBUG=1 ./splitcronsa devstats-helm/values.yaml new-values.yaml
# PATCH_ENV=NCPUs ALWAYS_PATCH=1 MONTHLY=1 DEBUG=1 ./splitcronsa devstats-helm/values.yaml new-values.yaml
# PATCH_ENV=NCPUs ALWAYS_PATCH=1 MONTHLY=1 DEBUG=1 ./splitcrons devstats-helm/values.yaml new-values.yaml
# MONTHLY=1 ONLY_SUSPEND=1 ./splitcrons devstats-helm/values.yaml new-values.yaml
./splitcrons devstats-helm/values.yaml new-values.yaml && echo "Now update devstats-helm/values.yaml with new-values.yaml"
