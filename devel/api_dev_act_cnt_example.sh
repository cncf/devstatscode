#!/bin/bash
API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' 'janetkuo'
API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' ''
./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' 'janetkuo'
./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' ''

./devel/api_dev_act_cnt.sh kubernete 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' 'janetkuo'
./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - noww' 'GitHub Events' 'SIG Apps' 'United States' 'janetkuo'
./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Eventsa' 'SIG Apps' 'United States' 'janetkuo'
./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Appsa' 'United States' 'janetkuo'
./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United Statesa' 'janetkuo'
./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' 'not_exist_for_sure'

API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernete 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' 'janetkuo'
API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - noww' 'GitHub Events' 'SIG Apps' 'United States' 'janetkuo'
API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Eventsa' 'SIG Apps' 'United States' 'janetkuo'
API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Appsa' 'United States' 'janetkuo'
API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United Statesa' 'janetkuo'
API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' 'not_exist_for_sure'

API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernete 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' 'janetkuo'
API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - noww' 'GitHub Events' 'SIG Apps' 'United States' 'janetkuo'
API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Eventsa' 'SIG Apps' 'United States' 'janetkuo'
API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Appsa' 'United States' 'janetkuo'
API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United Statesa' 'janetkuo'
API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt.sh kubernetes 'v1.17.0 - now' 'GitHub Events' 'SIG Apps' 'United States' 'not_exist_for_sure'

./devel/api_dev_act_cnt_comp.sh kubernetes 'v1.17.0 - now' 'Reviews' 'SIG Apps' 'All' '["Google", "Red Hat"]' 'janetkuo'
./devel/api_dev_act_cnt_comp.sh all 'Last month' 'PRs' 'Prometheus' 'All' '["Google", "Red Hat"]'

API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt_comp.sh kubernetes 'v1.17.0 - now' 'Reviews' 'SIG Apps' 'All' '["Google", "Red Hat"]' 'janetkuo'
API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt_comp.sh all 'Last month' 'PRs' 'Prometheus' 'All' '["Google", "Red Hat"]'
API_URL='https://devstats.cncf.io/api/v1' ./devel/api_dev_act_cnt_comp.sh all

BG=1 API_URL="https://teststats.cncf.io/api/v1" ./devel/api_dev_act_cnt.sh kubernetes 'range:2021-08-10,2021-08-15' 'Contributions' 'SIG Apps' 'United States' ''
BG=1 API_URL="https://teststats.cncf.io/api/v1" ./devel/api_dev_act_cnt_repos.sh kubernetes 'range:2021-08-10,2021-08-15' 'Commits' 'kubernetes/test-infra' 'United States' ''
BG=1 API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt_comp.sh kubernetes 'range:2021-08-10,2021-08-15' 'Reviews' 'SIG Apps' 'All' '["Google", "Red Hat"]' ''
BG=1 API_URL='https://teststats.cncf.io/api/v1' ./devel/api_dev_act_cnt_comp_repos.sh kubernetes 'range:2021-08-10,2021-08-15' 'Reviews' 'kubernetes/test-infra' 'All' '["Google", "Red Hat"]' ''
