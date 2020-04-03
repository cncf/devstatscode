# API documentation

API is available on `https://devstats.cncf.io/api/v1`. This is a standard REST API that expects JSON payloads and returns JSON data.

You can see how to call example `Health` API call [here](https://github.com/cncf/devstatscode/blob/master/devel/api_health.sh).

All API calls that result in error returns the following JSON response: `{"error": "some error message"}`.

List of APIs:

- `Health`: `{"api": "Health", "payload": {"project": "projectName"}}`.
  - Arguments: `projectName` for example: `kubernetes`, `Kuberentes`, `gRPC`, `grpc`, `all`, `All CNCF`.
  - Returns: `{"project": "projectName", "db_name": "projectDB", "events": int}`, `events` is the total number of all GitHub events that are recorded for given project.
  - Result contains the number of events present for the specified project.
  - Example API call (see last line of that script): `./devel/api_health.sh kubernetes`.

- `ListAPIs`: `{"api": "ListAPIs"}`.
  - Returns: `{"apis":["DevActCntRepoGrp","Health","Events","ListAPIs",...]}` - list of all possible APIs.
  - Example API call: `./devel/api_list_apis.sh`.

- `ListProjects`: `{"api": "ListProjects"}`.
  - Returns: `{"projects":["Kubernetes","Prometheus","All CNCF",...]}` - list of all possible projects.
  - Example API call: `[RAW=1] ./devel/api_list_projects.sh`.

- `RepoGroups`: `{"api": "RepoGroups", "payload": {"project": "projectName", "raw": "1"}}`.
  - Arguments:
    - `projectName`: see `Health` API.
    - `raw`: optional (but must be string if used, for example "1") - will return internal repository groups names as used in actual DB filters.
  - Returns: `{"project":"all","db_name":"allprj","repo_groups":["SPIFFE","CloudEvents",...]}`.
  - Result contains all possible repository groups defined in the specified project.
  - Example API call: `./devel/api_repo_groups.sh kubernetes [1]`.

- `Ranges`: `{"api": "Ranges", "payload": {"project": "projectName", "raw": "1"}}`.
  - Arguments:
    - `projectName`: see `Health` API.
    - `raw`: see `RepoGroups` API.
  - Returns: `{"project":"all","db_name":"allprj","ranges":["Last decade","Since graduation",...]}`.
  - Result contains all possible date ranges for the specified project: Last xyz, versionX - versionY, Before CNCF join, after CNCF join, since graduation and so on.
  - Example API call: `./devel/api_ranges.sh kubernetes`.
  - Example API call: `./devel/api_ranges.sh all 1`.

- `Countries`: `{"api": "Countries", "payload": {"project": "projectName", "raw": "1"}}`.
  - Arguments:
    - `projectName`: see `Health` API.
    - `raw`: see `RepoGroups` API.
  - Returns: `{"project":"all","db_name":"allprj","countries":["Poland","United States",...]}`.
  - Example API call: `./devel/api_countries.sh Kubernetes`.
  - Example API call: `./devel/api_countries.sh 'All CNCF' 1`.

- `Companies`: `{"api": "Companies", "payload": {"project": "projectName"}}`.
  - Arguments:
    - `projectName`: see `Health` API.
  - Returns: `{"project":"all","db_name":"allprj","companies":["Google","Red Hat","Independent",...]}`.
  - Result contains top companies contributing in the specified project.
  - Example API call: `./devel/api_companies.sh 'All CNCF'`.

- `Events`: `{"api": "Events", "payload": {"project": "projectName", "from": "2020-02-29", "to": "2020-03-01"}}`.
  - Arguments:
    - `projectName`: see `Health` API.
    - `from`: datetime from (string that Postgres understands)
    - `to`: datetime to (example '2020-02-01 11:00:00').
  - Returns:
  ```
  {
    "project": "kubernetes",
    "db_name": "gha",
    "from": "2020-03-01",
    "to": "2020-04-01",
    "timestamps": [
      "2020-02-29T00:00:00Z",
      "2020-02-29T01:00:00Z",
      ...
    ],
    "values": [
      441,
      170,
      ...
    ]
  }
  ```
  - Result contains hourly events counts for the specified period in the specified date range.
  - Example API call: `./devel/api_events.sh kubernetes 2020-01-01 2021-01-01`.

- `Repos`: `{"api": "Repos", "payload": {"project": "projectName", "repository_group": ["Other", "Not specified", "SIG Apps"]}}`.
  - Arguments:
    - `projectName`: see `Health` API.
    - `repository_group`: array of strings, some values are special: `"Not specified"` returns repositories without repository group defined.
      - If you specify one element array `["All"]` - data for all repositories will be returned. If there are more than 1 items `"All"` has no special meaning then.
  - Returns: `{"project":"kubernetes","db_name":"gha","repo_groups":["Other","Not specified",...],"repos":["kubernetes/application-images","kubernetes/example-not-specified",...]}`.
  - Result contains projects repositories - repository groups configuration information.
  - Example API call: `./devel/api_repos.sh all '["Harbor", "OPA"]'`.

- `CompaniesTable`: `{"api": "CompaniesTable", "payload": {"project": "projectName", "range": "range", "metric": "metric"}}`.
  - Arguments: (like in "Companies Table" DevStats dashboards).
    - `projectName`: see `Health` API.
    - `range`: value from `Range` drop-down in DevStats page, for example: `Last year`, `v1.17.0 - now`.
    - `metric`: value from `Metric` drop-down in DevStats page, for example: `Contributions`, `Issues`, `PRs`.
  - Returns:
  ```
  {
    "project": "all",
    "db_name": "allprj",
    "range": "Last week",
    "metric": "Commit commenters",
    "rank": [
      0,
      1,
      2,
      3,
      4,
      5,
      6,
      7
    ],
    "company": [
      "All",
      "Synadia",
      "Google",
      "Grafana Labs",
      "MayaData",
      "Postmates",
      "The Scale Factory",
      "Transit"
    ],
    "number": [
      16,
      2,
      1,
      1,
      1,
      1,
      1,
      1
    ]
  }
  ```
  - Result contains data in the same format as "Companies Table" DevStats dashboard for the given project.
  - Example API call: `./devel/api_companies_table.sh kubernetes 'v1.16.0 - v1.17.0' 'Contributors'`.

- `ComContribRepoGrp`: `{"api": "ComContribRepoGrp", "payload": {"project": "projectName", "from": "YYYY-MM-DD", "to": "YYYY-MM-DD", "period": "7 Days MA", "repository_group": "repoGroupName"}}`.
  - Arguments:
    - `projectName`: see `Health` API.
    - `from`: datetime from (string that Postgres understands)
    - `to`: datetime to (example '2020-02-01 11:00:00').
    - `period`: value from `Period` drop-down in Companies contributing in repository groups page, for example: `7 Days MA`, `28 Days MA`, `Week`, `Month`, `Quarter`.
    - `repository_group`: value from `Repository group` drop-down in DevStats pages, for example: `All`, `Kubernetes`, `SIG Apps`.
  - Returns:
  ```
  {
    "project": "all",
    "db_name": "allprj",
    "period": "Month",
    "repository_group": "All",
    "companies": [
      755
    ],
    "developers": [
      7394
    ],
    "companies_timestamps": [
      "2020-02-01T00:00:00Z"
    ],
    "developers_timestamps": [
      "2020-02-01T00:00:00Z"
    ]
  }
  ```
  - Result contains data in the same format as "Companies contributing in Repository Groups" DevStats dashboard for the given project.
  - Example API call: `./devel/api_com_contrib_repo_grp.sh kubernetes 2020-01-01 2020-04-01 'SIG Apps' Week`.

- `DevActCntRepoGrp`: `{"api": "DevActCntRepoGrp", "payload": {"project": "projectName", "range": "range", "metric": "metric", "repository_group": "repository_group", "country": "country", "github_id": "id"}}`.
  - Arguments: (like in "Developer Activity Counts by Repository Group" DevStats dashboards).
    - `projectName`: see `Health` API.
    - `range`: value from `Range` drop-down in DevStats page, for example: `Last year`, `v1.17.0 - now`.
    - `metric`: value from `Metric` drop-down in DevStats page, for example: `Contributions`, `Issues`, `PRs`.
    - `repository_group`: value from `Repository group` drop-down in DevStats pages, for example: `All`, `Kubernetes`, `SIG Apps`.
    - `country`: value from `Country` drop-down in DevStats page, for example: `All`, `United States`, `Poland`.
    - `github_id`: can be empty but must be provided in request payload. If non-empty - returns data for GitHub login/ID matching this parameter.
  - Returns:
  ```
  {
    "project": "kubernetes",
    "db_name": "gha",
    "range": "v1.17.0 - now",
    "metric": "GitHub Events",
    "repository_group": "SIG Apps",
    "country": "United States",
    "github_id": "",
    "filter": "series:hdev_eventssigappsunitedstates period:a_37_n",
    "rank": [
      1,
      2,
      3
    ],
    "login": [
      "mortent",
      "janetkuo",
      "JanetKuo"
    ],
    "number": [
      48,
      43,
      43
    ]
  }
  ```
  - Result contains data in the same format as "Developer Activity Counts by Repository Group" DevStats dashboard for the given project.
  - Example API call: `./devel/api_dev_act_cnt_repo_grp.sh all 'Last year' Contributions Prometheus 'United States'`.
  -Example API call: `./devel/api_dev_act_cnt_repo_grp.sh kubernetes 'v1.17.0 - v1.18.0' 'GitHub Events' 'SIG Apps' 'United States' idvoretskyi`.

- `DevActCntComp`: `{"api": "DevActCntComp", "payload": {"project": "projectName", "range": "range", "metric": "metric", "repository_group": "repository_group", "country": "country", "companies": ["Google", "Red Hat", ...], "github_id": "id"}}`.
  - Arguments: (like in "Developer Activity Counts by Companies" DevStats dashboards).
    - `projectName`: see `Health` API.
    - `range`: value from `Range` drop-down in DevStats page, for example: `Last year`, `v1.17.0 - now`.
    - `metric`: value from `Metric` drop-down in DevStats page, for example: `Contributions`, `Issues`, `PRs`.
    - `repository_group`: value from `Repository group` drop-down in DevStats pages, for example: `All`, `Kubernetes`, `SIG Apps`.
    - `companies`: values from `Companies` drop-down in DevStats pages, for example: ["Google", "Red Hat", "Independent"] - array of companies selections.
      - If you specify one element array `["All"]` - data for all companies will be returned. If there are more than 1 items `"All"` has no special meaning then.
    - `country`: value from `Country` drop-down in DevStats page, for example: `All`, `United States`, `Poland`.
    - `github_id`: can be empty but must be provided in request payload. If non-empty - returns data for GitHub login/ID matching this parameter.
  - Returns:
  ```
  {
    "project": "all",
    "db_name": "allprj",
    "range": "Last day",
    "metric": "Commits",
    "repository_group": "CNCF",
    "country": "All",
    "companies": [
      "CNCF"
    ],
    "github_id": "",
    "rank": [
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8
    ],
    "login": [
      "taylorwaggoner",
      "alexcontini",
      "caniszczyk",
      "lukaszgryglicki",
      "denverwilliams",
      "dankohn",
      "lucperkins",
      "nikhita"
    ],
    "company": [
      "CNCF",
      "CNCF",
      "CNCF",
      "CNCF",
      "CNCF",
      "CNCF",
      "CNCF",
      "CNCF"
    ],
    "number": [
      9,
      7,
      6,
      4,
      3,
      2,
      1,
      1
    ]
  }
  ```
  - Result contains data in the same format as "Developer Activity Counts by Companies" DevStats dashboard for the given project.
  - Example API call: `./devel/api_dev_act_cnt_comp.sh kubernetes 'Last decade' 'PRs' 'SIG Apps' 'United States' '["Google", "Amazon"]'`.

- `ComStatsRepoGrp`: `{"api":"ComStatsRepoGrp","payload":{"project":"projectName","from":"2019-01-01","to":"2020-01-01","period":"Day","metric":"Contributors","repository_group":"SIG Apps","companies":["Google", "Red Hat", ...]}}`.
  - Arguments: (like in "Companies Statistics by Repository Group" DevStats dashboards).
    - `projectName`: see `Health` API. Example: `Kubernetes`, `All`, `Harbor`.
    - `from`: datetime from (string that Postgres understands)
    - `to`: datetime to (example '2020-02-01 11:00:00').
    - `period`: value from `Period` drop-down in Companies Statistics by Repository Group page, for example: `Day`, `7 Days MA`, `Week`, `Month`, `Quarter`, `Year`.
    - `metric`: value from `Metric` drop-down in Companies Statistics by Repository Group page, for example: `Contributions`, `Contributors`, `All activity`, `Active authors`.
    - `repository_group`: value from `Repository group` drop-down in DevStats pages, for example: `All`, `Kubernetes`, `SIG Apps`.
    - `companies`: values from `Companies` drop-down in DevStats pages, for example: ["Google", "Red Hat", "Independent"] - array of companies selections.
      - If you specify one element array `["All"]` - data for all companies will be returned. If there are more than 1 items `"All"` has no special meaning then.
  - Returns:
  ```
  {
    "project": "all",
    "db_name": "allprj",
    "period": "Week",
    "metric": "Contributors",
    "repository_group": "Kubernetes",
    "companies": [
      "Google",
      "Red Hat",
      "VMware",
      "Independent"
    ],
    "from": "2020-03-01",
    "to": "2020-05-01",
    "values": [
      {
        "Google": 102,
        "Independent": 16,
        "Red Hat": 59,
        "VMware": 38
      },
      {
        "Google": 88,
        "Independent": 21,
        "Red Hat": 58,
        "VMware": 34
      },
      {
        "Google": 101,
        "Independent": 20,
        "Red Hat": 52,
        "VMware": 34
      }
    ],
    "timestamps": [
      "2020-03-02T00:00:00Z",
      "2020-03-09T00:00:00Z",
      "2020-03-23T00:00:00Z"
    ]
  }
  ```
  - Result contains data in the same format as "Companies Statistics by Repository Group" DevStats dashboard for the given project.
  - Example API call: `./devel/api_com_stats_repo_grp.sh all 2019-01-01 2020-05-01 Week 'Contributors' Kubernetes '["Google", "Red Hat", "VMware", "Independent"]'`


# Local API deployment and testing

- Start local API server via: `make; PG_PASS=... PG_PASS_RO=... PG_USER_RO=... PG_HOST_RO=127.0.0.1 ./api`.
- Call Health API: `./devel/api_health.sh kubernetes`.
- Call Developer Activity Counts Repository Groups API: `./devel/api_dev_act_cnt_repo_grp.sh kubernetes 'v1.17.0 - v1.18.0' 'GitHub Events' 'SIG Apps' 'United States' ''`.
- Manual `curl`: `curl -H "Content-Type: application/json" http://127.0.0.1:8080/api/v1 -d"{\"api\":\"Health\",\"payload\":{\"project\":\"kubernetes\"}}"`.
- Call all other API scripts examples using `./devel/api_*.sh` scripts.
