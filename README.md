[![Build Status](https://travis-ci.org/cncf/devstatscode.svg?branch=master)](https://travis-ci.org/cncf/devstatscode)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/1357/badge)](https://bestpractices.coreinfrastructure.org/projects/1357)

# DevStats code

This is a code reposotory for [DevStats](https://github.com/cncf/devstats) used to display [CNCF projects dashboards](https://devstats.cncf.io), [CDF projects dashboards](https://devstats.cd.foundation), [GraphQL projects dashboards](https://devstats.graphql.org) and [example Kubernetes/helm deployment](https://cncf.devstats-demo.net).

Authors: Łukasz Gryglicki <lgryglicki@cncf.io>, Justyna Gryglicka <lgryglicka@cncf.io>, Josh Berkus <jberkus@redhat.com>.

# Building and installing

- Follow [this guide](https://github.com/cncf/devstats-helm-example/blob/master/README.md) to see how to deploy on Kubernetes using Helm.
- Follow [this guide](https://github.com/cncf/devstats-helm-graphql/blob/master/README.md) to see GraphQL deployment using Kubernetes & Helm.
- Follow [this guide](https://github.com/cncf/devstats/blob/master/INSTALL_UBUNTU18.md#devstats-installation-on-ubuntu) for installing on bare metal.
- Follow [this guide](https://github.com/cncf/devstats-example/blob/master/README.md) to deploy your oiwn project on bare metal (this example deployes Homebrew statistics).
- Fetch dependency libraries.
- `make` then `make test` finally `make install`.

# Adding new projects

See `cncf/devstats-helm`:`ADDING_NEW_PROJECTS.md` for informations about how to add more projects on Kubernetes/Helm deployment.
See `cncf/devstats`:`ADDING_NEW_PROJECT.md` for informations about how to add more projects on bare metal deployment.

# API

API is available on `https://devstats.cncf.io:8080/api/v1`. This is a standard REST API that expects JSON payloads and returns JSON data.

You can see how to call example `Health` API call [here](https://github.com/cncf/devstatscode/blob/master/devel/api_health.sh).

All API calls that result in error returns the following JSON response: `{"error": "some error message"}`.

List of APIs:

- `Health`: `{"api": "Health", "payload": {"project": "projectName"}}`.
  - Arguments: `projectName` for example: `kubernetes`, `Kuberentes`, `gRPC`, `grpc`, `all`, `All CNCF`.
  - Returns: `{"project": "projectName", "db_name": "projectDB", "events": int}`, `events` is the total number of all GitHub events that are recorded for given project.

- `DevActCntRepoGrp`: `{"api": "DevActCntRepoGrp", "payload": {"project": "projectName", "range": "range", "metric": "metric", "repository_group": "repository_group", "country": "country", "github_id": "id"}}`.
  - Arguments: (like in "Developer Activity Counts by Repository Group" DevStats dashboards).
    - `projectName`: see `Health` API.
    - `range`: value from `Range` drop-down in DevStats page, for example: `Last year`, `v1.17.0 - now`.
    - `metric`: value from `Metric` drop-down in DevStats page, for example: `Contributions`, `Issues`, `PRs`.
    - `repository_group`: value from `Repository group` drop-down in DevStats page, for example: `All`, `Kubernetes`, `SIG Apps`.
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
