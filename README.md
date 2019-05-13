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

