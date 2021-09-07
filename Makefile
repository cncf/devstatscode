GO_LIB_FILES=pg_conn.go error.go mgetc.go map.go threads.go gha.go json.go time.go context.go exec.go structure.go log.go hash.go unicode.go const.go string.go annotations.go env.go ghapi.go io.go tags.go yaml.go es_conn.go ts_points.go convert.go
GO_BIN_FILES=cmd/structure/structure.go cmd/runq/runq.go cmd/gha2db/gha2db.go cmd/calc_metric/calc_metric.go cmd/gha2db_sync/gha2db_sync.go cmd/import_affs/import_affs.go cmd/annotations/annotations.go cmd/tags/tags.go cmd/webhook/webhook.go cmd/devstats/devstats.go cmd/get_repos/get_repos.go cmd/merge_dbs/merge_dbs.go cmd/replacer/replacer.go cmd/vars/vars.go cmd/ghapi2db/ghapi2db.go cmd/columns/columns.go cmd/hide_data/hide_data.go cmd/sqlitedb/sqlitedb.go cmd/website_data/website_data.go cmd/sync_issues/sync_issues.go cmd/gha2es/gha2es.go cmd/api/api.go cmd/tsplit/tsplit.go cmd/splitcrons/splitcrons.go
GO_TEST_FILES=context_test.go gha_test.go map_test.go mgetc_test.go threads_test.go time_test.go unicode_test.go string_test.go regexp_test.go annotations_test.go env_test.go convert_test.go
GO_DBTEST_FILES=pg_test.go series_test.go
GO_LIBTEST_FILES=test/compare.go test/time.go
GO_BIN_CMDS=github.com/cncf/devstatscode/cmd/structure github.com/cncf/devstatscode/cmd/runq github.com/cncf/devstatscode/cmd/gha2db github.com/cncf/devstatscode/cmd/calc_metric github.com/cncf/devstatscode/cmd/gha2db_sync github.com/cncf/devstatscode/cmd/import_affs github.com/cncf/devstatscode/cmd/annotations github.com/cncf/devstatscode/cmd/tags github.com/cncf/devstatscode/cmd/webhook github.com/cncf/devstatscode/cmd/devstats github.com/cncf/devstatscode/cmd/get_repos github.com/cncf/devstatscode/cmd/merge_dbs github.com/cncf/devstatscode/cmd/replacer github.com/cncf/devstatscode/cmd/vars github.com/cncf/devstatscode/cmd/ghapi2db github.com/cncf/devstatscode/cmd/columns github.com/cncf/devstatscode/cmd/hide_data github.com/cncf/devstatscode/cmd/sqlitedb github.com/cncf/devstatscode/cmd/website_data github.com/cncf/devstatscode/cmd/sync_issues github.com/cncf/devstatscode/cmd/gha2es github.com/cncf/devstatscode/cmd/api github.com/cncf/devstatscode/cmd/tsplit github.com/cncf/devstatscode/cmd/splitcrons
BUILD_TIME=`date -u '+%Y-%m-%d_%I:%M:%S%p'`
COMMIT=`git rev-parse HEAD`
HOSTNAME=`uname -a | sed "s/ /_/g"`
GO_VERSION=`go version | sed "s/ /_/g"`
#for race CGO_ENABLED=1
#GO_ENV=CGO_ENABLED=1
GO_ENV=CGO_ENABLED=0
# -ldflags '-s -w': create release binary - without debug info
GO_BUILD=go build -ldflags "-s -w -X github.com/cncf/devstatscode.BuildStamp=$(BUILD_TIME) -X github.com/cncf/devstatscode.GitHash=$(COMMIT) -X github.com/cncf/devstatscode.HostName=$(HOSTNAME) -X github.com/cncf/devstatscode.GoVersion=$(GO_VERSION)"
#GO_BUILD=go build -ldflags "-s -w -X github.com/cncf/devstatscode.BuildStamp=$(BUILD_TIME) -X github.com/cncf/devstatscode.GitHash=$(COMMIT) -X github.com/cncf/devstatscode.HostName=$(HOSTNAME) -X github.com/cncf/devstatscode.GoVersion=$(GO_VERSION)" -race
#  -ldflags '-s': instal stripped binary
#GO_INSTALL=go install
#For static gcc linking
GCC_STATIC=
#GCC_STATIC=-ldflags '-extldflags "-static"'
GO_INSTALL=go install -ldflags "-s -w -X github.com/cncf/devstatscode.BuildStamp=$(BUILD_TIME) -X github.com/cncf/devstatscode.GitHash=$(COMMIT) -X github.com/cncf/devstatscode.HostName=$(HOSTNAME) -X github.com/cncf/devstatscode.GoVersion=$(GO_VERSION)"
GO_FMT=gofmt -s -w
GO_LINT=golint -set_exit_status
GO_VET=go vet
GO_IMPORTS=goimports -w
GO_USEDEXPORTS=usedexports -ignore 'sqlitedb.go|vendor'
GO_ERRCHECK=errcheck -asserts -ignore '[FS]?[Pp]rint*' -ignoretests
GO_TEST=go test
BINARIES=structure gha2db calc_metric gha2db_sync import_affs annotations tags webhook devstats get_repos merge_dbs replacer vars ghapi2db columns hide_data website_data sync_issues gha2es runq api sqlitedb tsplit splitcrons
CRON_SCRIPTS=cron/cron_db_backup.sh cron/sysctl_config.sh cron/backup_artificial.sh
UTIL_SCRIPTS=devel/wait_for_command.sh devel/cronctl.sh devel/sync_lock.sh devel/sync_unlock.sh devel/db.sh
GIT_SCRIPTS=git/git_reset_pull.sh git/git_files.sh git/git_tags.sh git/last_tag.sh git/git_loc.sh
STRIP=strip

all: check ${BINARIES}

structure: cmd/structure/structure.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o structure cmd/structure/structure.go

runq: cmd/runq/runq.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o runq cmd/runq/runq.go

api: cmd/api/api.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o api cmd/api/api.go

gha2db: cmd/gha2db/gha2db.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o gha2db cmd/gha2db/gha2db.go

gha2es: cmd/gha2es/gha2es.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o gha2es cmd/gha2es/gha2es.go

calc_metric: cmd/calc_metric/calc_metric.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o calc_metric cmd/calc_metric/calc_metric.go

import_affs: cmd/import_affs/import_affs.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o import_affs cmd/import_affs/import_affs.go

gha2db_sync: cmd/gha2db_sync/gha2db_sync.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o gha2db_sync cmd/gha2db_sync/gha2db_sync.go

devstats: cmd/devstats/devstats.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o devstats cmd/devstats/devstats.go

annotations: cmd/annotations/annotations.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o annotations cmd/annotations/annotations.go

tags: cmd/tags/tags.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o tags cmd/tags/tags.go

columns: cmd/columns/columns.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o columns cmd/columns/columns.go

webhook: cmd/webhook/webhook.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o webhook cmd/webhook/webhook.go

get_repos: cmd/get_repos/get_repos.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o get_repos cmd/get_repos/get_repos.go

merge_dbs: cmd/merge_dbs/merge_dbs.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o merge_dbs cmd/merge_dbs/merge_dbs.go

vars: cmd/vars/vars.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o vars cmd/vars/vars.go

ghapi2db: cmd/ghapi2db/ghapi2db.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o ghapi2db cmd/ghapi2db/ghapi2db.go

sync_issues: cmd/sync_issues/sync_issues.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o sync_issues cmd/sync_issues/sync_issues.go

replacer: cmd/replacer/replacer.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o replacer cmd/replacer/replacer.go

hide_data: cmd/hide_data/hide_data.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o hide_data cmd/hide_data/hide_data.go

website_data: cmd/website_data/website_data.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o website_data cmd/website_data/website_data.go

sqlitedb: cmd/sqlitedb/sqlitedb.go ${GO_LIB_FILES}
	 ${GO_BUILD} ${GCC_STATIC} -o sqlitedb cmd/sqlitedb/sqlitedb.go

tsplit: cmd/tsplit/tsplit.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o tsplit cmd/tsplit/tsplit.go

splitcrons: cmd/splitcrons/splitcrons.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o splitcrons cmd/splitcrons/splitcrons.go

fmt: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_DBTEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_FMT}"

lint: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_DBTEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_LINT}"

vet: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_DBTEST_FILES} ${GO_LIBTEST_FILES}
	./vet_files.sh "${GO_VET}"

imports: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_DBTEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_IMPORTS}"

usedexports: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_DBTEST_FILES} ${GO_LIBTEST_FILES}
	${GO_USEDEXPORTS} ./...

errcheck: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_DBTEST_FILES} ${GO_LIBTEST_FILES}
	${GO_ERRCHECK} $(go list ./... | grep -v /vendor/)

test:
	${GO_TEST} ${GO_TEST_FILES}

dbtest:
	${GO_TEST} ${GO_DBTEST_FILES}

check: fmt lint imports vet usedexports errcheck

install: ${BINARIES}
	cp -v ${UTIL_SCRIPTS} ${GOPATH}/bin
	[ ! -f /tmp/deploy.wip ] || exit 1
ifneq (${NOWAIT},1)
	wait_for_command.sh 'devstats,devstats_others,devstats_kubernetes,devstats_allprj' 900 || exit 2
endif
	${GO_INSTALL} ${GO_BIN_CMDS}
	cp -v ${CRON_SCRIPTS} ${GOPATH}/bin
	cp -v ${GIT_SCRIPTS} ${GOPATH}/bin

strip: ${BINARIES}
	${STRIP} ${BINARIES}

clean:
	rm -f ${BINARIES}

.PHONY: test
