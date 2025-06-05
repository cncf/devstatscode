# To run columns manually for given project

- Complile columns: `make columns`.
- Copy it to devstats node: `sftp root@node-0`, `mput columns`.

1) Run `columns` on `prod`:
- Run columns on 'All CNCF' project `helm install --generate-name ./devstats-helm --set namespace='devstats-prod',skipSecrets=1,skipPVs=1,skipBackupsPV=1,skipVacuum=1,skipBackups=1,skipBootstrap=1,skipCrons=1,skipAffiliations=1,skipGrafanas=1,skipServices=1,skipPostgres=1,skipIngress=1,skipStatic=1,skipAPI=1,skipNamespaces=1,testServer='',prodServer='1',provisionImage='lukaszgryglicki/devstats-prod',provisionCommand='devstats-helm/columns.sh',indexProvisionsFrom=38,indexProvisionsTo=39`.

2) Run `columns` on `test`:
- Run columns on 'All CNCF' project `helm install --generate-name ./devstats-helm --set skipSecrets=1,skipPVs=1,skipBackupsPV=1,skipVacuum=1,skipBackups=1,skipBootstrap=1,skipCrons=1,skipAffiliations=1,skipGrafanas=1,skipServices=1,skipPostgres=1,skipIngress=1,skipStatic=1,skipAPI=1,skipNamespaces=1,provisionCommand='devstats-helm/columns.sh',indexProvisionsFrom=38,indexProvisionsTo=39,projectsOverride='+cncf\,+opencontainers\,+zephyr\,+linux\,+rkt\,+sam\,+azf\,+riff\,+fn\,+openwhisk\,+openfaas\,+cii\,+prestodb\,+godotengine\,+opentracing'`


3) Via `debug` pod:

a) Create debugging pod:
- On `test`: `helm install devstats-test-debug ./devstats-helm --set skipSecrets=1,skipPVs=1,skipBackupsPV=1,skipVacuum=1,skipBackups=1,skipProvisions=1,skipCrons=1,skipAffiliations=1,skipGrafanas=1,skipServices=1,skipIngress=1,skipStatic=1,skipAPI=1,skipNamespaces=1,skipPostgres=1,bootstrapPodName=debug,bootstrapCommand=sleep,bootstrapCommandArgs={36000s}`.
- On `prod`: `helm install devstats-prod-debug ./devstats-helm --set namespace='devstats-prod',skipSecrets=1,skipPVs=1,skipBackupsPV=1,skipVacuum=1,skipBackups=1,skipProvisions=1,skipCrons=1,skipAffiliations=1,skipGrafanas=1,skipServices=1,skipPostgres=1,skipIngress=1,skipStatic=1,skipAPI=1,skipNamespaces=1,bootstrapPodName=debug,bootstrapCommand=sleep,bootstrapCommandArgs={360000s},useBootstrapResourcesLimits=''`.

b) Eventually copy manually compiled binary into pod: `k cp -n devstats-test ~/columns debug:/columns`.

c) Shell into it: `../devstats-k8s-lf/util/pod_shell.sh debug` or `k exec -itn devstats-test debug -- bash`.

d) Exacute:
- `GHA2DB_DEBUG=2 GHA2DB_QOUT=2 GHA2DB_LOCAL=1 GHA2DB_PROJECT=jenkins PG_DB=jenkins /columns`.

e) Delete the debugging pod: `helm delete devstats-test-debug`.
