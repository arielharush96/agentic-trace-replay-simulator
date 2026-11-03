## Replay corpus

The corpus is assembled from recorded sessions, not generated:

```bash
python -m trace_replay_sim ingest --out data/replay.json
```

`ingest` streams [Exgentic/agent-llm-traces](https://huggingface.co/datasets/Exgentic/agent-llm-traces)
(1,781 sessions, CDLA-Permissive-2.0) and keeps 50 by default. The limit
truncates in dataset order, so raise it, or select with `--benchmark` and
`--harness`, to cover all four workloads:

```bash
python -m trace_replay_sim ingest --out data/replay.json --limit 1781
```

`pa` runs also need the AppWorld environment, which is not redistributable and
not part of the public image:

```bash
pip install appworld
appworld install
appworld download data
```

Point `TRACE_REPLAY_APPWORLD_DATA` at the directory holding `base_dbs/` and
`tasks/` (defaults to `../appworld-runtime/data`). The session-to-task mapping
ships in `config/appworld-session-task-map-full.json`.

To reuse a pre-populated customer-owned PVC instead of staging:

```bash
TRACE_REPLAY_SKIP_APPWORLD_STAGING=1 \
TRACE_REPLAY_APPWORLD_DATA_PVC=my-appworld-data \
  ./scripts/bench --agents 4 --nodes node-a,node-b --workload-dist pa \
  --corpus data/replay.json
```


## Cluster prerequisites

The tool targets Red Hat OpenShift AI and relies on the Kueue admission that
ships with it: every experiment Job is admitted through that webhook. If the
Kueue deployment lives outside `kueue-system`, point the controller at it with
`TRACE_REPLAY_KUEUE_NAMESPACE` and `TRACE_REPLAY_KUEUE_WEBHOOK_SERVICE`.

| Requirement | Notes |
| --- | --- |
| OpenShift AI cluster | `oc get svc -n kueue-system kueue-webhook-service` confirms the admission webhook. |
| OpenShell gateway | The controller reaches it at `TRACE_REPLAY_OPENSHELL_GATEWAY_ENDPOINT` and copies its `openshell-client-tls` Secret out of the OpenShell namespace (`openshell-tracesim` by default). |
| Agent image | Your own agent image; `k8s/35-openclaw-shell.yaml` carries `quay.io/example/...` placeholders and no image pull Secret, so add one if your registry is private. |
| Storage class | Cluster default for the workspace PVCs, or set `TRACE_REPLAY_WORKSPACE_STORAGE_CLASS`. |
| CLI tooling | `oc` with a kubeconfig, `bash` 4 or newer, Python 3.11. |

Apply the namespace objects the controller expects to find already present:

```bash
oc apply -f k8s/00-namespace.yaml -f k8s/10-mock-llm.yaml -f k8s/20-jaeger.yaml \
         -f k8s/34-openshell-sandbox-scc.yaml -f k8s/35-openclaw-shell.yaml
```

`k8s/50-monitoring.yaml` is optional. It only creates a Secret holding a 1s
cAdvisor scrape config for the two namespaces; it takes effect once that Secret
is referenced from `prometheusK8s.additionalScrapeConfigs` in the existing
`cluster-monitoring-config` ConfigMap. Without it the analysis falls back to the
platform's 30s samples.

The workload backends next to them are deployed by the controller for the
workloads a run selects. The gateway token `change-me-gateway-token` is a local
placeholder that appears in the manifest and in the controller defaults; change
it in every place or not at all.

### in cluster, supply these values explicitly:

| Input | Environment / flag | Required | Notes |
| --- | --- | --- | --- |
| Worker nodes | `NODES` / `--nodes` | yes for a real run | Comma-separated approved nodes, for example `node-a,node-b`. |
| Kubeconfig | `KUBECONFIG` / `--kubeconfig` | no | Defaults to the standard `oc` config; set it explicitly for a non-default cluster. |
| OpenShell namespace | `OPENSHELL_NAMESPACE` / `--openshell-namespace` | yes | Defaults to `openshell-tracesim`. |
| OpenShell gateway | `GATEWAY_ENDPOINT` (bench) or `TRACE_REPLAY_OPENSHELL_GATEWAY_ENDPOINT` / `--openshell-gateway-endpoint` | yes | Use the gateway endpoint reachable from the controller, for example `http://openshell.example:8080`. |
| Agent image | `TRACE_REPLAY_OPENCLAW_IMAGE` | yes | Must contain the OpenShell client used by your gateway. |
| Kueue webhook | `TRACE_REPLAY_KUEUE_NAMESPACE`, `TRACE_REPLAY_KUEUE_WEBHOOK_SERVICE` | only if non-default | Defaults to `kueue-system/kueue-webhook-service`. |
| Workspace storage class | `STORAGE_CLASS` (bench) or `TRACE_REPLAY_WORKSPACE_STORAGE_CLASS` / `--workspace-storage-class` | no | Empty uses the cluster default. |
| Workspace PVCs | `PVCS=node-a=pvc-a,node-b=pvc-b` (bench) / `--workspace-pvcs` | no | Existing RWO claims, one per node; otherwise the controller creates run-owned workspace PVCs. |
| AppWorld data PVC | `TRACE_REPLAY_APPWORLD_DATA_PVC` / `--appworld-data-pvc` | for `pa` workload | Bound RWO claim in the AppWorld backend namespace (`trace-replay` by default), schedulable on the selected control node. |
| AppWorld data source | `TRACE_REPLAY_APPWORLD_DATA` / `--appworld-data` | for `pa` staging | Directory containing `base_dbs/` and `tasks/`; staged into `/appworld/data/`. |
| AppWorld backend image | `k8s/appworld-backend.yaml` | for `pa` | Defaults to `ghcr.io/stonybrooknlp/appworld:latest`; mirror it if your registry cannot pull it. |
| AppWorld task map | `TRACE_REPLAY_APPWORLD_TASK_MAP` / `--appworld-task-map` | for `pa` | Must contain an entry for every selected PA session ID. The shipped full map covers 25 sessions; rebuild it for a custom corpus. |
| Reuse AppWorld data | `TRACE_REPLAY_SKIP_APPWORLD_STAGING=1` / `--skip-appworld-staging` | for a pre-populated PVC | Skips staging and checks `/appworld/data/base_dbs/spotify.db` and `/appworld/data/tasks`; the PVC must already contain the complete data and does not need the controller ownership label. |
| Thanos | `THANOS_HOST`; optional `PROM_TOKEN` | no | CPU/memory collection only. If unset or unreachable, the manifest records a warning and traces still complete. |

By default the controller creates or reuses an **owned** AppWorld PVC and stages
data from `--appworld-data`. An existing PVC without the
`managed-by: trace-replay-experiment` label is refused in that mode, so a
customer-owned volume cannot be overwritten by accident. Use
`--skip-appworld-staging` (or `TRACE_REPLAY_SKIP_APPWORLD_STAGING=1`) to reuse a
pre-populated claim instead.

Before using a pre-populated PVC, confirm:

- the claim is `Bound`, uses `ReadWriteOnce`, lives in the AppWorld backend
  namespace (`trace-replay` by default), and is schedulable on the selected
  control node;
- it contains complete AppWorld data under `/appworld/data/base_dbs/` and
  `/appworld/data/tasks/`;
- every task ID in the task map exists in that data;
- the AppWorld backend image is pullable by the cluster.

The preflight check is intentionally shallow: it verifies the claim, the
`/appworld/data/base_dbs/spotify.db` marker, and the `tasks/` directory, but not
every app database or task mapping. A missing database or a task-map/data
mismatch can pass preflight and fail when that specific PA tool call executes.
