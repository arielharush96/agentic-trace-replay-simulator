# Agentic Trace Replay Simulator

Want to benchmark your own agentic AI deployment? On different CPUs, with a
different harness, after an optimization, or as you scale from ten agents to a
hundred and on to the thousand you are planning. This tool replays recorded
agent sessions at the scale you choose and shows where the performance
bottlenecks actually are: the harness that assembles context, the sandbox that
runs tools, the services behind those tools, or the queue that forms when the
fleet runs at once.

Responses are replayed, not generated, so a run needs no GPU and no live
inference endpoint and repeats exactly: same traces, same pacing, same work.
That is what makes a before/after comparison mean something.

## How it works

![Trace replay architecture](docs/architecture.png)

Recorded multi-turn sessions go in. A replay driver spreads them across agents,
balances the workload mix, and schedules them at the concurrency you ask for.
The driver plays only the user: it sends a request at recorded turn 0 and at
each later recorded user message. Between those boundaries the agent owns the
tool loop, so an intermediate assistant turn without a tool call never
truncates the rest of the conversation.
Each agent then does its normal work - context assembly, tool decisions,
response processing against a simulator that returns the recorded response at
a configurable pace. Tool calls take the ordinary route into an OpenShell sandbox
and out to the workload backend or external service.

OpenTelemetry spans, CPU and memory samples, and tool and sandbox metrics are
correlated per turn, so the answer is "this stage, this much" rather than a
single end-to-end number.

Four workload classes are replayed with their behaviour intact:

| Workload | Backend | What it stresses |
| --- | --- | --- |
| `swe` software engineering | pinned SWE-bench repositories | long context, repeated execution, memory growth |
| `pa` personal assistant | AppWorld | interactive tool loops, application state |
| `research` deep search | BrowseCompPlus | variable tool depth, external waits |
| `cs` customer support | Tau2 | short transactions at high concurrency |

The three added backends are stateful replay environments, not official
benchmark score runs. The point is the platform cost around the work, not the
model's answer quality.

## Tuning a run

`scripts/bench` is the benchmark entrypoint. Every run starts by printing a box
with the fleet size, trace count, workload split, worker and warm-pool sizing,
pacing, replay mode, and output directory, followed by the session-to-agent
assignment. The controller's phase events then stream live, and the complete
log is kept in `<out>/controller.log`.

The knobs that matter:

| Flag | What it changes |
| --- | --- |
| `--agents N` | Fleet size, 1 to 100 agents in one run. |
| `--workload-dist DIST` | Mix: `even` (default), `swe`, `pa`, `research`, `cs`, `swe:60,pa:20,cs:20` (percent), or `swe=2,pa=1,research=1,cs=0` (counts). |
| `--traces N` | Traces per workload. Defaults to 1 on small fleets and scales with the agent count above 20. |
| `--session-ids IDS` | Run hand-picked sessions instead of the automatic selection. |
| `--warm-pool N` | Ready sandboxes available before the run starts. Defaults to one per agent. |
| `--cold-ratio R` | Share of agents forced through cold start, to price the warm pool against. |
| `--simulated-ttft MS`, `--simulated-itl MS` (aliases: `--ttft-simulated`, `--itl-simulated`) | Model pacing. Low values emulate a fast model and push the harness harder; high values emulate a slow one. |
| `--dispatch-interval SEC` | Spacing between trace admissions, to ramp load instead of starting flat out. |
| `--dry-run` | Print the plan, the session assignment, and the pacing without touching the cluster. |
| `--list-workloads`, `--list-sessions WL` | Inspect the registry and the sessions available in a corpus. |

`scripts/bench --help` has the full list, including retries, output directory, and
a set of worked examples.

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

### Customer-owned inputs

The defaults describe the local demo. For a customer cluster, supply these
values explicitly:

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

## Running

```bash
# once per checkout
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# see what would run, without touching the cluster
./scripts/bench --agents 100 --corpus data/replay.json --dry-run

# run it across two worker nodes
./scripts/bench --agents 100 --nodes node-a,node-b --corpus data/replay.json
```

`data/replay.json` is a four-trace sample (one per workload) so the demo runs
out of the box; replace it with your own corpus for production runs. The sample
targets the default `trace-replay` namespace and its `mock-llm` Service, so keep
that namespace or regenerate the corpus for a different one.

Common variations:

```bash
# classic four-trace demo: one agent per workload
# data/replay.json ships with the repo; THANOS_HOST is the Thanos querier route
THANOS_HOST=thanos-querier-openshift-monitoring.apps.example.com \
  ./scripts/demo.sh --nodes node-a,node-b

# software engineering only, 50 agents
./scripts/bench --agents 50 --nodes node-a,node-b --workload-dist swe --corpus data/replay.json

# 80 ready sandboxes, the rest cold, to price readiness
./scripts/bench --agents 100 --nodes node-a,node-b --warm-pool 80 --cold-ratio 0.2 --corpus data/replay.json

# fast model: 15 ms to first token, 5 ms between tokens
./scripts/bench --agents 100 --nodes node-a,node-b --simulated-ttft 15 --simulated-itl 5 --corpus data/replay.json

# the same pacing through the alias flags
./scripts/bench --agents 4 --nodes node-a,node-b --ttft-simulated 50 --itl-simulated 15 --corpus data/replay.json
```

`./run.sh` (or `scripts/run_experiment.sh`) is the same controller with the full
flag set instead of the benchmark shorthand: explicit per-workload plan, node
placement, storage class and access mode, gateway endpoint, and resume options.
Use `--plan` there for the non-mutating check.

Cluster settings are flags or environment variables: `NODES` (or `--nodes`),
`KUBECONFIG`, `THANOS_HOST` (the Thanos querier route used for CPU/memory
collection; optional - if it is unset or unreachable, the collection manifest
records a warning and the trace still completes), and the `TRACE_REPLAY_*`
set: `TRACE_REPLAY_NODE`, `TRACE_REPLAY_ALLOWED_NODES` (optional node
allow-list, empty accepts any), `TRACE_REPLAY_OPENCLAW_IMAGE`,
`TRACE_REPLAY_OPENSHELL_GATEWAY_ENDPOINT`,
`TRACE_REPLAY_WORKSPACE_STORAGE_CLASS`.

## What you get

Each run writes per-turn and per-tool CSV and JSON, OpenTelemetry traces, cgroup
and Prometheus CPU/memory series, sandbox and tool timing, node and warm-pool
assignments, and the analysis plots.

Before requesting cluster access, `scripts/check_local_readiness.sh` runs the
same 100-agent queue-building path locally, without touching the cluster.

## Layout

```text
src/trace_replay_sim/   replay driver, simulator, harnesses, telemetry, analysis
scripts/                bench entrypoint, cluster controller, backends, gates
k8s/                    namespace objects, agent service template, workload backends
config/                 backend manifest, SWE task manifest, AppWorld task map
docs/                   architecture diagram
tests/                  unit and integration tests
```

## Related

- [Opening the black box: Profiling a secured agentic pipeline on Red Hat OpenShift AI](https://www.redhat.com/en/blog/opening-black-box-profiling-secured-agentic-pipeline-red-hat-openshift-ai)
- [Red Hat OpenShift AI](https://www.redhat.com/en/technologies/cloud-computing/openshift/openshift-ai)
- [OpenShell](https://docs.nvidia.com/openshell/about/overview)
- [OpenClaw](https://openclaw.ai/)

Questions, or your own results to compare: the PSAP Agentic team.
