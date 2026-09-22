# Agentic Trace Replay Benchmark - Analyze your agent performance on OpenShift
<p align="center">
  <img width="256" height="256" alt="image" src="https://github.com/user-attachments/assets/bcd098dc-c97f-4d7e-a8ab-172a33a62cb9" />
</p>

Deterministic performance benchmarking for multi-step agentic workloads on
OpenShift. The current integration measures **OpenClaw as the agentic harness** and **OpenShell** with
a controlled replay backend instead of live LLM inference.
future work will allow to test any agentic harness, or testing with real inference.

The benchmark replays recorded agent sessions from the public
[`Exgentic/agent-llm-traces`](https://huggingface.co/datasets/Exgentic/agent-llm-traces)
dataset. It preserves the recorded user task, model decisions, tool calls, and
tool results while measuring the framework, context, response-processing, and
sandbox portions of the execution.

## What It Measures


- agent harness context assembly.
- Model or tool loop orchestration time.
- OpenShell sandbox initialization and tool execution.
- CPU and memory for OpenClaw and OpenShell.
- OpenTelemetry spans and per-step timing.

The benchmark does **not** measure live model quality or live GPU inference.

## Requirements

- Python 3.11+.
- OpenShift CLI (`oc`) and a logged-in cluster.
- OpenShell already installed and healthy.
- Agent Sandbox prerequisites already installed.
- OpenClaw image/plugins and required TLS/image-pull Secrets.
- Jaeger or a compatible OTLP collector.
- Prometheus/cAdvisor access for resource collection.

This repository does not install OpenShell or cluster-scoped operators.


## Corpus

The benchmark refers to the Hugging Face dataset; it does not redistribute the
raw dataset. Download or stream it using the dataset tooling, then select one
or more sessions:

```bash
trace-replay-sim ingest \
  --dataset Exgentic/agent-llm-traces \
  --harness claude_code \
  --benchmark appworld \
  --limit 10 \
  --out data/generated/replay.json
```

Select an individual replay corpus:

```bash
trace-replay-sim corpus \
  --in data/generated/replay.json \
  --session-id SESSION_ID \
  --out data/generated/SESSION_ID/corpus.json
```

The generated corpus records its source dataset, selected session, hashes, and
normalization decisions. A phantom all-zero turn may be removed; this is
reported explicitly.

## OpenShift Run

Cluster-specific values are supplied at runtime and are intentionally not
stored in the repository. Set `KUBECONFIG` (or use the default kubeconfig),
`TARGET_NODE`, and, when collecting monitoring data, `THANOS_HOST`. Set
`OPENCLAW_TOKEN_SECRET` and optionally `OPENCLAW_TOKEN_SECRET_KEY` when the
deployed OpenClaw endpoint requires authentication; the experiment mounts that
Kubernetes Secret into driver Jobs without placing the token in command lines.
Do not commit tokens or kubeconfig files.

## Live Trace Recording

The repository also includes a transport-neutral live event recorder for an
OpenClaw plugin, gateway middleware, or sidecar. It writes redacted,
timestamped `agent-event/v1` JSONL records for user requests, context assembly,
model calls, tool calls/results, sandbox execution, and session completion. See
[`docs/recording.md`](docs/recording.md).

Copy the example configuration and fill in cluster-specific values:

```bash
cp examples/openshift-config.example.yaml /tmp/trace-replay-config.yaml
```

Prepare the required namespace-scoped Secrets and apply the OpenShift base
manifests according to [`docs/openshift-quickstart.md`](docs/openshift-quickstart.md).

Run the canonical dataset workflow:

```bash
KUBECONFIG=/path/to/kubeconfig \
TARGET_NODE=worker.example.internal \
CORPUS_ROOT="$PWD/data/generated" \
bash scripts/run_dataset_100ms.sh
```

The default workflow runs OpenClaw+OpenShell only. Matched plain-OpenClaw
comparison is opt-in:

```bash
RUN_MATCHED=1 bash scripts/run_dataset_100ms.sh
```

Matched overhead is reported only when plain and OpenShell runs have identical
internal step-index sequences.

## Results

Every experiment is written to a timestamped directory. Each corpus is
organized as:

```text
<corpus>/
├── plots-analysis/  # waterfall, turn metrics, resource and overhead plots
├── logs/            # driver and benchmark logs
├── data/            # JSON, CSV, JSONL, traces, Prometheus, cgroup data
└── manifests/       # Kubernetes manifests, source snapshots, parameters
```

The experiment also records the command, corpus hash, dataset revision, image
references, node, sampler configuration, and limitations.

## Limitations

- Recorded model decisions are replayed; they are not regenerated.
- OpenClaw context spans are run-level in the tested release; per-step context
  timing uses trace-derived boundaries.
- Tool-output size is unavailable when content capture is disabled.
- Matched overhead requires equal internal step sequences.
- Sandbox commands use mapped recorded results and are not the original
  external AppWorld service.

See [`docs/methodology.md`](docs/methodology.md) and
[`docs/data-provenance.md`](docs/data-provenance.md).

## Optional AppWorld Mode

The default mode replays recorded tool results. An optional AppWorld-backed
mode is documented in [`docs/appworld-backed.md`](docs/appworld-backed.md).
It requires a separately installed AppWorld engine, its task-specific initial
state, and an explicit session-to-task mapping. It is never enabled by default.
