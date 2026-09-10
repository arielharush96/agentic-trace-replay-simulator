# Agentic Trace Replay Benchmark - Analyze your agent performance on OpenShift
<img width="256" height="256" alt="image" src="https://github.com/user-attachments/assets/bcd098dc-c97f-4d7e-a8ab-172a33a62cb9" />

Deterministic performance benchmarking for multi-step agentic workloads on
OpenShift. The current integration measures **OpenClaw** and **OpenShell** with
a controlled replay backend instead of live LLM inference.

The benchmark replays recorded agent sessions from the public
[`Exgentic/agent-llm-traces`](https://huggingface.co/datasets/Exgentic/agent-llm-traces)
dataset. It preserves the recorded user task, model decisions, tool calls, and
tool results while measuring the framework, context, response-processing, and
sandbox portions of the execution.

## What It Measures

```text
Replay driver -> OpenClaw -> controlled mock LLM
                         -> OpenShell -> agent sandbox
```

- OpenClaw harness and context assembly.
- Model/tool loop orchestration.
- OpenShell sandbox initialization and tool execution.
- CPU and memory for the exact OpenClaw and sandbox containers.
- OpenTelemetry spans and per-step timing.

The benchmark does **not** measure live model quality or live GPU inference.
The mock LLM uses controlled replay timing. Do not compare its TTFT/ITL values
directly with a production vLLM benchmark.

## Requirements

- Python 3.11+.
- OpenShift CLI (`oc`) and a logged-in cluster.
- OpenShell already installed and healthy.
- Agent Sandbox prerequisites already installed.
- OpenClaw image/plugins and required TLS/image-pull Secrets.
- Jaeger or a compatible OTLP collector.
- Prometheus/cAdvisor access for resource collection.

This repository does not install OpenShell or cluster-scoped operators.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,analyze]"
```

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

## Live Demo: Reproduce One Trace

This command reproduces the selected session
`2a21e94e0687_32bdfa3a` from the HF dataset through the real OpenClaw and
OpenShell path:

```bash
SESSION_ID=2a21e94e0687_32bdfa3a \
TARGET_NODE=<approved-node> \
KUBECONFIG=/path/to/kubeconfig \
bash scripts/replay_hf_session.sh
```

The command downloads only the filtered corpus needed to find the session,
creates the normalized replay corpus, runs it once, and writes a timestamped
result directory containing plots, logs, data, manifests, and the exact
reproduction command.

## OpenShift Run

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
