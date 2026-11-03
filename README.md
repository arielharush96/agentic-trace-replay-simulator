# Reproducible performance benchmarking and profiling for agentic systems using trace replay

This tool benchmarks agentic AI deployments by replaying recorded agent sessions at configurable concurrency. It supports comparisons across CPUs and agent harnesses, measures the impact of optimizations, and reveals how performance changes as the number of concurrent agents grows.
It breaks down where time and resources are spent: context assembly and response processing in the harness, tool execution in the sandbox and queueing under load. This helps identify performance and scale bottlenecks and measure whether a change actually improves performance.

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

for cluster deployment information, use this readme: https://github.com/arielharush96/agentic-trace-replay-simulator/blob/main/k8s/deployment-readme.md


## Running


Common variations:


# software engineering only, 50 agents
./scripts/bench --agents 50 --nodes node-a,node-b --workload-dist swe --corpus data/replay.json

# 80 ready sandboxes, the rest cold, to price readiness
./scripts/bench --agents 100 --nodes node-a,node-b --warm-pool 80 --cold-ratio 0.2 --corpus data/replay.json

# fast model: 15 ms to first token, 5 ms between tokens
./scripts/bench --agents 100 --nodes node-a,node-b --simulated-ttft 15 --simulated-itl 5 --corpus data/replay.json

# the same pacing through the alias flags
./scripts/bench --agents 4 --nodes node-a,node-b --ttft-simulated 50 --itl-simulated 15 --corpus data/replay.json
```

## What you get

Each run writes per-turn and per-tool CSV and JSON, OpenTelemetry traces, cgroup
and Prometheus CPU/memory series, sandbox and tool timing, node and warm-pool
assignments, and the analysis plots.

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
