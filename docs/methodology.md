# Methodology

The benchmark isolates orchestration overhead by replacing live inference with
controlled replay. Recorded model decisions, output tokens, tool calls, and
tool results remain deterministic; OpenClaw and OpenShell still execute their
real request, context, plugin, sandbox, and tool paths.

## Default Run

The default run measures OpenClaw plus OpenShell. It reports per-step:

- Context assembly boundary.
- Response processing.
- Sandbox initialization.
- Sandbox tool execution.
- CPU and resident memory.

## Matched Run

`RUN_MATCHED=1` additionally runs plain OpenClaw with the same corpus and
driver. The incremental OpenShell overhead plot is generated only when the
plain and shell runs have identical internal step-index sequences.

## Resource Measurement

- CPU is sampled from cgroup-v2 usage counters at 100 ms intervals.
- Elapsed time uses a monotonic clock.
- Prometheus/cAdvisor exact-pod queries provide an independent check.
- Resource summaries report mean, P50, and P95; unstable maxima are excluded
  from sizing conclusions.
