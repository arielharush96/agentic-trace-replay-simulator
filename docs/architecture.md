# Architecture

```text
Hugging Face traces
        |
        v
Replay corpus -> replay driver -> OpenClaw -> mock LLM
                                      |
                                      v
                                OpenShell gateway
                                      |
                                      v
                                agent sandbox
```

The driver submits one high-level task request. OpenClaw performs its internal
model/tool loop. The mock LLM returns recorded model decisions through an
OpenAI-compatible API. OpenShell executes mapped tool commands in the agent
sandbox and returns the recorded tool result.

Jaeger receives OpenTelemetry spans. Prometheus/cAdvisor and a bounded 100 ms
cgroup sampler provide CPU and memory measurements. The analysis layer joins
these sources using task, trace, and turn identifiers.

The full architecture diagram is `docs/architecture.drawio`.
