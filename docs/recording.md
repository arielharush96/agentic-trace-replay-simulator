# Live Trace Recording

The benchmark can record live agent execution events independently of the
mock-LLM replay path. An OpenClaw plugin, gateway middleware, or sidecar sends
JSON events to the collector endpoint:

```bash
export TRACE_RECORDER_AUTH_TOKEN="set-this-outside-the-repository"
PYTHONPATH=src python3 -m trace_replay_sim.live_recorder \
  --out results/live/events.jsonl \
  --host 127.0.0.1 \
  --port 8787 \
  --auth-token "$TRACE_RECORDER_AUTH_TOKEN"
```

The collector accepts authenticated `POST /v1/events` and provides `GET /healthz`. Each event
is normalized to `agent-event/v1` and contains an event ID, session ID, trace
ID, optional turn index, monotonic timestamp, wall-clock timestamp, and a
structured payload. Payloads are redacted before being written to disk; fields
such as authorization headers, API keys, tokens, passwords, cookies, secrets,
and credentials are replaced with `[REDACTED]`.

The recommended event boundaries are:

```text
user.request
context.assembled
model.request
model.response
tool.request
tool.result
sandbox.exec
session.end
```

The recorder is transport-neutral. It does not automatically instrument an
arbitrary OpenClaw installation; the OpenClaw integration must emit these
events or forward its OTEL/event hooks to the collector. The existing replay
benchmark continues to use `mock_edges.jsonl` as its authoritative per-turn
timing source, while the live recorder is intended to capture real sessions
and provide replay-relevant evidence for later corpus construction.

Do not record production credentials or unrestricted sensitive content. Use a
dedicated namespace, explicit retention, and review the JSONL output before
sharing it.

For a cluster deployment, keep the collector on a private Service, put the
token in a Secret, and send `Authorization: Bearer <token>`. Do not expose the
collector directly to the public internet.
