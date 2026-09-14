# Optional AppWorld Backend

This directory contains the optional stateful AppWorld integration. It is not
part of the default recorded-replay benchmark and is never enabled implicitly.

AppWorld provides simulated applications and databases. It does not call real
Gmail, Spotify, Venmo, or other external services.

## Installation

Run this only when AppWorld-backed tool execution is explicitly required:

```bash
bash scripts/setup_appworld.sh
```

## Required Mapping

The Hugging Face trace identifies a replay session, while AppWorld requires an
AppWorld task ID and its task-specific initial state. Create a mapping file:

```json
{
  "2a21e94e0687_32bdfa3a": "appworld-task-id"
}
```

Do not guess this mapping. A trace without the matching AppWorld task/state
cannot claim exact AppWorld reproduction.

## Service

Start the AppWorld adapter outside the OpenShell sandbox:

```bash
APPWORLD_ROOT=/path/to/appworld \
APPWORLD_TASK_MAP=/path/to/session-task-map.json \
python scripts/appworld_service.py --host 0.0.0.0 --port 8090
```

The OpenShell command calls this service with the original tool name and
arguments. The service executes the corresponding AppWorld API against the
stateful simulated world and returns a newly generated result.
