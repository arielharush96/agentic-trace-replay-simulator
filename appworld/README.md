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

## Runtime Architecture

AppWorld is a local simulation framework, not a remote database service. It
provides Python implementations of simulated applications such as Venmo,
Phone, Splitwise, SimpleNote, Gmail, Spotify, and Supervisor.

The runtime has this general layout:

```text
appworld-runtime/
├── data/
│   ├── base_dbs/
│   │   ├── venmo.db
│   │   ├── phone.db
│   │   ├── splitwise.db
│   │   ├── supervisor.db
│   │   └── ...
│   ├── tasks/
│   │   └── <task_id>/
│   │       ├── specs.json
│   │       ├── dbs/
│   │       │   ├── venmo.jsonl
│   │       │   ├── phone.jsonl
│   │       │   └── ...
│   │       └── ground_truth/
│   └── api_docs/
└── experiments/
    └── outputs/
        └── <experiment>/
            └── tasks/
                └── <task_id>/
                    ├── dbs/
                    └── logs/
```

The base databases are SQLite files. For example, `venmo.db` contains
relational tables such as `users`, `transactions`, `payment_cards`,
`payment_requests`, `bank_transfers`, `friendships`, notifications, and
transaction comments. The `transactions` table contains fields such as sender,
receiver, amount, description, timestamps, and payment-card information.

## Task Initialization

When the adapter creates a world:

```python
AppWorld(task_id="a30375d_2")
```

AppWorld loads the task specification, supervisor state, matching initial
application data, and the Python API implementations. It creates task-specific
database state, normally in memory for local execution, and exposes the APIs
through an `apis` collection.

An adapter request such as:

```text
mcp__environment__venmo__show_transactions
```

is executed internally as an AppWorld API call equivalent to:

```python
apis.venmo.show_transactions(**arguments)
```

The API implementation reads or mutates the simulated application state and
returns a structured Python result. The adapter serializes that result to JSON
and returns it to the sandbox command.

## End-To-End Flow

```text
Recorded agent decision
  -> Mock LLM returns the tool call
  -> OpenShell runs a Python command
  -> Python sends an HTTP request to the AppWorld adapter
  -> AppWorld executes the stateful application API
  -> The adapter returns the generated JSON result
  -> OpenShell returns it to OpenClaw
```

OpenShell provides the execution boundary, isolation, network policy, and
execution audit. It does not generate the application answer. The model supplies
the tool-call decision, while AppWorld supplies the tool result.

There are no real Venmo, Phone, Splitwise, Gmail, or Spotify services involved.
Accounts, databases, credentials, and state changes are simulated and isolated
to the mapped AppWorld task. AppWorld event logs should be exported into each
trace result directory; sandbox stdout alone is not evidence that an AppWorld API
call succeeded.
