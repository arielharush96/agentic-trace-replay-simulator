# AppWorld-Backed Mode

The default benchmark replays recorded tool results. AppWorld-backed mode is
optional and executes tools against the functional, stateful AppWorld engine.

## Install Locally

Use a dedicated AppWorld root and environment:

```bash
APPWORLD_ROOT=/path/to/appworld \
bash scripts/setup_appworld.sh
APPWORLD_ROOT=/path/to/appworld \
bash scripts/verify_appworld.sh
```

AppWorld is not installed by the normal benchmark setup and its data is not
included in this repository.

## Build A Stateful Corpus

The source trace session must be mapped to the matching AppWorld task ID and
initial state:

```bash
python scripts/build_appworld_corpus.py \
  --in data/generated/session/corpus.json \
  --out data/generated/session/appworld-corpus.json \
  --endpoint http://appworld:8090/execute
```

The adapter requires `APPWORLD_AUTH_TOKEN`; provide the same value to replay
workers as `APPWORLD_API_TOKEN`. Keep both values in namespace-scoped Secrets
or the runtime environment, never in the corpus or command history.

This changes the mapped sandbox command. It does not change the recorded model
decisions. The command invokes the AppWorld adapter, which calls the stateful
AppWorld API and returns a newly generated result.

## Security And Validity

- Use only simulated AppWorld accounts and databases.
- Do not use production credentials or unrestricted network access.
- Keep AppWorld outside the OpenShell sandbox when measuring sandbox overhead.
- Record the AppWorld package/data version and task mapping.
- Export the adapter's `appworld_tool` events into the per-trace result directory;
  sandbox stdout alone is not evidence that an AppWorld API call succeeded.
- Compare generated results with recorded trace results before claiming exact
  reproduction.
- A trace without its matching AppWorld task/state cannot be reproduced exactly.
