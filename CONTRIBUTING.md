# Contributing

Contributions should include tests and documentation for user-visible changes.

Run before submitting a change:

```bash
python -m compileall -q src scripts
pytest -q
```

Cluster changes must be namespaced, reversible, and documented. Never commit
real credentials, kubeconfigs, private image references, or raw sensitive
trace data.
