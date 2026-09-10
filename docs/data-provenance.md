# Data Provenance

The source dataset is:

https://huggingface.co/datasets/Exgentic/agent-llm-traces
```

The repository does not include the raw dataset. Corpus generation records:

- Dataset identifier and revision.
- Selected session IDs.
- Input and normalized corpus SHA-256 values.
- Turn and tool-call counts.
- Phantom-turn normalization.
- Recorded tool-result availability.

The normalized replay corpus is not byte-for-byte parquet data. It is a
replay-oriented representation that preserves source metadata, model decisions,
tool calls, and recorded tool results, while adding replay fields such as
mapped sandbox commands.
