---
title: Project metrics and experiments
description: Measure real local retrieval behavior and run explicit paired token/time experiments without retaining queries or content.
---

# Project metrics and experiments

Memoree has two deliberately separate measurement surfaces:

- `memoree profile retrieve` repeats one query for an ad hoc pipeline profile.
- Project metrics observe opted-in real CLI operations over time. Explicit experiments compare matched task arms. Passive traces are never treated as an A/B control.

Both are local. Nothing is sent to Memoree or another network service.

## Opt in

Metrics are disabled by default. Enable them from the initialized project:

```sh
memoree metrics configure \
  --enabled true \
  --retention-days 14 \
  --max-database-bytes 10485760 \
  --sample-rate 1.0
```

This writes only safe configuration to an atomic owner-private, project-ID-keyed settings file below Memoree's application data directory:

```toml
schema = 1

[metrics]
enabled = true
retention_days = 14
max_database_bytes = 10485760
sample_rate = 1.0
```

Neither configuration nor observations enter `.memoree.toml` or the repository. Each collaborator must opt in independently, and pre-0.6 clients continue to parse the unchanged marker. The separate disposable SQLite database lives below Memoree's private data directory, not in the authoritative memory database, CAS, or project index.

## Operational report

Normal `retrieve`, `recall`, `probe`, memory search, context build, feedback, and project-index operations record a sampled event after completion. Instrumentation uses zero lock-wait time: a contended sample is dropped, and any metrics error or panic is contained without changing the primary operation's result. A successful local insert still adds small post-operation CLI overhead; externally submitted experiment elapsed time is the end-to-end measurement surface.

```sh
memoree metrics status
memoree metrics report --days 7
memoree metrics doctor
memoree metrics export --days 7 --output /tmp/memoree-metrics.jsonl
```

Reports group counts, outcomes, errors, p50/p95 latency, response bytes, retrieval stages, model load/inference, breaker state, index freshness, reindex attempts, and indexing volume. Explicit `feedback.record` outcomes appear as useful, miss, incorrect, or stale counts. The hidden timing profile used for enabled real retrieval is removed from ordinary CLI output unless the caller explicitly requested `--profile`.

Operational traces diagnose latency and behavior. They cannot establish token savings because a production event does not observe the same task without Memoree.

## Privacy contract

The metrics database has no columns for queries, retrieved content, prompts, citations, file paths, source identifiers, free-text labels, raw errors, model names, or notes. Stored timestamps are rounded to minutes. Experiment and pair IDs are random opaque hashes without embedded task text or recoverable creation time.

`metrics doctor` checks SQLite integrity, live secure-delete/full-auto-vacuum/zero-wait pragmas, the exact closed column set, categorical allowlists, owner-private permissions for settings and observations, event retention, size, and that the database is outside the project tree. Export also requires a canonical destination outside the project tree, refuses an existing file through exclusive creation, writes mode `0600` on Unix, and includes only the same closed schema. Event retention is enforced whenever the database opens and uses secure deletion plus full auto-vacuum. Experiment assignments and observations are exempt from time-based retention so an extended matched study cannot be silently unpaired; the database size cap still applies, and a full store refuses further growth rather than deleting part of an experiment. Remove all observations and experiments explicitly with:

```sh
memoree metrics clear --yes
```

Deletion is irreversible. Disabling collection preserves event rows until retention or explicit clearing and preserves experiment rows until explicit clearing.

## Randomized paired experiments

Declare one primary metric before running tasks:

```sh
memoree experiment begin --primary tokens
memoree experiment pair --experiment exp_OPAQUE_ID
```

`pair` returns an opaque pair ID plus randomized `first_arm` and `second_arm`. Keep the mapping from that ID to the real task outside Memoree. Record the assigned first arm before the second:

```sh
memoree experiment record \
  --pair pair_OPAQUE_ID \
  --arm memory \
  --tokens 6020 \
  --elapsed-ms 8039 \
  --tool-calls 7 \
  --completed \
  --completeness 5
```

Omit `--completed` for an unsuccessful task. Completeness is optional and restricted to the ordinal range 1–5. Observations are immutable and accept no task name or note.

```sh
memoree experiment report exp_OPAQUE_ID --pairs
```

Only pairs with both arms are summarized. Reports show median memory-minus-baseline absolute and percentage deltas, direction counts, an exact two-sided sign test that excludes ties, completion counts, order balance, and optional raw opaque pair deltas. The declared primary metric is confirmatory; other fields are exploratory. With fewer than six non-tied pairs, a two-sided sign-test result below 0.05 is mathematically unattainable. Under 30 pairs, inspect individual deltas and treat the statistic as descriptive evidence because learning, order, optional stopping, project drift, and provider token-accounting differences can dominate the result.
