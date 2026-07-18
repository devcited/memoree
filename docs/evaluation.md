---
title: Retrieval evaluation
description: Run the deterministic retrieval regression suite and interpret its limits.
---

# Retrieval evaluation

Memoree ships a deterministic retrieval regression runner and a small versioned starter corpus. It exists to protect the agent-facing `memory.recall`, raw `search`, and `context.build` contracts—not to claim that FTS5 is universally superior.

Run it from the repository root:

```sh
cargo run --locked --bin memoree-eval -- eval/corpus/v1 --pretty
```

The runner creates a fresh temporary Memoree store, loads the corpus only through normal protocol mutations, executes every case through `memory.recall`, `search`, and `context.build`, prints one JSON report, and exits nonzero on a hard invariant or committed-baseline regression. It never opens the user's configured Memoree data directory.

Each versioned corpus directory contains:

- `seed.jsonl`: labelled artifacts, grounded claims, and lifecycle relations in creation order. Evidence may name a unique exact quote; the runner computes and stores its byte span.
- `cases.jsonl`: ambient context, query, optional explicitly justified horizon, relevant/forbidden labels, expected three-state recall presence, expected conflicts, and context byte budget.
- `baseline.json`: the reviewed aggregate recall/precision result for that corpus version and the allowed regression epsilon.

Hard checks cover:

- `claims`, `artifacts_only`, and `none` presence semantics;
- relevant and forbidden entity labels;
- open conflict surfacing;
- exact evidence revision/span resolution and excerpt round-trip;
- recall/search citation parity;
- ambient/workspace scope containment;
- context byte-budget compliance; and
- rendered claim evidence citations in context bundles.

Macro recall and precision are regression signals against the committed baseline. They are not downstream task success and must not be presented as a benchmark result. Latency is intentionally absent from the gate because one-shot development runs are too noisy; measure it separately in a controlled benchmark environment.

When adding cases, prefer anonymized failures from real agent retrieval. Keep known-hard cases as `"gate":"report"` until the current baseline handles them, then promote them to `hard` in a reviewed corpus-version change. A retrieval-engine or ranking change should reuse the same corpus and byte budgets; semantic or hybrid retrieval earns adoption only after it improves downstream agent task success without breaking these deterministic invariants.
