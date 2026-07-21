# Project-structure agent evaluation v1

This corpus tests whether Memoree's experimental project index improves real agent outcomes over ordinary repository search and file reads. It is separate from deterministic parser and retrieval correctness tests.

- `cases.jsonl` contains complex development questions. Cases 1–10 were used during development; later cases are frozen held-out tasks.
- `lookup-cases.jsonl` contains narrow definition and direct-relation questions.
- `results/2026-07-21.json` records aggregate matched outcomes. Raw model transcripts are not committed.

Run each task in an isolated repository clone with the same model, effort, tool permissions, and source snapshot. The baseline can use repository tools but not Memoree. The treatment receives the canonical skill and a prebuilt isolated project index. Include ordinary, cache-creation, and cache-read input in processed input; record elapsed time and turns separately. Blind-grade both answers for correctness and completeness before inspecting cost.

Do not tune on a frozen held-out batch and then report that batch as confirmation. Promotion needs two fresh pre-registered batches, at least 90% non-loss and zero severe misses in each routed stratum, equal-or-better aggregate blind quality, at least 10% lower processed input, and latency within +10%. Report initial index time and resource cost. Whole-corpus and graph-derived baselines do not qualify.
