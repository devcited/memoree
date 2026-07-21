---
title: Retrieval evaluation
description: Run the deterministic retrieval regression suite and interpret its limits.
---

# Retrieval evaluation

Memoree ships a deterministic retrieval regression runner and a small versioned starter corpus. It exists to protect the agent-facing `memory.recall`, raw `search`, and `context.build` contracts—not to claim that FTS5 is universally superior.

Run it from the repository root:

```sh
cargo run --locked --bin memoree-eval -- eval/corpus/v2 --pretty
```

The runner creates a fresh temporary Memoree store, loads the corpus only through normal protocol mutations, executes every case through `memory.recall`, `search`, and `context.build`, prints one JSON report, and exits nonzero on a hard invariant or committed-baseline regression. It never opens the user's configured Memoree data directory.

Each versioned corpus directory contains:

- `seed.jsonl`: labelled artifacts, grounded claims, and lifecycle relations in creation order. Evidence may name a unique exact quote; the runner computes and stores its byte span.
- `cases.jsonl`: ambient context, query, optional explicitly justified horizon, relevant/forbidden labels, expected three-state recall presence, expected conflicts, and context byte budget.
- `probe-recovery.json`: frozen original/reformulated query pairs, selected source labels, exact-fetch expectations, and refined recall checks for the bounded paraphrase-recovery path.
- `baseline.json`: the reviewed aggregate recall/precision result for that corpus version and the allowed regression epsilon.

Hard checks cover:

- `claims`, `artifacts_only`, and `none` presence semantics;
- relevant and forbidden entity labels;
- open conflict surfacing;
- exact evidence revision/span resolution and excerpt round-trip;
- recall/search citation parity;
- ambient/workspace scope containment;
- candidate suggestion recall without changing qualified presence;
- candidate scope, lifecycle, citation, and injection-label boundaries;
- context byte-budget compliance; and
- rendered claim evidence citations in context bundles.

The v2 corpus adds natural paraphrases, typo/abbreviation queries, long-document needles and distractors, honest-none cases, explicit broadening, conflict completeness, semantic bait sources, and a stale-claim/fresh-raw correction. Reports separate direct qualified recall, one-reformulation probe source availability, exact-fetch bytes, and independent refined qualified recovery. Each original/probe query pair is recorded. Reformulations are frozen from Fable 5 runs made outside the repository without memory/source context; title/fetch verdicts are frozen separately. Correctness is then checked deterministically by stable source labels, citations, and qualified returned entities rather than live model judgment.

The semantic gate currently requires paraphrase source recall of at least 80% at depth five and 90% at the shipped depth eight, end-to-end qualified recovery of at least 90%, zero false answers and bait fetches, false abstention at or below 10%, probe metadata below 2 KiB/3 KiB at depth five/eight, exact citation round trips, at most nine fetched references and 12 KiB of fetched text, and a complete serialized recovery pipeline no larger than 12 KiB. Wire accounting conservatively rounds each independently serialized response to a 64-byte block so timestamp formatting cannot make the gate nondeterministic. A recovery counts only when the selected source is fetched through at least one ranged `citation.get`, every fetched range verifies exactly, and the refined normal recall independently returns the relevant qualified claim or source.

For the 0.5.0 release candidate, the pinned local semantic/reranker configuration passes all 11 recovery cases: depth-five source recall is 10/11, depth-eight source recall and end-to-end recovery are 11/11, false answers, false abstentions, and bait fetches are zero, and the largest serialized recovery path is 8,320 bytes. Direct qualified paraphrase claim recall remains 20%; the measured improvement belongs to the explicit bounded recovery workflow, not to ordinary recall alone.

A separate isolated shadow on one real 390-document development project used ten natural paraphrases chosen before evidence inspection. Nine questions were answerable and all nine completed the probe → exact fetch → qualified recall path; the tenth correctly abstained because memory named the owner of an acknowledgment but not its recipient. No answer relied on an unqualified candidate, no false answer occurred, each exact packet stayed below 3.2 KiB and nine references, and the installed binary and live store were not touched. This is useful external-validity evidence, not a committed statistical gate.

The 11-case slice and ten-question shadow are regression evidence, not a universal benchmark; publish counts alongside rates and expand with anonymized real failures before making broad quality claims.

Macro recall and precision are regression signals against the committed baseline. They are not downstream task success and must not be presented as a universal benchmark result. Latency is intentionally absent from the deterministic gate because one-shot development runs are too noisy; measure model-load and inference latency separately in a controlled benchmark environment.

When adding cases, prefer anonymized failures from real agent retrieval. Keep known-hard cases as `"gate":"report"` until the current baseline handles them, then promote them to `hard` in a reviewed corpus-version change. A retrieval-engine or ranking change should reuse the same corpus and byte budgets; semantic or hybrid retrieval earns adoption only after it improves downstream agent task success without breaking these deterministic invariants.

Cross-encoder qualification has a separate disjoint pair evaluator and cannot be enabled by end-to-end recall scores alone:

```sh
memoree semantic evaluate-reranker \
  --model-directory /path/to/pinned/model \
  --calibration eval/reranker/calibration-v1.jsonl \
  --heldout eval/reranker/heldout-v1.jsonl
```

The checked-in pair sets intentionally remain underpowered and therefore cannot promote a qualification threshold. Ordering-only admission and default-on promotion have separate quality/latency gates documented in [Quality gates](quality.md).
