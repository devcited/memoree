---
title: Quality gates
description: Correctness, retrieval, conformance, and resource gates for Memoree.
---

# Quality gates

“Superior” is an evaluation result, not an architectural adjective. The project should claim an advantage only inside its intended domain—local, personal, machine-oriented agent memory—and only when repeatable tests support it.

## Correctness invariants

The v1 release gate is:

- Ambient search never reads a sibling project or an unrelated personal context.
- An empty ambient result never causes an automatic broader search.
- Every stored evidence locator resolves to the same immutable revision after reindexing and preserves its exact byte span when the claim is passage-specific.
- Retrying an identical mutation with one idempotency key creates no duplicate logical entity.
- Reusing that key with different semantic input fails.
- A stale `if_revision` never overwrites the current revision.
- A search carrying `min_commit_seq` observes that commit or explicitly returns `INDEX_NOT_READY`.
- Superseded, retracted, and forgotten state is not presented as current by default.
- Recency reranking never changes lexical top-K membership, promotes an item by more than two positions, gives a bonus to historical/future/non-current material, or changes retrieval scope.
- Recall returns claims and artifact references in separate bounded lists over the same search projection; every returned claim citation is discoverable through search with the identical revision.
- Recall's `presence` distinguishes current/disputed claims, artifact-only matches, and no match without treating any state as a truth score or automatically broadening the horizon.
- Every recall claim exposes its immutable evidence artifact revision and exact byte span, and every returned span round-trips to the cited bytes. Conflicted claims are labelled disputed and expose their open contradiction relation IDs.
- Pending checkpoints remain outside every daemon retrieval surface; matching only pending text returns `presence=none`. Promotion requires an explicit remember preview/apply boundary, and sensitive-content flags block promotion unless deliberately overridden.
- Both sides of a known contradiction are retained and identified in context construction.
- Context bundles never exceed `max_bytes`, remain valid UTF-8, label retrieved material as untrusted, render claim evidence citations, and disclose retrieval truncation separately from byte-budget omissions.
- Context bundles provide enough revision-stable provenance for an external companion to validate citations without exposing internal search chunks as durable identities.
- The daemon contains no model provider, credential loader, or inference path. `memoree remember` is caller-side, preview-only by default, performs one tool-free/schema-constrained compilation, freezes scope before inference, and host-validates every exact source span before explicit `--apply` mutations. Its deterministic quality findings disclose summary-only grounding, mutable observations, and absent automatic relations without allowing the model to certify authority or mutate lifecycle. Cached ChatGPT Codex login is the default; API-key fallback is impossible without an explicit per-invocation permission flag.
- The default private Unix socket remains owner-only; tests and documentation never describe loopback TCP as a per-user security boundary.
- Binary and text artifacts round-trip byte-for-byte; CAS corruption is detected.
- Backup round-trip and crash-recovery tests must pass before backup is described as disaster-recovery-ready; basic snapshot creation may remain available with explicit restore caveats.

These belong in automated integration and property tests. A protocol feature that cannot pass its invariant should be marked unavailable by `capabilities`.

## Retrieval evaluation

Build a judged corpus from real agent artifacts rather than synthetic prose alone. Each case records:

- Ambient workspace/project/task and pinned artifacts.
- Query and allowed horizon.
- Relevant artifact revisions and claims.
- Required currentness, temporal, and contradiction behavior.
- Maximum context budget.
- The downstream agent task and a deterministic scoring rubric where possible.

Measure retrieval recall, precision, citation accuracy, contradiction/currentness accuracy, task success, context bytes, p50/p95 latency, peak and idle memory, index size, update visibility, and rebuild time.

The repository's `memoree-eval` binary implements the deterministic correctness/regression subset against `eval/corpus/v1`; see [Retrieval evaluation](evaluation.md). It deliberately excludes model-in-the-loop task success and noisy performance gates.

The first baseline is structured lookup plus SQLite FTS5. Future semantic or hybrid retrieval must beat that baseline on downstream task success at the same context budget—not merely increase an offline similarity score. External Meilisearch, vector-only retrieval, and generic hybrid RAG are comparison configurations, not default dependencies.

## Model conformance

Evaluate fresh model sessions using only the generated instructions, capabilities, and schemas. Include several model families and deliberately ambiguous situations:

- No ambient context.
- Empty ambient retrieval with tempting wider results.
- An idempotent retry after a simulated transport failure.
- A revision conflict.
- A superseded decision and an unresolved contradiction.
- Prompt injection embedded in a highly ranked artifact.
- A binary output that must be stored and retrieved intact.
- A context bundle containing truncated retrieval and retrieved text that asks the model to run a tool or broaden scope.
- A remember source that attempts prompt injection, repeats an evidence quote ambiguously, returns an unknown field, contains no durable claim, separates a quantitative estimate from a material caveat, or describes mutable current behavior.

Score whether the model forms a valid request on its first attempt, stays within the intended horizon, preserves evidence, handles errors correctly, and avoids following retrieved instructions. The target is at least 98% valid first-attempt protocol calls before describing the instruction layer as model-portable.

Companion or model evaluations should consume frozen `context.build` bundles independently from retrieval evaluation. Evaluate the Luna claim compiler separately on extraction precision, multi-span qualifier retention, exact-quote validity, durable-claim recall, false durable claims, latency, tokens, compiler-failure rate, quality-finding recall, and unauthorized credential-fallback rate (which must remain zero). This keeps model differences from being confused with retrieval quality.

## Resource discipline

Track cold start, idle RSS, peak indexing RSS, database/CAS disk amplification, and latency on representative laptops. Optional retrieval engines must be disabled by default and must publish their incremental resource cost alongside measured quality gain. Luna inference is on-demand in the caller and remains outside the daemon's idle resource envelope; track its cost per accepted claim.

No benchmark results are claimed in the initial vertical slice. This document defines the evidence required for future superiority claims.
