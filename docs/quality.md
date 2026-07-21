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
- Candidate suggestions are additive and always labelled `unqualified_candidate`; they never affect `presence`, leak sibling scope, resurrect forgotten/retracted/superseded revisions, hydrate claim status/evidence, or enter `context.build`.
- Exact-tier qualification and ordering are byte-identical with local models absent, ready, errored, surface-disabled, or breaker-open. Dense similarity and cross-encoder logits are candidate/ordering signals only.
- Every recall claim exposes its immutable evidence artifact revision and exact byte span, and every returned span round-trips to the cited bytes. Conflicted claims are labelled disputed and expose their open contradiction relation IDs.
- Pending checkpoints remain outside every daemon retrieval surface; matching only pending text returns `presence=none`. Promotion requires an explicit remember preview/apply boundary, and sensitive-content flags block promotion unless deliberately overridden.
- Both sides of a known contradiction are retained and identified in context construction.
- Context bundles never exceed `max_bytes`, remain valid UTF-8, label retrieved material as untrusted, render claim evidence citations, and disclose retrieval truncation separately from byte-budget omissions.
- Explicit paraphrase recovery keeps candidate titles and fetched bytes outside qualified context, pins the target scope, uses exactly one meaning-preserving reformulation and one depth-eight probe, fetches the highest-ranked ranged lead followed by at most two title-selected leads only as needed, caps exact fetches at nine refs/12 KiB, and requires independently qualified recall with exact entity/predicate-role/facet coverage against the original question. Semantic-bait answers/fetches remain zero and unsupported cases abstain with the missing fact identified.
- `citation.get` round-trips exact immutable UTF-8 slices, safely narrows oversized spans, refuses revision-only prefixes and binary content, survives adversarial byte boundaries without panic, and keeps large exact fetches below 10% of whole-artifact response bytes in the committed gate.
- Derived projections can recover only unqualified artifact candidates, always resolve to exact immutable raw spans, never affect recall presence, and never enter `context.build` without independent raw qualification.
- Source ingestion is stable and idempotent by external id/revision/payload; revision reuse with different bytes fails closed, while withdrawal removes future retrieval without claiming physical erasure.
- Feedback capture is explicit, raw queries are absent by default, and only deliberately retained queries enter offline exports. Feedback never changes live ranking automatically.
- Automatic updates execute only an Ed25519-verified release manifest and digest-verified installer/archive, prompt only in an eligible terminal, serialize application, reconcile before re-exec, and fail closed on integrity errors.
- Context bundles provide enough revision-stable provenance for an external companion to validate citations without exposing internal search chunks as durable identities.
- The daemon contains no remote model provider or credential loader. Optional local embedding and cross-encoder inference uses explicitly installed, revision/digest-pinned bytes, performs no query-time downloads, and fails open without changing authority or qualification. `memoree remember` is caller-side, preview-only by default, performs one tool-free/schema-constrained compilation through a live-catalog-validated Codex or Claude selection, freezes scope before inference, and host-validates every exact source span before explicit `--apply` mutations. Its deterministic quality findings disclose summary-only grounding, mutable observations, and absent automatic relations without allowing the model to certify authority or mutate lifecycle. API-key and third-party auth do not count as automatic login; API-key fallback remains impossible without an explicit per-invocation Codex permission flag.
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

Keep explicit `miss`, `useful`, `incorrect`, and `stale` feedback as a privacy-aware failure-mining stream. Export only cases whose raw queries were deliberately retained, review/anonymize them, label gold evidence independently, and admit them to a versioned corpus before changing ranking. The live store does not learn from feedback online.

The repository's `memoree-eval` binary implements the deterministic correctness/regression subset against the versioned corpora, including realistic v2 paraphrase, typo, long-document, scope, abstention, temporal, and conflict slices; see [Retrieval evaluation](evaluation.md). It deliberately excludes model-in-the-loop task success and noisy performance gates.

The mandatory fail-open baseline is structured lookup plus SQLite FTS5/trigram fusion. The dense projection is admitted only as candidate recall because it improves candidate-pool coverage while leaving qualification unchanged. The release-pinned TinyBERT cross-encoder is default-installed during confirmed upgrades, remains claim-ordering-only, and returns a permutation rather than scores over a per-tier top-eight fused plus top-eight dense slate. Ten fixed startup samples set its inference-only breaker budget to twice the upper median within a 75–150 ms clamp; sustained overruns fail open without letting a healthy host permanently disable useful ordering. It can never qualify or suppress an answer; absence, offline install failure, model error, or breaker-open state falls back deterministically. Artifact reranking remains disabled. External Meilisearch, vector-only retrieval, and generic hybrid RAG remain comparison configurations, not default dependencies.

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

Companion or model evaluations should consume frozen `context.build` bundles independently from retrieval evaluation. Evaluate the Luna and Sonnet claim compilers separately on extraction precision, multi-span qualifier retention, exact-quote validity, durable-claim recall, false durable claims, latency, tokens, compiler-failure rate, quality-finding recall, selection drift, and unauthorized credential-fallback rate (which must remain zero). This keeps provider/model differences from being confused with retrieval quality.

## Resource discipline

Track cold start, model-load and inference latency separately, idle RSS, peak indexing RSS, database/CAS disk amplification, update/rebuild cost, breaker state, and latency on representative laptops. Optional retrieval engines remain explicit installs and must publish their incremental resource cost alongside measured quality gain. A latency breaker acts only after a measured call and must never be described as a timeout for that call. Luna/Sonnet compiler inference is on-demand in the caller and remains outside the daemon's idle resource envelope; track its cost per accepted claim.

No benchmark results are claimed in the initial vertical slice. This document defines the evidence required for future superiority claims.
