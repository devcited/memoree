# Changelog

All notable changes to Memoree are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use semantic versioning while the protocol remains pre-1.0.

## [Unreleased]

## [0.5.0] - 2026-07-21

### Added

- Explicit `memory.probe` paraphrase recovery with one audited reformulation, compact title routing, provenance-labeled exact source arrays, and original-question relevance checks.
- Deterministic clause-aware resolution of legacy whole-revision evidence into up to three non-overlapping, authority-hash-verified UTF-8 windows.
- Contextual semantic projection v3, exact bounded `citation.get`, and an expanded retrieval evaluation that measures source discovery, exact fetches, refined recovery, abstention, and token bounds separately.
- Automatic confirmed-upgrade installation of a signed-release-digest-pinned TinyBERT-L2 ordering model, with an explicit opt-out and deterministic offline fallback.

### Changed

- Claim-backed and raw-artifact candidates from the same immutable source remain separate leads, preventing relevance or evidence from being borrowed while preserving fresh raw corrections.
- The local reranker exposes only a stable permutation, cannot cross exact or qualification tiers, serializes no logits, orders a diversified top-eight fused plus top-eight dense slate in batches of eight, and calibrates a 75–150 ms host-specific breaker budget from ten fixed startup samples; five consecutive overruns open it, with faster two-probe recovery.
- The canonical `use-memoree` skill now pins the target repository, fetches the highest-ranked ranged lead followed by up to two title-selected leads only as needed within nine references/12 KiB, requires exact predicate-role/facet matching, and finishes with one same-scope qualified recall judged against the original question.
- Confirmed upgrades atomically refresh that canonical skill in every detected Codex and Claude home while preserving differing prior copies.

## [0.4.1] - 2026-07-20

### Added

- An out-of-process source synchronization contract with stable external identities, revision-safe ingestion, cursor and health checkpoints, and honest logical withdrawal.
- Evidence-linked derived retrieval projections that can discover cited artifact candidates without qualifying recall presence or entering context bundles by themselves.
- Explicit privacy-aware retrieval feedback with keyed query fingerprints by default and opt-in raw-query retention for offline evaluation.
- Signed release manifests and confirmation-based automatic updates for installer-managed copies, with bounded checks, per-version deferral, full reconciliation, and exact command re-execution.

### Changed

- Store schema 5 is created automatically, with a private verified pre-migration snapshot for schema 4 installations and the existing full migration chain for earlier stores.
- Every normal data-touching command now fails fast while a signed update or schema reconciliation owns the local upgrade lock.
- Release CI signs the installer digest and every target archive checksum before an immutable GitHub Release is published.

## [0.3.0] - 2026-07-20

### Added

- Deterministic query analysis, typo-tolerant trigram qualification, exact long-document chunk citations, and conflict-aware result retention.
- Explicitly installed, pinned local dense retrieval as a candidate-only projection with incremental rebuilds.
- Bounded `unqualified_candidate` claim and artifact suggestions with exact citations, risk signals, and no effect on recall presence or context construction.
- A disjoint reranker calibration evaluator and an opt-in claim-only ordering model with startup warm-up, fail-open behavior, and a latency circuit breaker.
- A realistic v2 retrieval corpus covering paraphrase, typos, long documents, honest abstention, scope, temporal behavior, explicit broadening, and conflicts.
- An idempotent upgrade reconciler with daemon version/ownership checks, durable phase state, and automatic synchronization of the canonical Codex/Claude skill.
- Private, verified pre-migration snapshots for every schema 1–3 store before schema 4 is committed.
- Authenticated Codex/Claude compiler discovery with live CLI model catalogs, private provider/model selection, Claude Sonnet compilation, and auditable compiler provenance.

### Changed

- Retrieval authority now filters scope, currentness, and lifecycle before any model work; exact-tier qualification and order remain model-independent.
- The generated machine instructions and Codex/Claude skills now teach agents to fetch and corroborate candidate citations rather than treating suggestions as facts.
- The stable installer now preserves running/stopped daemon state, reconciles existing local projections without downloading models, and rolls binaries back after a pre-migration failure.
- Existing installations retain the former Codex/Luna compiler default when it remains eligible; fresh dual-login setups prompt once, while non-interactive ambiguity and missing logins fail loudly.

## [0.2.0] - 2026-07-18

### Added

- Local Rust CLI and daemon over a private Unix socket, with deliberately gated TCP support.
- Ambient workspace, project, and task context with explicit broader retrieval horizons.
- Immutable artifact revisions, content-addressed storage, provenance-preserving claims, relations, lifecycle history, and conflict cases.
- Honest claim-first recall with exact evidence byte spans and separate artifact-only references.
- Bounded, injection-aware context construction for external agents.
- Deliberate session checkpoints quarantined outside durable recall until reviewed.
- Caller-side, preview-first natural-language claim compilation through a locally authenticated Codex CLI.
- Verification, atomic backup creation, deterministic retrieval evaluation, and a versioned machine protocol/schema.
- Static documentation, checksummed Unix release binaries, and a no-sudo installer.

[Unreleased]: https://github.com/devcited/memoree/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/devcited/memoree/releases/tag/v0.5.0
[0.4.1]: https://github.com/devcited/memoree/releases/tag/v0.4.1
[0.3.0]: https://github.com/devcited/memoree/releases/tag/v0.3.0
[0.2.0]: https://github.com/devcited/memoree/releases/tag/v0.2.0
