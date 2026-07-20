# Changelog

All notable changes to Memoree are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use semantic versioning while the protocol remains pre-1.0.

## [Unreleased]

## [0.3.0] - 2026-07-20

### Added

- Deterministic query analysis, typo-tolerant trigram qualification, exact long-document chunk citations, and conflict-aware result retention.
- Explicitly installed, pinned local dense retrieval as a candidate-only projection with incremental rebuilds.
- Bounded `unqualified_candidate` claim and artifact suggestions with exact citations, risk signals, and no effect on recall presence or context construction.
- A disjoint reranker calibration evaluator and an opt-in claim-only ordering model with startup warm-up, fail-open behavior, and a latency circuit breaker.
- A realistic v2 retrieval corpus covering paraphrase, typos, long documents, honest abstention, scope, temporal behavior, explicit broadening, and conflicts.
- An idempotent upgrade reconciler with daemon version/ownership checks, durable phase state, and automatic synchronization of the canonical Codex/Claude skill.
- Private, verified pre-migration snapshots for every schema 1–3 store before schema 4 is committed.

### Changed

- Retrieval authority now filters scope, currentness, and lifecycle before any model work; exact-tier qualification and order remain model-independent.
- The generated machine instructions and Codex/Claude skills now teach agents to fetch and corroborate candidate citations rather than treating suggestions as facts.
- The stable installer now preserves running/stopped daemon state, reconciles existing local projections without downloading models, and rolls binaries back after a pre-migration failure.

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

[Unreleased]: https://github.com/devcited/memoree/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/devcited/memoree/releases/tag/v0.3.0
[0.2.0]: https://github.com/devcited/memoree/releases/tag/v0.2.0
