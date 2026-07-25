# Changelog

All notable changes to Memoree are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use semantic versioning while the protocol remains pre-1.0.

## [Unreleased]

## [0.7.0] - 2026-07-25

### Changed

- Qualification is now `lexical_qualification_v2`: each free content unit also matches its own English inflections (`log`/`logs`/`logging`, `debug`/`debugging`, `autoscale`/`autoscaler`). Folding is query-side, deterministic, and bounded to six surface forms; the index, tokenizer, and `required_matches` threshold are unchanged, and a folded unit still counts exactly once. Quoted phrases and hyphenated identifiers stay exact.
- Typo tolerance is no longer a second, weaker gate. Fuzzy qualification now counts the same content units as the lexical gate, requires every phrase group to match, and must satisfy the stricter of its own historical ratio and the lexical requirement. Previously a query's distinguishing term could be missing entirely while common words carried an unrelated claim into `presence`.
- Recall and retrieve select the claim window from a slate four times the requested size: identical and near-identical statements collapse onto their best-ranked copy, and one source artifact may take at most two slots before another source is offered one. Claims in a conflict relation, and statements differing in any number, identifier, or negation, are never collapsed.
- Long artifacts return the excerpt window around the match instead of the head of the document, with a new `excerpt_citation` naming exactly the shown bytes. The parent `citation` still names the whole matched span, so recall and search stay comparable.
- Recovery evidence must share vocabulary with the routed question (`recovery_lexical_relation_v1`). Withheld spans are reported as `suppressed_references`; an unrelated fragment cannot route reasoning and can point a reader at the rejected side of a decision.
- The claim compiler is instructed to name each statement's subject explicitly, because claims are retrieved without their source document. `remember` reports a `REMEMBER_UNNAMED_SUBJECT` review finding for statements that open with a pronoun, demonstrative, or bare role noun.
- The canonical agent skill now tells agents to re-query with a rewording rather than pass it as `--reformulation`, which can only widen unqualified recovery evidence and can never qualify a claim.

### Added

- `artifact.forget` now returns `physically_erased: false` and a retention notice stating that bytes remain in the database, FTS indexes, WAL, blob store, snapshots, and backups. No version implements selective physical erasure, and a caller who believed otherwise might skip rotating a leaked credential or share a store they thought was clean. `docs/configuration.md` gains a data retention and erasure section with the whole-store destruction procedure.
- Write-time retrieval anchors. The claim compiler emits at most six short category or synonym terms per claim — `orm` and `query builder` for a claim naming Kysely, `tenant isolation` for one about row-level security — stored as a disposable derived projection bound to that claim's exact cited spans. An anchor match promotes the claim into the candidate tier and routes recovery to its authoritative source bytes; it can never qualify `presence`, be quoted, or enter a context bundle, and anchors carrying a value, number, or negation are refused by the compiler contract. Anchors must also be rare across stored anchors before they promote anything: a category word a dozen claims share says nothing about which one answers the question. `recovery.anchor_routed_references` reports how many spans were reached this way.
- `retrieve` runs its recovery step whenever no claim answered, not only on a bare `none`. A weak artifact match previously suppressed the entire recovery path, which is the state where routing help matters most.
- `retrieve` returns `next_action` when nothing qualified: why the question did not match, the content units that matched nothing, and vocabulary taken only from leads that share wording with the question. It never contains an answer.
- `retrieve` returns `claims_truncated`, so a caller can tell a complete answer from a window truncated by a heavily discussed entity.
- `semantic enable` and `semantic enable-reranker` report `daemon_reload` when a daemon is already running. Retrieval model runtimes are initialized during startup warm-up only, so a model installed underneath a resident daemon was silently unused and every query failed open to deterministic ordering.

### Known limitations

- **No selective physical erasure.** `artifact.forget`, `claim.retract`, and `source.withdraw` stop retrieval; bytes remain in the database, FTS indexes, WAL, blob store, snapshots, and backups. The forget response says so and `docs/configuration.md` documents whole-store destruction as the only remediation. Do not store secrets, credentials, or data under a deletion obligation.
- **Optional model installation has no network timeout.** `semantic enable` and `semantic enable-reranker` can block indefinitely on a degraded network; the pinned `hf-hub` sync API exposes no timeout setting. Interrupt and retry, or install from a local directory with `--from-directory`.
- **The installer/upgrade end-to-end tests are extremely slow** — the full 13-test CLI suite takes over two hours on a developer machine, dominated by update-check timeouts. 211 library tests, the nine non-upgrade end-to-end tests, and both committed retrieval corpora pass in about a minute. A 0.6-era store with 263 artifacts and 819 claims was opened under this build, migrated without schema change, and verified clean.
- **No task-level evidence.** Nothing here demonstrates that an agent using Memoree completes real development work better than one without it; the project's own 0.5 audit failed to show token savings at equal quality. Retrieval quality and safety are the only claims this release supports.
- Three known retrieval misses remain where the asker's distinguishing word appears in neither the claim nor its anchors. They abstain rather than answer wrongly.

## [0.6.0] - 2026-07-21

### Added

- `memory.retrieve` and `memoree retrieve`: one bounded qualified-or-recovery response that preserves normal presence semantics, returns at most 12 KiB of exact cited recovery evidence, admits automatic bytes only from evidence attached to candidate claims, and abstains from dated candidate hydration when a query explicitly asks for post-migration/current source state.
- Conservative query profiles for historical/current-source intent, identifiers, and Latin, Cyrillic, Arabic, CJK, mixed, or unknown scripts. Profiles are routing metadata only and cannot change qualification, scope, lifecycle, or citations.
- An experimental disposable Git-aware project source index with hash-bound `memoree-project://` citations, explicit index/status/map/search/get commands, incremental rebuilds, secret-shaped data/generated/binary filters, and bounded changed-byte transactions. It is off by default and not routed by the canonical agent skill.
- Native pinned Tree-sitter structural projection for Rust, Python, JavaScript, TypeScript/TSX, and Go. `memoree project map` returns one task-oriented packet capped at eight leads/12 KiB with exact current-source excerpts, calls/imports/inheritance/containment/test relations, explicit inferred or ambiguous confidence, identifier-aware symbol search, and verified FTS fallback.
- Guarded opt-in reindex modes: `off` by default, explicit `on-search`, and an explicit foreground `watch` mode with one worker, a lock, debounce, adaptive polling/backoff, and file/byte/change budgets that preserve the last valid index on overflow or transient Git snapshot failure.
- `memoree profile retrieve`, a local content-free performance profiler reporting stage latency distributions, byte counts, and result counts without recording queries or retrieved text.
- Opt-in per-project real-operation metrics with bounded retention, sampling, a size cap, a separate disposable SQLite store, closed numeric/categorical fields, minute timestamps, best-effort retrieval/project-index instrumentation, privacy/integrity doctor, JSONL export, and irreversible explicit clearing. Queries, content, citations, paths, prompts, task labels, model names, and raw errors have no storage column and no telemetry leaves the machine.
- Explicit `memoree experiment begin|pair|record|report` workflows for matched task comparisons. Pair IDs are opaque, arm order is randomized and enforced, observations accept only tokens/time/tool/completion fields, and reports exclude incomplete pairs while showing median deltas, direction counts, an exact sign test, order balance, and unavoidable statistical caveats.
- Evaluator case selection, recovery-only mode, single-worker enforcement, model prewarming, soft per-case budgets, a hard whole-suite deadline, honest timeout reporting, and machine-readable stage timing output.

### Changed

- The canonical `use-memoree` skill now prefers the one-call historical retrieval packet and sends current-code navigation to repository tools. It does not route agents through the experimental project index after matched evaluations found additive verification cost and completeness losses. At 369 words it remains substantially shorter than the previous skill, and confirmed upgrades continue to synchronize it atomically into detected Codex and Claude homes.
- Project-index and metrics preferences now live in one atomic owner-private project-ID-keyed settings file outside the repository. Configuring either feature never rewrites `.memoree.toml`, keeps pre-0.6 clients compatible, and requires each collaborator to opt in independently. Experiment rows are exempt from event retention so long-running matched studies cannot be silently unpaired.
- Semantic setup timing now separates corpus loading, model loading, embedding generation, projection I/O, reranker work, recall, probe, citation fetching, refined recall, and serialization.
- Cross-encoder and dense components remain candidate/ordering-only. No model output can qualify presence, widen scope, change lifecycle, create citations, or enter a context bundle by itself.

### Quality evidence

- The pinned isolated v2 recovery gate completes all 11 cases with zero false answers, semantic-bait fetches, and case-forbidden fetches. The one-call path recovers 10/11 answerable cases (90.9%) with one safe current-source abstention for an unpromoted raw correction, stays below 12 KiB, and meets the two-second recovery latency gate on the tested host.
- Memoree does **not** claim general token, speed, or downstream quality improvement in this release. The controlled 0.5.0 audit did not demonstrate savings at equal quality and found substantial fixed skill and multi-call overhead. A claim requires at least ten real development-task A/B cases with equal completeness and at least 10% lower tokens.
- A matched one-turn Fable 5 calibration measured the shortened skill at +1,168 processed input tokens versus +4,326 for the 0.5 skill, a 73.0% reduction in fixed overhead. This is an instruction-cost result only, not an end-to-end token-savings claim.
- A one-run equal-answer reproduction of the prior off-repository-history question used 6,020 processed input tokens through the 0.6 skill/packet versus 3,669 for a minimal oracle (1.64×, improved from 11.2× in the 0.5 audit). Memoree remained 3.83× the oracle cost and 3.43× the latency, so the decisive multi-task benchmark is still required.
- Structural projection gates cover exact spans, at least 95% labelled definition recall per supported language, explicit duplicate-target ambiguity, excluded-path isolation, incremental/clean rebuild equivalence, verified current-source citations, bounded packets, and a fast non-writing `not_ready` path. These are correctness and safety results, not a downstream token-savings claim.
- The structural map failed its downstream promotion gate. On a fresh ten-case complex-task batch it saved 7.38% processed input tokens but was slower by 5.22% and blind quality was worse in 8/10 cases. Ten narrow small-repository lookups cost 55.3% more tokens and 36.8% more latency; five narrow lookups in a 4,227-file repository cost 36.4% more tokens and 54.7% more latency with equal correctness. It therefore remains explicit and experimental.

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
- Daemon autostart and upgrade restart now share a bounded 30-second cold-model readiness window and fail immediately when the spawned process exits.

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

[Unreleased]: https://github.com/devcited/memoree/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/devcited/memoree/releases/tag/v0.6.0
[0.5.0]: https://github.com/devcited/memoree/releases/tag/v0.5.0
[0.4.1]: https://github.com/devcited/memoree/releases/tag/v0.4.1
[0.3.0]: https://github.com/devcited/memoree/releases/tag/v0.3.0
[0.2.0]: https://github.com/devcited/memoree/releases/tag/v0.2.0
