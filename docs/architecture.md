---
title: Architecture
description: Context resolution, durability boundaries, storage, retrieval, and trust.
---

# Architecture

## Product boundary

`memoree` is a local knowledge appliance, not an agent runtime. It owns artifact lifecycle, memory assertions, context selection, retrieval, and context construction. It is designed for any shell-capable model to use through the same JSON CLI contract; cross-model conformance results have not been published yet.

The first release has one trust domain: the local user. There are no accounts, grants, tenant tokens, or authorization policies. The recommended/default Unix transport uses an owner-private runtime directory and mode-`0600` socket. TCP loopback limits exposure to the host but not to the user: another local OS user/process can connect. It is appropriate only for a trusted single-user host or an independently enforced boundary. Non-loopback listeners require the conspicuous `--dangerously-allow-non-loopback-tcp` process flag. The bundled loopback-published Compose profile has the same single-user-host assumption.

## Runtime shape

One Rust binary provides the CLI and daemon. The CLI resolves client-local ambient settings and attaches the selected context to protocol requests. The daemon is the single writer and owns:

- SQLite metadata, immutable revision records, claims, relations, audit data, and the FTS5 index.
- A Git-style filesystem content-addressed store (CAS) for artifact bytes.
- Consumption and echoing of the attached context, plus retrieval-horizon enforcement. Scoped storage and retrieval paths validate context before use.
- The JSON request/response protocol.

SQLite is authoritative. The CAS is immutable. FTS rows and any future vector index are derived projections that can be rebuilt.

Running one process avoids separate Postgres, object-store, and search-server memory footprints. Docker Compose is packaging rather than an architectural dependency; the same binary can run directly on the host.

General model reasoning and tool execution remain outside the daemon. A caller can request one scoped, byte-bounded context bundle and pass it to any companion or model under that caller's own policy. The CLI additionally provides one bounded adapter, `memoree remember`, that invokes Codex/Luna solely to compile natural language into a strict claim proposal. The daemon never receives model-provider credentials, launches a model, or executes generated output.

## Ambient context

The normal caller does not send a project or scope on every request. The local CLI resolves ambient context in deterministic precedence order before contacting the daemon:

1. Explicit context in the request, intended for controlled integrations rather than routine model use.
2. A process-local session context inherited through `MEMOREE_CONTEXT`.
3. The nearest ancestor project marker (`.memoree.toml`).
4. An explicitly configured personal fallback.
5. Otherwise, a context-dependent operation fails with `NO_AMBIENT_CONTEXT`.

A resolved context contains stable workspace and project identifiers, an optional task identifier and component, and explicit pinned artifacts. Identifiers are not inferred from a mutable directory basename.

Task selection remains process-local. An agent launched under `memoree session exec` inherits a validated context document through `MEMOREE_CONTEXT`; it does not change a machine-global “active task” setting that could race with another agent.

Every successful response to a context-dependent operation echoes the resolved context, where it came from, the effective horizon, and whether search was broadened. This makes accidental context drift visible to the caller and audit tooling. Current error envelopes do not repeat resolved context.

## Retrieval horizons

Location and retrieval breadth are separate concepts. The v1 horizons are:

- `ambient`: the current task/project plus explicit pins. This is always the default.
- `workspace`: an explicit one-request retrieval across the current workspace.
- `personal`: an explicit one-request retrieval across all local personal memory.

Settings cannot persist `workspace` or `personal` as the default horizon. A broad request must include a reason. An empty ambient result may return a structured hint, but the service does not automatically inspect or search a broader horizon.

An exact lookup by stable artifact or claim identifier is not a search and can resolve the requested object directly.

Exact lookup and pins are read visibility, not write authority. Revisions, tombstones/retractions, and both endpoints of a new relation must belong to the ambient project/task. The service rejects a mismatch as `SCOPE_VIOLATION` before creating an event or applying relation lifecycle effects.

Graph inspection uses `relation.list`, a bounded one-hop adjacency read with direction/type filters and an exclusive commit-sequence cursor. Relation rows are independently filtered to the selected horizon. A pin can make a foreign artifact a valid anchor for an ambient request, but does not reveal that artifact's foreign graph; workspace or personal traversal remains an explicit broadened request with a reason.

## Bounded recency

Lexical relevance selects and freezes the top-K candidate membership first. The deterministic `bounded_recency_v1` policy then adds a small, type-aware freshness bonus and may promote an eligible item by no more than two positions. The response retains lexical score/position, bonus, final position, effective timestamp and basis, evaluation instant, decay class, eligibility, and policy version so the adjustment is inspectable.

Only current, non-terminal revisions at or before the evaluation instant are eligible; this includes a current conflicted claim so both sides remain retrievable. Artifact freshness uses immutable revision creation time; claims use `valid_from` when present and otherwise immutable revision creation time. Historical, superseded, retracted, forgotten, and future material receives no bonus. Decision and constraint classes decay slowly with small maximum bonuses; ephemeral observations can decay faster with a larger—but still bounded—bonus.

Recency is enabled by default for recall/search/context requests and can be disabled per request. It never changes the lexical candidate set, retrieval horizon, temporal/currentness filtering, or contradiction lifecycle. Newer evidence is not automatically more correct.

## Model boundary

`memory.recall` is the default agent-facing knowledge check. It owns no index, embedding, model call, or alternate ranker: it projects the existing current-only search path into separate claim and artifact-reference lists. Claims carry immutable evidence spans and open contradiction IDs; `presence` distinguishes claims, artifact-only material, and no match without asserting truth or broadening scope. This moves repetitive result assembly out of every agent while keeping reasoning outside the daemon.

`memoree checkpoint` is also caller-side. It stores one private, bounded, last-write-wins continuity note per session under a pending directory that the daemon, database, CAS, search index, recall, and context builder never inspect. Review and compiler preview remain local; only explicit `memoree pending apply` crosses the normal remember write boundary. This prevents lifecycle capture from becoming background self-mutation or artifact-only retrieval noise.

`context.build` is the explicit handoff from memory to an external reasoning system. It freezes the retrieval result, labels excerpts as untrusted, preserves exact citations, reports conflicts and truncation, and stays within the caller's byte budget. Ambient retrieval is the default; wider horizons still require a reason.

`memoree remember` is a narrow caller-side exception to manual orchestration, not a daemon operation or general agent loop. The CLI resolves and freezes ambient scope before inference, runs one ephemeral `gpt-5.6-luna` call in a private read-only work directory with tools, web search, user configuration, rules, hooks, apps, memories, and multi-agent behavior disabled, and requires a strict JSON Schema result. Luna returns only typed statements and exact source quotes. Rust enforces bounds, rejects unknown fields, duplicate statements, missing quotes, and non-unique quotes, then computes byte spans itself. Preview is the default; only `--apply` submits ordinary idempotent artifact and claim mutations. A compiler or authentication failure performs no write; raw preservation requires an intentional `--raw --apply` invocation.

The default subprocess environment exposes `HOME`/`CODEX_HOME` so Codex can reuse cached ChatGPT login, while excluding API keys and access tokens. API-key auth is a per-invocation fallback available only through explicit `--allow-api-key`; callers must ask the human before enabling it. Even then, the key is supplied to `codex exec`, not used by a direct API implementation.

This boundary intentionally denies the model authority over context, retrieval horizon, artifact identity, confidence, relations, conflicts, lifecycle changes, deletion, and write intent. `--raw` bypasses inference entirely. Other reasoning still belongs to the consuming companion, and model output is never itself a protocol request.

Contradiction edges remain immutable, while schema-v3 `conflict_cases` are stable-ID assessments that each freeze two exact claim revisions and retain their lifecycle (`open`, `stale`, or `resolved`). Every lifecycle change has an immutable `conflict_events` record. Claim revision atomically stales the old case and opens a new assessment over both current non-terminal endpoints for every still-live contradiction relation; a partial unique index enforces at most one open case per relation. Retraction and supersession resolve affected open cases, then non-terminal claim presentation is recomputed from surviving current cases. `conflict.list` exposes bounded ambient/workspace/personal views with frozen/current snapshots and a case-sequence cursor so a model can propose reconciliation without rewriting history. Schema-v1 stores reconstruct the relation-time assessment from immutable commit sequences and add a current case when drift occurred; schema-v2 heads become preserved cases and receive the same live reassessment. Neither migration rewrites logical claims, revisions, relations, events, or `commit_seq`.

## Knowledge model

### Blob

Immutable bytes addressed by a BLAKE3 digest. Multiple revisions or artifacts can reuse one blob.

### Artifact

A stable logical identity representing evidence or a produced object: a document, log, command result, decision record, image, PDF, or other file. Content changes create immutable revisions; they do not mutate historical bytes.

### Claim

An atomic fact, decision, constraint, preference, procedure, or observation. A claim has immutable revisions, lifecycle status, and temporal validity where relevant. Evidence locators can cite a complete artifact revision or an exact byte range for passage-specific evidence; evidence itself is optional when the assertion has no source artifact. Exact claim history is a globally readable, newest-first paginated lineage. Its revision fields are historical, while lifecycle fields on every page item reflect the logical claim's current state.

### Relation

An explicit `derived_from`, `supports`, `contradicts`, `supersedes`, `references`, or `duplicates` edge between artifacts and claims. For `supersedes`, the source is the new/current entity and the target is the older entity. A contradiction is retained and surfaced; it is not resolved by silently overwriting one side. Relation rows are immutable history; callers inspect endpoint lifecycle separately, and treat edge metadata as untrusted reference data.

### Chunk

The v0.1 lexical index uses one private, rebuildable FTS row per complete artifact or claim revision; sub-revision chunking is not implemented yet. If chunk projections are added, their identifiers will remain private. Search excerpts always cite a stable artifact/claim revision, and durable evidence locators additionally carry an exact byte span. An agent fetches the cited revision before turning an excerpt into evidence, so changing the retrieval projection cannot invalidate stored claims.

### Recall result

A deterministic claim-first read for “does memory have something about this?” It returns current or disputed claims with exact evidence revisions and byte spans, open contradiction summaries, and a separate bounded list of raw artifact references. It never generates prose, assigns truth confidence, searches history, or broadens the horizon.

### Context bundle

An ephemeral, byte-bounded selection of excerpts for model input. It includes a manifest, revision-stable citations, rendered claim evidence refs, inclusion reasons, byte-budget omission counts, retrieval-truncation metadata, and unresolved conflicts. Rendered source lines are kept inside explicitly labelled Markdown blockquotes so artifact headings and role-like text remain visually subordinate to the trust warning. The manifest also reports deterministic `risk_signals` for common instruction-override, role-spoofing, tool-execution, and sensitive-data language. Signals are explainable warnings, not a safety classifier: their absence never makes content trusted. These defenses reinforce, but do not replace, the `content_is_untrusted` boundary. `omitted_count` covers only candidates excluded by `max_bytes`; `retrieval_truncated` separately reports candidates beyond the search limit. Retrieved content is evidence, not executable instruction.

## Mutation and read consistency

All logical mutations accept an idempotency key. Repeating the identical request with the same key returns the original outcome; reusing that key with different input is an `IDEMPOTENCY_CONFLICT`.

Artifact and claim revisions require the current revision identifier. A stale `if_revision` fails with `REVISION_CONFLICT` rather than dropping a concurrent update.

Current-only claim search evaluates the half-open validity interval `[valid_from, valid_until)` at one instant per request. Future and expired claims are excluded unless `include_historical` is explicit; historical claim hits expose that evaluation and revision currentness in provenance.

Successful mutations return a monotonic `commit_seq`. A dependent search or context-build call can pass that number as `min_commit_seq`. The service must either make the index observe that commit or return `INDEX_NOT_READY`; it must never silently return an older view while claiming the bound was met.

Forgetting first tombstones the logical object. Physical garbage collection is separate because a blob may still be referenced by another revision, artifact, backup, or audit record.

## Storage now and later

The implemented default is SQLite plus filesystem CAS. It is the lowest-resource configuration and remains the reference behavior.

The non-streaming v0.1 protocol deliberately caps raw artifacts at 8 MiB, encoded content at 12 MiB, frames at 24 MiB, and concurrent connections at four. Those bounds contain JSON/base64 amplification while preserving a useful local artifact size; future streaming support must be measured before these limits are raised.

The storage boundary is intended to admit a generic S3-compatible blob adapter later. SeaweedFS is a good optional local implementation because it can expose S3 from a compact all-in-one process, as demonstrated by the referenced IU LMS development stack. That adapter and profile are not implemented in this repository yet.

When added, SeaweedFS should remain opt-in and non-authoritative. A usable profile must include deterministic credentials, idempotent bucket creation, authenticated put/get/delete readiness checks, disk-headroom reporting, and backups that pair a database checkpoint with a blob manifest at the same commit sequence. Merely seeing a healthy SeaweedFS process is not sufficient.

Postgres, external search, semantic embeddings, and model-based reranking are also deferred until measured workloads justify their additional processes and resource use. The implemented deterministic recency adjustment is deliberately not semantic reranking and adds no service or model dependency.
