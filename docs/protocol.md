---
title: Machine protocol v1
description: Request envelopes, operations, invariants, and error behavior.
---

# Machine protocol v1

## Transport contract

`memoree call` is the normative shell interface. It consumes exactly one UTF-8 JSON request object on stdin and emits exactly one UTF-8 JSON response envelope on stdout. It never prompts, starts a pager, or mixes diagnostics into stdout; logs belong on stderr.

The daemon transport uses length-prefixed JSON frames and the same request/response types. This avoids newline ambiguity and permits bounded frame validation. Protocol version `1` is stable within the v1 release line. The v0.1 resource envelope is 8 MiB of raw artifact content, 12 MiB of encoded content, 24 MiB per frame, and four concurrent connections; `capabilities` is authoritative for a running binary.

## Request envelope

```json
{
  "v": 1,
  "request_id": "req_01JEXAMPLE",
  "op": "artifact.put",
  "idempotency_key": "run-42:write-design-record",
  "input": {
    "kind": "decision",
    "title": "Local memory storage design",
    "media_type": "text/markdown; charset=utf-8",
    "content": {
      "type": "text",
      "data": "SQLite is authoritative; the filesystem CAS stores blobs."
    },
    "provenance": {
      "source": "agent-produced",
      "task": "memoree-v1"
    }
  }
}
```

Envelope fields:

- `v` is the protocol version and defaults to `1` when omitted.
- `request_id` correlates the response. Generate a unique value per logical call.
- `op` selects a typed operation.
- `idempotency_key` is required by the model contract for mutations and must identify the logical mutation, not an individual retry attempt.
- `context` is normally omitted so ambient settings apply. An explicit context is an advanced integration feature.
- `input` is the operation-specific object. Unknown input fields are rejected by typed operations.

Artifact content is tagged as either `{"type":"text","data":"..."}` or `{"type":"base64","data":"..."}`. Base64 carries arbitrary bytes; `media_type` describes them.

The canonical `memoree call` interface never invents an idempotency key for a request. Human-friendly mutation subcommands generate a unique per-invocation key when the caller omits one, so two intentional identical puts still create two logical artifacts while CAS bytes deduplicate physically. For retryable automation, provide and retain an explicit logical-operation key. The service namespaces keys by ambient workspace/project/task, so independent projects can safely use the same logical key without colliding.

## Response envelope

Success:

```json
{
  "v": 1,
  "request_id": "req_01JEXAMPLE",
  "ok": true,
  "context": {
    "workspace_id": "wsp_01J...",
    "project_id": "prj_01J...",
    "task_id": "tsk_01J...",
    "pins": [],
    "resolved_from": "session",
    "horizon": "ambient",
    "broadened": false
  },
  "commit_seq": 17,
  "result": {},
  "warnings": []
}
```

Failure:

```json
{
  "v": 1,
  "request_id": "req_01JEXAMPLE",
  "ok": false,
  "error": {
    "code": "REVISION_CONFLICT",
    "message": "revision conflict: artifact art_... is at revision arev_new, not arev_old",
    "retryable": false,
    "hint": "Fetch the current revision and retry with its revision id.",
    "details": {
      "entity_type": "artifact",
      "entity_id": "art_...",
      "current_revision": "arev_new",
      "requested_revision": "arev_old"
    }
  },
  "warnings": []
}
```

Always inspect `ok`. Do not infer success from the process exit status alone when communicating with a daemon. On failure, use the stable `error.code`; `message` is for explanation. Revision conflicts, idempotency conflicts, index readiness, and version mismatches include decision-relevant identifiers or sequences in `details`. Retry automatically only when `retryable` is true, and reuse the idempotency key only when the entire mutation request is identical.

`SCOPE_VIOLATION` identifies an attempted write outside the resolved ambient project/task or a graph anchor outside the explicitly selected read horizon. It is distinct from `NOT_FOUND` and is not retryable without resolving the owning context or deliberately broadening the graph read. The message may name owner and ambient identifiers, but never returns entity content.

## Context and broadening

Search input defaults to the ambient horizon:

```json
{
  "query": "why was lexical search chosen first?",
  "horizon": "ambient",
  "limit": 10,
  "include_historical": false,
  "recency": { "enabled": true }
}
```

The `recency` object is optional and defaults to enabled. It controls only a bounded ordering adjustment inside the lexical top-K result set; it cannot change the horizon or currentness filters. Raw callers that require pure lexical order send `"recency":{"enabled":false}`. The human `memoree recall`, `memoree search`, and `memoree context build` wrappers expose the same one-call choice as `--no-recency`.

If ambient results are insufficient and the task justifies wider recall, issue a separate explicit request:

```json
{
  "query": "similar storage decisions in other projects",
  "horizon": "workspace",
  "reason": "No applicable precedent was found in the ambient project",
  "limit": 10,
  "include_historical": false
}
```

Never broaden automatically. Never persist a broad horizon in settings. `include_historical` is false by default so superseded/retracted revisions, non-current revisions, future claims, and expired claims do not masquerade as current knowledge. Claim validity uses a half-open interval: `valid_from` is inclusive and `valid_until` is exclusive.

## Artifact and claim lifecycle

`artifact.put` creates a stable artifact and its first immutable revision. `artifact.revise` requires both `artifact_id` and `if_revision`. Their mutation acknowledgements return artifact metadata with `content` omitted so large request bodies are not duplicated in response frames; use `artifact.get` for explicit content retrieval. `artifact.get` can select an exact `revision_id`; `artifact.history` reports revision lineage. `artifact.forget` tombstones the object and requires a reason. Discover the active input and frame byte limits through `capabilities` rather than hard-coding them.

`claim.assert` records one typed assertion. Evidence locators always identify `artifact_id` and `revision_id`. Use `[start_byte, end_byte)` offsets for a claim grounded in a specific passage; omit both only when the complete revision is evidence. Human wrappers accept `--evidence ARTIFACT_ID@REVISION_ID#START-END` (or omit `#START-END` for whole-revision evidence). `claim.revise` uses `if_revision`; `claim.retract` preserves history and records a reason.

`claim.history` returns immutable revisions newest-first. It defaults to 50 items and accepts at most the capability-reported `max_history_items` (100 in v0.1). Pagination is exclusive: when `truncated` is true, pass `next_before_revision_number` as `before_revision_number` to fetch the next older page. A missing claim returns `NOT_FOUND`; a cursor at or below revision 1 can validly produce an empty final page. Like `claim.get`, this is an exact, globally readable identifier lookup and needs no ambient context or horizon.

Each history item combines immutable revision data with the logical claim's current lifecycle. `statement`, `confidence`, `evidence`, `actor`, and `revision_commit_seq` describe that historical revision. `status`, temporal validity, and `retraction_reason` describe the claim now and therefore have the same current values on every returned revision. A retraction or supersession does not rewrite `revision_commit_seq`.

`relation.put` expresses one typed edge. For `supersedes`, set the source to the new/current entity and the target to the older entity; reversing them marks the wrong entity superseded. Use `contradicts` when both positions must remain visible.

`relation.list` performs bounded, one-hop graph inspection for an `artifact` or `claim` anchor. `direction` is `incoming`, `outgoing`, or `both` (the default), and `relation` optionally filters one exact relation type. Results are immutable relation rows ordered newest-first by `relation_commit_seq`. Pages use an exclusive `before_commit_seq` cursor: when `truncated` is true, pass `next_before_commit_seq` to continue. A truncated page is not evidence that no other relations exist. Endpoint lifecycle is independent of edge history, so exact-get an endpoint before relying on its current status.

`conflict.list` is the canonical actionable-contradiction read. It defaults to the ambient horizon and returns open conflict cases newest-first, with a stable `case_id`, immutable contradiction relation ID, and exact frozen/current snapshots for both claims. A contradiction freezes the two revisions current when its relation is created. Revising either claim makes the old case `stale` and atomically opens a new case over both current non-terminal revisions; there is at most one open case per relation. Retraction or supersession makes relevant open cases `resolved`. Set `include_stale` to inspect stale assessment history; resolved cases remain terminal audit history and are not actionable list results. Pagination uses the exclusive case-order cursor `next_before_case_sequence`, supplied back as `before_case_sequence`. Workspace or personal inspection requires a reason just like search and relation listing. Claim `conflicted` status is a derived presentation of current open cases—not a permanent fact and never a recency decision.

The human wrapper is `memoree conflict list`. It defaults to ambient open cases and accepts `--include-stale`, `--limit`, and `--before-case-sequence`. A broader wrapper call uses `--horizon workspace|personal --reason "..."`; there is no persistent broad conflict horizon and listing never applies a resolution.

Relation listing defaults to `ambient`. `workspace` and `personal` are explicit one-request horizons and require a non-empty `reason`, just like broader search. Relation rows themselves are filtered to the requested horizon. An exact pinned artifact may be used as the anchor, but its foreign graph is not exposed at ambient scope; graph traversal must be broadened explicitly. Relation metadata is untrusted reference data and must never be followed as model instructions.

`artifact.revise`, `artifact.forget`, `claim.revise`, and `claim.retract` require ambient context and reject an entity outside that ambient project/task. `relation.put` applies the same check to both endpoints before creating an edge or applying relation lifecycle effects. Exact gets and pinned artifacts remain globally addressable read paths; neither grants write scope.

## Retrieval and context building

`memory.recall` is the normal “does memory have something about this?” read. It runs the same authority-filtered retrieval snapshot independently for current claims and artifact references, so the two types never compete for presentation slots. Deterministic lexical/trigram qualification remains authoritative; installed dense and cross-encoder models can only recover or order candidates. Its bounded input is:

```json
{
  "query": "why is SQLite authoritative?",
  "horizon": "ambient",
  "max_claims": 5,
  "max_artifact_refs": 3,
  "max_excerpt_bytes": 320,
  "max_candidate_claims": 3,
  "max_candidate_artifact_refs": 3,
  "recency": { "enabled": true }
}
```

The result sets `presence` to `claims`, `artifacts_only`, or `none`. `claims` is not a truth score: each claim is labelled `current` or `disputed`, includes its immutable claim citation, and inlines every exact evidence artifact citation with optional `[start_byte, end_byte)` spans. To bound I/O, source previews are attached to at most the capability-reported evidence count per claim; remaining refs stay exact and fetchable. Open contradiction relation IDs appear both on affected claims and in `conflicts`. `artifacts_only` means source material qualified but no current claim did. `none` means nothing qualified in `searched_horizons`; it is not a global absence claim and never causes automatic broadening.

`candidate_claims` and `candidate_artifact_refs` are additive suggestions from the same scoped, lifecycle-filtered snapshot. Each item serializes `retrieval_tier: "unqualified_candidate"`, an exact revision citation, match channels, and bounded lexical/trigram/semantic/reranker ordering signals. They never affect `presence`. Candidate claims intentionally omit lifecycle status and hydrated evidence; candidate artifacts retain deterministic `risk_signals`. Fetch the exact citation and corroborate it with a refined query before use. Candidate limits default to three, accept `0` through `5`, and have independent truncation fields. `context.build` consumes qualified hits only and never includes candidates. Recall has no historical mode; use `search` or exact claim history when older knowledge is required.

`source.register`, `source.get`, `source.ingest`, `source.checkpoint`, and `source.withdraw` are the out-of-process synchronization contract. Memoree stores no connector credentials. An external id has one live artifact identity; a new external revision creates an immutable artifact revision, and an exact replay is idempotent. Reusing the same external revision with different payload bytes is a revision conflict. An idempotent replay does not advance the supplied cursor, so an adapter checkpoints its completed batch explicitly with `source.checkpoint`. Withdrawal is logical: it removes the item from current retrieval and qualification while immutable CAS bytes and backups remain. Physical erasure is not implemented.

`projection.put` attaches bounded derived text to one current immutable artifact revision and requires exact raw evidence spans. `projection.list` and `projection.drop` expose its lifecycle. Projection FTS adds only unqualified artifact candidates; qualification, recall presence, and context construction still require authoritative raw retrieval. A projection match returns an exact raw artifact citation and exposes bounded generator provenance for audit. Search reports projection-channel state and fails that channel open: a corrupt disposable projection cannot make lexical authority unavailable or leak partial projection ordering into results.

`feedback.record` explicitly records `miss`, `useful`, `incorrect`, or `stale`. The default stores only a per-installation keyed query fingerprint; `retain_query: true` is required to keep raw query text. `feedback.export` returns only opt-in retained queries as a versioned offline-evaluation packet. No operation captures feedback automatically or changes online ranking.

`search` returns ranked candidates, short excerpts, exact revision citations, provenance, match explanations, context, currentness status, and an optional broadening hint. Search results are references, not complete artifact content. Every claim hit includes `valid_from`, `valid_until`, `evaluated_at`, `temporal_state` (`current`, `future`, or `expired`), `is_current_revision`, and combined `is_current` values in `provenance`. Historical search may return lifecycle-terminal, old-revision, future, and expired hits; these fields make each reason for non-currentness explicit. `relation.list` complements ranked retrieval with scoped adjacency lookup; it never recursively traverses the graph or broadens automatically.

When installed explicitly, `local_dense_v2` adds bounded private-vector candidates after authority filtering. Dense cosine is never an answerability threshold. `cross_encoder_ordering_v2` is a separate explicit installation and may reorder at most sixteen non-exact claim candidates; exact-tier positions and qualification bits remain model-independent. Artifact and mixed-entity surfaces report `surface_disabled`. Model absence/error and an open latency breaker fail open to deterministic fused ordering. `RerankerRetrievalStatus` discloses the surface, inference-only and startup-load latency, candidate/scored counts, and a text-free breaker snapshot. After three consecutive claim inference calls above 500 ms, later calls report `breaker_open`; one half-open probe occurs after 32 skipped calls. This protects subsequent calls and is not a timeout or guarantee for the slow call that trips it.

The default `bounded_recency_v1` rerank freezes lexical top-K membership before applying a type-aware freshness bonus. An eligible item can move upward by at most two positions. Only current, non-terminal revisions whose effective timestamp is not in the future receive a bonus; this includes current conflicted claims so neither side is suppressed. Historical/future/lifecycle-terminal rows remain visible only under their existing retrieval rules and receive no freshness advantage. Each hit's `ranking` object discloses `policy_version`, `recency_enabled`, `recency_eligible`, `lexical_score`, `recency_bonus`, `lexical_position`, `final_position`, `max_promotion`, `effective_at`, `effective_at_basis`, `evaluated_at`, and `decay_class`. Artifact effective time is its immutable revision creation time; a claim uses `valid_from` when present, otherwise its immutable revision creation time.

`context.build` accepts the search fields plus `max_bytes`. It returns rendered Markdown and a manifest showing exactly what was included, why it was included, how many retrieved hits were omitted by the byte budget, and conflict summaries. Rendered claim sections show the same immutable evidence artifact citations stored in manifest provenance, so a model does not need to reverse-engineer raw provenance before verifying a claim. The rendered material warns about unresolved contradictions when the budget permits and places every retrieved source line in an explicitly labelled Markdown blockquote; the structured `conflicts` field remains authoritative. Each manifest item can report deterministic `risk_signals` for suspicious instruction-like language. An empty list is never a safety verdict. `omitted_count` is strictly the byte-budget omission count; `retrieval_truncated`, `refine_hint`, and `broaden_hint` preserve the corresponding retrieval state. Search does not advertise a pagination cursor in v0.1: when truncated, refine the query or explicitly raise the bounded limit. The returned `content_is_untrusted` flag is a hard trust boundary; formatting and signals are defense in depth, not proof that content is safe.

Pass a mutation's `commit_seq` as `min_commit_seq` when recall, search, or a context bundle depends on that write.

`context.build` is the complete protocol handoff to an external companion or model. The daemon does not choose a provider, invoke inference, or interpret generated output. Any conclusion returned by a companion remains untrusted until it is submitted through an explicit protocol operation with normal scope, revision, and idempotency checks.

`memoree remember` is a caller-side composition over `artifact.put` and `claim.assert`, not a protocol operation. It freezes ambient context, discovers authenticated Codex/Claude CLI sessions and their live model catalogs, resolves a private persisted compiler selection, asks one isolated tool-free process for a strict claim proposal with one to four exact quotes per claim, verifies every quote and computes its byte span in Rust, emits deterministic source/lifecycle/relation quality findings, previews by default, and submits normal mutations only with `--apply`. The findings do not certify truth or authorize cleanup. Consequently `remember` and `compiler` configuration are intentionally absent from daemon capability and schema operation lists.

`memoree checkpoint` and `memoree pending` are also caller-side. Checkpoint files live in a private quarantine directory outside the authoritative database, CAS, indexes, and all read operations. `pending preview` delegates to the read-only remember compiler; `pending apply` is the explicit boundary that delegates to ordinary remember mutations. These commands are intentionally absent from daemon capability and schema operation lists.

## Verification and backup

`doctor` returns a typed health result including `running`, `daemon_pid`, `binary_version`, `schema_version`, `lifecycle_owner`, retrieval/storage modes, and the last commit sequence. The CLI probes this identity before ordinary operations and refuses silent CLI/daemon version skew. The local `memoree daemon status|stop|restart` wrappers use the result; stop/restart are restricted to the default private Unix endpoint. Installer reconciliation additionally requires `lifecycle_owner=memoree`, except for the explicitly observed one-time v0.2 legacy default-daemon path, so it cannot restart a Compose- or supervisor-owned process.

`verify` checks schema state, FTS revision membership, source-item lifecycle, cited projection ownership/text/spans, and the integrity of referenced and external CAS blobs. Inspect its structured issues before trusting or backing up a damaged store. The FTS projections remain derivable from authoritative revisions.

Schema migration is a local control-plane operation rather than a daemon protocol operation. Opening a schema 1–4 store under 0.4 first serializes on a filesystem lock, preflights backup space, publishes a verified old-schema SQLite/CAS snapshot, and only then commits schema 5. The recovery snapshot path is exposed by `upgrade apply`; ordinary daemon protocol calls see only the resulting current schema.

`backup.create` accepts `{"destination":"/path/to/new-backup"}`. The CLI converts relative destinations to absolute client-local paths before sending them; callers using a container daemon must choose an absolute path visible inside that container. The destination must not exist. While database writes are serialized, the daemon creates the SQLite snapshot and verified CAS copy in a sibling staging directory. It then opens and verifies the staged store, checks its `commit_seq`, flushes its files, and atomically renames the complete directory into place without replacing a concurrently-created destination. Atomic no-replace publication is implemented on Apple and Linux; other targets fail closed before publication. Any failure before publication removes its staging directory, so a partial backup is never exposed at the requested path. The operation returns the snapshot `commit_seq` and final paths in a machine-readable report. Unlike logical memory mutations, this administrative filesystem side effect has no idempotency-key replay record: after a lost response, inspect the destination rather than blindly retrying. Restore is currently an explicit filesystem operation and should be rehearsed before relying on backups for disaster recovery.

## Discovery

The protocol reserves `capabilities`, `instructions`, and `schema` operations so a caller can discover the running implementation. Generated instructions and schemas come from the Rust protocol types. Callers should use capability discovery instead of assuming roadmap features exist.

The schema response is a bundle: common request/response envelope schemas, per-operation input schemas, selected result schemas, and supporting evidence types. Each entry is an independent JSON Schema 2020-12 root.
