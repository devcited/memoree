# Memoree

[memoree.dev](https://memoree.dev)

Memoree is a local, artifact-first memory service for shell-capable agents. It is one Rust program with a machine-oriented CLI, ambient project/task context, immutable revisions, provenance-preserving claims, and bounded retrieval for model prompts.

The project is intentionally not an agent framework and does not require MCP or Python. Agents interact through a stable JSON protocol exposed by `memoree call`; the same program can run as a daemon for shared local access.

## What is different

- **Context is ambient.** Normal calls inherit the current workspace, project, and optional task from settings or the process session. Agents do not repeat project identifiers in every command.
- **Broader retrieval is explicit.** Search and relation listing default to the ambient project/task horizon. A caller must request `workspace` or `personal` for that request and explain why.
- **Write scope stays ambient.** Exact gets and pins can read an entity from elsewhere, but revise, forget, retract, and relation operations cannot mutate or link outside the resolved project/task.
- **Artifacts are stable evidence, not index identities.** Artifacts have stable identities and immutable revisions. Claims can cite a whole artifact revision or an exact byte range. Private lexical, trigram, chunk, and vector projections are disposable; returned spans always resolve to immutable authority bytes.
- **Correctness is part of the protocol.** Mutations are idempotent, revisions use optimistic concurrency, dependent reads can carry a commit sequence, and citations remain revision-stable.
- **Recall is claim-first and honest.** `memory.recall` answers “do we know anything about this?” with current or disputed claims, exact evidence spans, open conflicts, and separate artifact references. Plausible near-matches appear only in bounded `unqualified_candidate` arrays: they carry citations and ranking diagnostics but never change `presence`, become facts, or enter model context automatically.
- **Session continuity is quarantined.** `memoree checkpoint` stages one bounded, agent-authored note per session outside the database and every retrieval surface. `memoree pending preview|apply` reuses the explicit remember flow; no transcript hook or background process can silently create memory.
- **Model context is bounded.** `context.build` returns provenance-rich excerpts within a byte budget, keeps source lines in labelled blockquotes, reports heuristic prompt-injection signals, and always marks retrieved content as untrusted.
- **Freshness is bounded.** Search keeps the lexical top-K candidate set, then applies a deterministic, type-aware recency bonus that can promote a current item by at most two positions. Recency never broadens scope, hides contradictions, or makes historical/future content current.
- **Reasoning stays outside the daemon.** `context.build` returns a scoped, byte-bounded, citation-rich packet for any model. The optional caller-side `memoree remember` wrapper makes one isolated Codex CLI invocation using Luna to structure natural language; the daemon never invokes a model, receives an API key, or executes generated output.
- **The local default stays small.** SQLite is authoritative, SQLite FTS5 plus deterministic trigram matching provides retrieval, and larger content lives in a filesystem content-addressed store. A pinned local dense projection and claim-only cross-encoder ordering are explicit installs, remain disposable, and fail open to deterministic retrieval.

The v0.1 resource envelope accepts artifacts up to 8 MiB, encoded content up to 12 MiB, transport frames up to 24 MiB, and at most four concurrent connections. These conservative bounds limit JSON/base64 memory amplification until a streaming artifact transport exists. Query `capabilities` for the running binary's exact values instead of hard-coding them.

Authorization is deliberately out of scope for the initial local, personal release. The recommended/default endpoint is the private Unix socket: its runtime directory is owner-only and the socket is mode `0600`. Loopback TCP is host-local but not user-private—other users or processes on the same host can connect—so use it only on a trusted single-user machine or behind an independent boundary. A non-loopback bind is rejected unless the operator supplies the deliberately alarming `--dangerously-allow-non-loopback-tcp` flag.

## Install

Install the current checksummed release on macOS or Linux without `sudo`:

```sh
curl --proto '=https' --tlsv1.2 -sfL https://memoree.dev/install.sh | sh
```

The installer supports Apple Silicon and Intel macOS plus ARM64 and x86_64 Linux, downloads the matching checksummed release through `memoree.dev`, writes to `~/.local/bin` by default, and starts no service. Inspect the script first or choose a destination and version as documented at [memoree.dev/install](https://memoree.dev/install/). Windows is not yet a native target; use WSL2 until the local transport has Windows parity.

Memoree is not published to crates.io. The public GitHub repository is the immutable release origin; `memoree.dev` is the stable installation, version discovery, and download surface.

## Build from source

The current toolchain requirement is Rust 1.94 or newer.

```sh
cargo build --locked --release
cargo test --locked
```

The resulting binary is `target/release/memoree`.

Install a local checkout into Cargo's binary directory:

```sh
cargo install --locked --path .
memoree --version
```

The default local endpoint auto-starts a single background daemon on the first daemon-backed operation. Pass `--no-autostart` when a supervisor or Compose owns daemon lifecycle.

The auto-started daemon has explicit lifecycle controls:

```sh
memoree daemon status   # exits 1, with an ok JSON envelope, when stopped
memoree daemon restart  # replaces it with the currently installed binary
memoree daemon stop
```

Stop and restart intentionally control only the default private Unix endpoint. For an explicit TCP/Unix endpoint, use the process supervisor or Docker Compose that owns it.

Initialize a project once; normal calls then inherit its stable identity:

```sh
cd /path/to/project
memoree init --name my-project
memoree context show
```

For a task-local agent process, use `memoree session exec --task task-name -- your-agent-command`. The task context is inherited only by that process tree.

## Machine protocol

`memoree call` reads exactly one JSON request from stdin and emits exactly one JSON response envelope on stdout. Logs and diagnostics go to stderr.

```sh
printf '%s\n' '{
  "v": 1,
  "request_id": "req-example-search",
  "op": "search",
  "input": {
    "query": "why was the storage design chosen?",
    "horizon": "ambient",
    "limit": 5
  }
}' | memoree call
```

Canonical mutating requests sent through `memoree call` must add an `idempotency_key`. Human-friendly mutation wrappers generate a fresh key when none is supplied. Artifact and claim revisions also require `if_revision`. Keep the returned `commit_seq` and pass it as `min_commit_seq` when a dependent search must observe that write.

Ambient lookup never broadens itself. If project-scoped retrieval is insufficient, an agent may issue a new request with `"horizon": "workspace"` or `"personal"` and a non-empty `reason`.

`relation.list` provides bounded, one-hop incoming/outgoing graph inspection. It is newest-first and cursor-paginated; exact artifact pins can identify a foreign anchor but do not grant ambient access to that anchor's relations.

`conflict.list` provides the actionable contradiction queue. Every case has a stable `case_id` and freezes the exact two claim revisions it assessed. Revising either claim preserves that case as stale history and automatically opens one fresh case over the relation's two current non-terminal revisions, so a cosmetic edit cannot hide a still-live contradiction. Retraction or supersession resolves the current open case. Results include both frozen and current snapshots; visible `conflicted` status is derived only from the one surviving open case per relation. Pagination uses `next_before_case_sequence`/`before_case_sequence` (the wrapper flag is `--before-case-sequence`). SQLite schema v3 migrates v1 relation history and v2 conflict heads without rewriting claims, revisions, relations, logical events, or `commit_seq`.

Current-only search also enforces claim validity windows. Future and expired claims appear only when `include_historical` is explicitly enabled, with machine-readable currentness and temporal state in each claim hit's provenance.

Human-friendly wrappers produce the same JSON envelopes:

```sh
memoree remember "SQLite is authoritative; keep the daemon credential-free."
memoree remember --apply "SQLite is authoritative; keep the daemon credential-free."
memoree checkpoint --session SESSION_ID --task TASK_NAME "bounded continuity note"
memoree pending list
memoree pending preview CHECKPOINT_ID
memoree artifact put ./decision.md --kind decision --title "Storage decision"
memoree claim assert observation "Checkout terms are draft." --valid-until 2026-08-01T00:00:00Z --evidence ARTIFACT_ID@REVISION_ID#START-END
memoree recall "what do we know about storage?"
memoree search "why was the storage design chosen?"
memoree context build "storage constraints" --max-bytes 4096
memoree relation list artifact:ARTIFACT_ID --direction outgoing
memoree conflict list --include-stale --limit 50
memoree claim history CLAIM_ID --limit 50
memoree artifact get ARTIFACT_ID --revision REVISION_ID --output ./restored.md
```

The final command materializes text or binary content atomically and reports the path without echoing the content back to stdout. It refuses to replace an existing output unless `--force` is supplied.

Recall is the normal agent-facing lookup. It keeps claims and raw source matches in separate arrays, attaches immutable evidence citations such as `memoree://artifact/ARTIFACT_ID@REVISION_ID#START-END`, marks conflicted claims as `disputed`, and reports only the horizon it actually searched. `artifacts_only` means useful source material matched but no current claim did; `none` means no match at that horizon, not permission to broaden automatically.

Recall also returns up to three candidate claims and artifact references by default. Every item is labelled `retrieval_tier=unqualified_candidate`; candidates never affect `presence`, omit claim status/evidence hydration, and are excluded from `context.build`. Use them as cited leads: fetch the exact revision, inspect artifact risk signals, and corroborate with a refined query before relying on one. Set either candidate limit to `0` to suppress that channel; the bounded maximum is five.

Optional local semantic retrieval is candidate-only. It is installed and rebuilt deliberately; query paths never download model bytes:

```sh
memoree semantic enable
memoree semantic status
memoree semantic enable-reranker   # opt-in, claim ordering only
memoree semantic reranker-status
```

Dense similarity cannot qualify an answer. Exact-tier ordering is model-independent. The cross-encoder cannot qualify or suppress evidence, is disabled for artifacts and mixed searches, warms at daemon startup, and trips open after three consecutive inference calls above its 500 ms budget. An open breaker skips later model ordering and preserves deterministic fused results; it is not a per-query latency guarantee.

Recall, search, and context construction apply the bounded recency policy by default. The `memoree recall`, `memoree search`, and `memoree context build` wrappers accept `--no-recency` for one retrieval; raw `memoree call` clients can send `"recency":{"enabled":false}` on `memory.recall`, `search`, or `context.build`.

`memoree remember` is the one bounded reasoning convenience in this release. It previews by default. With `--apply`, it preserves the original UTF-8 source as an ambient-scoped artifact and asserts only claims whose one or more exact evidence quotes Rust can locate uniquely in that immutable revision. Multiple spans let a claim retain a non-contiguous caveat or scope condition. The plan includes machine-readable quality findings for inline/stdin self-attestation, mutable observations without validity, and the deliberate absence of automatic graph relations. These findings expose epistemic limits; they do not let Luna certify source authority. Luna cannot choose scope, confidence, relations, conflicts, lifecycle, supersession, deletion, or whether a write occurs. Use `--raw` to bypass Codex and preserve only the artifact.

An inline synthesis is useful operating context, but its claims prove only what that stored note says. When long-term auditability matters, preserve the smallest relevant primary artifacts or excerpts and connect the synthesis with explicit `derived-from`, `references`, or `supports` links. Do not dump an entire repository for provenance. For mutable observations, use explicit `claim assert --valid-from/--valid-until` when a real validity window is known, or revise, retract, or supersede the claim when verified state changes.

Authentication defaults strictly to the existing ChatGPT-authenticated `codex` CLI session from `codex login`. In this mode the child receives no API key or access-token environment variables and never reads `~/.openai_env`. If CLI authentication fails, the command performs no write and tells the caller to ask the human. Only the explicit `--allow-api-key` flag permits a one-run API-key fallback: it reads `CODEX_API_KEY`, `OPENAI_API_KEY`, or a safely parsed `~/.openai_env`, then supplies the value to `codex exec` as `CODEX_API_KEY`. There is no direct HTTP API client, and no credential enters the daemon or stored provenance.

A preview and a later apply are independent model calls, so the applied response—not an earlier preview—is authoritative. Automated callers that already intend to persist should call `--apply` directly and inspect its returned plan and stored records. Claim mutation identity is anchored to the exact source span, so a changed compilation for the same passage conflicts instead of creating a duplicate.

The compiler is deliberately fixed to `gpt-5.6-luna` at low reasoning effort for this repeatable extraction task. General answering, planning, claim qualification, and conflict resolution remain outside the daemon. Optional pinned local models may add candidates or advisory ordering only. A companion can still call `context.build`, use any suitable model, and submit explicit protocol mutations.

Mutation wrappers generate a fresh idempotency key when one is omitted. Pass `--idempotency-key` when an automated logical action may need an exact retry; reusing that key with changed input is rejected.

See [Protocol](https://memoree.dev/docs/protocol/) for envelopes and operation semantics, [Context and configuration](https://memoree.dev/docs/configuration/) for ambient resolution, [Model integration](https://memoree.dev/docs/model-integration/) for the vendor-neutral instruction contract, [Session checkpoints](https://memoree.dev/docs/checkpoints/) for quarantined continuity notes, and [Retrieval evaluation](https://memoree.dev/docs/evaluation/) for the isolated versioned regression loop.

## Local daemon

The daemon listens on a local transport and owns database/index writes. Prefer the default private Unix socket:

```sh
memoree serve
```

An explicit `memoree serve --listen tcp://127.0.0.1:17878` is unauthenticated and reachable by other local host users/processes. Non-loopback TCP cannot be selected by accident. The standalone image defaults to container loopback; a deployment that genuinely needs the process on a container interface must pass `--dangerously-allow-non-loopback-tcp` and independently restrict the published port.

For container use:

```sh
docker compose up --build --wait -d
MEMOREE_ENDPOINT=tcp://127.0.0.1:17878 memoree doctor
```

Compose explicitly opts the process into its container-wide listener, but publishes that port only on host loopback. Because loopback TCP is not isolated between local OS users, this Compose profile is intended for a trusted single-user host. It stores all state in one named volume. Set `MEMOREE_ENDPOINT=tcp://127.0.0.1:17878` (or pass global `--endpoint`) on host CLI calls that should use the container. The filesystem content-addressed store is the only implemented blob backend in v0.1.

Create a consistent local backup after verification:

```sh
memoree verify
memoree backup create /path/to/new-backup-directory
```

The destination must not exist; existing files and directories are never replaced. The daemon builds the backup in a sibling staging directory, verifies the complete SQLite snapshot and CAS copy, flushes its files, and atomically publishes it only after verification succeeds. A failed backup removes its staging directory and never leaves a partial final destination. A backup contains a SQLite snapshot, external CAS blobs, and the snapshot commit sequence.

Atomic no-replace backup publication is implemented on Apple and Linux targets. Other targets fail closed before publishing a backup rather than falling back to a rename with weaker replacement semantics.

`backup.create` is an atomic administrative side effect, not an idempotency-keyed logical mutation. If its success response is lost, inspect the requested destination before deciding what to do next; a retry will never overwrite it.

The CLI converts relative backup destinations to absolute client paths before sending them; the daemon performs the write. With Compose, the destination therefore must be an absolute path visible inside the container. Write under `/data` so the backup remains in the named volume (for example `memoree backup create /data/backups/backup-001`), then export it explicitly if an independent copy is required.

## Design

```text
agent or human
      |
      | memoree call / memoree remember
      v
caller-side CLI (optional isolated Luna claim compilation)
      |
      | validated protocol mutations / framed transport
      v
  memoree daemon
      |-- SQLite: authoritative metadata, revisions, claims, relations, FTS
      |-- filesystem CAS: content bytes addressed by BLAKE3
      `-- optional private semantic/reranker projections (rebuildable)
```

The SQLite database is authoritative; retrieval indexes are rebuildable. Every artifact revision is addressed and verified by digest without collapsing distinct logical artifacts. External CAS objects are physically deduplicated; small inline content may be repeated in immutable revision rows.

FTS body indexing covers `text/*` plus common structured-text media such as JSON, XML, YAML, TOML, JavaScript, SQL, GraphQL, and SVG. Other binary formats remain title-searchable until an explicit extractor is added; exact bytes are still stored and retrievable.

Read [Architecture](https://memoree.dev/docs/architecture/) for context resolution, horizons, durability boundaries, and the deliberately deferred S3/SeaweedFS adapter. [Quality gates](https://memoree.dev/docs/quality/) defines how correctness and any future “superior” claim must be measured.

## Project status

This repository is a usable local vertical slice. Query `capabilities` rather than assuming a roadmap feature is available; `remember` is a CLI composition and intentionally is not advertised as a daemon operation. Evidence-first recall, exact long-document citations, deterministic lexical/trigram fusion, optional local dense candidate retrieval, guarded claim-only cross-encoder ordering, bounded recency, and Luna claim compilation are implemented. Authorization, generic S3 storage, and a SeaweedFS Compose profile are not implemented.

## License

Memoree is source-available under the Apache License 2.0 subject to the Commons Clause License Condition v1.0. See [`LICENSE`](LICENSE) for the controlling terms.

You may use, copy, modify, and redistribute Memoree, including for personal use and internal use within commercial organizations and agentic systems. You may not sell Memoree itself, a renamed or modified fork, or a hosted, managed, consulting, or support offering whose value derives entirely or substantially from Memoree without a separate commercial agreement with the Licensor.

For commercial licensing, contact Valeriy Efimenko at [licensing@memoree.dev](mailto:licensing@memoree.dev). This summary is provided for convenience; the `LICENSE` text controls. Memoree is source-available, not open source as defined by the Open Source Initiative.

Issues and documentation feedback are welcome. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) before proposing changes; code and documentation contributions are not accepted until a contributor agreement is published.
