---
title: Context and configuration
description: Configure ambient project, task, personal, and endpoint context.
---

# Context and configuration

## Project marker

Run `memoree init` in a project directory. It creates `.memoree.toml` with stable workspace and project IDs:

```toml
schema = 1
workspace_id = "wsp_01J..."
project_id = "prj_01J..."
name = "memoree"
pins = []
```

The resolver walks from the current directory toward the filesystem root and selects the nearest marker. Moving or renaming the project does not change its identity. A malformed nearest marker is an error; the resolver will not silently skip it and select a different context.

`memoree init` creates the marker atomically and refuses to replace an existing file. Pass an existing workspace ID when several projects should share a workspace; otherwise initialization creates a new one.

Pins are explicit artifact references inherited by that context. They do not grant an implicit search of a wider horizon.

## Project source index

The separate disposable working-tree index has owner-local settings:

```toml
schema = 1

[project_index]
auto_reindex = "off"
include_untracked = false
max_files = 50000
max_total_bytes = 268435456
max_file_bytes = 524288
max_changed_bytes = 33554432
```

These values are stored atomically in an owner-private, project-ID-keyed file below Memoree's application data directory—not in `.memoree.toml` or the repository. This keeps the shared marker readable by older Memoree clients and makes every collaborator opt in separately. Unreleased beta marker sections are accepted for parsing but never activate the index or metrics; only the private file is authoritative. Do not edit the private file; use `memoree project configure --auto-reindex off|on-search|watch` and inspect it with `memoree project status`.

The project index is experimental and absent from the canonical agent route. `off` requires deliberate `memoree project index`; `project map` returns `not_ready` without creating an index. Opt-in `on_search` checks a cheap Git snapshot and reconciles a stale index only when `memoree project map` or `project search` is used; `watch` permits the explicit foreground adaptive watcher. The watcher is never started by the daemon or installer and retains the prior projection after transient Git-snapshot or reindex failure.

File, total-byte, per-file, and changed-byte limits prevent an unexpected repository scan from expanding without bound. Reindexing is single-worker and transactional; change-budget failure keeps the prior index. See [Project source indexing](project-index.md) for filters, citations, and watcher behavior.

## Project metrics

Real-operation metrics are disabled by default and must be enabled separately by each project user:

```toml
schema = 1

[metrics]
enabled = true
retention_days = 14
max_database_bytes = 10485760
sample_rate = 1.0
```

This configuration shares the same owner-private project-ID-keyed local settings file as the index and never rewrites `.memoree.toml`. Use `memoree metrics configure` rather than editing it. Retention must be 1–365 days, the cap 1 MiB–1 GiB, and sampling 0.0–1.0. The database is a separate disposable, owner-private store below the application data directory. It never enters the repository, memory authority database, backups, project index, or network. Its closed schema has no query, content, citation, prompt, path, free-text label, or raw-error fields. See [Project metrics and experiments](metrics.md).

## Process-local task context

Use the session launcher for a task-specific agent process:

```sh
memoree session exec --task fix-auth -- your-agent-command
```

The child and its descendants receive a validated JSON context through `MEMOREE_CONTEXT`. The task ID applies only to that process tree, so parallel agents cannot overwrite one another's active task. An explicitly selected `MEMOREE_ENDPOINT`/`--endpoint` and the no-autostart policy are propagated too, preventing a task agent from silently switching to another local store.

`MEMOREE_CONTEXT` has this shape:

```json
{
  "workspace_id": "wsp_01J...",
  "project_id": "prj_01J...",
  "task_id": "tsk_<stable hash of workspace, project, and fix-auth>",
  "component": null,
  "pins": []
}
```

An invalid or empty required identity is an error. Session context takes precedence over a project marker.

## Personal fallback

The optional application configuration can define a context for commands executed outside initialized projects:

```toml
schema = 1

[personal]
workspace_id = "wsp_personal"
project_id = "prj_inbox"
pins = []
```

This is only a location fallback. It does not set the search horizon to `personal`; search remains `ambient` unless a caller explicitly broadens one request.

Without a session, marker, or configured fallback, context-dependent operations fail with `NO_AMBIENT_CONTEXT`. Writes never silently fall into a global inbox.

## Application paths

By default, `memoree` uses the platform's standard application data, configuration, and runtime directories. For a relocatable installation, set `MEMOREE_HOME`; it produces this layout:

```text
$MEMOREE_HOME/
|-- config.toml
|-- data/
`-- run/
    `-- memoree.sock
```

Individual paths can be overridden when embedding or packaging the daemon:

- `MEMOREE_CONFIG`
- `MEMOREE_DATA_DIR`
- `MEMOREE_RUNTIME_DIR`
- `MEMOREE_SOCKET`
- `MEMOREE_ENDPOINT` selects the daemon transport, for example `tcp://127.0.0.1:17878`; the global `--endpoint` CLI option overrides it.
- `MEMOREE_NO_AUTOSTART=true` disables automatic daemon startup.
- `MEMOREE_ACTOR` records the caller identity on supported mutations.
- `MEMOREE_SKIP_SKILL_SYNC=true` keeps `upgrade apply` from changing Codex/Claude skills when those integrations are managed independently.
- `MEMOREE_SKIP_RERANKER_INSTALL=true` keeps a confirmed upgrade on deterministic ordering without downloading the release-pinned local TinyBERT model. The equivalent one-run flag is `memoree upgrade apply --without-reranker`.

On Unix, leave `MEMOREE_ENDPOINT` unset for the recommended default: an owner-private runtime directory and mode-`0600` Unix socket. This provides a per-user boundary that TCP does not.

Server TCP binds are a separate concern from the client endpoint. `memoree serve` rejects wildcard and other non-loopback TCP listeners because this release has no authentication. Loopback TCP is host-local, not user-private: any local process/user able to connect can use the protocol. Container supervisors that must bind the process to a container interface can explicitly pass `--dangerously-allow-non-loopback-tcp`, but must enforce their own network boundary. The bundled Compose file makes that opt-in visible and publishes the port on host `127.0.0.1` only, but is intended for a trusted single-user host. It sets `MEMOREE_HOME=/data` and persists that directory in one named volume. The standalone Docker image remains loopback-only by default.

The auto-started private daemon reports `lifecycle_owner=memoree`; a process started directly with `memoree serve` reports `external`. The stable installer may stop and restart only the former. The one-time v0.2 compatibility path accepts a missing ownership field only when the installer has independently observed a running legacy default daemon. Explicit endpoints and supervisor-owned processes are never reconciled by the installer.

## Upgrade state

`memoree upgrade apply` serializes reconciliation under a private lock and writes an atomic `upgrade-state.json` beside the store. The state records the target binary, prior daemon state, phase, schema, recovery snapshot, and embedded skill digest so a retry after interruption preserves “running before means running after.” `memoree upgrade status` reads this state without starting a daemon.

Before schema 1–4 becomes schema 5, Memoree checks free space and publishes a private, verified pre-migration SQLite/CAS snapshot below `migration-backups/`. An already-installed stale semantic projection is rebuilt locally. A user-confirmed upgrade installs the small, release-pinned ordering-only reranker unless explicitly opted out; offline or verification failure is reported and degrades to deterministic retrieval. No query-time path downloads models.

Installer-managed copies record their exact binary path during reconciliation. On eligible interactive starts they check for a release at most every six hours and ask once before applying a newly detected version. The signed release manifest pins the installer and platform archive digests; success re-executes the original command exactly once. Non-interactive, CI, protocol-stdin, daemon, upgrade, update, eval, and session surfaces never prompt. `MEMOREE_AUTO_UPDATE=off` disables automatic checks; `memoree update status|check|apply` provides explicit control. Versions before 0.4 have no startup checker and need one manual installer run.

There is intentionally no setting for a default `workspace` or `personal` retrieval horizon. Broadening is a decision made explicitly by the caller for one search or context-build request and must include a reason.

There is intentionally no daemon model-provider configuration. `memoree compiler status` discovers authenticated local Codex and Claude CLIs and requests their current model catalogs. `memoree compiler configure` persists one private user-level selection in `compiler-selection.json` beside the data directory. If one eligible login exists, the first compiler use selects its recommended model automatically; if both exist, an interactive caller chooses and a non-interactive caller must configure explicitly. Existing installations seed the former Codex/Luna behavior during upgrade when that login and model remain available.

The isolated child receives only a small environment allowlist, including `HOME`, `CODEX_HOME`, and `CLAUDE_CONFIG_DIR` when set for cached CLI authentication. `OPENAI_API_KEY`, `CODEX_API_KEY`, `ANTHROPIC_API_KEY`, OAuth-token variables, and other credentials are not forwarded, and `~/.openai_env` is not read. If neither CLI has an eligible subscription login, the command fails without writing and names both login commands. Only `memoree remember --allow-api-key ...` enables Codex fallback key loading for that invocation. It accepts `CODEX_API_KEY`, `OPENAI_API_KEY`, or an `OPENAI_API_KEY` assignment safely parsed from `~/.openai_env`, and passes the value to Codex as `CODEX_API_KEY`. The file is never sourced as shell code. Claude API-key fallback is not implemented. Other reasoning clients consume `context.build` and manage their own models, tools, and privacy policy outside the daemon.
