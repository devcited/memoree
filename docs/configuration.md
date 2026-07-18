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

On Unix, leave `MEMOREE_ENDPOINT` unset for the recommended default: an owner-private runtime directory and mode-`0600` Unix socket. This provides a per-user boundary that TCP does not.

Server TCP binds are a separate concern from the client endpoint. `memoree serve` rejects wildcard and other non-loopback TCP listeners because this release has no authentication. Loopback TCP is host-local, not user-private: any local process/user able to connect can use the protocol. Container supervisors that must bind the process to a container interface can explicitly pass `--dangerously-allow-non-loopback-tcp`, but must enforce their own network boundary. The bundled Compose file makes that opt-in visible and publishes the port on host `127.0.0.1` only, but is intended for a trusted single-user host. It sets `MEMOREE_HOME=/data` and persists that directory in one named volume. The standalone Docker image remains loopback-only by default.

There is intentionally no setting for a default `workspace` or `personal` retrieval horizon. Broadening is a decision made explicitly by the caller for one search or context-build request and must include a reason.

There is intentionally no daemon model-provider configuration. `memoree remember` uses the local `codex` executable with a fixed Luna policy and, by default, the existing ChatGPT session created by `codex login`. The isolated child receives only a small environment allowlist, including `HOME` and `CODEX_HOME` for cached CLI authentication. `OPENAI_API_KEY`, `CODEX_API_KEY`, access tokens, and other credentials are not forwarded, and `~/.openai_env` is not read.

If cached ChatGPT authentication is unavailable, the command fails without writing and asks the caller to obtain explicit human permission. Only `memoree remember --allow-api-key ...` enables fallback key loading for that invocation. It accepts `CODEX_API_KEY`, `OPENAI_API_KEY`, or an `OPENAI_API_KEY` assignment safely parsed from `~/.openai_env`, and passes the value to the Codex CLI as `CODEX_API_KEY`. The file is never sourced as shell code. Other reasoning clients consume `context.build` and manage their own models, tools, and privacy policy outside the daemon.
