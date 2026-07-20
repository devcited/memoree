---
title: Session checkpoints
description: Preserve deliberate continuity notes without polluting durable recall.
---

# Session checkpoints

Checkpoints preserve a small, deliberate continuity note without immediately turning it into durable or recallable memory. They are useful before a manual compaction, a session handoff, or a pause where running the full remember compiler would interrupt the agent loop.

Stage one note per stable session/thread ID:

```sh
printf '%s' 'Storage recall is implemented; next verify the regression corpus.' |
  memoree checkpoint --session SESSION_ID --task retrieval-improvement -
```

Checkpoint text is UTF-8, capped at 4 KiB with an explicit truncation marker, and stored in a private `pending-checkpoints` directory under the local Memoree data directory. Input above 64 KiB is rejected. A later checkpoint with the same session ID replaces the earlier staged text while preserving the original creation time.

Pending checkpoints are deliberately outside SQLite, CAS, FTS, `search`, `memory.recall`, and `context.build`. A query matching only pending text must still return `presence=none`. This quarantine keeps incomplete session notes from polluting claim or artifact-only retrieval.

Review and promotion are explicit:

```sh
memoree pending list
memoree pending show CHECKPOINT_ID
memoree pending preview CHECKPOINT_ID
memoree pending apply CHECKPOINT_ID
memoree pending drop CHECKPOINT_ID
```

`list` omits staged text and, by default, items older than the 14-day review window; `list --all` includes them. The review window is not automatic deletion. `show` reveals the text. `preview` runs the selected isolated claim compiler without writing. `apply` is the deliberate write boundary and runs the same compiler through `memoree remember --apply`; the resulting checkpoint artifact is the exact evidence source for accepted claims. Successful preview/apply leaves the pending item available for inspection and exact retry. Only `drop` deletes it, and deletion is not recoverable.

Checkpointing performs a deterministic warning scan for common AWS access-key shapes, bearer tokens, private-key blocks, and named credential assignments. Flagged notes remain quarantined and `preview`/`apply` refuse them unless the caller explicitly passes `--allow-flagged` after inspection. This is a warning net, not a secret classifier; agents must not checkpoint credentials, transcript dumps, prompt bodies, tool payloads, chain-of-thought, or routine progress.

## Why lifecycle hooks are not the default

Current [Codex hooks](https://learn.chatgpt.com/docs/hooks) provide session IDs and lifecycle events, but `PreCompact` does not provide an agent-authored summary and Codex currently skips prompt/agent hook handlers. `Stop` can expose the last assistant message or continue the turn, but it fires at turn scope; treating every final response as durable continuity creates noise and may capture sensitive material. Current [Claude Code hooks](https://code.claude.com/docs/en/hooks) have the same fundamental issue: `PreCompact` exposes identifiers and a transcript path, while `Stop` exposes the last assistant message—not a deliberate memory candidate.

Memoree therefore ships no transcript-capture or auto-apply hook. Add an agent instruction to call `memoree checkpoint` deliberately when a concise continuity note is actually needed. Revisit automatic event capture only after real usage shows a deterministic consumer for event-level data and a benefit that checkpoint distillation cannot provide.
