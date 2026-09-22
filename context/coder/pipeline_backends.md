# Multi-Backend Pipeline Architecture & Stream-JSON Protocols

This document details `src/agy_pipeline.py`, `src/claude_pipeline.py`, and
`src/pipeline_factory.py` — the layer that lets `AgyManager` drive either the **Antigravity CLI
(`agy`)** or the **Claude Code CLI (`claude`)**, and the two backends' differing stream-json
protocols.

---

## 1. Independent, self-contained pipeline modules — no shared base class

`AgyPipeline` (`src/agy_pipeline.py`, pre-existing and unchanged) and `ClaudePipeline`
(`src/claude_pipeline.py`, new) are deliberately **not** built on a shared base class. Both CLIs
happen to be launched the same way in principle — a long-running subprocess with
`--input-format stream-json --output-format stream-json`, one JSON object per line on stdin to
drive a turn, a stream of JSON objects on stdout — but the two protocols differ enough in event
shape (`agy`'s own `{"event": "...", ...}` scheme vs. Claude Code's Anthropic-Messages-API-shaped
`{"type": "...", ...}` events) that each pipeline owns its full subprocess lifecycle
independently: process spawn, background reader thread, turn-boundary synchronization via
`threading.Event`, live-output buffering, `_handle_event()` parsing, and clean shutdown are all
implemented separately in each file (`claude_pipeline.py` mirrors `agy_pipeline.py`'s structure
and method names for consistency, but does not import from or subclass it). This keeps
`agy_pipeline.py` exactly as it was before Claude support existed — nothing outside
`claude_pipeline.py` and the two callers that opt into it (`pipeline_factory.py`,
`AgyManager.initialize()`) had to change to add the new backend.

`ToolExecution` and `AgentTurnResult` (the public per-turn result data models) are defined once
in each pipeline module — `agy_pipeline.py` keeps its original copy, and `claude_pipeline.py`
defines its own structurally-identical copy. `AgyManager` and the MCP server only ever access
these by attribute (`.response`, `.status`, `.tool_calls`, ...), so the duplication costs nothing
at the call site and avoids coupling the two backends through a shared type.

---

## 2. Backend resolution (`src/pipeline_factory.py`)

```python
from pipeline_factory import create_pipeline, normalize_backend

pipeline = create_pipeline("claude", cwd=..., model="sonnet")
```

`normalize_backend()` maps case-insensitive aliases to a canonical key and raises `ValueError` on
anything else:

| Alias(es) | Canonical | Class |
| :--- | :--- | :--- |
| `agy`, `antigravity`, `antigravity-cli`, `gemini` | `antigravity` | `AgyPipeline` |
| `claude`, `claude-code`, `claude-cli`, `anthropic` | `claude` | `ClaudePipeline` |

`AgyManager.initialize(..., backend=...)` calls this before constructing a pipeline, so an
unknown backend name fails fast with a clear error instead of falling through to a default.

---

## 3. Antigravity protocol (`agy`, unchanged from before)

```
agy --input-format stream-json --output-format stream-json --disable-slash-commands \
    [--dangerously-skip-permissions] [--model M] [--effort E] [--conversation ID]
```

- **stdin**: `{"event": "user", "message": {"content": "<prompt>"}}`
- **stdout**: `init` (session handshake: `conversation_id`, `init.tools`, `init.permission_mode`)
  → `step_update` (tool calls, keyed by `step_index`; `tool_name == "ask_question"` is agy's
  ask-the-user signal) → `result` (`response`, `status`, `duration_seconds`, `usage`,
  `denied_actions`).
- **Transcript fallback**: `~/.gemini/antigravity-{cli,ide}/brain/<conversation_id>/.system_generated/logs/transcript.jsonl`
  (or `$ANTIGRAVITY_APP_DATA_DIR/brain/...`), used by `_extract_recent_questions()` when a
  question doesn't show up as a discrete stream event.
- **Forking**: `AgyManager.fork_conversation()` clones the conversation's SQLite DB
  (`~/.gemini/antigravity-cli/conversations/<id>.db`) and its `brain/<id>/` artifacts directory
  onto a new id *before* the pipeline starts — agy has no native "fork on resume" flag.

---

## 4. Claude Code protocol (`claude`)

```
claude --print --input-format stream-json --output-format stream-json --verbose \
       --disable-slash-commands \
       [--dangerously-skip-permissions | --permission-mode MODE] [--model M] [--effort E] \
       [--resume SESSION_ID [--fork-session]]
```

`--verbose` is required for `--output-format stream-json` in print mode. Unlike agy, Claude Code
natively supports forking a resumed session (`--fork-session`) — the CLI assigns the new session
id itself, which is why `ClaudePipeline` takes a `fork_session` flag instead of the manager
pre-computing an id.

### stdin (one line per turn)

```json
{"type": "user", "message": {"role": "user", "content": "<prompt>"}}
```

### stdout events

This is a thin wrapper around the Anthropic Messages API content-block format — the same shape
used by Claude Code's own persisted transcripts (see §5). Verified empirically against a real
`~/.claude/projects/.../<session>.jsonl` file, since Claude Code does not publish a formal schema
for this and treats it as an internal, versioned implementation detail:

1. **`system` / `init`** — session handshake:
   ```json
   {"type": "system", "subtype": "init", "session_id": "...", "tools": [...], "permissionMode": "..."}
   ```
2. **`assistant`** — one event per assistant message; `message.content` is a list of blocks.
   A `{"type": "tool_use", "id": ..., "name": ..., "input": {...}}` block becomes a
   `ToolExecution` (state `CALL_STARTED`), keyed by `id` in `_current_tools_by_id` so the matching
   result can be found later. `name in {"AskUserQuestion"}` is Claude's ask-the-user signal
   (there's no separate question event type — it's a built-in tool call).
3. **`user`** — tool results are echoed back on the *output* stream as `user` events carrying
   `{"type": "tool_result", "tool_use_id": ..., "content": ..., "is_error": bool}` blocks. These
   resolve the matching `ToolExecution` to `CALL_DONE` or `ERROR`.
4. **`result`** — final event of the turn: `result` (response text), `is_error`, `duration_ms`,
   `usage` (`input_tokens`/`output_tokens`/cache fields), `total_cost_usd`. Maps to
   `AgentTurnResult.status` of `SUCCESS`/`ERROR`.

### On-disk transcript & project directory hashing

Claude Code persists each session at:

```
~/.claude/projects/<project-dir>/<session-id>.jsonl   (override root via $CLAUDE_CONFIG_DIR)
```

`<project-dir>` is derived from the absolute `cwd` by replacing every non-alphanumeric character
with `-` (`ClaudePipeline.project_dir_for_cwd()`). This was verified against a real
`~/.claude/projects/` listing on a dev machine — e.g. `/Users/vs/function/AgentLandline` →
`-Users-vs-function-AgentLandline` — but Claude Code treats this layout as undocumented and
subject to change between releases, so it's used only as a best-effort fallback (question
recovery, `extract_last_command`), never on the critical path of a turn.

---

## 5. Manager-level differences

`AgyManager` (`src/agy_manager.py`) is otherwise backend-agnostic — `send_prompt`, `status`, and
`exit` operate on whichever pipeline instance (`AgyPipeline` or `ClaudePipeline`) is active via
`self.pipeline`/`self.backend`, accessed only by shared attribute names. `initialize()`
constructs the right class explicitly (`AgyPipeline(...)` or `ClaudePipeline(...)`) since their
constructor kwargs aren't identical — only `ClaudePipeline` takes `permission_mode`/
`fork_session`. Two methods branch on backend:

- **`initialize(..., fork=True, backend=...)`**: Antigravity pre-forks via
  `fork_conversation()` (see §3) before constructing the pipeline; Claude passes
  `fork_session=True` into `ClaudePipeline` and lets the CLI mint the new session id, which
  comes back as `conversation_id` in the result dict only *after* `pipeline.start()` returns.
- **`extract_last_command(conversation_id, backend=None, cwd=None)`**: dispatches to
  `_extract_last_command_antigravity()` (step-based `transcript.jsonl` schema) or
  `_extract_last_command_claude()` (Anthropic-message-shaped `.jsonl`, needs `cwd` to compute the
  project directory hash).

---

## 6. Testing

- `tests/test_pipeline_backends.py` — offline unit tests: backend alias resolution, launch-argv
  construction per backend, `ClaudePipeline._handle_event()` fed synthetic stream-json events
  (init / tool_use / tool_result / AskUserQuestion / result, success and error paths),
  `project_dir_for_cwd()` against the real observed directory name, and `AgyManager.initialize()`
  backend dispatch with `pipeline.start()` mocked out (no subprocess spawned).
- `tests/test_claude_pipeline.py` — live integration test mirroring `test_agy_pipeline.py`;
  spawns a real `claude` subprocess, requires `claude` in `PATH`.
