# Multi-Agent Hub (`hub.py`, `message_log.py`, `events.py`, `cli.py`)

The hub runs several named agent sessions side by side (any mix of `agy` and `claude`), routes
messages between them, records who sent which message to whom, and exposes live activity. It is
started with `landline up <config.json>`. The single-session MCP tools (`initialize_agent`,
`send_prompt`, ...) are unaffected and work without a hub.

The design is deliberately minimal: plain JSON files, no database, no copied conversation history.
Add complexity only when a real run shows the need for it.

---

## 1. Components

```
landline up run.json
   └── Hub (hub.py)
        ├── one AgyPipeline / ClaudePipeline per agent (+ inbox queue + worker thread)
        ├── MessageLog  -> ~/.agentlandline/runs/<run>/messages.jsonl
        ├── run.json    -> ~/.agentlandline/runs/<run>/run.json (agents, session ids)
        ├── live events -> subscribers (events.normalize + message/turn records)
        └── HubServer   -> Unix socket ~/.agentlandline/hub.sock
              ├── landline send / ls / tail / messages   (cli.py, HubClient)
              └── agent-landline MCP server inside each agent (hub mode: message_agent, list_agents)
```

`$LANDLINE_HOME` overrides `~/.agentlandline`; `$LANDLINE_SOCKET` overrides the socket path.

---

## 2. Message flow

- `Hub.send(sender, to, text, wait=True)`: `sender` is an agent name or `'user'`.
  1. The message is written to the log and emitted as a `message` event.
  2. It goes into `to`'s inbox. The agent's worker delivers inbox messages one at a time, so a message
     that arrives mid-turn waits until the current turn ends.
  3. The worker sends the framed prompt (`[landline message <id> from <sender>]\n<text>`) with no timeout.
     The first message to each agent is prefixed with an intro: its name and role, the other agents,
     and how replying and `message_agent` work.
  4. The agent's final response is logged as a message back to the sender (`caused_by` = the message
     it answers). With `wait=True`, the caller gets this reply back.
- **Spawning**: `Hub.spawn(spec)` starts an agent in a running hub (socket op `spawn`, `landline spawn`).
  It gets an inbox and worker like the others, run.json is rewritten, and subscribers get an
  `agent_added` event. Agents already introduced are told about newcomers once, at the start of their
  next message (`New agents have joined the session:` + name and role); a new agent's intro lists
  everyone. Names must match `[A-Za-z][A-Za-z0-9_-]{0,31}` and can't be `user`. Agents are always read
  through `_slots()`, a locked snapshot, because spawning mutates `hub.agents` from another thread.
- **Stopping**: `Hub.stop_agent(name)` (op `stop`, `landline stop`) closes one agent's CLI. Deliveries still in
  its inbox fail with "was stopped"; a turn cut off mid-way records no reply (the waiting sender gets an
  error instead). It moves to `_stopped` and stays in run.json with `"stopped": true`, so resuming the run
  brings it back; re-spawning the name clears that. Agents are told `Agents that have left the session: …`
  with their next message. A message that lands just as the agent stops is failed too (`slot.stopping`).
- **Renaming**: `Hub.rename(new_id)` (op `rename`, `landline rename`) moves `runs/<old>` to `runs/<new>`
  while holding the message log's and run.json's locks, then emits `run_renamed`. Names match
  `[A-Za-z0-9][A-Za-z0-9._-]{0,63}` and must not exist yet.
- **Adopting**: `Hub.adopt(run_id)` (op `adopt`) makes a hub whose run has no messages yet continue a
  saved run instead: the log moves to that run (numbering continues), its agents stay in run.json as saved
  (`_dormant`) until spawned again, the empty run the hub started with is removed, and viewers get
  `run_switched`. This is how a viewer resumes a session into a fresh hub without losing its history.
- **Context management** (`context.py`): an agent's `manages` setting (normalized to `{agent: rules}`,
  validated when the config is loaded) lets it list and rewind other agents, through op `context` and
  the `manage_context` MCP tool. The tool is registered only when the hub sets `LANDLINE_MANAGES`.
  - **Checkpoints**: when a claude agent finishes a turn successfully, the hub appends
    `{agent, message_id, from, reply_id, session_id, cut, tools}` to `runs/<run>/checkpoints.jsonl`.
    `cut` is the `uuid` of the turn's last `assistant` stream event, and `tools` counts its `tool_start`
    events.
  - **Rewinding**: `Hub.rewind(caller, agent, after, note)` checks the rules, then starts a new slot with
    `spec.rewind = {at: cut, session_id: <new uuid>}`. That makes ClaudePipeline run
    `--resume <old> --fork-session --session-id <new> --resume-session-at <cut>`, so the fork's id is
    known before its first turn. Under the lock it swaps the slot in (or aborts if a message arrived
    meanwhile), closes the old pipeline, and appends `{agent, rewound_to, session_id, previous_session}`
    to checkpoints.jsonl. Folding the file drops the turns after a rewind point. The new slot's `notice`
    goes in front of its next message. The old session id goes into `spec.previous_sessions` (saved in
    run.json; `rewind` itself never is). Emits `session`, then `agent_rewound`. `note` is sent from the
    caller with `wait=False`.
  - Fork-at-message was checked live against claude 2.1.299: the fork keeps the message uuids up to
    the cut, and the original session file is unchanged. agy isn't supported (REWIND_BACKENDS); its
    conversations can be cut by deleting `steps` rows from a copy of its SQLite file, but that format is
    private.
- **Images** (`images.py`): `send(..., images=[paths])` checks them (type, size), copies them to
  `runs/<run>/images/<random>-<name>`, and stores the copies' paths in the record (`images`). The worker
  passes them to ClaudePipeline.send(images=...), which adds base64 image blocks after the text
  (≤ 5 MB each). agy's stream-json input refuses non-text blocks ("only text"), so `_frame` appends the
  paths and asks the agent to open them with its file viewing tool (agy's `view_file` reads images).
- **Forking at start**: `AgentSpec.fork` (with `session_id`). `_new_slot` creates the pipeline with
  `spec.rewind = {session_id: <new uuid>}` and no cut point. For claude that's
  `--resume <base> --fork-session --session-id <new>`. For agy, `AgyPipeline.fork_conversation` copies
  `conversations/<base>.db` (plus `-wal`/`-shm`) and `brain/<base>/` to the new id. The slot gets a copy
  of the spec with the new `session_id`, `fork: false` and `forked_from: <base>`, so run.json resumes the
  copy rather than forking again.
- Sending to `'user'` only records the message, since there is no inbox to deliver it to.
- **Wait-cycle guard**: the hub tracks which agent is waiting on which. If A is waiting on B and B
  tries to wait on A (directly or through a longer chain), B gets a `HubError` explaining the
  deadlock instead of hanging. `wait=False` sends are always allowed.
- Claude agents get `MCP_TOOL_TIMEOUT` raised to 24h, because an agent waiting on another agent's
  reply is itself in the middle of a `message_agent` tool call.

---

## 3. Message log schema (`messages.jsonl`)

One JSON object per line, appended (crash-safe, readable with `cat`):

| Field | Meaning |
| :--- | :--- |
| `id` | `m1`, `m2`, ... per run (numbering continues if a run directory is reused) |
| `ts` | UTC timestamp |
| `from`, `to` | Agent names or `user` |
| `text` | The message as sent (before framing) |
| `caused_by` | The message that started the sender's current turn, so records chain back to the original request |
| `from_session` | Sender's CLI session/conversation id |
| `from_transcript` | Path to the sender's own CLI transcript |
| `from_transcript_line` | That transcript's line count when the message was sent: where to start reading for the reasoning behind it |

Transcript locations: claude `~/.claude/projects/<cwd-dir>/<session>.jsonl` (`ClaudePipeline.transcript_path()`),
agy `.../brain/<id>/.system_generated/logs/transcript.jsonl` (`AgyPipeline.transcript_path()`).
Claude Code deletes transcripts after `cleanupPeriodDays` (30 by default), so links in old runs can go stale.
Copy the transcripts of runs worth keeping.

---

## 4. Live events (`events.py`)

`normalize(backend, raw)` maps one raw stream-json event to zero or more of:

| `kind` | Fields |
| :--- | :--- |
| `session` | `session_id` |
| `text_delta` | `text` (streamed chunk; claude needs `--include-partial-messages`, which the hub enables) |
| `text` | `text` (complete block; claude only) |
| `thinking` | `text` (may be empty) |
| `tool_start` | `id`, `name`, `input` |
| `tool_end` | `id`, `ok`, `output` (clipped to 2000 chars) |
| `turn_end` | `ok`, `duration_s`, `usage` |

The hub adds `agent` and `ts` to each one, and also emits `message` (a full log record), `turn_start`
(`message_id`), `idle`, `agent_added` (`info`: the new agent's `list` entry), `agent_removed`, and
`run_renamed` (`run`, `previous`), `agent_rewound` (`to`, `by`, `dropped`, `session_id`, `previous_session`,
`transcript`). The `session` event
carries `model`: the model in use (claude reports it at startup; agy's comes from its settings). Live events are not persisted.

**Reasoning visibility (verified against claude 2.1.x and agy 1.3.x):**
- agy streams only a thinking token count, but writes each model step's `thinking` text to its
  transcript. When an `agent_response` step completes, the hub reads that text back
  (`agy_step_thinking`, retried briefly in case the line lands late) and emits a `thinking` event.
- Claude Code emits `thinking` blocks, but with empty text in both the stream and its saved transcript.
  For claude agents, the visible reasoning is the text between tool calls.

---

## 5. Socket API (`HubServer` / `HubClient`)

One JSON request line per connection, answered with one JSON line (`subscribe` keeps streaming):

```json
{"op": "send", "from": "user", "to": "coder", "text": "...", "wait": true, "timeout": null, "images": ["/abs/plot.png"]}
{"op": "list"}
{"op": "messages"}
{"op": "stop", "name": "reviewer"}
{"op": "rename", "run": "opamp-review"}
{"op": "adopt", "run": "run-20261012-022025-7b62"}
{"op": "spawn", "name": "reviewer", "backend": "claude", "model": null, "effort": null, "cwd": null, "role": "...", "manages": null, "fork": false}
{"op": "context", "from": "lead", "agent": "fixer", "action": "list"}
{"op": "context", "from": "lead", "agent": "fixer", "action": "rewind", "after": "m7", "note": "..."}
{"op": "subscribe"}
```

`spawn` replies once the agent is up (or failed to start), with `"agent"`: its `list` entry.
Replies carry `"ok": true|false` (`"error"` when false). `subscribe` first returns a snapshot
(`run`, `agents`), then one event per line until the client disconnects. Starting a second hub on a
socket that is in use fails. A stale socket file left by a crashed hub is removed on start.

---

## 6. Agent requirements

- An agent can only *initiate* messages if agent-landline is in its CLI's MCP config (claude:
  `~/.claude.json` or project config; agy: `agy mcp add`). The hub passes `LANDLINE_AGENT`,
  `LANDLINE_SOCKET` and `LANDLINE_RUN` in the agent's environment, and the MCP server it launches
  inherits them and switches to hub mode.
- Agents without the MCP server can still receive messages and reply (their final response is
  delivered back automatically).

---

## 7. Testing

- `tests/test_hub.py`: routing, `caused_by` chains, the wait-cycle guard, parallel start, run.json,
  and the socket API, using fake pipelines (no CLI processes).
- `tests/test_message_log.py`, `tests/test_events.py` (shapes taken from real captures),
  `tests/test_cli.py`, `tests/test_server_hub_mode.py`.
- Live check: `landline up` with a claude agent and an agy agent, then `landline send <agent> ...`
  asking one to consult the other through `message_agent`.
