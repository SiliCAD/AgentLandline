# AgentLandline 📞

**AgentLandline** is an Model Context Protocol (MCP) server and Python backend engine that provides a direct, programmatic "landline" connection to running headless agent CLI sessions — **Antigravity CLI (`agy`)** and **Claude Code CLI (`claude`)**.

It allows external AI agents (such as Antigravity IDE, Claude Code, Cursor, or custom tools) to headlessly initialize, send prompts to, monitor, and control `agy` or `claude` agent sessions with full multi-turn conversational context, tool execution tracking, and automated GitHub issue reporting.

---

## ✨ Features

- **🔌 MCP Server Integration**: Exposes clean, standardized MCP tools using [FastMCP](https://github.com/jlowin/fastmcp) for easy integration into any MCP-compatible environment.
- **🧩 Multi-Backend Pipelines**: Drives either the Antigravity (`agy`) or Claude Code (`claude`) CLI through the same manager interface, selected per-session via `backend=`.
- **🔄 Multi-Turn Session Orchestration**: Seamlessly resumes and maintains agent conversation sessions using background streaming JSON subprocess communication.
- **📊 Real-time Status & Transcript Monitoring**: Inspect live process health, streaming output buffers, and per-backend on-disk transcripts.
- **👥 Multi-Agent Sessions (`landline`)**: Run several named agents side by side (any mix of `agy` and `claude`), let them message each other through a hub, step in with your own messages, and follow everything live from a terminal. Every message is recorded with who sent it to whom and a link to the sender's own CLI transcript.
- **🐛 Automated GitHub Issue Reporting**: Instantly report bugs, tracebacks, or feature requests directly to GitHub via the `gh` CLI with automatic client-agent detection, smart label normalization (`new_lable` <-> `new lable`, typo matching), and auto-creation of missing labels with random colors.
- **🛠 Robust Process Recovery**: Automatic re-initialization and stream re-connection upon process interrupts or stdin pipe errors.

---

## 🏗 Architecture

```
AgentLandline/
├── src/
│   ├── server.py                 # Compatibility shim for MCP configs that launch src/server.py
│   └── agentlandline/            # The installable package
│       ├── server.py             # MCP Server Entrypoint (FastMCP tool definitions, hub-mode tools)
│       ├── agent_manager.py      # High-level Orchestration Manager & Transcript Reader (backend-aware)
│       ├── pipeline_factory.py   # Backend name/alias -> pipeline class resolution
│       ├── agy_pipeline.py       # Antigravity backend: launches `agy` (stream-json)
│       ├── claude_pipeline.py    # Claude Code backend: launches `claude` (stream-json)
│       ├── hub.py                # Multi-agent hub: agents, inboxes, routing, local socket API
│       ├── message_log.py        # messages.jsonl: who sent which message to whom
│       ├── events.py             # Normalizes both CLIs' live events to one shape
│       ├── cli.py                # `landline` command (up / send / ls / tail / messages)
│       └── issue_reporter.py     # GitHub Issue Reporter & gh CLI wrapper
├── tests/                        # pytest suites (unit tests are offline; *_pipeline.py are live)
└── pyproject.toml                # Package metadata (pip install -e .)
```

### Core Components

Modules below live in `src/agentlandline/`.

1. **`server.py`**: The FastMCP server hosting tools for client agents. Automatically extracts client agent metadata (`clientInfo`) on initialization.
2. **`agent_manager.py`**: High-level manager (`AgentManager`) that maintains the active pipeline instance (whichever backend), and provides backend-aware transcript search. Named generically (not `agy_manager`) because it orchestrates any registered backend, not just Antigravity.
3. **`agy_pipeline.py`**: Non-blocking subprocess bridge (`AgyPipeline`) for the Antigravity backend — untouched by the Claude Code addition.
4. **`claude_pipeline.py`**: Non-blocking subprocess bridge (`ClaudePipeline`) for the Claude Code backend — a separate, self-contained module (no shared base class with `agy_pipeline.py`) implementing Claude Code's own stream-json protocol (Anthropic-Messages-API-shaped events, vs. agy's `event`-keyed protocol).
5. **`pipeline_factory.py`**: Resolves a backend name/alias (`'agy'`, `'antigravity'`, `'claude'`, `'claude-code'`, ...) to its pipeline class.
6. **`issue_reporter.py`**: Utility (`IssueReporter`) formatting GitHub issues with session metadata, smart label normalization/matching (typography tolerance), and auto-creating missing labels with random colors.
7. **`hub.py`, `message_log.py`, `events.py`, `cli.py`**: Multi-agent sessions; see [Multi-Agent Sessions](#-multi-agent-sessions-landline) below and [`context/coder/multi_agent_hub.md`](context/coder/multi_agent_hub.md).

See [`context/coder/pipeline_backends.md`](context/coder/pipeline_backends.md) for a deep-dive on the two protocols and how forking/transcripts differ per backend.

---

## 🚀 Quick Start

### 1. Prerequisites

- **Python 3.10+**
- **Antigravity CLI (`agy`)** and/or **Claude Code CLI (`claude`)**: Ensure whichever backend(s) you plan to use are installed and available in your `PATH`.
- **GitHub CLI (`gh`)** *(Optional)*: Required for the `report_issue` tool. Must be authenticated (`gh auth login`).

### 2. Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/SiliCAD/AgentLandline.git
cd AgentLandline
pip install -e .
```

This installs the `agentlandline` package plus two commands: `agentlandline-mcp` (the MCP server) and `landline` (multi-agent sessions). Without installing, use `PYTHONPATH=src python3 -m agentlandline ...` in place of `landline`.

---

## ⚙️ MCP Configuration

Add **AgentLandline** to your MCP client configuration file (e.g., `~/.gemini/antigravity/mcp_config.json` or `claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "agent-landline": {
      "command": "python3",
      "args": [
        "/absolute/path/to/AgentLandline/src/server.py"
      ]
    }
  }
}
```

`src/server.py` keeps working as an entrypoint. After `pip install -e .` you can use `"command": "agentlandline-mcp"` with no args instead.

---

## 🧰 MCP Tools Reference

| Tool Name | Parameters | Description |
| :--- | :--- | :--- |
| `initialize_agent` | `conversation_id` *(str)*<br>`cwd` *(str, optional)*<br>`fork` *(bool, optional)*<br>`new_conversation_id` *(str, optional)*<br>`backend` *(str, default: 'antigravity')*<br>`model` *(str, optional)*<br>`effort` *(str, optional)* | Resumes and initializes an agent process backend for a specific session ID. `backend` selects `'antigravity'`/`'agy'` (default) or `'claude'`/`'claude-code'`. |
| `send_prompt` | `prompt` *(str)*<br>`timeout` *(float, default: 120.0)* | Sends a prompt to the active session (whichever backend was initialized) and returns the agent's turn response and tool metrics. |
| `agent_status` | `mode` *(str, default: 'process')* | Queries status. Modes: `process` (PID, health & active backend), `output` (live output buffer), `transcript` (last turn transcript). |
| `exit_agent` | *None* | Gracefully terminates and cleans up the current agent process. |
| `report_issue` | `title` *(str)*<br>`body` *(str)*<br>`label` *(str)*<br>`repo` *(str, default: 'SiliCAD/AgentLandline')*<br>`agent_model` *(str)*<br>`session_id` *(str)* | Creates a GitHub issue in the target repository (default: `SiliCAD/AgentLandline`) with formatted metadata, smart typography/typo label matching (`new_lable` <-> `new lable`), and auto-creation of missing labels with random colors. |
| `message_agent` *(hub mode)* | `to` *(str)*<br>`text` *(str)*<br>`wait` *(bool, default: true)*<br>`images` *(list of paths, optional)* | Only in agents started by `landline up`. Sends a message to another agent (or `'user'`) through the hub and, with `wait`, returns its reply. `images` attaches image files (see [Images](#images)). |
| `list_agents` *(hub mode)* | *None* | Only in agents started by `landline up`. Lists the other agents with their roles and busy state. |
| `start_agent` *(hub mode)* | `name`, `role`, `backend`, `model`, `effort`<br>`session_id`, `fork`<br>`from_agent`, `after`<br>`cwd` | Only in agents with a `spawns` setting. Starts a new agent in the session: a new conversation, an existing session (continued, or copied with `fork`), or a copy of a teammate, optionally cut after one of its replies. See [Agents starting agents](#agents-starting-agents). |
| `stop_agent` *(hub mode)* | `name` | Only with `spawns`. Stops an agent this agent started. |
| `find_sessions` *(hub mode)* | `query`, `limit` | Only with `spawns`. Saved sessions on this machine, newest first, with their session ids. |
| `manage_context` *(hub mode)* | `agent` *(str)*<br>`action` *(str: 'list' or 'rewind')*<br>`after` *(str, message id)*<br>`note` *(str, optional)* | Only in agents with a `manages` setting. `list` shows another agent's turns; `rewind` cuts its conversation back to just after its reply to message `after`. See [Managing another agent's context](#managing-another-agents-context). |

---

## 👥 Multi-Agent Sessions (`landline`)

Run several named agents at once and let them work together, with you able to watch and step in.

**1. Describe the agents** in a JSON run config, e.g. `run.json`:

```json
{
  "agents": {
    "planner":  {"backend": "claude", "role": "Plans the work, splits it into tasks, hands them to the coder and the reviewer, and reports the result to the user."},
    "coder":    {"backend": "claude", "role": "Writes and changes code in this folder as asked."},
    "reviewer": {"backend": "agy",    "role": "Reviews and runs the coder's work and reports any problems."}
  }
}
```

Per agent: `backend` (`claude`/`agy`, required), and optionally:

- `role`: what the agent is for. Each agent is told its own role and the other agents' roles, so it knows whom to ask.
- `cwd`: the folder it works in (default: the folder you run `landline up` from).
- `model`, `effort`: passed to the CLI (default: the CLI's own). Each agent can have its own, and one team
  can mix them (checked live with claude haiku/low, sonnet/high and opus/medium, and agy Gemini 3.8 Flash
  low and high, and Claude Sonnet 4.6 in agy). agy is strict about the pair: `gemini-3.8-flash` takes
  `--effort low|medium|high`; names with the level built in (`gemini-3.8-flash-low`) take no other effort;
  its Claude models (`claude-sonnet-4-6`, `claude-opus-4-6-thinking`) take no effort at all. A pair agy
  rejects fails the agent's start with agy's own message. `agy models` lists the names.
- `session_id`: resume an existing CLI session instead of starting a new one.
- `fork`: with `session_id`, start on a copy of that session instead of continuing it. The agent begins
  with everything that session knew, without spending tokens or a first prompt to rebuild that context,
  and the original session stays as it is, so the same base can seed every run of a team. The copy's id
  replaces `session_id` in the run's `run.json` (with `forked_from` naming the original), so resuming
  the run continues the copy. claude forks with `--resume <id> --fork-session`; agy has no fork option,
  so the hub copies the conversation's files.
- `manages`: the agents whose context this agent may manage (see below).
- `spawns`: whether this agent may start agents of its own (see below).

At the top level, `run` names the run (default: a timestamp).

**2. Start the hub.** It starts every agent and prints live activity: messages between agents, tool calls, text, and thinking where the CLI exposes it.

```bash
landline up run.json
```

**3. Talk to it** from another terminal in the same folder. Each folder has its own hub, so hubs started in different folders run independently; elsewhere, pass `--socket` (the path `landline up` prints) or set `$LANDLINE_SOCKET`.

```bash
landline send planner "Write a small script that prints the first 10 prime numbers, get it reviewed, and tell me the result"
landline spawn claude reviewer --model sonnet --role "Reviews the coder's changes"   # add an agent
landline spawn claude fixer --session <session id> --fork   # start on a copy of an existing session
landline send coder "What does this show?" --image plot.png   # attach an image
landline send coder "Stop, use the other netlist" --force   # interrupt its current turn, handle this first
landline stop reviewer           # stop one agent (resuming the session brings it back)
landline rename opamp-review     # give the session a memorable name
landline ls          # agents, busy/idle, session ids
landline tail        # follow live activity
landline messages    # the run's message log
```

Agents talk to each other with the `message_agent(to, text, wait=True)` and `list_agents()` MCP tools. These appear automatically in agents started by the hub, provided agent-landline is in that CLI's MCP config. A message that arrives while an agent is busy waits until its current turn ends, and an agent's final response to a message is delivered back to the sender.

**Adding agents while it runs.** `landline up` with no config starts an empty hub.
`landline spawn <claude|agy> <name> [--model --effort --cwd --role --session]` starts another agent in the
running hub (`--session` resumes an existing CLI session). The agents already there are told who joined, and its role, with their
next message.

**Stopping one agent and naming the session.** `landline stop <name>` closes one agent: messages still
queued for it fail with "stopped", and the other agents are told it left. It stays in the run's `run.json`
marked `"stopped": true`, so resuming the session brings it back. `landline rename <name>` renames the
running session: its folder under `runs/` moves to the new name, which then identifies it everywhere.

**Interrupting an agent.** `landline send <name> "…" --force` (socket: `"force": true` in the `send` op,
user only; used by MultiMind's `!force`) puts the message ahead of the ones waiting for that agent and
interrupts the turn it's running, so the agent starts on this message right away. The agent and its
conversation stay; only the running turn stops (claude: an `interrupt` control request, like Esc;
agy: SIGINT, after which the hub resumes its conversation in a new agy process). The message whose turn
was cut short gets `[interrupted by the user]` as its reply, so an agent waiting on it isn't left
hanging, and the forced message tells the agent which message it was taken off. The hub emits
`{"kind": "interrupted", "agent", "message_id", "by"}`, and that turn's `turn_end` carries
`"interrupted": true`. If the agent is idle, the message is simply delivered first.

**Managing another agent's context.** An agent with a `manages` setting gets one more MCP tool,
`manage_context`. With it, a lead can list another agent's turns and rewind that agent to just after one
of its replies. The rewound agent keeps its name and continues on a copy of its conversation cut at that
point, so it carries less context and its turns are faster.

```json
"lead":  {"backend": "claude", "role": "…", "manages": ["fixer"]},
"fixer": {"backend": "claude", "role": "…"}
```

`manages` lists agent names, or sets each one's rules (`"*"` covers agents not named):

```json
"manages": {"fixer": {"actions": ["list", "rewind"], "note": "required"}, "*": {"actions": ["list"]}}
```

| Setting | Values | Default |
| :--- | :--- | :--- |
| `actions` | `list`, `rewind` | both |
| `note` | `optional`, `required`, `off`: whether the lead must (or may) tell the agent what it learned in the turns it loses | `optional` |
| `backends` | `claude`, `agy` | `["claude"]` |
| `cut` | `turn`: cut only at the end of a turn | `turn` |
| `files` | `keep`: files on disk are left as they are | `keep` |

- **What a rewind does.** The agent's claude session is forked at its reply to the chosen message
  (`--resume --fork-session --resume-session-at`), and the agent restarts on the fork. The old session id
  is kept in run.json under `previous_sessions`, and nothing is deleted. The agent's next message starts
  by telling it that it was rewound, and that files on disk weren't changed.
- **Files are not rewound.** Anything the agent wrote after the cut is still on disk. Use `note` to say
  what was tried and why it failed, so the agent doesn't repeat it.
- **Not supported yet:** agy agents (their conversations can be cut, but only by editing agy's private
  database), cuts inside a turn (`cut: step`), and restoring files (`files: snapshot`). A config that asks
  for one of these is refused when it's loaded.
- An agent can't be rewound while it is busy or has messages waiting. The person running the session
  can rewind any claude agent over the socket (`{"op": "context", "from": "user", …}`); they aren't
  limited by `manages`.

<a id="agents-starting-agents"></a>**Agents starting agents.** An agent with `"spawns": true` (or rules, below) gets
three more MCP tools:
- `start_agent` starts a new agent in the session, which joins the team like any other. It can start:
  - a new conversation (`name`, `role`, and optionally `backend`, `model`, `effort`);
  - an existing session by id, continued (`session_id`), or copied with `fork: true` so the original
    stays as it is. A session another running agent uses can only be copied;
  - a copy of a teammate (`from_agent`), as it is now or cut just after its reply to message `after`.
    The copy starts with the teammate's context (and its backend, model, effort and role unless given),
    so it can try a different approach in parallel without a long briefing. This is the message-level
    fork of a rewind, but the original keeps going.
- `stop_agent` stops an agent it started.
- `find_sessions` lists saved sessions on this machine (every run under `~/.agentlandline/runs/`),
  newest first: run, agent, backend, model, role, folder, session id, and which running agent uses it.

```json
"lead": {"backend": "claude", "role": "…", "spawns": {"max": 2, "fork_from": ["fixer"]}}
```

| Setting | Values | Default |
| :--- | :--- | :--- |
| `max` | agents it started that may run at once | `3` |
| `backends` | `claude`, `agy` | both |
| `sessions` | may open existing sessions and use `find_sessions` | `true` |
| `fork_from` | agents it may copy (`"*"`: any) | `["*"]` |
| `stop` | may stop the agents it started | `true` |

Started agents record `spawned_by` (and `forked_from` for copies) in run.json, so resuming the run brings
them back.

**Changing permissions while a session runs.** `manages` and `spawns` can be changed for a running agent
(socket op `permissions`, used by MultiMind's `!permission`; only `from: "user"` is accepted). The new rules
apply at once and are saved in run.json. An agent's CLI only learns which tools it has when it starts, so
when a change gives it tools or takes them away (`manage_context`, `start_agent` and the rest), the hub
restarts its CLI on its own conversation: right away if it's idle, or right after its current turn, with
messages waiting for it kept. Changing only the rules (a limit, which agents, notes) needs no restart.
This is not a security boundary: agents run commands without prompts, so an agent could reach the hub's
socket itself. Copying an agy agent works only as it is now: agy conversations can't be cut at a message yet.

<a id="images"></a>**Images.** A message can carry images (`.png`, `.jpg`, `.gif`, `.webp`, up to 20 MB each):
`landline send coder "What's wrong with this plot?" --image plot.png`, the `images` argument of
`message_agent`, or `"images": [paths]` in the socket's `send` op. The hub copies them into the run
(`runs/<run>/images/`) and lists their paths in the message record. claude agents get them with the
message itself (up to 5 MB each; larger ones by path). agy's input is text only, so agy agents get the
paths and open the images with their `view_file` tool. Both were checked live: each read the text in a
test image.

**Restarting a team.** The run's `run.json` lists every agent, including spawned ones, with their session
ids, and is itself a valid config: `landline up ~/.agentlandline/runs/<run>/run.json` brings the whole team
back with each agent resuming its conversation.

**Message log.** Each run is stored in `~/.agentlandline/runs/<run>/` (override with `$LANDLINE_HOME`): `run.json` holds the agents and their session ids, and `messages.jsonl` has one line per message:

```json
{"id": "m3", "ts": "2026-10-11T09:49:30Z", "from": "tester", "to": "architect", "text": "...",
 "caused_by": "m2", "from_session": "<session id>", "from_transcript": "<path>", "from_transcript_line": 34}
```

Conversation history isn't copied. Each record links to the sender's own CLI transcript and the line it had reached when the message was sent, and `caused_by` chains each message back to the original request. Claude Code deletes transcripts after 30 days by default (`cleanupPeriodDays`), so copy the transcripts of runs you want to keep.

**What reasoning is visible.** agy streams text and tool calls live, and writes each step's thinking to its transcript, which the hub reads back and shows. Claude Code streams text and tool calls, but in current versions saves its thinking blocks with empty text, so for claude agents the visible reasoning is their text between tool calls.

---

## 🐍 Python Library Usage

You can also use `AgentManager`, or a backend pipeline directly, in Python scripts.

### Using `AgentManager` with either backend

```python
from agentlandline.agent_manager import AgentManager

manager = AgentManager()

# Antigravity (default)
manager.initialize(conversation_id="my-session", backend="antigravity")

# ...or Claude Code
manager.initialize(conversation_id="my-session", backend="claude", model="sonnet")

result = manager.send_prompt("Hello! Remember the keyword 'Landline'.")
print(result["response"])

manager.exit()
```

### Using a pipeline directly (`AgyPipeline` / `ClaudePipeline`)

```python
from agentlandline.agy_pipeline import AgyPipeline
# from agentlandline.claude_pipeline import ClaudePipeline  # swap in for the Claude Code backend

# Start headless pipeline session
pipeline = AgyPipeline(skip_permissions=True, model="gemini-3.6-flash")
pipeline.start()

print(f"Session started! Conversation ID: {pipeline.conversation_id}")
print(f"Available tools: {pipeline.available_tools}")

# Turn 1: Send prompt
result1 = pipeline.send("Hello! Remember the keyword 'Landline'.")
print(f"Agent response: {result1.response}")

# Turn 2: Verify memory
result2 = pipeline.send("What was the keyword?")
print(f"Agent response: {result2.response}")

# Close pipeline cleanly
pipeline.close()
```

### Interactive CLI Session

Run an interactive test session in terminal:

```bash
python src/agentlandline/agy_pipeline.py --model gemini-3.6-flash
```

---

## 🧪 Testing

Run the included test suites to verify transcript extraction and pipeline functionality:

```bash
# Unit tests: transcript extraction & DB forking (AgentManager)
pytest tests/test_agent_manager.py -v

# Unit tests: backend selection & stream-json event parsing (offline, no subprocess)
pytest tests/test_pipeline_backends.py -v

# All offline unit tests (hub, message log, events, CLI, MCP tools, ...)
pytest tests --ignore=tests/test_agy_pipeline.py --ignore=tests/test_claude_pipeline.py

# Integration test: multi-turn agy session (requires `agy` in PATH)
python tests/test_agy_pipeline.py

# Integration test: multi-turn claude session (requires `claude` in PATH)
python tests/test_claude_pipeline.py
```

---

