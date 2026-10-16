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
| `message_agent` *(hub mode)* | `to` *(str)*<br>`text` *(str)*<br>`wait` *(bool, default: true)* | Only in agents started by `landline up`. Sends a message to another agent (or `'user'`) through the hub and, with `wait`, returns its reply. |
| `list_agents` *(hub mode)* | *None* | Only in agents started by `landline up`. Lists the other agents with their roles and busy state. |

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
- `model`, `effort`: passed to the CLI (default: the CLI's own).
- `session_id`: resume an existing CLI session instead of starting a new one.

At the top level, `run` names the run (default: a timestamp).

**2. Start the hub.** It starts every agent and prints live activity: messages between agents, tool calls, text, and thinking where the CLI exposes it.

```bash
landline up run.json
```

**3. Talk to it** from another terminal in the same folder. Each folder has its own hub, so hubs started in different folders run independently; elsewhere, pass `--socket` (the path `landline up` prints) or set `$LANDLINE_SOCKET`.

```bash
landline send planner "Write a small script that prints the first 10 prime numbers, get it reviewed, and tell me the result"
landline spawn claude reviewer --model sonnet --role "Reviews the coder's changes"   # add an agent
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

