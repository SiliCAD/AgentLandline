# AgentLandline 📞

**AgentLandline** is an Model Context Protocol (MCP) server and Python backend engine that provides a direct, programmatic "landline" connection to running headless agent CLI sessions — **Antigravity CLI (`agy`)** and **Claude Code CLI (`claude`)**.

It allows external AI agents (such as Antigravity IDE, Claude Code, Cursor, or custom tools) to headlessly initialize, send prompts to, monitor, and control `agy` or `claude` agent sessions with full multi-turn conversational context, tool execution tracking, and automated GitHub issue reporting.

---

## ✨ Features

- **🔌 MCP Server Integration**: Exposes clean, standardized MCP tools using [FastMCP](https://github.com/jlowin/fastmcp) for easy integration into any MCP-compatible environment.
- **🧩 Multi-Backend Pipelines**: Drives either the Antigravity (`agy`) or Claude Code (`claude`) CLI through the same manager interface, selected per-session via `backend=`.
- **🔄 Multi-Turn Session Orchestration**: Seamlessly resumes and maintains agent conversation sessions using background streaming JSON subprocess communication.
- **📊 Real-time Status & Transcript Monitoring**: Inspect live process health, streaming output buffers, and per-backend on-disk transcripts.
- **🐛 Automated GitHub Issue Reporting**: Instantly report bugs, tracebacks, or feature requests directly to GitHub via the `gh` CLI with automatic client-agent detection, smart label normalization (`new_lable` <-> `new lable`, typo matching), and auto-creation of missing labels with random colors.
- **🛠 Robust Process Recovery**: Automatic re-initialization and stream re-connection upon process interrupts or stdin pipe errors.

---

## 🏗 Architecture

```
AgentLandline/
├── src/
│   ├── server.py           # MCP Server Entrypoint (FastMCP tool definitions)
│   ├── agy_manager.py      # High-level Orchestration Manager & Transcript Reader (backend-aware)
│   ├── pipeline_factory.py # Backend name/alias -> pipeline class resolution
│   ├── agy_pipeline.py     # Antigravity backend: launches `agy` (stream-json) — unchanged
│   ├── claude_pipeline.py  # Claude Code backend: launches `claude` (stream-json) — independent, self-contained
│   └── issue_reporter.py   # GitHub Issue Reporter & gh CLI wrapper
├── tests/
│   ├── test_agy_manager.py       # Unit tests for transcript parser & DB forking
│   ├── test_pipeline_backends.py # Unit tests for backend selection & stream-json parsing
│   ├── test_agy_pipeline.py      # Integration test for multi-turn agy sessions
│   ├── test_claude_pipeline.py   # Integration test for multi-turn claude sessions
│   └── test_issue_reporter.py    # Unit tests for smart label matching and auto-creation
└── requirements.txt        # Dependencies
```

### Core Components

1. **`src/server.py`**: The FastMCP server hosting tools for client agents. Automatically extracts client agent metadata (`clientInfo`) on initialization.
2. **`src/agy_manager.py`**: High-level manager (`AgyManager`) that maintains the active pipeline instance (whichever backend), and provides backend-aware transcript search.
3. **`src/agy_pipeline.py`**: Non-blocking subprocess bridge (`AgyPipeline`) for the Antigravity backend — untouched by the Claude Code addition.
4. **`src/claude_pipeline.py`**: Non-blocking subprocess bridge (`ClaudePipeline`) for the Claude Code backend — a separate, self-contained module (no shared base class with `agy_pipeline.py`) implementing Claude Code's own stream-json protocol (Anthropic-Messages-API-shaped events, vs. agy's `event`-keyed protocol).
5. **`src/pipeline_factory.py`**: Resolves a backend name/alias (`'agy'`, `'antigravity'`, `'claude'`, `'claude-code'`, ...) to its pipeline class.
6. **`src/issue_reporter.py`**: Utility (`IssueReporter`) formatting GitHub issues with session metadata, smart label normalization/matching (typography tolerance), and auto-creating missing labels with random colors.

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
git clone https://github.com/your-org/AgentLandline.git
cd AgentLandline
pip install -r requirements.txt
```

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

---

## 🧰 MCP Tools Reference

| Tool Name | Parameters | Description |
| :--- | :--- | :--- |
| `initialize_agent` | `conversation_id` *(str)*<br>`cwd` *(str, optional)*<br>`fork` *(bool, optional)*<br>`new_conversation_id` *(str, optional)*<br>`backend` *(str, default: 'antigravity')*<br>`model` *(str, optional)*<br>`effort` *(str, optional)* | Resumes and initializes an agent process backend for a specific session ID. `backend` selects `'antigravity'`/`'agy'` (default) or `'claude'`/`'claude-code'`. |
| `send_prompt` | `prompt` *(str)*<br>`timeout` *(float, default: 120.0)* | Sends a prompt to the active session (whichever backend was initialized) and returns the agent's turn response and tool metrics. |
| `agent_status` | `mode` *(str, default: 'process')* | Queries status. Modes: `process` (PID, health & active backend), `output` (live output buffer), `transcript` (last turn transcript). |
| `exit_agent` | *None* | Gracefully terminates and cleans up the current agent process. |
| `report_issue` | `title` *(str)*<br>`body` *(str)*<br>`label` *(str)*<br>`agent_model` *(str)*<br>`session_id` *(str)* | Creates a GitHub issue with formatted metadata, smart typography/typo label matching (`new_lable` <-> `new lable`), and auto-creation of missing labels with random colors. |

---

## 🐍 Python Library Usage

You can also use `AgyManager`, or a backend pipeline directly, in Python scripts.

### Using `AgyManager` with either backend

```python
from agy_manager import AgyManager

manager = AgyManager()

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
from agy_pipeline import AgyPipeline
# from claude_pipeline import ClaudePipeline  # swap in for the Claude Code backend

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
python src/agy_pipeline.py --model gemini-3.6-flash
```

---

## 🧪 Testing

Run the included test suites to verify transcript extraction and pipeline functionality:

```bash
# Unit tests: transcript extraction & DB forking (AgyManager)
pytest tests/test_agy_manager.py -v

# Unit tests: backend selection & stream-json event parsing (offline, no subprocess)
pytest tests/test_pipeline_backends.py -v

# Integration test: multi-turn agy session (requires `agy` in PATH)
python tests/test_agy_pipeline.py

# Integration test: multi-turn claude session (requires `claude` in PATH)
python tests/test_claude_pipeline.py
```

---

