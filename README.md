# AgentLandline 📞

**AgentLandline** is an Model Context Protocol (MCP) server and Python backend engine that provides a direct, programmatic "landline" connection to running **Antigravity CLI (`agy`)** agent sessions.

It allows external AI agents (such as Antigravity IDE, Claude Code, Cursor, or custom tools) to headlessly initialize, send prompts to, monitor, and control `agy` agent sessions with full multi-turn conversational context, tool execution tracking, and automated GitHub issue reporting.

---

## ✨ Features

- **🔌 MCP Server Integration**: Exposes clean, standardized MCP tools using [FastMCP](https://github.com/jlowin/fastmcp) for easy integration into any MCP-compatible environment.
- **🔄 Multi-Turn Session Orchestration**: Seamlessly resumes and maintains `agy` conversation sessions using background streaming JSON subprocess communication.
- **📊 Real-time Status & Transcript Monitoring**: Inspect live process health, streaming output buffers, and line-by-line step transcripts (`transcript.jsonl`).
- **🐛 Automated GitHub Issue Reporting**: Instantly report bugs, tracebacks, or feature requests directly to GitHub via the `gh` CLI with automatic client-agent detection, smart label normalization (`new_lable` <-> `new lable`, typo matching), and auto-creation of missing labels with random colors.
- **🛠 Robust Process Recovery**: Automatic re-initialization and stream re-connection upon process interrupts or stdin pipe errors.

---

## 🏗 Architecture

```
AgentLandline/
├── src/
│   ├── server.py         # MCP Server Entrypoint (FastMCP tool definitions)
│   ├── agy_manager.py     # High-level Orchestration Manager & Transcript Reader
│   ├── agy_pipeline.py    # Headless Subprocess Engine for agy CLI (stream-json)
│   └── issue_reporter.py # GitHub Issue Reporter & gh CLI wrapper
├── tests/
│   ├── test_agy_manager.py    # Unit test for transcript parser
│   ├── test_agy_pipeline.py   # Integration test for multi-turn agy sessions
│   └── test_issue_reporter.py # Unit tests for smart label matching and auto-creation
└── requirements.txt      # Dependencies
```

### Core Components

1. **`src/server.py`**: The FastMCP server hosting tools for client agents. Automatically extracts client agent metadata (`clientInfo`) on initialization.
2. **`src/agy_manager.py`**: High-level manager (`AgyManager`) that maintains active pipeline instances and provides transcript search across standard CLI/IDE brain directories.
3. **`src/agy_pipeline.py`**: Non-blocking subprocess bridge (`AgyPipeline`) that launches `agy` with `--input-format stream-json --output-format stream-json`, parsing tool calls (`ToolExecution`), token usage, questions (`ask_question`), and status in real time.
4. **`src/issue_reporter.py`**: Utility (`IssueReporter`) formatting GitHub issues with session metadata, smart label normalization/matching (typography tolerance), and auto-creating missing labels with random colors.

---

## 🚀 Quick Start

### 1. Prerequisites

- **Python 3.10+**
- **Antigravity CLI (`agy`)**: Ensure `agy` is installed and available in your `PATH`.
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
| `initialize_agent` | `conversation_id` *(str)*<br>`cwd` *(str, optional)* | Resumes and initializes an `agy` agent process backend for a specific session ID. |
| `send_prompt` | `prompt` *(str)*<br>`timeout` *(float, default: 120.0)* | Sends a prompt to the active `agy` session and returns the agent's turn response and tool metrics. |
| `agent_status` | `mode` *(str, default: 'process')* | Queries status. Modes: `process` (PID & health), `output` (live output buffer), `transcript` (last turn transcript). |
| `exit_agent` | *None* | Gracefully terminates and cleans up the current `agy` agent process. |
| `report_issue` | `title` *(str)*<br>`body` *(str)*<br>`label` *(str)*<br>`agent_model` *(str)*<br>`session_id` *(str)* | Creates a GitHub issue with formatted metadata, smart typography/typo label matching (`new_lable` <-> `new lable`), and auto-creation of missing labels with random colors. |

---

## 🐍 Python Library Usage

You can also use `AgyPipeline` or `AgyManager` directly in Python scripts:

### Using `AgyPipeline`

```python
from agy_pipeline import AgyPipeline

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
# Test transcript extraction with AgyManager
python tests/test_agy_manager.py

# Test multi-turn agy session with AgyPipeline
python tests/test_agy_pipeline.py
```

---

