# FastMCP Server & Tool Integration Specification

This document details the FastMCP server architecture, tool contracts, parameter schemas, and client metadata handling for **AgentLandline**.

---

## 1. FastMCP Architecture Overview

`src/server.py` hosts the MCP server entrypoint using [FastMCP](https://github.com/jlowin/fastmcp). The server communicates over standard input/output (stdio JSON-RPC) with MCP host applications (Antigravity IDE, `agy` CLI, Claude Desktop, Cursor, or custom runners).

```
┌────────────────────────────────────────────────────────┐
│             MCP Host (Client Application)              │
│       (Antigravity IDE / Claude Code / Custom Agent)   │
└───────────────────────────┬────────────────────────────┘
                            │ stdio (JSON-RPC)
                            ▼
┌────────────────────────────────────────────────────────┐
│             AgentLandline (FastMCP Server)             │
│                     src/server.py                      │
│                                                        │
│  - initialize_agent  -> Launches/resumes agy session   │
│  - send_prompt       -> Bidirectional turn dispatch    │
│  - agent_status      -> Health, live buffer, transcript│
│  - exit_agent        -> Clean process termination      │
│  - report_issue      -> Automated GitHub issue logging │
└─────────────┬────────────────────────────┬─────────────┘
              │                            │
              ▼                            ▼
┌───────────────────────────┐ ┌──────────────────────────┐
│        AgyManager         │ │      IssueReporter       │
│     src/agy_manager.py    │ │   src/issue_reporter.py  │
│  (Session & DB Forking)   │ │  (Smart Label Matching)  │
└─────────────┬─────────────┘ └──────────────────────────┘
              │
              ▼
┌───────────────────────────┐
│        AgyPipeline        │
│     src/agy_pipeline.py   │
│ (Subprocess stream-json)  │
└───────────────────────────┘
```

---

## 2. Tool Reference & Method Contracts

### `initialize_agent`
Resumes or starts an `agy` agent process backend for a specified conversation session.
```python
@mcp.tool()
def initialize_agent(
    conversation_id: str,
    cwd: str = "",
    fork: bool = False,
    new_conversation_id: str = ""
) -> str:
```
- **Arguments**:
  - `conversation_id`: The session ID of the target agent to resume or fork.
  - `cwd`: Optional working directory where the agent process will execute.
  - `fork`: When `True`, clones the existing SQLite conversation database and brain artifacts to create a branched session.
  - `new_conversation_id`: Optional custom identifier for the forked session (auto-generated if omitted).
- **Return Value**: JSON string containing:
  ```json
  {
    "status": "initialized",
    "conversation_id": "c76d1778-...",
    "pid": 48210,
    "forked_from": null,
    "available_tools": ["view_file", "run_command", "write_to_file", ...]
  }
  ```

---

### `send_prompt`
Sends a prompt message to the active `agy` session and awaits turn completion.
```python
@mcp.tool()
def send_prompt(
    prompt: str,
    timeout: float = 120.0
) -> str:
```
- **Arguments**:
  - `prompt`: The user prompt text sent to the agent.
  - `timeout`: Maximum wait time in seconds (default: `120.0`).
- **Return Value**: JSON string containing:
  ```json
  {
    "status": "COMPLETED",
    "response": "Agent response text...",
    "tool_calls": [
      {
        "name": "view_file",
        "state": "CALL_DONE",
        "parameters": {"AbsolutePath": "..."},
        "error": null
      }
    ],
    "tokens": {
      "input_tokens": 1250,
      "output_tokens": 420
    }
  }
  ```

---

### `agent_status`
Queries the live state, buffer, or transcript of the active session.
```python
@mcp.tool()
def agent_status(mode: str = "process") -> str:
```
- **Arguments**:
  - `mode`: Query mode:
    - `"process"`: Process health, PID, active conversation ID, and available tools.
    - `"output"`: Live stream reader output accumulated in memory.
    - `"transcript"`: Reads the latest step from `transcript.jsonl` and returns the last prompt & response.
- **Return Value**: JSON string containing the requested status attributes.

---

### `exit_agent`
Gracefully terminates the running `agy` subprocess and cleans up all stream pipes.
```python
@mcp.tool()
def exit_agent() -> str:
```
- **Return Value**: JSON string indicating clean shutdown:
  ```json
  {
    "status": "closed",
    "conversation_id": "c76d1778-..."
  }
  ```

---

### `report_issue`
Creates a GitHub issue directly via the `gh` CLI with smart label matching.
```python
@mcp.tool()
def report_issue(
    title: str,
    body: str = "",
    label: str = "bug",
    agent_model: str = "",
    session_id: str = "unknown",
    ctx: Context = None
) -> str:
```
- **Arguments**:
  - `title`: Concise issue title.
  - `body`: Markdown-formatted issue body.
  - `label`: Target label (e.g. `'bug'`, `'enhancement'`). Automatically fuzzy-matched and created with a random hex color if missing.
  - `agent_model`: Model name (e.g. `'gemini-3.8-flash'`, `'claude-3.5-sonnet'`).
  - `session_id`: Caller session ID (defaults to `'unknown'`).
  - `ctx`: FastMCP context injected by the runtime.
- **Return Value**: Formatted confirmation string with the created GitHub issue URL.

---

## 3. Client Agent Detection

`src/server.py` implements `get_client_agent_name(ctx: Context)`:
```python
def get_client_agent_name(ctx: Context = None) -> str:
    """Extracts client agent name from MCP initialization clientInfo metadata."""
    try:
        if ctx and ctx.request_context and ctx.request_context.session:
            client_params = getattr(ctx.request_context.session, "_client_params", None)
            if client_params and hasattr(client_params, "clientInfo") and client_params.clientInfo:
                name = getattr(client_params.clientInfo, "name", None)
                if name and str(name).strip():
                    return str(name).strip()
    except Exception as e:
        logger.debug(f"Could not extract clientInfo from MCP context: {e}")
    return "unknown"
```
This enables `report_issue` to automatically credit the calling agent environment (e.g., `Antigravity`, `claude-code`) without requiring manual input.

---

## 4. Architectural Resilience

- **JSON Serialization**: All tools serialize outputs via `json.dumps(..., indent=2)` to ensure strict machine-readability across agent calls.
- **Subprocess Isolation**: Subprocess crashes, EOFs, or pipe errors in `AgyPipeline` are caught, logged, and surfaced gracefully in tool responses rather than crashing the FastMCP host process.
