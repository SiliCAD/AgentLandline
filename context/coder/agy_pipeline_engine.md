# Agy Subprocess Engine & Bidirectional Streaming Protocol

This document provides a technical deep-dive into `src/agy_pipeline.py`, the core headless engine that bridges AgentLandline to running Antigravity CLI (`agy`) agent sessions.

---

## 1. Engine Overview

`AgyPipeline` manages a long-running subprocess invocation of the `agy` CLI using bidirectional JSON streaming (`--input-format stream-json --output-format stream-json`).

```
┌────────────────────────────────────────────────────────┐
│                   AgentLandline Engine                 │
│                   (src/agy_pipeline.py)                │
└──────────────┬──────────────────────────▲──────────────┘
               │                          │
   stdin (JSON │                          │ stdout (JSON events)
   stream)     │                          │ via background reader
               ▼                          │
┌────────────────────────────────────────────────────────┐
│                   agy CLI Subprocess                   │
│   agy --conversation <id> --input-format stream-json   │
│                 --output-format stream-json            │
└────────────────────────────────────────────────────────┘
```

---

## 2. Command Launch Specification

`AgyPipeline` launches `agy` via `subprocess.Popen` with unbuffered line-based pipes:

```python
cmd = [
    "agy",
    "--conversation", self.conversation_id,
    "--input-format", "stream-json",
    "--output-format", "stream-json"
]
if self.skip_permissions:
    cmd.append("--skip-permissions")

self._process = subprocess.Popen(
    cmd,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    cwd=self.cwd,
    text=True,
    bufsize=1
)
```

---

## 3. Bidirectional Streaming Protocol (`stream-json`)

### Input Stream (Python -> `agy` stdin)
User prompts are submitted as single-line JSON objects terminated by a newline (`\n`):

```json
{"type": "user_input", "content": "Analyze the schematic for DRC issues"}
```

### Output Stream (`agy` stdout -> Python)
The `agy` CLI emits structured JSON lines corresponding to internal agent steps:

1. **Session Handshake / Tool Registration**:
   On launch, `agy` emits available tool schemas and session parameters:
   ```json
   {
     "type": "init",
     "conversation_id": "...",
     "tools": [
       {"name": "view_file", "description": "..."},
       {"name": "run_command", "description": "..."}
     ]
   }
   ```
2. **Step / Turn Execution**:
   During turn processing, `agy` streams incremental planner responses, tool calls, and tool execution status:
   ```json
   {
     "type": "step",
     "status": "IN_PROGRESS",
     "tool_calls": [
       {
         "name": "run_command",
         "parameters": {"CommandLine": "ls -la"}
       }
     ]
   }
   ```
3. **Turn Completion**:
   Emitted when the turn finishes:
   ```json
   {
     "type": "turn_complete",
     "status": "COMPLETED",
     "response": "Final model text response",
     "usage": {
       "input_tokens": 1400,
       "output_tokens": 350
     }
   }
   ```

---

## 4. Concurrency & Reader Thread Architecture

To prevent pipe buffer deadlocks while `agy` executes complex tool loops:

- **Dedicated Reader Thread**: A background daemon thread continuously reads lines from `self._process.stdout`.
- **Event Synchronization**: Turn boundaries are synchronized using `threading.Event()` with a configurable timeout.
- **Pipe Flushing**: Lines are parsed into `ToolExecution` and `AgentTurnResult` data classes immediately upon receipt.

```python
def _reader():
    for line in iter(self._process.stdout.readline, ""):
        line_str = line.strip()
        if not line_str:
            continue
        try:
            data = json.loads(line_str)
            self._handle_stream_event(data)
        except json.JSONDecodeError:
            self._raw_output_buffer.append(line_str)
```

---

## 5. Subprocess Lifecycle & Process Safety

Orphaned `agy` processes can hold locks on database files or consume significant system memory. To guarantee clean process termination:

1. **Context Manager / Explicit Close**:
   Always close the pipeline when done:
   ```python
   pipeline.close()
   ```
2. **Termination Sequence**:
   - Close `stdin` to signal EOF to `agy`.
   - Send `SIGTERM` if the process does not terminate within a grace period.
   - Fall back to `SIGKILL` if the process is unresponsive.
3. **Database & Brain Integrity**:
   Because `agy` persists state into SQLite databases at `~/.gemini/antigravity-cli/conversations/<id>.db` and artifacts at `~/.gemini/antigravity-cli/brain/<id>/`, sessions can be safely resumed across process restarts.
