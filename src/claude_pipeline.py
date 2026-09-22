"""
ClaudePipeline: Robust headless backend pipeline for communicating with the Claude Code CLI (claude).
Supports multi-turn conversations, tool call tracking, question detection, and streaming event hooks.

Self-contained mirror of AgyPipeline (src/agy_pipeline.py) for the Claude Code backend: same
public interface (start/send/close, ToolExecution/AgentTurnResult result shapes) so AgyManager
can drive either CLI interchangeably, but built independently on top of Claude Code's own
bidirectional stream-json protocol rather than sharing implementation with AgyPipeline.
"""

import os
import re
import sys
import time
import json
import logging
import threading
import subprocess
import argparse
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Callable

logger = logging.getLogger("ClaudePipeline")

# Claude Code surfaces "ask the user" turns as a built-in tool call rather than
# a dedicated stream event, unlike agy's discrete `ask_question` step type.
ASK_USER_TOOL_NAMES = {"AskUserQuestion"}


@dataclass
class ToolExecution:
    name: str
    parameters: Dict[str, Any]
    state: str  # 'CALL_STARTED', 'CALL_DONE', 'ERROR'
    error: Optional[str] = None
    step_index: Optional[int] = None


@dataclass
class AgentTurnResult:
    conversation_id: str
    response: str
    status: str  # 'SUCCESS', 'ERROR', 'TIMEOUT'
    duration_seconds: float = 0.0
    tool_calls: List[ToolExecution] = field(default_factory=list)
    questions: List[Dict[str, Any]] = field(default_factory=list)
    denied_actions: List[Dict[str, Any]] = field(default_factory=list)
    usage: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    raw_events: List[Dict[str, Any]] = field(default_factory=list)


class ClaudePipeline:
    """
    Headless communication pipeline for the Claude Code CLI (`claude`).
    Runs claude with `--print --input-format stream-json --output-format stream-json`.
    """

    def __init__(
        self,
        cwd: Optional[str] = None,
        skip_permissions: bool = True,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        conversation_id: Optional[str] = None,
        permission_mode: Optional[str] = None,
        fork_session: bool = False,
        extra_args: Optional[List[str]] = None,
    ):
        self.cwd = cwd or os.getcwd()
        self.skip_permissions = skip_permissions
        self.model = model
        self.effort = effort
        self.resume_conversation_id = conversation_id
        # When resuming (`conversation_id` set), fork_session asks Claude Code to mint a new
        # session id instead of continuing the original one (`--resume <id> --fork-session`).
        self.fork_session = fork_session
        self.extra_args = extra_args or []

        self.proc: Optional[subprocess.Popen] = None
        self.conversation_id: Optional[str] = conversation_id
        self.available_tools: List[str] = []
        # Set from the constructor as the requested mode, then overwritten with the CLI's
        # actual reported mode once the init event arrives (mirrors AgyPipeline.permission_mode).
        self.permission_mode: Optional[str] = permission_mode

        self._running = False
        self._reader_thread: Optional[threading.Thread] = None
        self._event_listeners: List[Callable[[Dict[str, Any]], None]] = []
        self._live_output_buffer: List[str] = []
        self._buffer_lock = threading.Lock()

        # Turn synchronization
        self._turn_lock = threading.Lock()
        self._turn_done_event = threading.Event()
        self._current_turn_result: Optional[AgentTurnResult] = None
        self._current_tools_by_id: Dict[str, ToolExecution] = {}

    def set_conversation_id(self, conversation_id: Optional[str]):
        """Configures or updates the session ID to resume before starting the pipeline."""
        self.resume_conversation_id = conversation_id
        self.conversation_id = conversation_id
        if self._running:
            logger.warning("Pipeline is already running. Changing conversation_id will take effect on next process restart.")

    def add_event_listener(self, listener: Callable[[Dict[str, Any]], None]):
        """Register a callback for all raw stream-json events."""
        self._event_listeners.append(listener)

    def get_live_output(self, clear: bool = False) -> str:
        """Returns accumulated raw reader stream output lines as a single string."""
        with self._buffer_lock:
            out_str = "\n".join(self._live_output_buffer)
            if clear:
                self._live_output_buffer.clear()
            return out_str

    def _build_command(self) -> List[str]:
        """Builds the `claude` launch argv from the pipeline's configured options."""
        cmd = [
            "claude",
            "--print",
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--verbose",
            "--disable-slash-commands"
        ]
        if self.skip_permissions:
            cmd.append("--dangerously-skip-permissions")
        elif self.permission_mode:
            cmd.extend(["--permission-mode", self.permission_mode])
        if self.model:
            cmd.extend(["--model", self.model])
        if self.effort:
            cmd.extend(["--effort", self.effort])
        if self.resume_conversation_id:
            cmd.extend(["--resume", self.resume_conversation_id])
            if self.fork_session:
                cmd.append("--fork-session")
        if self.extra_args:
            cmd.extend(self.extra_args)
        return cmd

    def start(self, timeout: float = 30.0):
        """Starts the claude background process and waits for the 'system'/'init' event."""
        if self._running:
            return

        cmd = self._build_command()
        logger.info(f"Starting claude pipeline: {' '.join(cmd)} (cwd={self.cwd})")

        self.proc = subprocess.Popen(
            cmd,
            cwd=self.cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        self._running = True

        init_ready = threading.Event()

        def _reader():
            while self._running and self.proc and self.proc.stdout:
                line = self.proc.stdout.readline()
                if not line:
                    break
                line_str = line.strip()
                if not line_str:
                    continue

                with self._buffer_lock:
                    self._live_output_buffer.append(line_str)
                    if len(self._live_output_buffer) > 500:
                        self._live_output_buffer.pop(0)

                try:
                    event_data = json.loads(line_str)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse json line from claude: {line_str}")
                    continue

                self._handle_event(event_data, init_ready)

            self._running = False
            # Unblock waiting turn if reader thread exits unexpectedly
            self._turn_done_event.set()

        self._reader_thread = threading.Thread(target=_reader, daemon=True)
        self._reader_thread.start()

        # Wait for init event
        if not init_ready.wait(timeout=timeout):
            self.close()
            raise TimeoutError(f"Timed out waiting for claude init event (conversation_id={self.resume_conversation_id}).")

    def _handle_event(self, event: Dict[str, Any], init_ready: threading.Event):
        event_type = event.get("type")

        # Notify external listeners
        for listener in self._event_listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"Error in event listener: {e}")

        if event_type == "system" and event.get("subtype") == "init":
            self.conversation_id = event.get("session_id") or self.resume_conversation_id
            self.available_tools = event.get("tools", [])
            self.permission_mode = event.get("permissionMode") or event.get("permission_mode") or self.permission_mode
            logger.info(f"Pipeline initialized! Session ID: {self.conversation_id}")
            init_ready.set()

        elif event_type == "assistant":
            if self._current_turn_result is not None:
                self._current_turn_result.raw_events.append(event)
                message = event.get("message") or {}
                for block in message.get("content") or []:
                    if not isinstance(block, dict) or block.get("type") != "tool_use":
                        continue

                    tool_id = block.get("id")
                    tool_name = block.get("name", "unknown")
                    tool_params = block.get("input", {}) or {}

                    tool_exec = ToolExecution(
                        name=tool_name,
                        parameters=tool_params,
                        state="CALL_STARTED",
                        step_index=len(self._current_turn_result.tool_calls)
                    )
                    if tool_id:
                        self._current_tools_by_id[tool_id] = tool_exec
                    self._current_turn_result.tool_calls.append(tool_exec)

                    # Check if tool is Claude's built-in ask-the-user tool
                    if tool_name in ASK_USER_TOOL_NAMES:
                        self._current_turn_result.questions.append(tool_params)

        elif event_type == "user":
            # Tool results are echoed back on the output stream as "user" events
            # carrying tool_result content blocks (Anthropic Messages API shape).
            if self._current_turn_result is not None:
                self._current_turn_result.raw_events.append(event)
                message = event.get("message") or {}
                content = message.get("content")
                blocks = content if isinstance(content, list) else []
                for block in blocks:
                    if not isinstance(block, dict) or block.get("type") != "tool_result":
                        continue
                    tool_id = block.get("tool_use_id")
                    tool_exec = self._current_tools_by_id.get(tool_id)
                    if tool_exec is None:
                        continue
                    is_error = bool(block.get("is_error"))
                    tool_exec.state = "ERROR" if is_error else "CALL_DONE"
                    if is_error:
                        result_content = block.get("content")
                        tool_exec.error = result_content if isinstance(result_content, str) else json.dumps(result_content)

        elif event_type == "result":
            if self._current_turn_result is not None:
                self._current_turn_result.raw_events.append(event)
                is_error = bool(event.get("is_error"))
                self._current_turn_result.response = event.get("result") or ""
                self._current_turn_result.status = "ERROR" if is_error else "SUCCESS"
                self._current_turn_result.duration_seconds = (event.get("duration_ms") or 0) / 1000.0
                usage = dict(event.get("usage") or {})
                if event.get("total_cost_usd") is not None:
                    usage["total_cost_usd"] = event.get("total_cost_usd")
                self._current_turn_result.usage = usage
                if is_error:
                    self._current_turn_result.error = event.get("result") or event.get("subtype") or "Unknown error"

            self._turn_done_event.set()

        elif event_type == "system" and event.get("subtype") == "permission_denied":
            if self._current_turn_result is not None:
                self._current_turn_result.denied_actions.append(event)

    @staticmethod
    def project_dir_for_cwd(cwd: str) -> str:
        """
        Reproduces Claude Code's undocumented mapping from an absolute working
        directory to its `~/.claude/projects/<dir>` transcript directory name:
        every non-alphanumeric character in the path is replaced with '-'.
        Best-effort only; Claude Code treats this layout as an internal
        implementation detail that may change between releases.
        """
        abspath = os.path.abspath(cwd)
        return re.sub(r"[^a-zA-Z0-9]", "-", abspath)

    def _extract_recent_questions(self) -> List[Dict[str, Any]]:
        """Extracts any AskUserQuestion tool calls from the recent turn in the session transcript."""
        if not self.conversation_id:
            return []

        config_dir = os.environ.get("CLAUDE_CONFIG_DIR") or "~/.claude"
        project_dir = self.project_dir_for_cwd(self.cwd)
        transcript_path = os.path.expanduser(os.path.join(config_dir, "projects", project_dir, f"{self.conversation_id}.jsonl"))

        if not os.path.exists(transcript_path):
            return []

        questions = []
        try:
            with open(transcript_path, "r", encoding="utf-8") as f:
                recent_lines = deque(f, maxlen=50)
                for line in recent_lines:
                    try:
                        entry = json.loads(line)
                    except Exception:
                        continue
                    message = entry.get("message")
                    if not isinstance(message, dict):
                        continue
                    content = message.get("content")
                    if not isinstance(content, list):
                        continue
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "tool_use" and block.get("name") in ASK_USER_TOOL_NAMES:
                            questions.append({
                                "step_index": None,
                                "question_data": block.get("input"),
                                "summary": None,
                                "action": None
                            })
        except Exception as e:
            logger.debug(f"Could not read recent transcript for questions: {e}")

        return questions

    def send(self, prompt: str, timeout: float = 120.0) -> AgentTurnResult:
        """
        Sends a user prompt to the agent and waits for the turn to complete.
        Maintains conversational context across calls.
        """
        # Auto-recover if claude process was killed or closed
        if not self._running or self.proc is None or self.proc.poll() is not None:
            logger.warning("Pipeline process not running. Auto-restarting session...")
            if self.conversation_id:
                self.resume_conversation_id = self.conversation_id
            self.start()

        with self._turn_lock:
            self._turn_done_event.clear()
            self._current_tools_by_id = {}
            self._current_turn_result = AgentTurnResult(
                conversation_id=self.conversation_id or "",
                response="",
                status="UNKNOWN"
            )

            payload = {
                "type": "user",
                "message": {
                    "role": "user",
                    "content": prompt
                }
            }

            logger.debug(f"Sending prompt: {prompt[:100]}...")
            msg_str = json.dumps(payload) + "\n"

            try:
                if self.proc and self.proc.stdin:
                    self.proc.stdin.write(msg_str)
                    self.proc.stdin.flush()
                else:
                    raise IOError("Process stdin unavailable.")
            except (BrokenPipeError, IOError, AttributeError) as e:
                logger.warning(f"Failed writing to process stdin ({e}). Restarting pipeline...")
                self.close()
                self.start()
                if self.proc and self.proc.stdin:
                    self.proc.stdin.write(msg_str)
                    self.proc.stdin.flush()

            finished = self._turn_done_event.wait(timeout=timeout)
            if not finished:
                self._current_turn_result.status = "TIMEOUT"
                self._current_turn_result.error = f"Agent turn timed out after {timeout} seconds."
                logger.error(self._current_turn_result.error)
                # Restart process on timeout to avoid out-of-order event corruption
                self.close()

            # Extract any questions called during this turn if not already captured
            if not self._current_turn_result.questions:
                recent_questions = self._extract_recent_questions()
                if recent_questions:
                    self._current_turn_result.questions = recent_questions

            return self._current_turn_result

    def close(self):
        """Terminates the claude pipeline process cleanly."""
        self._running = False
        self._turn_done_event.set()
        if self.proc:
            try:
                if self.proc.stdin:
                    self.proc.stdin.close()
            except Exception:
                pass
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None
        logger.info("Pipeline closed.")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    """Interactive CLI test for ClaudePipeline."""
    parser = argparse.ArgumentParser(description="Run ClaudePipeline interactive session.")
    parser.add_argument(
        "-c", "--conversation",
        dest="conversation_id",
        type=str,
        default=None,
        help="Session ID to resume (e.g., --conversation 39231db7-012f-4be3-b3c8-938fd86e6c18)"
    )
    parser.add_argument(
        "-m", "--model",
        type=str,
        default=None,
        help="Model name/alias to use (e.g., sonnet)"
    )
    parser.add_argument(
        "-e", "--effort",
        type=str,
        default=None,
        help="Reasoning effort level"
    )
    parser.add_argument(
        "--cwd",
        type=str,
        default=None,
        help="Working directory for the pipeline"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    print("==================================================")
    print("1) Starting ClaudePipeline...")
    if args.conversation_id:
        print(f"Resuming Session ID: {args.conversation_id}")
    print("==================================================")

    pipeline = ClaudePipeline(
        cwd=args.cwd,
        skip_permissions=True,
        model=args.model,
        effort=args.effort,
        conversation_id=args.conversation_id,
    )
    pipeline.start()
    print(f"Session ID: {pipeline.conversation_id}")
    print(f"Tools detected: {len(pipeline.available_tools)}")
    print("==================================================")

    try:
        for i in range(1, 6):
            try:
                user_prompt = input(f"\n[Turn {i}/5] Enter prompt: ").strip()
            except EOFError:
                print("\nReceived EOF. Exiting loop early.")
                break

            if not user_prompt:
                print("Empty input, skipping turn.")
                continue

            if user_prompt.lower() in ("/exit", "/quit", "exit", "quit"):
                print("Exit requested. Ending session.")
                break

            start_time = time.time()
            result = pipeline.send(user_prompt)
            elapsed = time.time() - start_time

            print(f"\n[Turn {i}/5 Result]")
            print(f"Time Taken : {elapsed:.2f}s")
            print(f"Status     : {result.status}")
            if result.error:
                print(f"Error      : {result.error}")
            if result.tool_calls:
                print(f"Tools Used : {[t.name for t in result.tool_calls]}")
            if result.questions:
                print(f"\n[Question(s) from Agent]:")
                for q in result.questions:
                    q_data = q.get("question_data") or q.get("questions") or q
                    if isinstance(q_data, list):
                        for item in q_data:
                            print(f"  ? {item.get('question') if isinstance(item, dict) else item}")
                            if isinstance(item, dict):
                                for opt in item.get('options', []):
                                    print(f"    - {opt}")
                    elif isinstance(q_data, dict):
                        print(f"  ? {q_data.get('question')}")
                        for opt in q_data.get('options', []):
                            print(f"    - {opt}")
                    else:
                        print(f"  ? {q_data}")

            print(f"Agent Output:\n{result.response.strip()}")
            print("-" * 50)

        print("\n< loop end > - Completed 5 turns.")

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        print("\nClosing pipeline...")
        pipeline.close()
        print("Pipeline closed cleanly.")


if __name__ == "__main__":
    main()
