"""
AgyPipeline: Robust headless backend pipeline for communicating with the Antigravity CLI (agy).
Supports multi-turn conversations, tool call tracking, question detection, and streaming event hooks.
"""

import os
import sys
import json
import time
import queue
import logging
import threading
import subprocess
import argparse
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Callable

logger = logging.getLogger("AgyPipeline")


@dataclass
class ToolExecution:
    name: str
    parameters: Dict[str, Any]
    state: str  # 'ACTIVE', 'DONE', 'ERROR'
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
    usage: Dict[str, int] = field(default_factory=dict)
    error: Optional[str] = None
    raw_events: List[Dict[str, Any]] = field(default_factory=list)


class AgyPipeline:
    """
    Headless communication pipeline for the Antigravity (agy) CLI.
    Runs agy with `--input-format stream-json --output-format stream-json`.
    """

    def __init__(
        self,
        cwd: Optional[str] = None,
        skip_permissions: bool = True,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        conversation_id: Optional[str] = None,
        extra_args: Optional[List[str]] = None,
    ):
        self.cwd = cwd or os.getcwd()
        self.skip_permissions = skip_permissions
        self.model = model
        self.effort = effort
        self.resume_conversation_id = conversation_id
        self.extra_args = extra_args or []

        self.proc: Optional[subprocess.Popen] = None
        self.conversation_id: Optional[str] = conversation_id
        self.available_tools: List[str] = []
        self.permission_mode: Optional[str] = None

        self._running = False
        self._reader_thread: Optional[threading.Thread] = None
        self._event_listeners: List[Callable[[Dict[str, Any]], None]] = []
        self._live_output_buffer: List[str] = []
        self._buffer_lock = threading.Lock()

        # Turn synchronization
        self._turn_lock = threading.Lock()
        self._turn_done_event = threading.Event()
        self._current_turn_result: Optional[AgentTurnResult] = None
        self._current_tools_by_step: Dict[int, ToolExecution] = {}

    def set_conversation_id(self, conversation_id: Optional[str]):
        """Configures or updates the conversation ID to resume before starting the pipeline."""
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

    def start(self, timeout: float = 30.0):
        """Starts the agy background process and waits for the 'init' event."""
        if self._running:
            return

        cmd = [
            "agy",
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--disable-slash-commands"
        ]
        if self.skip_permissions:
            cmd.append("--dangerously-skip-permissions")
        if self.model:
            cmd.extend(["--model", self.model])
        if self.effort:
            cmd.extend(["--effort", self.effort])
        if self.resume_conversation_id:
            cmd.extend(["--conversation", self.resume_conversation_id])
        if self.extra_args:
            cmd.extend(self.extra_args)

        logger.info(f"Starting agy pipeline: {' '.join(cmd)} (cwd={self.cwd})")

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
                    logger.warning(f"Failed to parse json line from agy: {line_str}")
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
            raise TimeoutError(f"Timed out waiting for agy init event (conversation_id={self.resume_conversation_id}).")

    def _handle_event(self, event: Dict[str, Any], init_ready: threading.Event):
        event_type = event.get("event")

        # Notify external listeners
        for listener in self._event_listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"Error in event listener: {e}")

        if event_type == "init":
            self.conversation_id = event.get("conversation_id") or self.resume_conversation_id
            init_data = event.get("init", {})
            self.available_tools = init_data.get("tools", [])
            self.permission_mode = init_data.get("permission_mode")
            logger.info(f"Pipeline initialized! Conversation ID: {self.conversation_id}")
            init_ready.set()

        elif event_type == "step_update":
            step = event.get("step_update", {})
            step_type = step.get("step_type")
            step_idx = step.get("step_index")
            state = step.get("state")

            if self._current_turn_result is not None:
                self._current_turn_result.raw_events.append(event)

                # Tool calls
                if step_type == "tool":
                    tool_info = step.get("tool_info", {})
                    tool_name = step.get("tool_name") or tool_info.get("name", "unknown")
                    tool_params = tool_info.get("parameters", {})
                    err_obj = tool_info.get("error")
                    err_msg = err_obj.get("message") if isinstance(err_obj, dict) else (str(err_obj) if err_obj else None)

                    if step_idx in self._current_tools_by_step:
                        existing = self._current_tools_by_step[step_idx]
                        existing.state = state
                        if err_msg:
                            existing.error = err_msg
                    else:
                        tool_exec = ToolExecution(
                            name=tool_name,
                            parameters=tool_params,
                            state=state,
                            error=err_msg,
                            step_index=step_idx
                        )
                        self._current_tools_by_step[step_idx] = tool_exec
                        self._current_turn_result.tool_calls.append(tool_exec)

                        # Check if tool is ask_question
                        if tool_name == "ask_question":
                            self._current_turn_result.questions.append(tool_params)

        elif event_type == "result":
            result_data = event.get("result", {})
            if self._current_turn_result is not None:
                self._current_turn_result.raw_events.append(event)
                self._current_turn_result.response = result_data.get("response", "")
                self._current_turn_result.status = result_data.get("status", "SUCCESS")
                self._current_turn_result.duration_seconds = result_data.get("duration_seconds", 0.0)
                self._current_turn_result.usage = result_data.get("usage", {})
                self._current_turn_result.denied_actions = result_data.get("denied_actions", [])
                self._current_turn_result.error = result_data.get("error")

            self._turn_done_event.set()

    def _extract_recent_questions(self) -> List[Dict[str, Any]]:
        """Extracts any ask_question tool calls from the recent turn in the conversation transcript."""
        if not self.conversation_id:
            return []

        candidates = [
            os.path.expanduser(f"~/.gemini/antigravity-cli/brain/{self.conversation_id}/.system_generated/logs/transcript.jsonl"),
            os.path.expanduser(f"~/.gemini/antigravity-ide/brain/{self.conversation_id}/.system_generated/logs/transcript.jsonl"),
        ]
        app_data = os.environ.get("ANTIGRAVITY_APP_DATA_DIR")
        if app_data:
            candidates.insert(0, os.path.join(app_data, "brain", self.conversation_id, ".system_generated", "logs", "transcript.jsonl"))

        transcript_path = None
        for path in candidates:
            if os.path.exists(path):
                transcript_path = path
                break

        if not transcript_path:
            return []

        questions = []
        try:
            from collections import deque
            with open(transcript_path, "r", encoding="utf-8") as f:
                recent_lines = deque(f, maxlen=30)
                for line in recent_lines:
                    try:
                        step_obj = json.loads(line)
                        for tc in step_obj.get("tool_calls", []):
                            if tc.get("name") == "ask_question":
                                args = tc.get("args", {})
                                q_data = args.get("questions")
                                if isinstance(q_data, str):
                                    try:
                                        q_data = json.loads(q_data)
                                    except Exception:
                                        pass
                                questions.append({
                                    "step_index": step_obj.get("step_index"),
                                    "question_data": q_data,
                                    "summary": args.get("toolSummary"),
                                    "action": args.get("toolAction")
                                })
                    except Exception:
                        continue
        except Exception as e:
            logger.debug(f"Could not read recent transcript for questions: {e}")

        return questions

    def send(self, prompt: str, timeout: float = 120.0) -> AgentTurnResult:
        """
        Sends a user prompt to the agent and waits for the turn to complete.
        Maintains conversational context across calls.
        """
        # Auto-recover if agy process was killed or closed
        if not self._running or self.proc is None or self.proc.poll() is not None:
            logger.warning("Pipeline process not running. Auto-restarting session...")
            if self.conversation_id:
                self.resume_conversation_id = self.conversation_id
            self.start()

        with self._turn_lock:
            self._turn_done_event.clear()
            self._current_tools_by_step = {}
            self._current_turn_result = AgentTurnResult(
                conversation_id=self.conversation_id or "",
                response="",
                status="UNKNOWN"
            )

            payload = {
                "event": "user",
                "message": {
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
        """Terminates the agy pipeline process cleanly."""
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
    """Interactive CLI test for AgyPipeline."""
    parser = argparse.ArgumentParser(description="Run AgyPipeline interactive session.")
    parser.add_argument(
        "-c", "--conversation",
        dest="conversation_id",
        type=str,
        default=None,
        help="Conversation ID to resume (e.g., --conversation 39231db7-012f-4be3-b3c8-938fd86e6c18)"
    )
    parser.add_argument(
        "-m", "--model",
        type=str,
        default=None,
        help="Model name to use (e.g., gemini-3.6-flash)"
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
    print("1) Starting AgyPipeline...")
    if args.conversation_id:
        print(f"Resuming Conversation ID: {args.conversation_id}")
    print("==================================================")

    pipeline = AgyPipeline(
        cwd=args.cwd,
        skip_permissions=True,
        model=args.model,
        effort=args.effort,
        conversation_id=args.conversation_id,
    )
    pipeline.start()
    print(f"Conversation ID: {pipeline.conversation_id}")
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
