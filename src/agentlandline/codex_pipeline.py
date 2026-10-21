"""Codex CLI bridge: one JSONL `exec` process per turn, resumed by thread id."""

import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .claude_pipeline import AgentTurnResult, ToolExecution

logger = logging.getLogger("CodexPipeline")


class CodexPipeline:
    def __init__(self, cwd: Optional[str] = None, skip_permissions: bool = True,
                 model: Optional[str] = None, effort: Optional[str] = None,
                 conversation_id: Optional[str] = None, fork_session: bool = False,
                 extra_args: Optional[List[str]] = None, extra_env: Optional[Dict[str, str]] = None):
        self.cwd = cwd or os.getcwd()
        self.skip_permissions, self.model, self.effort = skip_permissions, model, effort
        self.conversation_id = conversation_id
        self.fork_session = fork_session
        self.extra_args, self.extra_env = list(extra_args or []), dict(extra_env or {})
        self.available_tools: List[str] = []
        self.permission_mode = "bypass" if skip_permissions else "read-only"
        self.proc: Optional[subprocess.Popen] = None
        self._running = False
        self._turn_lock, self._process_lock = threading.Lock(), threading.Lock()
        self._in_turn = self._interrupted = False
        self._event_listeners: List[Callable] = []
        self._live_output_buffer = deque(maxlen=1000)

    def set_conversation_id(self, conversation_id: Optional[str]):
        self.conversation_id = conversation_id

    def add_event_listener(self, listener: Callable):
        self._event_listeners.append(listener)

    def get_live_output(self, clear: bool = False) -> str:
        with self._process_lock:
            output = "\n".join(self._live_output_buffer)
            if clear:
                self._live_output_buffer.clear()
            return output

    def _build_env(self) -> Dict[str, str]:
        # Reuse the CLI's login, without inheriting API billing credentials or harness state.
        keys = {"PATH", "HOME", "USER", "LOGNAME", "SHELL", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE", "TERM", "CODEX_HOME"}
        return {**{k: v for k, v in os.environ.items() if k in keys}, **self.extra_env}

    def _build_command(self, images: Optional[List[str]] = None) -> List[str]:
        cmd = ["codex", "exec"]
        if self.conversation_id:
            cmd += ["fork" if self.fork_session else "resume", self.conversation_id]
        cmd += ["--json", "--skip-git-repo-check"]
        if self.skip_permissions:
            cmd += ["--dangerously-bypass-approvals-and-sandbox"]
        else:
            cmd += ["-c", 'sandbox_mode="read-only"', "-c", 'approval_policy="never"']
        if self.model:
            cmd += ["--model", self.model]
        if self.effort:
            cmd += ["-c", "model_reasoning_effort=" + json.dumps(self.effort)]
        if self.extra_env.get("LANDLINE_AGENT"):
            tools = ["message_agent", "list_agents"]
            if self.extra_env.get("LANDLINE_MANAGES"):
                tools += ["manage_context"]
            if self.extra_env.get("LANDLINE_SPAWNS"):
                tools += ["start_agent", "stop_agent", "find_sessions"]
            settings = {
                "command": json.dumps(sys.executable),
                "args": json.dumps(["-m", "agentlandline.server"]),
                "env": "{" + ", ".join(json.dumps(k) + " = " + json.dumps(v) for k, v in self.extra_env.items()) + "}",
                "required": "true",
                "enabled": "true",
                "enabled_tools": json.dumps(tools),
                "default_tools_approval_mode": '"approve"',
                "tool_timeout_sec": "86400",
            }
            for key, value in settings.items():
                cmd += ["-c", f"mcp_servers.agent-landline.{key}={value}"]
        for path in images or []:
            cmd += ["--image", path]
        return cmd + self.extra_args + ["-"]

    def start(self, timeout: float = 30.0):
        # exec has no idle process or initialization handshake; defer inference until send().
        if not shutil.which("codex", path=self._build_env().get("PATH")):
            raise FileNotFoundError("Codex CLI not found. Install it and run `codex login` first.")
        self._running = True

    @staticmethod
    def transcript_path_for(conversation_id: Optional[str]) -> Optional[str]:
        if not conversation_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in conversation_id):
            return None
        root = Path(os.path.expanduser(os.environ.get("CODEX_HOME") or "~/.codex"))
        for folder in ("sessions", "archived_sessions"):
            match = next((root / folder).glob(f"**/rollout-*-{conversation_id}.jsonl"), None)
            if match:
                return str(match)
        return None

    def transcript_path(self) -> Optional[str]:
        return self.transcript_path_for(self.conversation_id)

    def _emit(self, event: Dict[str, Any]):
        for listener in self._event_listeners:
            try:
                listener(event)
            except Exception:
                logger.exception("Codex event listener failed")

    @staticmethod
    def _stop_process(proc):
        if proc is None:
            return
        try:
            # Include commands and stdio MCP servers spawned by Codex.
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()

    def interrupt(self) -> bool:
        with self._process_lock:
            if not self._in_turn:
                return False
            self._interrupted = True
            proc = self.proc
        self._stop_process(proc)
        return True

    def _context_event(self):
        """Use the last model call, not exec's cumulative turn usage, for the context meter."""
        path = self.transcript_path()
        if not path:
            return
        try:
            with open(path, encoding="utf-8") as stream:
                lines = stream.readlines()
        except OSError:
            return
        info = model = None
        for line in reversed(lines):
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if not isinstance(obj, dict):
                continue
            payload = obj.get("payload") or {}
            if obj.get("type") == "turn_context" and not model:
                model = payload.get("model")
            if obj.get("type") == "event_msg" and payload.get("type") == "token_count" and info is None:
                info = payload.get("info")
            if info and model:
                break
        if info:
            self._emit({"type": "codex.context", "info": info, "model": model})

    def send(self, prompt: str, timeout: Optional[float] = 120.0,
             images: Optional[List[str]] = None) -> AgentTurnResult:
        with self._turn_lock:
            if not self._running:
                self.start()
            started = time.monotonic()
            result = AgentTurnResult(self.conversation_id or "", "", "ERROR")
            done = threading.Event()
            tools = {}
            stderr = deque(maxlen=40)
            with self._process_lock:
                self._in_turn, self._interrupted = True, False
            proc = None
            readers = []
            terminal = False
            timed_out = False
            try:
                proc = subprocess.Popen(self._build_command(images), cwd=self.cwd, env=self._build_env(),
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        text=True, encoding="utf-8", errors="replace", start_new_session=True)
                with self._process_lock:
                    self.proc = proc
                    interrupted = self._interrupted
                if interrupted:
                    self._stop_process(proc)

                def read_stdout():
                    nonlocal terminal
                    try:
                        for line in proc.stdout:
                            with self._process_lock:
                                self._live_output_buffer.append(line.rstrip())
                            try:
                                event = json.loads(line)
                            except ValueError:
                                continue
                            if not isinstance(event, dict):
                                continue
                            kind = event.get("type")
                            if kind == "thread.started":
                                self.conversation_id = event["thread_id"]
                                self.fork_session = False
                            elif kind == "item.completed" and event.get("item", {}).get("type") == "agent_message":
                                result.response = event["item"].get("text", "")
                            elif kind == "turn.completed":
                                terminal = True
                                result.status, result.usage = "SUCCESS", event.get("usage") or {}
                                result.error = None
                            elif kind in ("turn.failed", "error"):
                                error = event.get("error") or event
                                result.error = error.get("message", str(error)) if isinstance(error, dict) else str(error)
                                if kind == "turn.failed":
                                    terminal = True
                                    result.status = "ERROR"
                            item = event.get("item") or {}
                            if item.get("type") in ("command_execution", "mcp_tool_call", "file_change", "web_search"):
                                tools[item.get("id")] = ToolExecution(
                                    name=item.get("tool") or item["type"], parameters=item,
                                    state=("ERROR" if item.get("error") or (item.get("result") or {}).get("isError")
                                           or item.get("exit_code") not in (None, 0)
                                           or item.get("status") in ("failed", "declined")
                                           else "CALL_DONE" if kind == "item.completed" else "CALL_STARTED"),
                                    error=str(item["error"]) if item.get("error") else None)
                            self._emit(event)
                    except Exception as exc:
                        result.status, result.error = "ERROR", str(exc)
                    finally:
                        done.set()

                def read_stderr():
                    for line in proc.stderr:
                        stderr.append(line.rstrip())

                readers = [threading.Thread(target=read_stdout, daemon=True), threading.Thread(target=read_stderr, daemon=True)]
                for reader in readers:
                    reader.start()
                try:
                    proc.stdin.write(prompt)
                    proc.stdin.close()
                except (BrokenPipeError, OSError):
                    pass  # the exit code/stderr below explains early CLI failures
                if not done.wait(timeout):
                    timed_out = True
                    self._stop_process(proc)
                try:
                    code = proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self._stop_process(proc)
                    code = proc.returncode
                for reader in readers:
                    reader.join(timeout=3)
                if timed_out:
                    result.status, result.error = "TIMEOUT", f"Agent turn timed out after {timeout} seconds."
                if result.status != "TIMEOUT" and (code or result.status != "SUCCESS"):
                    result.status = "ERROR"
                    result.error = result.error or "\n".join(stderr) or f"Codex exited without completing a turn (exit {code})."
                self._context_event()
            except OSError as exc:
                result.error = str(exc)
            finally:
                self._stop_process(proc)
                for reader in readers:
                    reader.join(timeout=3)
                if proc:
                    for stream in (proc.stdin, proc.stdout, proc.stderr):
                        stream.close()
                with self._process_lock:
                    interrupted = self._interrupted
                    self.proc = None
                    self._in_turn = self._interrupted = False
            if interrupted:
                result.status, result.error = "INTERRUPTED", "Interrupted."
            result.conversation_id = self.conversation_id or ""
            result.duration_seconds = time.monotonic() - started
            result.tool_calls = list(tools.values())
            if result.status != "SUCCESS" and (not terminal or result.status in ("INTERRUPTED", "TIMEOUT")):
                self._emit({"type": "turn.failed", "error": {"message": result.error}})
            return result

    def close(self):
        self._running = False
        self.interrupt()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.close()
