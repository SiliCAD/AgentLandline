"""
AgyManager: High-level manager wrapping agent CLI pipelines for orchestration and MCP integration.
Exposes initialize, send_prompt, status, extract_last_command, and exit public methods, and can
drive either the Antigravity (agy) or Claude Code (claude) backend via `backend=`.
"""

import os
import json
import time
import shutil
import sqlite3
import logging
from typing import Optional, Dict, Any, List, Union
from agy_pipeline import AgyPipeline, AgentTurnResult
from claude_pipeline import ClaudePipeline
from pipeline_factory import normalize_backend, DEFAULT_BACKEND

logger = logging.getLogger("AgyManager")


class AgyManager:
    """
    Manager class orchestrating an agent CLI pipeline (Antigravity or Claude Code).
    Provides public methods: initialize, send_prompt, status, extract_last_command, and exit.
    """

    def __init__(self):
        self.pipeline: Optional[Union[AgyPipeline, ClaudePipeline]] = None
        self.backend: Optional[str] = None
        self.last_prompt_output: Optional[str] = None
        self.last_turn_status: Optional[str] = None

    def fork_conversation(self, parent_id: str, new_id: Optional[str] = None) -> str:
        """
        Forks an existing agy conversation session into a new conversation ID.
        """
        fork_id = new_id or f"fork-{int(time.time())}"
        cli_dir = os.path.expanduser("~/.gemini/antigravity-cli")

        parent_db = os.path.join(cli_dir, "conversations", f"{parent_id}.db")
        if not os.path.exists(parent_db):
            raise FileNotFoundError(f"Conversation '{parent_id}' not found in {cli_dir}/conversations/")

        # 1. Clone DB and update internal cascade_id
        fork_db = os.path.join(cli_dir, "conversations", f"{fork_id}.db")
        shutil.copy2(parent_db, fork_db)
        with sqlite3.connect(fork_db) as conn:
            conn.execute("UPDATE trajectory_meta SET cascade_id = ?;", (fork_id,))

        # 2. Clone brain artifacts directory if present
        parent_brain = os.path.join(cli_dir, "brain", parent_id)
        if os.path.isdir(parent_brain):
            shutil.copytree(parent_brain, os.path.join(cli_dir, "brain", fork_id), dirs_exist_ok=True)

        return fork_id

    def initialize(
        self,
        conversation_id: str,
        cwd: Optional[str] = None,
        skip_permissions: bool = True,
        timeout: float = 30.0,
        fork: bool = False,
        new_conversation_id: Optional[str] = None,
        backend: str = DEFAULT_BACKEND,
        model: Optional[str] = None,
        effort: Optional[str] = None,
        permission_mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Initializes and starts the underlying agent pipeline to resume a specified conversation_id.

        `backend` selects which CLI to drive: 'antigravity'/'agy' (default) or 'claude'/'claude-code'.
        Forking behaves differently per backend: Antigravity forks by cloning its SQLite/brain
        state up front into a caller-chosen `new_conversation_id`; Claude Code forks natively via
        `--resume <id> --fork-session`, so the new session id is only known after the CLI starts
        and is returned as `conversation_id` in the result.
        """
        if self.pipeline and self.pipeline._running:
            logger.info("Closing existing active pipeline session before initializing new one...")
            self.exit()

        self.last_prompt_output = None
        self.last_turn_status = None

        try:
            resolved_backend = normalize_backend(backend)
        except ValueError as e:
            return {"status": "ERROR", "error": str(e), "conversation_id": conversation_id}

        fork_session = False
        if fork:
            if resolved_backend == "antigravity":
                try:
                    conversation_id = self.fork_conversation(conversation_id, new_conversation_id)
                except Exception as e:
                    logger.error(f"Failed to fork conversation '{conversation_id}': {e}")
                    return {
                        "status": "ERROR",
                        "error": f"Failed to fork conversation: {e}",
                        "conversation_id": conversation_id
                    }
            else:
                # Claude Code assigns the forked session's ID itself on start().
                fork_session = True

        # AgyPipeline and ClaudePipeline are independent, self-contained classes with
        # slightly different constructor kwargs (only Claude has permission_mode/fork_session),
        # so each backend is constructed explicitly rather than through one shared call.
        if resolved_backend == "claude":
            self.pipeline = ClaudePipeline(
                cwd=cwd,
                skip_permissions=skip_permissions,
                model=model,
                effort=effort,
                conversation_id=conversation_id,
                permission_mode=permission_mode,
                fork_session=fork_session
            )
        else:
            self.pipeline = AgyPipeline(
                cwd=cwd,
                skip_permissions=skip_permissions,
                model=model,
                effort=effort,
                conversation_id=conversation_id
            )
        self.backend = resolved_backend

        try:
            self.pipeline.start(timeout=timeout)
            return {
                "status": "INITIALIZED",
                "backend": self.backend,
                "conversation_id": self.pipeline.conversation_id,
                "available_tools": self.pipeline.available_tools,
                "permission_mode": self.pipeline.permission_mode,
                "cwd": self.pipeline.cwd
            }
        except Exception as e:
            logger.error(f"Failed to initialize {resolved_backend} pipeline: {e}")
            return {
                "status": "ERROR",
                "backend": self.backend,
                "error": str(e),
                "conversation_id": conversation_id
            }

    def send_prompt(self, prompt: str, timeout: float = 120.0) -> Dict[str, Any]:
        """
        Sends a prompt to the running agy session and waits for turn result.
        Returns only the response and metadata from this prompt turn.
        """
        if not self.pipeline:
            return {
                "status": "ERROR",
                "error": "AgyPipeline is not initialized. Call initialize() first."
            }

        turn_result: AgentTurnResult = self.pipeline.send(prompt, timeout=timeout)

        self.last_prompt_output = turn_result.response
        self.last_turn_status = turn_result.status

        tool_calls_data = [
            {
                "name": t.name,
                "parameters": t.parameters,
                "state": t.state,
                "error": t.error,
                "step_index": t.step_index
            }
            for t in turn_result.tool_calls
        ]

        return {
            "conversation_id": turn_result.conversation_id,
            "response": turn_result.response,
            "status": turn_result.status,
            "duration_seconds": turn_result.duration_seconds,
            "tool_calls": tool_calls_data,
            "questions": turn_result.questions,
            "denied_actions": turn_result.denied_actions,
            "usage": turn_result.usage,
            "error": turn_result.error
        }

    def extract_last_command(
        self,
        conversation_id: Optional[str] = None,
        backend: Optional[str] = None,
        cwd: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extracts the last user prompt and corresponding agent response from the
        backend's on-disk transcript. Dispatches to the backend-specific parser
        (`backend`, defaulting to the active pipeline's backend, then 'antigravity'
        for compatibility with existing single-backend callers).
        """
        target_conv_id = conversation_id or (self.pipeline.conversation_id if self.pipeline else None)
        if not target_conv_id:
            return {"error": "No conversation_id provided or available."}

        resolved_backend = backend or self.backend or DEFAULT_BACKEND
        try:
            resolved_backend = normalize_backend(resolved_backend)
        except ValueError as e:
            return {"error": str(e)}

        if resolved_backend == "claude":
            target_cwd = cwd or (self.pipeline.cwd if self.pipeline else None) or os.getcwd()
            return self._extract_last_command_claude(target_conv_id, target_cwd)

        return self._extract_last_command_antigravity(target_conv_id)

    def _extract_last_command_antigravity(self, target_conv_id: str) -> Dict[str, Any]:
        candidates = [
            os.path.expanduser(f"~/.gemini/antigravity-cli/brain/{target_conv_id}/.system_generated/logs/transcript.jsonl"),
            os.path.expanduser(f"~/.gemini/antigravity-ide/brain/{target_conv_id}/.system_generated/logs/transcript.jsonl"),
        ]
        app_data = os.environ.get("ANTIGRAVITY_APP_DATA_DIR")
        if app_data:
            candidates.insert(0, os.path.join(app_data, "brain", target_conv_id, ".system_generated", "logs", "transcript.jsonl"))

        transcript_path = None
        for path in candidates:
            if os.path.exists(path):
                transcript_path = path
                break

        if not transcript_path:
            return {"error": f"Transcript file not found for conversation_id={target_conv_id}"}

        last_user_prompt = None
        last_agent_response = None
        last_step_index = None

        try:
            with open(transcript_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        step_type = obj.get("type") or obj.get("event")

                        if step_type == "USER_INPUT" or obj.get("source") == "USER_EXPLICIT":
                            content = obj.get("content")
                            if isinstance(content, str):
                                last_user_prompt = content
                            elif isinstance(content, dict):
                                last_user_prompt = content.get("text") or content.get("content") or str(content)
                            last_step_index = obj.get("step_index")

                        elif step_type in ("PLANNER_RESPONSE", "result") or "result" in obj or "response" in obj:
                            content = obj.get("content")
                            if not content and "result" in obj:
                                res_obj = obj.get("result", {})
                                content = res_obj.get("response") if isinstance(res_obj, dict) else str(res_obj)
                            elif not content and "response" in obj:
                                content = obj.get("response")

                            if content:
                                if isinstance(content, str):
                                    last_agent_response = content
                                elif isinstance(content, dict):
                                    last_agent_response = content.get("text") or str(content)

                    except Exception:
                        continue

            return {
                "conversation_id": target_conv_id,
                "transcript_path": transcript_path,
                "last_step_index": last_step_index,
                "last_user_prompt": last_user_prompt,
                "last_agent_response": last_agent_response
            }
        except Exception as e:
            return {"error": f"Failed reading transcript: {e}"}

    def _extract_last_command_claude(self, target_conv_id: str, cwd: str) -> Dict[str, Any]:
        """Reads Claude Code's `~/.claude/projects/<dir>/<session-id>.jsonl` transcript."""
        config_dir = os.environ.get("CLAUDE_CONFIG_DIR") or "~/.claude"
        project_dir = ClaudePipeline.project_dir_for_cwd(cwd)
        transcript_path = os.path.expanduser(os.path.join(config_dir, "projects", project_dir, f"{target_conv_id}.jsonl"))

        if not os.path.exists(transcript_path):
            return {"error": f"Transcript file not found for conversation_id={target_conv_id}"}

        def _flatten_text(content_value) -> Optional[str]:
            if isinstance(content_value, str):
                return content_value
            if isinstance(content_value, list):
                texts = [b.get("text") for b in content_value if isinstance(b, dict) and b.get("type") == "text" and b.get("text")]
                return "\n".join(texts) if texts else None
            return None

        last_user_prompt = None
        last_agent_response = None
        last_step_index = None
        step_index = 0

        try:
            with open(transcript_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue

                    message = obj.get("message")
                    if not isinstance(message, dict):
                        continue
                    step_index += 1
                    content = message.get("content")

                    if message.get("role") == "user":
                        text = _flatten_text(content)
                        if text:
                            last_user_prompt = text
                            last_step_index = obj.get("step_index", step_index)
                    elif message.get("role") == "assistant":
                        text = _flatten_text(content)
                        if text:
                            last_agent_response = text

            return {
                "conversation_id": target_conv_id,
                "transcript_path": transcript_path,
                "last_step_index": last_step_index,
                "last_user_prompt": last_user_prompt,
                "last_agent_response": last_agent_response
            }
        except Exception as e:
            return {"error": f"Failed reading transcript: {e}"}

    def status(self, mode: str = "process") -> Dict[str, Any]:
        """
        Returns status information depending on mode:
          - 'process': Check process health, PID, conversation_id, and available tools.
          - 'output': Return whatever live stream reader output has been collected so far.
          - 'transcript': Read transcript.jsonl and extract the last user prompt & response.
        """
        clean_mode = mode.lower().strip() if mode else "process"

        if not self.pipeline:
            return {
                "mode": clean_mode,
                "is_running": False,
                "status": "UNINITIALIZED",
                "backend": self.backend,
                "conversation_id": None
            }

        if clean_mode == "process":
            is_proc_alive = (
                self.pipeline._running and
                self.pipeline.proc is not None and
                self.pipeline.proc.poll() is None
            )
            return {
                "mode": "process",
                "is_running": is_proc_alive,
                "status": "RUNNING" if is_proc_alive else "STOPPED",
                "backend": self.backend,
                "conversation_id": self.pipeline.conversation_id,
                "available_tools": self.pipeline.available_tools,
                "permission_mode": self.pipeline.permission_mode,
                "cwd": self.pipeline.cwd,
                "pid": self.pipeline.proc.pid if (self.pipeline.proc and is_proc_alive) else None
            }

        elif clean_mode == "output":
            live_out = self.pipeline.get_live_output()
            return {
                "mode": "output",
                "conversation_id": self.pipeline.conversation_id,
                "output_lines_count": len(self.pipeline._live_output_buffer),
                "output": live_out
            }

        elif clean_mode == "transcript":
            transcript_turn = self.extract_last_command()
            return {
                "mode": "transcript",
                "conversation_id": self.pipeline.conversation_id,
                "transcript_turn": transcript_turn
            }

        else:
            return {
                "error": f"Invalid mode '{mode}'. Valid modes are 'process', 'output', and 'transcript'."
            }

    def exit(self) -> Dict[str, Any]:
        """
        Terminates the agy pipeline process cleanly.
        """
        if not self.pipeline:
            return {"status": "SUCCESS", "message": "No active pipeline to close."}

        conv_id = self.pipeline.conversation_id
        try:
            self.pipeline.close()
            self.pipeline = None
            self.backend = None
            self.last_prompt_output = None
            self.last_turn_status = None
            logger.info(f"Pipeline session {conv_id} exited cleanly.")
            return {
                "status": "SUCCESS",
                "message": f"Pipeline session {conv_id} exited cleanly."
            }
        except Exception as e:
            logger.error(f"Error while exiting pipeline: {e}")
            return {
                "status": "ERROR",
                "error": str(e)
            }
