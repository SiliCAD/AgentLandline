"""
AgyManager: High-level manager wrapping AgyPipeline for orchestration and MCP integration.
Exposes initialize, send_prompt, status, extract_last_command, and exit public methods.
"""

import os
import json
import logging
from typing import Optional, Dict, Any, List
from agy_pipeline import AgyPipeline, AgentTurnResult

logger = logging.getLogger("AgyManager")


class AgyManager:
    """
    Manager class orchestrating the AgyPipeline backend.
    Provides public methods: initialize, send_prompt, status, extract_last_command, and exit.
    """

    def __init__(self):
        self.pipeline: Optional[AgyPipeline] = None
        self.last_prompt_output: Optional[str] = None
        self.last_turn_status: Optional[str] = None

    def initialize(
        self,
        conversation_id: str,
        cwd: Optional[str] = None,
        skip_permissions: bool = True,
        timeout: float = 30.0
    ) -> Dict[str, Any]:
        """
        Initializes and starts the underlying AgyPipeline to resume a specified conversation_id.
        """
        if self.pipeline and self.pipeline._running:
            logger.info("Closing existing active pipeline session before initializing new one...")
            self.exit()

        self.last_prompt_output = None
        self.last_turn_status = None

        self.pipeline = AgyPipeline(
            cwd=cwd,
            skip_permissions=skip_permissions,
            conversation_id=conversation_id
        )

        try:
            self.pipeline.start(timeout=timeout)
            return {
                "status": "INITIALIZED",
                "conversation_id": self.pipeline.conversation_id,
                "available_tools": self.pipeline.available_tools,
                "permission_mode": self.pipeline.permission_mode,
                "cwd": self.pipeline.cwd
            }
        except Exception as e:
            logger.error(f"Failed to initialize AgyPipeline: {e}")
            return {
                "status": "ERROR",
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

    def extract_last_command(self, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Extracts the last user prompt and corresponding agent response from transcript.jsonl.
        """
        target_conv_id = conversation_id or (self.pipeline.conversation_id if self.pipeline else None)
        if not target_conv_id:
            return {"error": "No conversation_id provided or available."}

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
            from collections import deque
            with open(transcript_path, "r", encoding="utf-8") as f:
                recent_lines = deque(f, maxlen=100)
                for line in recent_lines:
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
