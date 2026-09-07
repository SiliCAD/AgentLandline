"""
AgyManager: High-level manager wrapping AgyPipeline for orchestration and MCP integration.
Exposes initialize, send_prompt, status, and exit public methods.
"""

import logging
from typing import Optional, Dict, Any, List
from agy_pipeline import AgyPipeline, AgentTurnResult

logger = logging.getLogger("AgyManager")


class AgyManager:
    """
    Manager class orchestrating the AgyPipeline backend.
    Provides public methods: initialize, send_prompt, status, and exit.
    """

    def __init__(self):
        self.pipeline: Optional[AgyPipeline] = None

    def initialize(
        self,
        conversation_id: str,
        cwd: Optional[str] = None,
        skip_permissions: bool = True,
        timeout: float = 30.0
    ) -> Dict[str, Any]:
        """
        Initializes and starts the underlying AgyPipeline to resume a specified conversation_id.
        
        Args:
            conversation_id: Conversation ID of the agent turn session to resume.
            cwd: Working directory for the agy process.
            skip_permissions: Pass --dangerously-skip-permissions flag.
            timeout: Maximum wait time for initialization event.
            
        Returns:
            Dict containing initialization status, conversation_id, and available tools.
        """
        if self.pipeline and self.pipeline._running:
            logger.info("Closing existing active pipeline session before initializing new one...")
            self.exit()

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
        
        Args:
            prompt: User prompt text.
            timeout: Maximum wait time for agent turn.
            
        Returns:
            Dict containing agent response, tools used, questions, status, and metadata.
        """
        if not self.pipeline:
            return {
                "status": "ERROR",
                "error": "AgyPipeline is not initialized. Call initialize() first."
            }

        turn_result: AgentTurnResult = self.pipeline.send(prompt, timeout=timeout)

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

    def status(self) -> Dict[str, Any]:
        """
        Returns the current status of the manager and underlying pipeline process.
        """
        if not self.pipeline:
            return {
                "is_running": False,
                "status": "UNINITIALIZED",
                "conversation_id": None,
                "available_tools": []
            }

        is_proc_alive = (
            self.pipeline._running and
            self.pipeline.proc is not None and
            self.pipeline.proc.poll() is None
        )

        return {
            "is_running": is_proc_alive,
            "status": "RUNNING" if is_proc_alive else "STOPPED",
            "conversation_id": self.pipeline.conversation_id,
            "available_tools": self.pipeline.available_tools,
            "permission_mode": self.pipeline.permission_mode,
            "cwd": self.pipeline.cwd,
            "pid": self.pipeline.proc.pid if (self.pipeline.proc and is_proc_alive) else None
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
