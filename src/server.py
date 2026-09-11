"""
Main MCP Server Entrypoint for AgentLandline.
Integrates AgyManager and issue reporter tools.
"""
import json
import logging
from typing import Optional
from mcp.server.fastmcp import FastMCP, Context
from issue_reporter import IssueReporter
from agy_manager import AgyManager

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("AgentLandline")

# Initialize FastMCP Server and global AgyManager instance
mcp = FastMCP("AgentLandline")
manager = AgyManager()


def get_client_agent_name(ctx: Context = None) -> str:
    """Extracts client agent name (e.g. 'Antigravity', 'claude-code') from MCP initialization clientInfo metadata."""
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


@mcp.tool()
def initialize_agent(
    conversation_id: str,
    cwd: str = "",
    fork: bool = False,
    new_conversation_id: str = ""
) -> str:
    """
    Initialize and resume an agy agent process backend for a specified conversation ID.
    
    Args:
        conversation_id: The conversation ID of the existing agent session to resume (or fork from).
        cwd: Working directory path for the agent process (optional).
        fork: Whether to fork the conversation into a new session (optional).
        new_conversation_id: Optional custom conversation ID for the fork (auto-generated if omitted).
    """
    res = manager.initialize(
        conversation_id=conversation_id,
        cwd=cwd if cwd else None,
        fork=fork,
        new_conversation_id=new_conversation_id if new_conversation_id else None
    )
    return json.dumps(res, indent=2)


@mcp.tool()
def send_prompt(prompt: str, timeout: float = 120.0) -> str:
    """
    Send a prompt to the running agy agent session and wait for the response.
    
    Args:
        prompt: User prompt text to send to the agent.
        timeout: Maximum wait time in seconds for the agent turn.
    """
    res = manager.send_prompt(prompt, timeout=timeout)
    return json.dumps(res, indent=2)


@mcp.tool()
def agent_status(mode: str = "process") -> str:
    """
    Get status info for the agy agent session.
    
    Args:
        mode: Status mode:
            - 'process': Check process health, PID, conversation_id, and available tools.
            - 'output': Return live stream reader output collected so far.
            - 'transcript': Read transcript.jsonl and return the last prompt & response.
    """
    res = manager.status(mode=mode)
    return json.dumps(res, indent=2)


@mcp.tool()
def exit_agent() -> str:
    """
    Close and terminate the current agy agent session cleanly.
    """
    res = manager.exit()
    return json.dumps(res, indent=2)


@mcp.tool()
def report_issue(
    title: str,
    body: str = "",
    label: str = "bug",
    agent_model: str = "",
    session_id: str = "unknown",
    ctx: Context = None
) -> str:
    """
    Report an issue, bug, or feature request directly to GitHub.
    
    Args:
        title: Issue Title describing the problem, bug, or feature request.
        body: Issue Content in Markdown format (bug report, feature request, tracebacks, proposals, etc.).
        label: Issue Label ('bug', 'enhancement', 'feature-request', etc.).
        agent_model: Model name/version of the agent (e.g. 'gemini-3.6-flash', 'claude-3.5-sonnet').
        session_id: The conversation/session ID of the current agent turn.
    """
    detected_agent_name = get_client_agent_name(ctx)
    logger.info(f"[TOOL CALL] report_issue: title={title!r}, detected_agent={detected_agent_name!r}, model={agent_model!r}, label={label!r}")
    return IssueReporter.create_issue(
        title=title,
        body=body,
        label=label,
        agent_model=agent_model,
        session_id=session_id,
        agent_name=detected_agent_name
    )


if __name__ == "__main__":
    logger.info("Starting AgentLandline MCP Server...")
    mcp.run()
