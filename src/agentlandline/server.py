"""
Main MCP Server Entrypoint for AgentLandline.
Integrates AgentManager and issue reporter tools.
"""
import os
import json
import logging
from typing import Optional
from mcp.server.fastmcp import FastMCP, Context
from .issue_reporter import IssueReporter
from .agent_manager import AgentManager
from .hub import HubClient, HubError

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("AgentLandline")

# Initialize FastMCP Server and global AgentManager instance
mcp = FastMCP("AgentLandline")
manager = AgentManager()


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
    new_conversation_id: str = "",
    backend: str = "antigravity",
    model: str = "",
    effort: str = ""
) -> str:
    """
    Initialize and resume an agent process backend for a specified conversation ID.

    Args:
        conversation_id: The conversation/session ID of the existing agent session to resume (or fork from).
        cwd: Working directory path for the agent process (optional).
        fork: Whether to fork the conversation into a new session (optional).
        new_conversation_id: Optional custom conversation ID for the fork (Antigravity only; Claude Code assigns its own forked session ID).
        backend: Which agent CLI to drive: 'antigravity'/'agy' (default) or 'claude'/'claude-code'.
        model: Optional model name/alias to pass through to the backend CLI.
        effort: Optional reasoning effort level to pass through to the backend CLI.
    """
    res = manager.initialize(
        conversation_id=conversation_id,
        cwd=cwd if cwd else None,
        fork=fork,
        new_conversation_id=new_conversation_id if new_conversation_id else None,
        backend=backend if backend else "antigravity",
        model=model if model else None,
        effort=effort if effort else None
    )
    return json.dumps(res, indent=2)


@mcp.tool()
def send_prompt(prompt: str, timeout: float = 120.0) -> str:
    """
    Send a prompt to the running agent session and wait for the response.

    Args:
        prompt: User prompt text to send to the agent.
        timeout: Maximum wait time in seconds for the agent turn.
    """
    res = manager.send_prompt(prompt, timeout=timeout)
    return json.dumps(res, indent=2)


@mcp.tool()
def agent_status(mode: str = "process") -> str:
    """
    Get status info for the agent session.

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
    Close and terminate the current agent session cleanly.
    """
    res = manager.exit()
    return json.dumps(res, indent=2)


@mcp.tool()
def report_issue(
    title: str,
    body: str = "",
    label: str = "bug",
    repo: str = "SiliCAD/AgentLandline",
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
        repo: Target GitHub repository ('owner/repo', default: 'SiliCAD/AgentLandline').
        agent_model: Model name/version of the agent (e.g. 'gemini-3.6-flash', 'claude-3.5-sonnet').
        session_id: The conversation/session ID of the current agent turn.
    """
    detected_agent_name = get_client_agent_name(ctx)
    logger.info(f"[TOOL CALL] report_issue: title={title!r}, repo={repo!r}, detected_agent={detected_agent_name!r}, model={agent_model!r}, label={label!r}")
    return IssueReporter.create_issue(
        title=title,
        body=body,
        label=label,
        repo=repo,
        agent_model=agent_model,
        session_id=session_id,
        agent_name=detected_agent_name
    )


# ---------------------------------------------------------------------------
# Hub mode: when this server runs inside an agent launched by the hub
# (`landline up`), the hub sets LANDLINE_AGENT (this agent's name) and
# LANDLINE_SOCKET. The agent then gets tools to message the other agents.
# ---------------------------------------------------------------------------

HUB_AGENT = os.environ.get("LANDLINE_AGENT")


def _hub_client() -> HubClient:
    return HubClient(os.environ.get("LANDLINE_SOCKET") or None)


def message_agent(to: str, text: str, wait: bool = True) -> str:
    """
    Send a message to another agent in this multi-agent session (or to 'user', the human watching).

    Args:
        to: Name of the agent to message (see list_agents), or 'user'.
        text: The message. Be self-contained: the other agent can't see your conversation.
        wait: If true (default), wait until the agent has handled the message and return its reply.
              If false, return once the message is queued.
    """
    logger.info(f"[TOOL CALL] message_agent: from={HUB_AGENT!r} to={to!r} wait={wait}")
    try:
        res = _hub_client().send(HUB_AGENT, to, text, wait=wait)
    except HubError as e:
        return json.dumps({"status": "ERROR", "error": str(e)}, indent=2)
    if not res.get("ok"):
        return json.dumps({"status": "ERROR", "error": res.get("error")}, indent=2)
    out = {"status": res["status"], "message_id": res["message"]["id"]}
    if res.get("reply"):
        out["reply"] = res["reply"]["text"]
        out["reply_id"] = res["reply"]["id"]
    if res.get("error"):
        out["error"] = res["error"]
    return json.dumps(out, indent=2)


def list_agents() -> str:
    """
    List the agents in this multi-agent session: name, role, backend, and whether each is busy.
    """
    try:
        res = _hub_client().list_agents()
    except HubError as e:
        return json.dumps({"status": "ERROR", "error": str(e)}, indent=2)
    agents = [
        {k: a.get(k) for k in ("name", "role", "backend", "model", "busy", "queued")}
        for a in res.get("agents", [])
    ]
    return json.dumps({"you": HUB_AGENT, "agents": agents}, indent=2)


if HUB_AGENT:
    mcp.tool()(message_agent)
    mcp.tool()(list_agents)


def main():
    logger.info("Starting AgentLandline MCP Server...")
    mcp.run()


if __name__ == "__main__":
    main()
