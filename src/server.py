"""
Main MCP Server Entrypoint for AgentLandline.
"""
import sys
import logging
from mcp.server.fastmcp import FastMCP, Context
from issue_reporter import IssueReporter

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("AgentLandline")

# Initialize FastMCP Server
mcp = FastMCP("AgentLandline")


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
