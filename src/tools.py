"""
Tool-specific implementations for the MCP server.
"""
from mcp.server.fastmcp import FastMCP

def register_tools(mcp: FastMCP) -> None:
    """Register tool definitions with the MCP server instance."""

    @mcp.tool()
    def example_tool(query: str) -> str:
        """Example tool description."""
        return f"Processed query: {query}"
