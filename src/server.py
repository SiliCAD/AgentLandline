"""
Main MCP Server Entrypoint.
"""
from mcp.server.fastmcp import FastMCP
from tools import register_tools

# Initialize FastMCP Server
mcp = FastMCP("AgentLandline")

# Register tools
register_tools(mcp)

if __name__ == "__main__":
    mcp.run()
