"""
Compatibility entrypoint: existing MCP client configs launch `python3 <repo>/src/server.py`.
The server now lives in the `agentlandline` package (`agentlandline-mcp` once installed).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agentlandline.server import main

if __name__ == "__main__":
    main()
