"""
Hub-mode MCP tools: message_agent/list_agents are registered only when the server
runs inside a hub-launched agent (LANDLINE_AGENT set), and talk to the hub socket.
"""

import importlib
import json
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
sys.path.insert(0, os.path.dirname(__file__))

from agentlandline import server
from agentlandline.hub import HubServer
from test_hub import make_hub, short_dir  # noqa: F401  (fixture)


def _tool_names(module):
    return {t.name for t in module.mcp._tool_manager.list_tools()}


def test_hub_tools_absent_outside_hub_mode():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("LANDLINE_AGENT", None)
        mod = importlib.reload(server)
    names = _tool_names(mod)
    assert "message_agent" not in names and "list_agents" not in names
    assert {"initialize_agent", "send_prompt", "agent_status", "exit_agent", "report_issue"} <= names


def test_message_agent_roundtrip_through_hub(short_dir):  # noqa: F811
    hub, _ = make_hub(short_dir, {}, architect={"role": "lead"}, coder={})
    hub_server = HubServer(hub)
    hub_server.start()
    try:
        with patch.dict(os.environ, {"LANDLINE_AGENT": "architect", "LANDLINE_SOCKET": hub.socket_path}):
            mod = importlib.reload(server)
            assert {"message_agent", "list_agents"} <= _tool_names(mod)

            out = json.loads(mod.message_agent("coder", "size M1"))
            assert out == {"status": "REPLIED", "message_id": "m1", "reply": "coder got it", "reply_id": "m2"}

            listing = json.loads(mod.list_agents())
            assert listing["you"] == "architect"
            assert [a["name"] for a in listing["agents"]] == ["architect", "coder"]

            err = json.loads(mod.message_agent("nobody", "x"))
            assert err["status"] == "ERROR" and "Unknown agent" in err["error"]

        assert [(m["from"], m["to"]) for m in hub.log.read_all()] == [("architect", "coder"), ("coder", "architect")]
    finally:
        hub_server.stop()
        hub.stop()
        os.environ.pop("LANDLINE_AGENT", None)
        importlib.reload(server)
