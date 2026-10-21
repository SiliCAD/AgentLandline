"""Opt-in smoke test: LANDLINE_CODEX_LIVE=1 pytest tests/test_codex_live.py -q."""

import os
import tempfile

import pytest

from agentlandline.codex_pipeline import CodexPipeline
from agentlandline.hub import AgentSpec, Hub, HubServer


@pytest.mark.skipif(os.environ.get("LANDLINE_CODEX_LIVE") != "1", reason="requires Codex CLI login and live model calls")
def test_codex_mcp_resume_and_fork():
    with tempfile.TemporaryDirectory(prefix="ll-codex-", dir="/tmp") as home:
        def factory(spec, env):
            return CodexPipeline(cwd=home, skip_permissions=False, conversation_id=spec.session_id,
                                 fork_session=spec.fork, extra_env=env, extra_args=["--ignore-user-config"])

        hub = Hub([AgentSpec("coder", backend="codex", cwd=home)], home=home, run_id="live",
                  socket_path=os.path.join(home, "hub.sock"), pipeline_factory=factory)
        server = HubServer(hub)
        events = []
        hub.subscribe(events.append)
        try:
            server.start()
            hub.start()
            reply = hub.send("user", "coder", "Call the MCP tool mcp__agent_landline__list_agents once. Remember the phrase "
                             "amber-orbit-47 and reply with that phrase. Do not use any other tools.", timeout=120)
            assert "amber-orbit-47" in reply["reply"]["text"], reply
            if not any(e["kind"] == "tool_start" and e["name"] == "list_agents" for e in events):
                hub.send("user", "coder", "This verification requires an actual mcp__agent_landline__list_agents tool call. "
                         "Call it now and report the exact value of its 'you' field. Do not use any other tools.", timeout=120)
            assert any(e["kind"] == "tool_start" and e["name"] == "list_agents" for e in events)
            assert any(e["kind"] == "tool_end" and e["ok"] and 'you' in e["output"] and 'coder' in e["output"]
                       for e in events), events
            session = hub.agents["coder"].session_id
            assert session and hub.agents["coder"].pipeline.transcript_path()
            for target in ("coder", "copy"):
                if target == "copy":
                    hub.spawn(AgentSpec("copy", backend="codex", cwd=home, session_id=session, fork=True))
                reply = hub.send("user", target, "What verification phrase did I ask you to remember? "
                                 "Reply with only the phrase and do not use tools.", timeout=120)
                assert "amber-orbit-47" in reply["reply"]["text"], reply
            assert hub.agents["coder"].session_id == session
            assert hub.agents["copy"].session_id != session
            assert not hub.agents["copy"].spec.fork
        finally:
            hub.stop()
            server.stop()
