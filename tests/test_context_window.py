"""
How full each agent's context window is: the live "context" events from both CLIs'
streams, the window sizes (reported by claude, a table for agy, or the agent's own
setting), and what the hub reports and saves. No real CLI is started.
"""

import json
import os
import shutil
import sys
import tempfile
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline.context_windows import ContextWindows, describe, short_tokens
from agentlandline.events import normalize
from agentlandline.hub import AgentSpec, Hub, HubError, load_config


def _start(usage, parent=None):
    return {"type": "stream_event", "parent_tool_use_id": parent,
            "event": {"type": "message_start", "message": {"usage": usage}}}


# --- events -------------------------------------------------------------------------

def test_claude_reports_context_at_each_call_and_as_it_writes():
    usage = {"input_tokens": 2, "cache_read_input_tokens": 8607, "cache_creation_input_tokens": 11880, "output_tokens": 1}
    assert normalize("claude", _start(usage)) == [{"kind": "context", "prompt": 20489, "output": 1}]
    delta = {"type": "stream_event", "event": {"type": "message_delta", "usage": {"output_tokens": 129}}}
    assert normalize("claude", delta) == [{"kind": "context", "output": 129}]
    assert normalize("claude", _start(usage, parent="toolu_1")) == []  # a subagent's own context


def test_claude_turn_end_carries_the_window_and_compaction_is_reported():
    result = {"type": "result", "is_error": False, "usage": {},
              "modelUsage": {"claude-haiku-5-5": {"inputTokens": 2, "contextWindow": 1000000}}}
    assert normalize("claude", result)[0]["context_windows"] == {"claude-haiku-5-5": 1000000}
    assert "context_windows" not in normalize("claude", {"type": "result", "usage": {}})[0]
    compact = {"type": "system", "subtype": "compact_boundary",
               "compact_metadata": {"trigger": "auto", "pre_tokens": 180000, "post_tokens": 12000}}
    assert normalize("claude", compact) == [{"kind": "compacted", "trigger": "auto", "pre_tokens": 180000,
                                             "post_tokens": 12000}]


def test_agy_reports_context_as_each_step_ends():
    step = {"event": "step_update", "step_update": {"step_type": "agent_response", "state": "DONE", "step_index": 3,
                                                    "usage": {"input_tokens": 13496, "output_tokens": 124}}}
    assert normalize("antigravity", step) == [{"kind": "context", "prompt": 13496, "output": 124}]
    step["step_update"]["text_delta"] = "done."  # the last step carries its text as well
    assert normalize("antigravity", step) == [{"kind": "text_delta", "text": "done."},
                                              {"kind": "context", "prompt": 13496, "output": 124}]
    # Seen live: once agy caches the conversation, input_tokens is only the new part.
    step["step_update"].update(text_delta=None, usage={"input_tokens": 2210, "cache_read_tokens": 79717, "output_tokens": 94})
    assert normalize("antigravity", step) == [{"kind": "context", "prompt": 81927, "output": 94}]
    active = {"event": "step_update", "step_update": {"step_type": "agent_response", "state": "ACTIVE", "text_delta": "d"}}
    assert normalize("antigravity", active) == [{"kind": "text_delta", "text": "d"}]


# --- window sizes ---------------------------------------------------------------------

def test_windows_come_from_the_table_and_what_claude_reported(tmp_path):
    windows = ContextWindows(str(tmp_path))
    assert windows.window("Gemini 3.8 Flash (High)") == 1_048_576
    assert windows.window("claude-sonnet-4-6") == 200_000 and windows.window("Claude Opus 4.6 (Thinking)") == 200_000
    assert windows.window("claude-haiku-5-5") is None and windows.window(None) is None
    windows.learn("claude-haiku-5-5", 1_000_000)
    assert ContextWindows(str(tmp_path)).window("Claude-Haiku-5-5") == 1_000_000  # saved for later sessions


def test_describe():
    assert describe({"used": 410_000, "window": 1_000_000}) == "410k / 1M (41%)"
    assert describe({"used": 14_025, "window": None}) == "14k"
    assert describe(None) is None and describe({"used": None, "window": 5}) is None
    assert [short_tokens(n) for n in (999, 12_345, 999_999, 1_048_576)] == ["999", "12.3k", "1M", "1.05M"]


# --- the hub ----------------------------------------------------------------------------

class StreamingPipeline:
    """Emits the given raw events while handling a message."""

    def __init__(self, spec, env):
        self.spec, self.conversation_id, self.listeners, self.script = spec, f"session-{spec.name}", [], []

    def add_event_listener(self, listener):
        self.listeners.append(listener)

    def start(self, timeout=None):
        pass

    def close(self):
        pass

    def transcript_path(self):
        return None

    def send(self, prompt, timeout=None, images=None):
        for raw in self.script:
            for listener in self.listeners:
                listener(raw)
        return SimpleNamespace(response="ok", status="SUCCESS", error=None)


@pytest.fixture
def home():
    path = tempfile.mkdtemp(prefix="ll-", dir="/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def make_hub(home, *specs):
    pipelines = {}

    def factory(spec, env):
        pipelines[spec.name] = StreamingPipeline(spec, env)
        return pipelines[spec.name]

    hub = Hub(list(specs), run_id="ctx", home=home, socket_path=os.path.join(home, "h.sock"), pipeline_factory=factory)
    hub.start()
    return hub, pipelines


def test_hub_tracks_each_agents_context_live_and_saves_it(home):
    hub, pipelines = make_hub(home, AgentSpec("cl", backend="claude", model="haiku"), AgentSpec("ag", backend="antigravity",
                                                                                                 model="Gemini 3.8 Flash (High)"))
    events = []
    hub.subscribe(lambda e: e["kind"] == "context" and events.append(e))
    try:
        assert {a["name"]: a["context"] for a in hub.list_agents()} == {"cl": None, "ag": None}
        pipelines["cl"].script = [
            {"type": "system", "subtype": "init", "session_id": "s1", "model": "claude-haiku-5-5"},
            _start({"input_tokens": 3, "cache_read_input_tokens": 20000, "output_tokens": 1}),
            {"type": "stream_event", "event": {"type": "message_delta", "usage": {"output_tokens": 50}}},
            _start({"input_tokens": 3, "cache_read_input_tokens": 21000, "output_tokens": 1}),
            {"type": "stream_event", "event": {"type": "message_delta", "usage": {"output_tokens": 200}}},
            {"type": "result", "usage": {}, "modelUsage": {"claude-haiku-5-5": {"contextWindow": 1000000}}},
        ]
        hub.send("user", "cl", "go")
        cl = [(e["used"], e["window"]) for e in events if e["agent"] == "cl"]
        # Each call is sent at once (the window isn't known until the turn ends); the deltas
        # in between are held back by the once-a-second limit; the turn's end sends the last count.
        assert cl == [(20004, None), (21004, None), (21203, 1000000)]
        assert hub.list_agents()[0]["context"] == {"used": 21203, "window": 1000000}
        assert hub.context_windows.window("haiku") == 1000000  # the name it was asked for, too

        pipelines["ag"].script = [{"event": "step_update", "step_update": {
            "step_type": "agent_response", "state": "DONE", "usage": {"input_tokens": 13000, "output_tokens": 120}}}]
        hub.send("user", "ag", "go")
        assert [(e["used"], e["window"]) for e in events if e["agent"] == "ag"] == [(13120, 1_048_576)]  # sent once
    finally:
        hub.stop()
    run_file = os.path.join(home, "runs", "ctx", "run.json")
    saved = json.load(open(run_file))["agents"]
    assert saved["cl"]["context"] == {"used": 21203} and saved["ag"]["context"] == {"used": 13120}
    assert "last_context" not in saved["cl"] and "context_window" not in saved["cl"]

    resumed, _ = make_hub(home, *load_config(run_file)["agents"])  # a resumed session shows it at once
    try:
        assert {a["name"]: a["context"] for a in resumed.list_agents()} == {
            "cl": {"used": 21203, "window": 1000000}, "ag": {"used": 13120, "window": 1_048_576}}
    finally:
        resumed.stop()


def test_compaction_and_copies_reset_the_count(home):
    hub, pipelines = make_hub(home, AgentSpec("cl", backend="claude", context_window=200000))
    try:
        pipelines["cl"].script = [
            _start({"input_tokens": 180000}),
            {"type": "system", "subtype": "compact_boundary", "compact_metadata": {"pre_tokens": 180000, "post_tokens": 9000}},
        ]
        hub.send("user", "cl", "go")
        assert hub.list_agents()[0]["context"] == {"used": 9000, "window": 200000}  # its own setting
        copy = AgentSpec.from_dict("copy", {"backend": "claude", "session_id": "s1", "fork": True, "context": {"used": 5}})
        assert copy.last_context == {"used": 5}
        hub.spawn(copy)
        assert next(a for a in hub.list_agents() if a["name"] == "copy")["context"] is None
    finally:
        hub.stop()


def test_context_window_setting_is_checked():
    assert AgentSpec.from_dict("a", {"context_window": 128000}).context_window == 128000
    for bad in (0, -5, "big", True):
        with pytest.raises(HubError, match="context_window"):
            AgentSpec.from_dict("a", {"context_window": bad})
