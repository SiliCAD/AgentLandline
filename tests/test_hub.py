"""
Unit tests for agentlandline.hub: message routing, caused_by chaining, the wait-cycle
guard, live event fan-out, and the local socket API. Agents are fake pipelines whose
send() runs a scripted behaviour, so no `agy`/`claude` process is spawned.
"""

import json
import os
import shutil
import sys
import tempfile
import threading
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline.hub import Hub, HubServer, HubClient, HubError, AgentSpec, load_config


class FakePipeline:
    """Stands in for AgyPipeline/ClaudePipeline. `behaviour(prompt)` returns the reply text."""

    def __init__(self, spec, env, behaviour=None):
        self.spec, self.env = spec, env
        self.conversation_id = f"session-{spec.name}"
        self.behaviour = behaviour or (lambda prompt: f"{spec.name} got it")
        self.prompts = []
        self.listeners = []

    def add_event_listener(self, listener):
        self.listeners.append(listener)

    def start(self, timeout=None):
        pass

    def close(self):
        pass

    def transcript_path(self):
        return None

    def send(self, prompt, timeout=None):
        self.prompts.append(prompt)
        return SimpleNamespace(response=self.behaviour(prompt), status="SUCCESS", error=None)


@pytest.fixture
def short_dir():
    # Unix socket paths are limited to ~104 chars on macOS; pytest's tmp_path is too long.
    path = tempfile.mkdtemp(prefix="ll-", dir="/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def make_hub(home, behaviours, **specs):
    pipelines = {}

    def factory(spec, env):
        pipelines[spec.name] = FakePipeline(spec, env, behaviours.get(spec.name))
        return pipelines[spec.name]

    agent_specs = [AgentSpec(name=n, **(s or {})) for n, s in specs.items()] or [AgentSpec("a"), AgentSpec("b")]
    hub = Hub(agent_specs, run_id="test-run", home=home, socket_path=os.path.join(home, "hub.sock"), pipeline_factory=factory)
    hub.start()
    return hub, pipelines


def test_user_message_gets_reply_and_both_are_logged(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    try:
        result = hub.send("user", "a", "hello")
        assert result["status"] == "REPLIED"
        assert result["reply"]["text"] == "a got it"
        msgs = hub.log.read_all()
        assert [(m["id"], m["from"], m["to"], m["caused_by"]) for m in msgs] == [
            ("m1", "user", "a", None),
            ("m2", "a", "user", "m1"),
        ]
        assert msgs[1]["from_session"] == "session-a"
    finally:
        hub.stop()


def test_agent_to_agent_messages_chain_through_caused_by(short_dir):
    holder = {}

    def a_behaviour(prompt):
        reply = holder["hub"].send("a", "b", "please size M1")
        return "a summary: " + reply["reply"]["text"]

    hub, pipelines = make_hub(short_dir, {"a": a_behaviour}, a={}, b={})
    holder["hub"] = hub
    try:
        result = hub.send("user", "a", "design an opamp")
        assert result["reply"]["text"] == "a summary: b got it"
        chain = [(m["id"], m["from"], m["to"], m["caused_by"]) for m in hub.log.read_all()]
        assert chain == [
            ("m1", "user", "a", None),
            ("m2", "a", "b", "m1"),   # sent during the turn m1 started
            ("m3", "b", "a", "m2"),   # b's reply
            ("m4", "a", "user", "m1"),  # a's final reply to the user
        ]
        assert pipelines["b"].env["LANDLINE_AGENT"] == "b"
    finally:
        hub.stop()


def test_wait_cycle_is_rejected_instead_of_deadlocking(short_dir):
    holder = {}

    def a_behaviour(prompt):
        return holder["hub"].send("a", "b", "question for b")["reply"]["text"]

    def b_behaviour(prompt):
        try:
            holder["hub"].send("b", "a", "counter-question")
        except HubError as e:
            return f"refused: {e}"
        return "unexpected"

    hub, _ = make_hub(short_dir, {"a": a_behaviour, "b": b_behaviour}, a={}, b={})
    holder["hub"] = hub
    try:
        result = hub.send("user", "a", "go", timeout=5)
        assert result["status"] == "REPLIED"
        assert result["reply"]["text"].startswith("refused:")
        assert "deadlock" in result["reply"]["text"]
    finally:
        hub.stop()


def test_no_wait_send_is_queued_and_delivered_later(short_dir):
    hub, pipelines = make_hub(short_dir, {}, a={}, b={})
    try:
        result = hub.send("user", "b", "fyi", wait=False)
        assert result["status"] == "QUEUED"
        hub.send("user", "b", "second")  # waits, so the first has been handled too
        assert len(pipelines["b"].prompts) == 2
    finally:
        hub.stop()


def test_first_message_carries_intro_with_roles(short_dir):
    hub, pipelines = make_hub(short_dir, {}, a={"role": "architect"}, b={"role": "verification"})
    try:
        hub.send("user", "a", "one")
        hub.send("user", "a", "two")
        first, second = pipelines["a"].prompts
        assert "You are agent 'a'" in first and "Your role: architect" in first
        assert "- b: verification" in first and "message_agent" in first
        assert first.endswith("[landline message m1 from user]\none")
        assert second == "[landline message m3 from user]\ntwo"
    finally:
        hub.stop()


def test_send_validation(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    try:
        with pytest.raises(HubError, match="Unknown agent"):
            hub.send("user", "nobody", "x")
        with pytest.raises(HubError, match="itself"):
            hub.send("a", "a", "x")
        with pytest.raises(HubError, match="empty"):
            hub.send("user", "a", "  ")
        assert hub.send("a", "user", "status update")["status"] == "DELIVERED"
    finally:
        hub.stop()


def test_raw_events_are_normalized_and_fanned_out(short_dir):
    hub, pipelines = make_hub(short_dir, {}, a={"backend": "claude"}, b={})
    seen = []
    hub.subscribe(seen.append)
    try:
        for listener in pipelines["a"].listeners:
            listener({"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}})
        assert {"kind": "text", "text": "hi", "agent": "a"}.items() <= seen[-1].items()
        hub.send("user", "b", "x")
        kinds = [e["kind"] for e in seen]
        assert kinds[1:] == ["message", "turn_start", "message", "idle"]
    finally:
        hub.stop()


def test_run_file_records_agents_and_sessions(short_dir):
    hub, _ = make_hub(short_dir, {}, a={"backend": "claude", "model": "opus"}, b={})
    try:
        data = json.load(open(os.path.join(short_dir, "runs", "test-run", "run.json")))
        assert data["agents"]["a"]["model"] == "opus"
        assert data["agents"]["a"]["session_id"] == "session-a"
        assert data["messages"].endswith("messages.jsonl")
    finally:
        hub.stop()


def test_socket_api_roundtrip(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        assert [a["name"] for a in client.list_agents()["agents"]] == ["a", "b"]

        stream = client.subscribe()
        snapshot = next(stream)
        assert snapshot["run"] == "test-run"
        got = []
        reader = threading.Thread(target=lambda: got.extend(next(stream) for _ in range(4)), daemon=True)
        reader.start()

        reply = client.send("user", "a", "ping")
        assert reply["ok"] and reply["reply"]["text"] == "a got it"
        reader.join(timeout=5)
        assert [e["kind"] for e in got] == ["message", "turn_start", "message", "idle"]

        assert len(client.messages()["messages"]) == 2
        bad = client.send("user", "zzz", "x")
        assert bad["ok"] is False and "Unknown agent" in bad["error"]
    finally:
        server.stop()
        hub.stop()


def test_second_server_on_same_socket_is_refused(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    server = HubServer(hub)
    server.start()
    try:
        with pytest.raises(HubError, match="already listening"):
            HubServer(hub).start()
    finally:
        server.stop()
        hub.stop()


def test_client_without_hub_gives_clear_error(short_dir):
    with pytest.raises(HubError, match="landline up"):
        HubClient(os.path.join(short_dir, "none.sock")).list_agents()


def test_load_config(tmp_path):
    cfg = tmp_path / "run.json"
    cfg.write_text(json.dumps({"run": "opamp", "agents": {
        "architect": {"backend": "claude-code", "model": "opus", "role": "lead"},
        "tester": {"backend": "agy", "cwd": "~/x"},
    }}))
    loaded = load_config(str(cfg))
    assert loaded["run"] == "opamp"
    architect, tester = loaded["agents"]
    assert (architect.name, architect.backend, architect.model) == ("architect", "claude", "opus")
    assert tester.backend == "antigravity" and tester.cwd == os.path.expanduser("~/x")

    cfg.write_text(json.dumps({"agents": {"user": {}}}))
    with pytest.raises(HubError, match="reserved"):
        load_config(str(cfg))
