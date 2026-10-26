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
import time
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


def test_agents_start_in_parallel_with_backend_timeouts(short_dir):
    started = []

    class SlowStart(FakePipeline):
        def start(self, timeout=None):
            started.append((self.spec.name, timeout))
            import time
            time.sleep(0.3)

    def factory(spec, env):
        return SlowStart(spec, env)

    import time
    hub = Hub([AgentSpec("a", backend="claude"), AgentSpec("b", backend="antigravity"), AgentSpec("c", backend="claude")],
              run_id="t", home=short_dir, socket_path=os.path.join(short_dir, "s.sock"), pipeline_factory=factory)
    t0 = time.monotonic()
    hub.start()
    try:
        assert time.monotonic() - t0 < 0.8  # three 0.3s starts overlapped
        assert dict(started) == {"a": 5.0, "b": 60.0, "c": 5.0}
    finally:
        hub.stop()


def test_failed_agent_start_raises(short_dir):
    class Broken(FakePipeline):
        def start(self, timeout=None):
            raise RuntimeError("claude process exited during startup")

    hub = Hub([AgentSpec("a")], run_id="t", home=short_dir, socket_path=os.path.join(short_dir, "s.sock"),
              pipeline_factory=lambda spec, env: Broken(spec, env))
    with pytest.raises(HubError, match="a: claude process exited"):
        hub.start()


def test_load_config_errors_are_readable(tmp_path):
    with pytest.raises(HubError, match="Run config not found: .*missing.json"):
        load_config(str(tmp_path / "missing.json"))
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    with pytest.raises(HubError, match="not valid JSON"):
        load_config(str(bad))
    bad.write_text("[]")
    with pytest.raises(HubError, match="expected a JSON object"):
        load_config(str(bad))


def test_model_in_use_prefers_reported_then_configured_then_agy_default(short_dir, tmp_path):
    settings = tmp_path / "agy"
    settings.mkdir()
    (settings / "settings.json").write_text(json.dumps({"model": "Gemini 3.8 Flash (High)"}))
    from unittest.mock import patch
    with patch.dict(os.environ, {"ANTIGRAVITY_APP_DATA_DIR": str(settings)}):
        hub, pipelines = make_hub(short_dir, {}, a={"backend": "claude"}, b={"backend": "claude", "model": "opus"},
                                  c={"backend": "antigravity"})
        seen = []
        hub.subscribe(seen.append)
        try:
            models = {a["name"]: a["model"] for a in hub.list_agents()}
            assert models == {"a": None, "b": "opus", "c": "Gemini 3.8 Flash (High)"}
            for listener in pipelines["a"].listeners:
                listener({"type": "system", "subtype": "init", "session_id": "s", "model": "claude-sonnet-5-5"})
            assert {a["name"]: a["model"] for a in hub.list_agents()}["a"] == "claude-sonnet-5-5"
            assert seen[-1] == {**seen[-1], "kind": "session", "agent": "a", "model": "claude-sonnet-5-5"}
            data = json.load(open(os.path.join(short_dir, "runs", "test-run", "run.json")))
            assert data["agents"]["a"]["model_in_use"] == "claude-sonnet-5-5"
        finally:
            hub.stop()


def test_agy_default_model_missing_settings(tmp_path):
    from unittest.mock import patch
    from agentlandline.agy_pipeline import AgyPipeline
    with patch.dict(os.environ, {"ANTIGRAVITY_APP_DATA_DIR": str(tmp_path)}):
        assert AgyPipeline.default_model() is None


# --- spawning agents at runtime ----------------------------------------------

def test_spawn_adds_a_working_agent(short_dir):
    hub, pipelines = make_hub(short_dir, {}, a={"role": "lead"}, b={})
    seen = []
    hub.subscribe(seen.append)
    try:
        info = hub.spawn(AgentSpec("c", backend="claude", model="sonnet", role="reviews"))
        assert info["name"] == "c" and info["model"] == "sonnet" and info["role"] == "reviews"
        assert [a["name"] for a in hub.list_agents()] == ["a", "b", "c"]
        added = [e for e in seen if e["kind"] == "agent_added"]
        assert added and added[0]["agent"] == "c" and added[0]["info"]["role"] == "reviews"
        assert hub.send("user", "c", "hi")["reply"]["text"] == "c got it"
        data = json.load(open(os.path.join(short_dir, "runs", "test-run", "run.json")))
        assert data["agents"]["c"]["role"] == "reviews"
    finally:
        hub.stop()


def test_spawn_rejects_bad_requests_and_failed_starts(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    try:
        with pytest.raises(HubError, match="already exists"):
            hub.spawn(AgentSpec("a"))
        with pytest.raises(HubError, match="Invalid agent name"):
            AgentSpec.from_dict("bad name!", {"backend": "claude"})
        with pytest.raises(HubError, match="Invalid agent name"):
            AgentSpec.from_dict("user", {"backend": "claude"})
        with pytest.raises(HubError, match="Unknown agent backend"):
            AgentSpec.from_dict("x", {"backend": "gpt"})

        class Broken(FakePipeline):
            def start(self, timeout=None):
                raise RuntimeError("claude process exited during startup")
        hub._pipeline_factory = lambda spec, env: Broken(spec, env)
        with pytest.raises(HubError, match="Couldn't start 'x'"):
            hub.spawn(AgentSpec("x"))
        assert "x" not in hub.agents
    finally:
        hub.stop()


def test_existing_agents_are_told_once_about_new_ones(short_dir):
    hub, pipelines = make_hub(short_dir, {}, a={"role": "lead"}, b={})
    try:
        hub.send("user", "a", "first")  # intro: knows b
        hub.spawn(AgentSpec("c", role="reviews"))
        hub.send("user", "a", "second")
        hub.send("user", "a", "third")
        hub.send("user", "c", "hello")
        first, second, third = pipelines["a"].prompts
        assert "- b" in first and "- c" not in first
        assert second.startswith("New agents have joined the session:\n- c: reviews\n")
        assert "joined" not in third
        c_intro = hub.agents["c"].pipeline.prompts[0]
        assert "You are agent 'c'" in c_intro and "- a: lead" in c_intro and "- b" in c_intro
    finally:
        hub.stop()


def test_spawn_over_the_socket(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        res = client.spawn("tester", "agy", role="runs tests")
        assert res["ok"] and res["agent"]["backend"] == "antigravity"
        assert client.spawn("tester", "claude")["error"].startswith("An agent named 'tester' already exists")
        assert "Invalid agent name" in client.spawn("no spaces", "claude")["error"]
    finally:
        server.stop()
        hub.stop()


def test_run_file_can_restart_the_whole_team(short_dir):
    hub, _ = make_hub(short_dir, {}, a={"role": "lead"}, b={})
    try:
        hub.spawn(AgentSpec("c", backend="claude", model="sonnet", role="reviews"))
    finally:
        hub.stop()
    loaded = load_config(os.path.join(short_dir, "runs", "test-run", "run.json"))
    assert loaded["run"] == "test-run"
    by_name = {a.name: a for a in loaded["agents"]}
    assert set(by_name) == {"a", "b", "c"}
    assert by_name["c"].model == "sonnet" and by_name["c"].role == "reviews"
    assert by_name["c"].session_id == "session-c"  # resumes the same CLI session


def test_run_file_gets_session_id_even_before_pipeline_stores_it(short_dir):
    # Real pipelines call listeners before setting conversation_id themselves.
    hub, pipelines = make_hub(short_dir, {}, a={"backend": "claude"}, b={})
    try:
        pipelines["a"].conversation_id = None
        for listener in pipelines["a"].listeners:
            listener({"type": "system", "subtype": "init", "session_id": "late-session"})
        data = json.load(open(os.path.join(short_dir, "runs", "test-run", "run.json")))
        assert data["agents"]["a"]["session_id"] == "late-session"
        assert {a["name"]: a["session_id"] for a in hub.list_agents()}["a"] == "late-session"
    finally:
        hub.stop()


def test_spawn_over_the_socket_can_resume_a_session(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    server = HubServer(hub)
    server.start()
    try:
        assert HubClient(hub.socket_path).spawn("c", "claude", session_id="old-session")["ok"]
        assert hub.agents["c"].spec.session_id == "old-session"
    finally:
        server.stop()
        hub.stop()


def test_empty_hub_starts_and_takes_spawns(short_dir):
    hub = Hub([], run_id="empty", home=short_dir, socket_path=os.path.join(short_dir, "e.sock"),
              pipeline_factory=lambda spec, env: FakePipeline(spec, env))
    hub.start()
    try:
        assert hub.list_agents() == []
        hub.spawn(AgentSpec("solo"))
        assert hub.send("user", "solo", "hi")["reply"]["text"] == "solo got it"
    finally:
        hub.stop()


def test_spawns_start_in_parallel_and_names_stay_unique(short_dir):
    import time

    class SlowStart(FakePipeline):
        def start(self, timeout=None):
            time.sleep(0.4)

    hub = Hub([], run_id="par", home=short_dir, socket_path=os.path.join(short_dir, "p.sock"),
              pipeline_factory=lambda spec, env: SlowStart(spec, env))
    hub.start()
    results, errors = [], []

    def spawn(name):
        try:
            results.append(hub.spawn(AgentSpec(name))["name"])
        except HubError as e:
            errors.append(str(e))
    try:
        t0 = time.monotonic()
        threads = [threading.Thread(target=spawn, args=(n,)) for n in ("a", "b", "c", "a")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert time.monotonic() - t0 < 1.0  # four 0.4s starts overlapped
        assert sorted(results) == ["a", "b", "c"]
        assert errors == ["An agent named 'a' already exists."]
    finally:
        hub.stop()


def test_run_file_records_the_folder_each_agent_works_in(short_dir):
    hub, pipelines = make_hub(short_dir, {}, a={}, b={"cwd": "/tmp"})
    try:
        pipelines["a"].cwd = "/Users/someone/project"  # pipelines default to the hub's folder
        hub._write_run_file()
        data = json.load(open(os.path.join(short_dir, "runs", "test-run", "run.json")))
        assert data["agents"]["a"]["cwd"] == "/Users/someone/project"
        assert data["agents"]["b"]["cwd"] == "/tmp"
        assert data["cwd"] == os.getcwd()
        reloaded = {s.name: s for s in load_config(os.path.join(short_dir, "runs", "test-run", "run.json"))["agents"]}
        assert reloaded["a"].cwd == "/Users/someone/project"
    finally:
        hub.stop()


# --- stopping single agents and renaming the session --------------------------

class GatedPipeline(FakePipeline):
    """send() blocks until released, like an agent in the middle of a long turn."""

    def __init__(self, spec, env, behaviour=None):
        super().__init__(spec, env, behaviour)
        self.release = threading.Event()
        self.entered = threading.Event()

    def send(self, prompt, timeout=None):
        self.entered.set()
        self.release.wait(5)
        return super().send(prompt, timeout)

    def close(self):
        self.release.set()  # closing the CLI ends the turn


def test_stop_agent_removes_it_but_keeps_it_resumable(short_dir):
    hub, _ = make_hub(short_dir, {}, a={"role": "lead"}, b={"role": "helper"})
    seen = []
    hub.subscribe(seen.append)
    try:
        info = hub.stop_agent("b")
        assert info["name"] == "b"
        assert [a["name"] for a in hub.list_agents()] == ["a"]
        assert {"kind": "agent_removed", "agent": "b"}.items() <= seen[-1].items()
        run_file = os.path.join(short_dir, "runs", "test-run", "run.json")
        data = json.load(open(run_file))
        assert data["agents"]["b"]["stopped"] is True and "stopped" not in data["agents"]["a"]
        assert {s.name for s in load_config(run_file)["agents"]} == {"a", "b"}  # resume brings it back
        with pytest.raises(HubError, match="Unknown agent 'b'"):
            hub.send("user", "b", "hi")
        with pytest.raises(HubError, match="Unknown agent 'zz'"):
            hub.stop_agent("zz")
    finally:
        hub.stop()


def test_stopping_a_busy_agent_fails_its_messages_without_fake_replies(short_dir):
    pipelines = {}

    def factory(spec, env):
        pipelines[spec.name] = (GatedPipeline if spec.name == "b" else FakePipeline)(spec, env)
        return pipelines[spec.name]
    hub = Hub([AgentSpec("a"), AgentSpec("b")], run_id="busy", home=short_dir,
              socket_path=os.path.join(short_dir, "b.sock"), pipeline_factory=factory)
    hub.start()
    results = {}
    try:
        first = threading.Thread(target=lambda: results.setdefault("first", hub.send("user", "b", "long job")))
        first.start()
        assert pipelines["b"].entered.wait(5)  # b is mid-turn
        second = threading.Thread(target=lambda: results.setdefault("second", hub.send("a", "b", "queued")))
        second.start()
        time.sleep(0.2)
        hub.stop_agent("b")
        first.join(5)
        second.join(5)
        assert results["first"]["status"] == "ERROR" and "stopped before replying" in results["first"]["error"]
        assert results["second"]["status"] == "ERROR" and "was stopped" in results["second"]["error"]
        assert [m["from"] for m in hub.log.read_all()] == ["user", "a"]  # no reply from b was invented
    finally:
        hub.stop()


def test_agents_are_told_who_left_and_names_can_be_reused(short_dir):
    hub, pipelines = make_hub(short_dir, {}, a={}, b={})
    try:
        hub.send("user", "a", "first")
        hub.stop_agent("b")
        hub.send("user", "a", "second")
        assert pipelines["a"].prompts[1].startswith("Agents that have left the session: b\n")
        hub.spawn(AgentSpec("b", role="back again"))
        assert hub.send("user", "b", "hi")["status"] == "REPLIED"
        data = json.load(open(os.path.join(short_dir, "runs", "test-run", "run.json")))
        assert "stopped" not in data["agents"]["b"]
    finally:
        hub.stop()


def test_rename_moves_the_session_and_logging_continues(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    seen = []
    hub.subscribe(seen.append)
    try:
        hub.send("user", "a", "before")
        assert hub.rename("opamp-review") == "opamp-review"
        runs = os.path.join(short_dir, "runs")
        assert not os.path.exists(os.path.join(runs, "test-run"))
        hub.send("user", "a", "after")
        texts = [m["text"] for m in MessageLogReader(os.path.join(runs, "opamp-review"))]
        assert texts == ["before", "a got it", "after", "a got it"]
        data = json.load(open(os.path.join(runs, "opamp-review", "run.json")))
        assert data["run"] == "opamp-review" and data["messages"].endswith("opamp-review/messages.jsonl")
        renamed = [e for e in seen if e["kind"] == "run_renamed"]
        assert len(renamed) == 1 and renamed[0]["run"] == "opamp-review" and renamed[0]["previous"] == "test-run"
        assert hub.rename("opamp-review") == "opamp-review"  # same name: nothing to do
        os.makedirs(os.path.join(runs, "taken"))
        with pytest.raises(HubError, match="already exists"):
            hub.rename("taken")
        with pytest.raises(HubError, match="Invalid session name"):
            hub.rename("has spaces")
    finally:
        hub.stop()


def MessageLogReader(run_dir):
    from agentlandline.message_log import MessageLog
    return MessageLog(run_dir).read_all()


def test_stop_and_rename_over_the_socket(short_dir):
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        assert client.stop_agent("b")["ok"]
        assert "Unknown agent" in client.stop_agent("b")["error"]
        assert client.rename("my-session") == {"ok": True, "run": "my-session"}
        assert client.list_agents()["run"] == "my-session"
    finally:
        server.stop()
        hub.stop()


def test_all_is_reserved_like_user(tmp_path):
    for name in ("all", "All", "user"):
        with pytest.raises(HubError, match="Invalid agent name"):
            AgentSpec.from_dict(name, {"backend": "claude"})
    cfg = tmp_path / "run.json"
    cfg.write_text(json.dumps({"agents": {"all": {}}}))
    with pytest.raises(HubError, match="reserved"):
        load_config(str(cfg))


# --- continuing a saved run -------------------------------------------------

def _saved_run(home, run_id, agents, messages):
    run_dir = os.path.join(home, "runs", run_id)
    os.makedirs(run_dir)
    with open(os.path.join(run_dir, "run.json"), "w") as f:
        json.dump({"run": run_id, "started": "2026-10-01T00:00:00Z", "agents": agents}, f)
    from agentlandline.message_log import MessageLog
    log = MessageLog(run_dir)
    for text in messages:
        log.append("user", next(iter(agents)), text)


def test_adopt_continues_a_saved_run(short_dir):
    _saved_run(short_dir, "old-session", {"a": {"backend": "claude", "session_id": "s-a"},
                                          "z": {"backend": "agy", "session_id": "s-z", "role": "helper"}}, ["one", "two"])
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    seen = []
    hub.subscribe(seen.append)
    try:
        fresh_dir = hub.run_dir
        assert hub.adopt("old-session") == "old-session"
        assert hub.run_id == "old-session" and not os.path.exists(fresh_dir)  # empty fresh run removed
        hub.send("user", "a", "three")
        assert [m["id"] for m in hub.log.read_all()] == ["m1", "m2", "m3", "m4"]  # numbering continues
        data = json.load(open(os.path.join(short_dir, "runs", "old-session", "run.json")))
        assert data["started"] == "2026-10-01T00:00:00Z"
        assert data["agents"]["z"]["role"] == "helper"  # not running yet, still listed
        hub.spawn(AgentSpec("z", backend="claude"))
        data = json.load(open(os.path.join(short_dir, "runs", "old-session", "run.json")))
        assert data["agents"]["z"]["backend"] == "claude"  # the running agent's entry now
        assert [e for e in seen if e["kind"] == "run_switched"][0]["previous"] == "test-run"
    finally:
        hub.stop()


def test_adopt_refuses_when_the_current_run_has_messages_or_the_run_is_unknown(short_dir):
    _saved_run(short_dir, "old-session", {"a": {"backend": "claude"}}, ["one"])
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    try:
        with pytest.raises(HubError, match="No saved session 'nope'"):
            hub.adopt("nope")
        hub.send("user", "a", "hi")
        with pytest.raises(HubError, match="already has messages"):
            hub.adopt("old-session")
        assert hub.run_id == "test-run"
    finally:
        hub.stop()


def test_adopt_over_the_socket(short_dir):
    _saved_run(short_dir, "old-session", {"a": {"backend": "claude"}}, [])
    hub, _ = make_hub(short_dir, {}, a={}, b={})
    server = HubServer(hub)
    server.start()
    try:
        assert HubClient(hub.socket_path).adopt("old-session") == {"ok": True, "run": "old-session"}
        assert HubClient(hub.socket_path).list_agents()["run"] == "old-session"
    finally:
        server.stop()
        hub.stop()


def test_default_socket_is_per_folder(tmp_path, monkeypatch):
    from agentlandline.hub import default_socket_path
    monkeypatch.delenv("LANDLINE_SOCKET", raising=False)
    monkeypatch.setenv("LANDLINE_HOME", str(tmp_path / "home"))
    a, b = tmp_path / "opamp", tmp_path / "bandgap"
    a.mkdir()
    b.mkdir()
    sock_a, sock_b = default_socket_path(str(a)), default_socket_path(str(b))
    assert sock_a != sock_b
    assert os.path.dirname(sock_a) == str(tmp_path / "home" / "hubs")
    assert os.path.basename(sock_a).startswith("opamp-") and sock_a.endswith(".sock")
    assert default_socket_path(str(a / ".")) == sock_a  # same folder, same hub
    monkeypatch.chdir(a)
    assert default_socket_path() == sock_a
    monkeypatch.setenv("LANDLINE_SOCKET", "/tmp/explicit.sock")
    assert default_socket_path(str(b)) == "/tmp/explicit.sock"


def test_a_sender_that_is_not_an_agent_gets_the_reply_not_the_user(short_dir):
    """A tool (eda-mcp) sends a watcher agent its calls: the reply goes back to it, not to the user."""
    holder = {}

    def watcher(prompt):
        if "needs the user" in prompt:
            holder["hub"].send("watcher", "user", "Please look at the DRC run.", wait=False)
        return "ok"

    hub, pipelines = make_hub(short_dir, {"watcher": watcher}, watcher={})
    holder["hub"] = hub
    try:
        with pytest.raises(HubError, match="Unknown sender"):
            hub.send("eda-mcp", "watcher", "batch 1")
        assert hub.add_sender("eda-mcp") == ["eda-mcp"] == hub.add_sender("eda-mcp")
        result = hub.send("eda-mcp", "watcher", "batch 1")
        assert result["status"] == "REPLIED" and result["reply"]["to"] == "eda-mcp"
        assert "[landline message m1 from eda-mcp]\nbatch 1" in pipelines["watcher"].prompts[-1]
        hub.send("eda-mcp", "watcher", "batch 2: needs the user")
        log = [(m["from"], m["to"], m["caused_by"]) for m in hub.log.read_all()]
        assert log == [("eda-mcp", "watcher", None), ("watcher", "eda-mcp", "m1"),
                       ("eda-mcp", "watcher", None), ("watcher", "user", "m3"), ("watcher", "eda-mcp", "m3")]
        assert hub.log.read_all()[0]["from_session"] is None  # not an agent: no session behind it
        with pytest.raises(HubError, match="Only the user can interrupt"):
            hub.send("eda-mcp", "watcher", "now", force=True)
    finally:
        hub.stop()


def test_sender_names_are_checked_and_never_agents(short_dir):
    hub, _ = make_hub(short_dir, {}, a={})
    try:
        for bad in ("user", "all", "9lives", "", "has space"):
            with pytest.raises(HubError, match="Invalid sender name"):
                hub.add_sender(bad)
        with pytest.raises(HubError, match="is an agent"):
            hub.add_sender("a")
        hub.add_sender("eda-mcp")
        with pytest.raises(HubError, match="is a sender in this session"):
            hub.spawn(AgentSpec("eda-mcp"))
    finally:
        hub.stop()


def test_the_sender_op_is_for_the_user(short_dir):
    hub, _ = make_hub(short_dir, {}, a={})
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        assert client.add_sender("eda-mcp") == {"ok": True, "senders": ["eda-mcp"]}
        refused = client.request({"op": "sender", "from": "a", "name": "other"})
        assert not refused["ok"] and "Only the user" in refused["error"]
        assert client.send("eda-mcp", "a", "batch", wait=True)["reply"]["to"] == "eda-mcp"
    finally:
        server.stop()
        hub.stop()
