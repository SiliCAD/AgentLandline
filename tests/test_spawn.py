"""
Agents starting agents: the `spawns` setting, start_agent (new, existing session, copy of a
teammate, copy cut at a message), stop_agent for agents it started, and find_sessions.
"""

import json
import os
import shutil
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline.context import ContextError, DEFAULT_SPAWNS, normalize_spawns
from agentlandline.hub import AgentSpec, Hub, HubClient, HubError, HubServer, load_config
from test_context import FakeClaude, SESSIONS


@pytest.fixture
def short_dir():
    path = tempfile.mkdtemp(prefix="ll-", dir="/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def make_hub(home, run_id="team-run", **specs):
    made = []

    def factory(spec, env):
        made.append(FakeClaude(spec, env))
        return made[-1]

    hub = Hub([AgentSpec.from_dict(n, s) for n, s in specs.items()], run_id=run_id, home=home,
              socket_path=os.path.join(home, f"{run_id}.sock"), pipeline_factory=factory)
    hub.start()
    return hub, made


def latest(made, name):
    return [p for p in made if p.spec.name == name][-1]


def words(pipeline):
    return [p.split("\n")[-1] for p in pipeline.memory()]


# ------------------------------------------------------------------ settings

def test_spawns_settings():
    assert normalize_spawns(None) is None and normalize_spawns(False) is None
    assert normalize_spawns(True) == DEFAULT_SPAWNS
    rules = normalize_spawns({"max": 1, "backends": ["agy"], "fork_from": ["fixer"]})
    assert rules["backends"] == ["antigravity"] and rules["sessions"] is True and rules["max"] == 1
    for bad, message in [({"max": 0}, "max"), ({"colour": 1}, "unknown"), ({"backends": []}, "backends"),
                         ({"stop": "yes"}, "stop"), ("lots", "true or an object")]:
        with pytest.raises(ContextError, match=message):
            normalize_spawns(bad)


def test_spawns_is_read_saved_and_gives_the_tools(short_dir, tmp_path):
    config = tmp_path / "team.json"
    config.write_text(json.dumps({"agents": {"lead": {"spawns": {"max": 2}}, "fixer": {}}}))
    lead, fixer = load_config(str(config))["agents"]
    assert lead.spawns["max"] == 2 and fixer.spawns is None
    hub, made = make_hub(short_dir, lead={"spawns": True}, fixer={})
    try:
        assert latest(made, "lead").env.get("LANDLINE_SPAWNS") == "1" and "LANDLINE_SPAWNS" not in latest(made, "fixer").env
        saved = json.load(open(os.path.join(hub.run_dir, "run.json")))["agents"]
        assert saved["lead"]["spawns"] == DEFAULT_SPAWNS and "spawns" not in saved["fixer"]
    finally:
        hub.stop()


# ----------------------------------------------------------------- start/stop

def test_an_agent_starts_a_new_agent_and_can_message_and_stop_it(short_dir):
    hub, made = make_hub(short_dir, lead={"spawns": True}, fixer={})
    events = []
    hub.subscribe(events.append)
    try:
        hub.send("user", "fixer", "hello")  # fixer has been introduced to the team
        info = hub.start_agent("lead", "helper", role="checks units")
        assert (info["name"], info["spawned_by"], info["backend"]) == ("helper", "lead", "claude")
        assert any(e["kind"] == "agent_added" and e["agent"] == "helper" for e in events)
        assert hub.send("lead", "helper", "check this")["status"] == "REPLIED"
        hub.send("user", "fixer", "again")
        told = latest(made, "fixer").memory()[-1]
        assert "New agents have joined the session" in told and "- helper: checks units" in told
        saved = json.load(open(os.path.join(hub.run_dir, "run.json")))["agents"]["helper"]
        assert saved["spawned_by"] == "lead" and saved["role"] == "checks units"

        with pytest.raises(HubError, match="can only stop agents it started"):
            hub.stop_started("lead", "fixer")
        hub.stop_started("lead", "helper")
        assert "helper" not in hub.agents
    finally:
        hub.stop()


def test_a_copy_of_a_teammate_cut_at_a_message(short_dir):
    hub, made = make_hub(short_dir, lead={"spawns": True}, fixer={"model": "haiku", "effort": "low", "role": "fixes"})
    try:
        for text in ("approach A", "approach B", "approach C"):
            hub.send("lead", "fixer", text)
        info = hub.start_agent("lead", "fixer-2", from_agent="fixer", after="m1", role="tries approach D")
        copy, original = latest(made, "fixer-2"), latest(made, "fixer")
        assert words(copy) == ["approach A"]                           # cut after its reply to m1
        assert words(original) == ["approach A", "approach B", "approach C"]  # the original goes on as it was
        slot = hub.agents["fixer-2"]
        assert slot.spec.forked_from == original.conversation_id and slot.session_id != original.conversation_id
        assert (slot.spec.model, slot.spec.effort, slot.spec.role) == ("haiku", "low", "tries approach D")
        assert info["forked_from"] == original.conversation_id

        whole = hub.start_agent("lead", "fixer-3", from_agent="fixer")  # as it is now
        assert words(latest(made, "fixer-3")) == ["approach A", "approach B", "approach C"]
        assert whole["spawned_by"] == "lead" and hub.agents["fixer-3"].spec.role == "fixes"

        with pytest.raises(HubError, match="no turn that answered m2"):
            hub.start_agent("lead", "fixer-4", from_agent="fixer", after="m2")
    finally:
        hub.stop()


def test_opening_existing_sessions(short_dir):
    SESSIONS["s-saved"] = [("u1", "remember OTTER")]
    hub, made = make_hub(short_dir, lead={"spawns": True}, fixer={})
    try:
        opened = hub.start_agent("lead", "old", session_id="s-saved")
        assert opened["session_id"] == "s-saved" and words(latest(made, "old")) == ["remember OTTER"]
        in_use = hub.agents["fixer"].session_id
        with pytest.raises(HubError, match="in use by 'fixer'. Use fork=true"):
            hub.start_agent("lead", "twin", session_id=in_use)
        twin = hub.start_agent("lead", "twin", session_id=in_use, fork=True)
        assert twin["session_id"] != in_use and twin["forked_from"] == in_use
    finally:
        hub.stop()


def test_limits_and_permissions(short_dir):
    hub, _ = make_hub(short_dir,
                      lead={"spawns": {"max": 1, "backends": ["claude"], "sessions": False, "fork_from": ["fixer"], "stop": False}},
                      fixer={}, tester={})
    try:
        with pytest.raises(HubError, match="'fixer' isn't allowed to start agents"):
            hub.start_agent("fixer", "x")
        with pytest.raises(HubError, match="may start claude agents, not antigravity"):
            hub.start_agent("lead", "x", backend="agy")
        with pytest.raises(HubError, match="isn't allowed to open existing sessions"):
            hub.start_agent("lead", "x", session_id="s1")
        with pytest.raises(HubError, match="isn't allowed to open existing sessions"):
            hub.find_sessions("lead")
        hub.send("user", "tester", "hi")
        with pytest.raises(HubError, match="isn't allowed to copy tester"):
            hub.start_agent("lead", "x", from_agent="tester")
        with pytest.raises(HubError, match="'after' needs from_agent"):
            hub.start_agent("lead", "x", after="m1")
        with pytest.raises(HubError, match="already exists"):
            hub.start_agent("lead", "tester")
        hub.start_agent("lead", "one")
        with pytest.raises(HubError, match="its limit is 1"):
            hub.start_agent("lead", "two")
        with pytest.raises(HubError, match="spawns.stop is false"):
            hub.stop_started("lead", "one")
    finally:
        hub.stop()


def test_find_sessions_lists_saved_runs_newest_first(short_dir):
    old, _ = make_hub(short_dir, run_id="opamp-review", reviewer={"role": "reviews the opamp"})
    old.stop()
    hub, _ = make_hub(short_dir, run_id="bandgap", lead={"spawns": True}, coder={"role": "writes the bandgap"})
    try:
        found = hub.find_sessions("lead")
        runs = [(f["run"], f["agent"]) for f in found]
        assert ("opamp-review", "reviewer") in runs and ("bandgap", "coder") in runs
        here = next(f for f in found if f["agent"] == "coder")
        assert here["in_use_by"] == "coder" and here["session_id"] == hub.agents["coder"].session_id
        assert [f["agent"] for f in hub.find_sessions("lead", "OPAMP")] == ["reviewer"]
        assert hub.find_sessions("user", "nothing like this") == []
    finally:
        hub.stop()


def test_team_over_the_socket(short_dir):
    hub, _ = make_hub(short_dir, lead={"spawns": True}, fixer={})
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        hub.send("lead", "fixer", "one")
        hub.send("lead", "fixer", "two")
        started = client.team("lead", "start", name="fixer-2", from_agent="fixer", after="m1")
        assert started["ok"] and started["agent"]["spawned_by"] == "lead"
        assert client.team("lead", "sessions", query="fixer")["ok"]
        assert client.team("lead", "stop", name="fixer-2")["ok"]
        refused = client.team("fixer", "start", name="x")
        assert not refused["ok"] and "isn't allowed to start agents" in refused["error"]
        assert not client.team("lead", "dance")["ok"]
        spawned = client.spawn("boss", "claude", spawns={"max": 2})
        assert spawned["ok"] and hub.agents["boss"].spec.spawns["max"] == 2
    finally:
        server.stop()
        hub.stop()
