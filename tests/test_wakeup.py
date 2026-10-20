"""Wake-up rules: the hub messages an agent when the agents it watches have all been idle for a while."""

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

from agentlandline.context import ContextError, normalize_wakeup
from agentlandline.hub import AgentSpec, Hub, HubClient, HubError, HubServer


class FakeAgent:
    """`behaviour(prompt)` returns the reply text, or (text, status, error) for a turn that fails."""

    def __init__(self, spec, env, behaviour=None):
        self.spec, self.env = spec, env
        self.conversation_id = f"session-{spec.name}"
        self.behaviour = behaviour or (lambda prompt: f"{spec.name} done")
        self.prompts = []

    def add_event_listener(self, listener):
        pass

    def start(self, timeout=None):
        pass

    def close(self):
        pass

    def transcript_path(self):
        return None

    def send(self, prompt, timeout=None):
        self.prompts.append(prompt)
        out = self.behaviour(prompt)
        text, status, error = out if isinstance(out, tuple) else (out, "SUCCESS", None)
        return SimpleNamespace(response=text, status=status, error=error)


@pytest.fixture
def short_dir():
    path = tempfile.mkdtemp(prefix="ll-", dir="/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def make_hub(home, behaviours=None, **specs):
    made = {}

    def factory(spec, env):
        made[spec.name] = FakeAgent(spec, env, (behaviours or {}).get(spec.name))
        return made[spec.name]

    hub = Hub([AgentSpec.from_dict(n, s) for n, s in specs.items()], run_id="wake-run", home=home,
              socket_path=os.path.join(home, "hub.sock"), pipeline_factory=factory)
    hub.start()
    return hub, made


def wait_until(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    raise AssertionError("condition not met in time")


ALL_IDLE = {"when": "all", "idle_for": 30, "message": "The team stopped. Take over on agy."}


# ------------------------------------------------------------------ settings

def test_wakeup_settings():
    assert normalize_wakeup(None) is None and normalize_wakeup([]) is None and normalize_wakeup(False) is None
    assert normalize_wakeup({"message": "go"}) == [{"when": "all", "idle_for": 60, "message": "go"}]
    assert normalize_wakeup([{"when": "coder", "idle_for": 5, "message": "go"}]) == \
        [{"when": ["coder"], "idle_for": 5, "message": "go"}]
    for bad, message in [({"when": "all"}, "message"), ({"message": "go", "idle_for": 0}, "idle_for"),
                         ({"message": "go", "idle_for": True}, "idle_for"), ({"message": "go", "when": []}, "when"),
                         ({"message": "go", "colour": 1}, "unknown"), ("go", "must be an object")]:
        with pytest.raises(ContextError, match=message):
            normalize_wakeup(bad)
    with pytest.raises(ContextError, match="wait for itself"):
        normalize_wakeup({"when": ["rescuer"], "message": "go"}, "rescuer")


# ------------------------------------------------------------------ firing

def test_all_idle_wakes_the_agent_once_per_idle_stretch(short_dir):
    hub, made = make_hub(short_dir, coder={}, tester={}, rescuer={"backend": "agy", "wakeup": ALL_IDLE})
    try:
        assert hub.check_wakeups(time.monotonic() + 3600) == []  # nobody has worked yet: nothing to take over
        hub.send("user", "coder", "build it")
        assert hub.check_wakeups(time.monotonic()) == []        # not idle long enough
        assert hub.check_wakeups(time.monotonic() + 31) == ["rescuer"]
        wait_until(lambda: made["rescuer"].prompts)
        text = made["rescuer"].prompts[0]
        assert "[wake-up: All the other agents have been idle for 30s or more]" in text
        assert "The team stopped. Take over on agy." in text
        assert "- coder: idle 31s; its last turn, on m1 from user, finished" in text
        assert "- tester: hasn't handled a message yet" in text
        wait_until(lambda: not hub.agents["rescuer"].busy)

        # The rescuer's own work doesn't count: no new idle stretch, no second message.
        assert hub.check_wakeups(time.monotonic() + 100) == []
        hub.send("user", "tester", "test it")
        assert hub.check_wakeups(time.monotonic() + 100) == ["rescuer"]
        wait_until(lambda: len(made["rescuer"].prompts) == 2)
    finally:
        hub.stop()


def test_failed_turns_are_reported_in_the_wakeup(short_dir):
    hub, made = make_hub(short_dir, {"coder": lambda p: ("", "ERROR", "usage limit reached")},
                         coder={}, rescuer={"wakeup": ALL_IDLE})
    try:
        hub.send("user", "coder", "build it")
        assert hub.check_wakeups(time.monotonic() + 31) == ["rescuer"]
        wait_until(lambda: made["rescuer"].prompts)
        assert "- coder: idle 31s; its last turn, on m1 from user, failed: usage limit reached" in made["rescuer"].prompts[0]
    finally:
        hub.stop()


def test_agent_wise_rules_watch_only_their_agents(short_dir):
    release = threading.Event()
    hub, made = make_hub(short_dir, {"tester": lambda p: release.wait(5) and "tested"},
                         coder={}, tester={},
                         watcher={"wakeup": {"when": ["coder"], "idle_for": 10, "message": "coder stopped"}},
                         rescuer={"wakeup": ALL_IDLE})
    try:
        hub.send("user", "coder", "build it")
        hub.send("user", "tester", "test it", wait=False)
        wait_until(lambda: hub.agents["tester"].busy)
        # tester is still working: only the rule that watches just coder fires.
        assert hub.check_wakeups(time.monotonic() + 60) == ["watcher"]
        wait_until(lambda: made["watcher"].prompts)
        wakeup = made["watcher"].prompts[0].split("[wake-up:")[1]
        assert wakeup.count("\n- ") == 1 and wakeup.startswith(" coder has been idle for 10s or more]")
        release.set()
        wait_until(lambda: not hub.agents["tester"].busy)
        assert hub.check_wakeups(time.monotonic() + 60) == ["rescuer"]
    finally:
        release.set()
        hub.stop()


def test_queued_messages_count_as_work(short_dir):
    hub, made = make_hub(short_dir, coder={}, rescuer={"wakeup": ALL_IDLE})
    try:
        hub.send("user", "coder", "build it")
        hub.agents["coder"].inbox.put(None)  # stands in for a message waiting to be handled
        assert hub.check_wakeups(time.monotonic() + 60) == []
    finally:
        hub.stop()


def test_the_hub_checks_on_its_own(short_dir, monkeypatch):
    monkeypatch.setattr(Hub, "WAKEUP_TICK", 0.02)
    hub, made = make_hub(short_dir, coder={},
                         rescuer={"wakeup": {"when": "all", "idle_for": 0.1, "message": "take over"}})
    try:
        hub.send("user", "coder", "build it")
        wait_until(lambda: made["rescuer"].prompts)
        assert "take over" in made["rescuer"].prompts[0]
        records = hub.log.read_all()
        assert (records[-2]["from"], records[-2]["to"]) == ("user", "rescuer")  # the wake-up, sent as the user
    finally:
        hub.stop()


# ------------------------------------------------------------------ settings while running

def test_wakeup_rules_can_be_set_kept_and_cleared(short_dir):
    hub, made = make_hub(short_dir, coder={}, rescuer={})
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        result = client.set_permissions("rescuer", wakeup=ALL_IDLE)
        assert result["ok"] and result["restart"] is None  # no new tools: no restart
        assert result["agent"]["wakeup"] == [ALL_IDLE]
        saved = json.load(open(os.path.join(hub.run_dir, "run.json")))["agents"]
        assert saved["rescuer"]["wakeup"] == [ALL_IDLE] and "wakeup" not in saved["coder"]

        # A client that doesn't send wakeup (an older viewer) leaves the rules alone.
        assert client.set_permissions("rescuer", spawns=True)["agent"]["wakeup"] == [ALL_IDLE]
        assert client.set_permissions("rescuer", wakeup=None)["agent"]["wakeup"] is None

        bad = client.set_permissions("rescuer", wakeup={"when": ["rescuer"], "message": "x"})
        assert not bad["ok"] and "wait for itself" in bad["error"]

        spawned = client.spawn("helper", "claude", wakeup={"when": ["coder"], "message": "go"})
        assert spawned["ok"] and spawned["agent"]["wakeup"][0]["when"] == ["coder"]
    finally:
        server.stop()
        hub.stop()


def test_changed_rules_fire_again_but_saving_the_same_rules_doesnt(short_dir):
    hub, made = make_hub(short_dir, coder={}, rescuer={"wakeup": ALL_IDLE})
    try:
        hub.send("user", "coder", "build it")
        assert hub.check_wakeups(time.monotonic() + 31) == ["rescuer"]
        hub.set_permissions("rescuer", wakeup=ALL_IDLE)
        assert hub.check_wakeups(time.monotonic() + 31) == []
        hub.set_permissions("rescuer", wakeup={**ALL_IDLE, "message": "Something else"})
        assert hub.check_wakeups(time.monotonic() + 31) == ["rescuer"]
    finally:
        hub.stop()


def test_wakeup_rules_survive_a_restart_of_the_team(short_dir):
    hub, made = make_hub(short_dir, coder={}, rescuer={"wakeup": ALL_IDLE})
    hub.stop()
    from agentlandline.hub import load_config
    specs = {s.name: s for s in load_config(os.path.join(hub.run_dir, "run.json"))["agents"]}
    assert specs["rescuer"].wakeup == [ALL_IDLE] and specs["coder"].wakeup is None
