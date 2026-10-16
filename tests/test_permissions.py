"""Changing an agent's permissions (manages, spawns) while it runs."""

import json
import os
import shutil
import sys
import tempfile
import threading
import time

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline.context import DEFAULT_SPAWNS
from agentlandline.hub import HubClient, HubError, HubServer
from test_spawn import latest, make_hub, words


@pytest.fixture
def short_dir():
    path = tempfile.mkdtemp(prefix="ll-", dir="/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def test_rules_apply_at_once_and_new_tools_restart_the_agent_on_its_conversation(short_dir):
    hub, made = make_hub(short_dir, lead={}, fixer={})
    events = []
    hub.subscribe(events.append)
    try:
        hub.send("user", "lead", "remember PELICAN")
        before = latest(made, "lead")
        result = hub.set_permissions("lead", manages=["fixer"], spawns={"max": 1})
        assert result["restart"] == "now"
        after = latest(made, "lead")
        assert after is not before and before.closed
        assert after.env.get("LANDLINE_MANAGES") == "1" and after.env.get("LANDLINE_SPAWNS") == "1"
        assert after.conversation_id == before.conversation_id and words(after) == ["remember PELICAN"]
        assert result["agent"]["manages"] == {"fixer": {}} and result["agent"]["spawns"]["max"] == 1
        assert hub.context_turns("lead", "fixer")["agent"] == "fixer"  # allowed now
        hub.start_agent("lead", "helper")
        saved = json.load(open(os.path.join(hub.run_dir, "run.json")))["agents"]["lead"]
        assert saved["manages"] == {"fixer": {}} and saved["spawns"]["max"] == 1
        updated = [e for e in events if e["kind"] == "agent_updated"]
        assert updated and updated[-1]["agent"] == "lead" and updated[-1]["restart"] == "now"

        # Only the rules change: no restart.
        assert hub.set_permissions("lead", manages={"fixer": {"actions": ["list"]}}, spawns={"max": 2})["restart"] is None
        assert latest(made, "lead") is after
        with pytest.raises(HubError, match=r"'rewind' isn't allowed"):
            hub.rewind("lead", "fixer", "m1")

        # Taking them away removes the tools.
        assert hub.set_permissions("lead")["restart"] == "now"
        assert "LANDLINE_MANAGES" not in latest(made, "lead").env
        with pytest.raises(HubError, match="isn't allowed to start agents"):
            hub.start_agent("lead", "other")
    finally:
        hub.stop()


def test_a_busy_agent_restarts_after_its_turn_and_keeps_waiting_messages(short_dir):
    hub, made = make_hub(short_dir, lead={}, fixer={})
    gate = threading.Event()
    first = latest(made, "lead")
    original_send = first.send

    def slow_send(prompt, timeout=None, **kwargs):
        gate.wait(5)
        return original_send(prompt, timeout, **kwargs)

    first.send = slow_send
    try:
        hub.send("user", "lead", "long job", wait=False)
        deadline = time.monotonic() + 5
        while not hub.agents["lead"].busy:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        hub.send("user", "lead", "queued job", wait=False)
        assert hub.set_permissions("lead", spawns=True)["restart"] == "after its turn"
        assert hub.agents["lead"].spec.spawns == DEFAULT_SPAWNS  # the rules apply already
        gate.set()
        deadline = time.monotonic() + 5
        while len([m for m in hub.log.read_all() if m["from"] == "lead"]) < 2:
            assert time.monotonic() < deadline, "the queued message wasn't answered"
            time.sleep(0.02)
        second = latest(made, "lead")
        assert second is not first and second.env.get("LANDLINE_SPAWNS") == "1"
        assert words(second)[-1] == "queued job"  # answered by the restarted CLI, same conversation
    finally:
        gate.set()
        hub.stop()


def test_bad_permissions_are_refused(short_dir):
    hub, _ = make_hub(short_dir, lead={}, fixer={})
    try:
        with pytest.raises(HubError, match="Unknown agent"):
            hub.set_permissions("nobody")
        with pytest.raises(HubError, match="can't manage its own context"):
            hub.set_permissions("lead", manages=["lead"])
        with pytest.raises(HubError, match="spawns.max"):
            hub.set_permissions("lead", spawns={"max": 0})
        assert hub.agents["lead"].spec.manages is None and hub.agents["lead"].spec.spawns is None
    finally:
        hub.stop()


def test_permissions_over_the_socket_are_for_the_user_only(short_dir):
    hub, _ = make_hub(short_dir, lead={}, fixer={})
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        done = client.set_permissions("lead", manages=["fixer"])
        assert done["ok"] and done["agent"]["manages"] == {"fixer": {}}
        refused = client.request({"op": "permissions", "from": "fixer", "agent": "fixer", "spawns": True})
        assert not refused["ok"] and "Only the user" in refused["error"]
        listed = {a["name"]: a for a in client.list_agents()["agents"]}
        assert listed["lead"]["manages"] == {"fixer": {}} and listed["fixer"]["spawns"] is None
    finally:
        server.stop()
        hub.stop()
