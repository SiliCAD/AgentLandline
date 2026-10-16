"""
Context management: 'manages' settings, per-turn checkpoints, and rewinding an
agent to just after one of its replies. The fake claude keeps every session's
history, and a rewound copy starts from its parent's history cut at the given
message uuid, so tests can check what the rewound agent still "remembers".
"""

import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline.claude_pipeline import ClaudePipeline
from agentlandline.context import ContextError, normalize_manages, rules_for
from agentlandline.hub import AgentSpec, Hub, HubClient, HubError, HubServer, load_config


SESSIONS = {}  # session id -> [(assistant uuid, prompt), ...]


class FakeClaude:
    def __init__(self, spec, env):
        self.spec, self.env = spec, env
        self.listeners = []
        self.closed = False
        rewind = spec.rewind
        if rewind:
            parent = SESSIONS[spec.session_id]
            cut = next(i for i, (u, _) in enumerate(parent) if u == rewind["at"])
            self.conversation_id = rewind["session_id"]
            SESSIONS[self.conversation_id] = list(parent[: cut + 1])
        else:
            self.conversation_id = spec.session_id or f"session-{spec.name}-{uuid.uuid4().hex[:6]}"
            SESSIONS.setdefault(self.conversation_id, [])

    def add_event_listener(self, listener):
        self.listeners.append(listener)

    def start(self, timeout=None):
        pass

    def close(self):
        self.closed = True

    def transcript_path(self):
        return None

    def memory(self):
        return [p for _, p in SESSIONS[self.conversation_id]]

    def send(self, prompt, timeout=None):
        message_uuid = str(uuid.uuid4())
        SESSIONS[self.conversation_id].append((message_uuid, prompt))
        for listener in self.listeners:
            listener({"type": "assistant", "uuid": message_uuid, "message": {"content": [
                {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}]}})
        return SimpleNamespace(response=f"{self.spec.name} did {len(SESSIONS[self.conversation_id])}",
                               status="SUCCESS", error=None)


@pytest.fixture
def short_dir():
    path = tempfile.mkdtemp(prefix="ll-", dir="/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


def make_hub(home, **specs):
    pipelines = []

    def factory(spec, env):
        pipelines.append(FakeClaude(spec, env))
        return pipelines[-1]

    hub = Hub([AgentSpec(name=n, **s) for n, s in specs.items()], run_id="ctx-run", home=home,
              socket_path=os.path.join(home, "hub.sock"), pipeline_factory=factory)
    hub.start()
    return hub, pipelines


def latest(pipelines, name):
    return [p for p in pipelines if p.spec.name == name][-1]


LEAD_FIXER = dict(lead={"manages": normalize_manages(["fixer"])}, fixer={})


# ------------------------------------------------------------------ settings

def test_manages_short_and_full_forms():
    assert normalize_manages(None) is None and normalize_manages([]) is None
    assert normalize_manages(["fixer"]) == {"fixer": {}}
    assert normalize_manages("fixer") == {"fixer": {}}
    full = normalize_manages({"fixer": {"note": "required", "backends": ["claude-code"]}, "*": {"actions": ["list"]}})
    assert full["fixer"]["backends"] == ["claude"]
    assert rules_for(full, "fixer")["note"] == "required"
    assert rules_for(full, "fixer")["actions"] == ["list", "rewind"]  # default filled in
    assert rules_for(full, "tester")["actions"] == ["list"]          # from "*"
    assert rules_for(normalize_manages(["fixer"]), "tester") is None


@pytest.mark.parametrize("value, message", [
    ({"fixer": {"actions": ["fork"]}}, "actions"),
    ({"fixer": {"colour": "red"}}, "unknown setting"),
    ({"fixer": {"note": "maybe"}}, "note"),
    ({"fixer": {"cut": "step"}}, "isn't supported yet"),
    ({"fixer": {"files": "snapshot"}}, "isn't supported yet"),
    ({"fixer": {"backends": ["gpt"]}}, "backends"),
    ([1, 2], "agent names"),
    (5, "list of agent names"),
])
def test_bad_manages_settings_are_refused(value, message):
    with pytest.raises(ContextError, match=message):
        normalize_manages(value)


def test_manages_is_read_from_run_configs_and_saved_in_run_json(short_dir, tmp_path):
    config = tmp_path / "run.json"
    config.write_text(json.dumps({"agents": {"lead": {"manages": ["fixer"]}, "fixer": {}}}))
    specs = load_config(str(config))["agents"]
    assert specs[0].manages == {"fixer": {}} and specs[1].manages is None

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"agents": {"lead": {"manages": {"fixer": {"files": "snapshot"}}}}}))
    with pytest.raises(HubError, match="Agent 'lead'.*files: snapshot"):
        load_config(str(bad))

    hub, pipelines = make_hub(short_dir, **LEAD_FIXER)
    try:
        saved = json.load(open(os.path.join(hub.run_dir, "run.json")))["agents"]
        assert saved["lead"]["manages"] == {"fixer": {}}
        assert "manages" not in saved["fixer"] and "rewind" not in saved["lead"]
        # Only agents that manage someone get the tool.
        assert latest(pipelines, "lead").env.get("LANDLINE_MANAGES") == "1"
        assert "LANDLINE_MANAGES" not in latest(pipelines, "fixer").env
    finally:
        hub.stop()


# --------------------------------------------------------------- list/rewind

def test_turns_are_listed_with_messages_replies_and_tool_calls(short_dir):
    hub, _ = make_hub(short_dir, **LEAD_FIXER)
    try:
        hub.send("lead", "fixer", "try approach A")
        hub.send("lead", "fixer", "try approach B")
        listing = hub.context_turns("lead", "fixer")
        assert [(t["message_id"], t["from"], t["message"], t["reply_id"], t["tool_calls"]) for t in listing["turns"]] == [
            ("m1", "lead", "try approach A", "m2", 1),
            ("m3", "lead", "try approach B", "m4", 1),
        ]
        assert listing["turns"][1]["reply"] == "fixer did 2"
    finally:
        hub.stop()


def test_rewind_cuts_the_conversation_and_keeps_the_name(short_dir):
    hub, pipelines = make_hub(short_dir, **LEAD_FIXER)
    try:
        for text in ("step 1", "step 2", "step 3"):
            hub.send("lead", "fixer", text)
        old = latest(pipelines, "fixer")
        old_session = old.conversation_id
        events = []
        hub.subscribe(events.append)

        result = hub.rewind("lead", "fixer", "m1")
        assert result["turns_dropped"] == 2 and result["previous_session"] == old_session
        assert old.closed
        new = latest(pipelines, "fixer")
        assert new is not old and new.conversation_id == result["session_id"] != old_session
        assert [p.split("\n")[-1] for p in new.memory()] == ["step 1"]

        # The next message tells it what happened, once.
        hub.send("lead", "fixer", "step 2, differently")
        assert "rewound by lead to just after your reply to m1" in new.memory()[-1]
        assert "Files on disk were not changed" in new.memory()[-1]
        hub.send("lead", "fixer", "and again")
        assert "rewound" not in new.memory()[-1]

        # Turns from the dropped branch are gone from the list; new ones follow the cut.
        assert [t["message_id"] for t in hub.context_turns("lead", "fixer")["turns"]] == ["m1", "m7", "m9"]
        saved = json.load(open(os.path.join(hub.run_dir, "run.json")))["agents"]["fixer"]
        assert saved["session_id"] == result["session_id"] and saved["previous_sessions"] == [old_session]
        kinds = [e["kind"] for e in events]
        assert "agent_rewound" in kinds and "session" in kinds
        rewound = next(e for e in events if e["kind"] == "agent_rewound")
        assert (rewound["agent"], rewound["to"], rewound["by"], rewound["dropped"]) == ("fixer", "m1", "lead", 2)
    finally:
        hub.stop()


def test_rewind_sends_the_note_from_the_caller(short_dir):
    hub, pipelines = make_hub(short_dir, **LEAD_FIXER)
    try:
        hub.send("lead", "fixer", "step 1")
        hub.send("lead", "fixer", "step 2")
        result = hub.rewind("lead", "fixer", "m1", note="Approach 2 failed: the sim diverges. Try a smaller step.")
        note = next(m for m in hub.log.read_all() if m["id"] == result["note_id"])
        assert (note["from"], note["to"]) == ("lead", "fixer")
        new = latest(pipelines, "fixer")
        deadline = time.monotonic() + 5  # the note is delivered in the background
        while not any("Approach 2 failed" in p for p in new.memory()):
            assert time.monotonic() < deadline, "note not delivered"
            time.sleep(0.02)
        assert "rewound by lead" in next(p for p in new.memory() if "Approach 2 failed" in p)
    finally:
        hub.stop()


def test_a_rewound_agent_can_be_rewound_again_to_an_earlier_turn(short_dir):
    hub, pipelines = make_hub(short_dir, **LEAD_FIXER)
    try:
        for text in ("a", "b", "c"):
            hub.send("lead", "fixer", text)
        hub.rewind("lead", "fixer", "m3")
        hub.send("lead", "fixer", "d")
        hub.rewind("lead", "fixer", "m1")
        assert [p.split("\n")[-1] for p in latest(pipelines, "fixer").memory()] == ["a"]
        saved = json.load(open(os.path.join(hub.run_dir, "run.json")))["agents"]["fixer"]
        assert len(saved["previous_sessions"]) == 2
    finally:
        hub.stop()


def test_permission_checks(short_dir):
    hub, _ = make_hub(short_dir,
                      lead={"manages": normalize_manages({"fixer": {"actions": ["list"]}})},
                      boss={"manages": normalize_manages({"fixer": {"note": "required"}, "*": {"note": "off"}})},
                      fixer={}, tester={})
    try:
        hub.send("user", "fixer", "one")
        hub.send("user", "fixer", "two")
        with pytest.raises(HubError, match="'tester' isn't allowed to manage 'fixer'"):
            hub.context_turns("tester", "fixer")
        with pytest.raises(HubError, match="'lead' isn't allowed to manage 'tester'"):
            hub.context_turns("lead", "tester")
        assert hub.context_turns("lead", "fixer")["turns"]
        with pytest.raises(HubError, match=r"'rewind' isn't allowed for fixer \(actions: list\)"):
            hub.rewind("lead", "fixer", "m1")
        with pytest.raises(HubError, match="needs a note"):
            hub.rewind("boss", "fixer", "m1")
        with pytest.raises(HubError, match="can't manage its own context"):
            hub.context_turns("fixer", "fixer")
        with pytest.raises(HubError, match="Unknown agent 'nobody'"):
            hub.context_turns("boss", "nobody")
        with pytest.raises(HubError, match="no turn that answered m2"):
            hub.rewind("boss", "fixer", "m2", note="x")
        with pytest.raises(HubError, match="already right after m3"):
            hub.rewind("boss", "fixer", "m3", note="x")
        hub.send("user", "tester", "one")
        hub.send("user", "tester", "two")
        with pytest.raises(HubError, match="Notes aren't allowed"):
            hub.rewind("boss", "tester", "m5", note="x")
        # The user isn't limited by 'manages'.
        assert hub.rewind("user", "tester", "m5")["turns_dropped"] == 1
    finally:
        hub.stop()


def test_agy_agents_cant_be_rewound_yet(short_dir):
    hub, _ = make_hub(short_dir, lead={"manages": normalize_manages({"coder": {"backends": ["claude", "agy"]}})},
                      coder={"backend": "antigravity"})
    try:
        with pytest.raises(HubError, match="isn't supported yet"):
            hub.rewind("lead", "coder", "m1")
    finally:
        hub.stop()
    hub, _ = make_hub(short_dir, lead={"manages": normalize_manages(["coder"])}, coder={"backend": "antigravity"})
    try:
        with pytest.raises(HubError, match=r"isn't allowed for coder \(backends: claude\)"):
            hub.rewind("lead", "coder", "m1")
    finally:
        hub.stop()


def test_a_busy_agent_is_not_rewound(short_dir):
    hub, _ = make_hub(short_dir, **LEAD_FIXER)
    try:
        hub.send("lead", "fixer", "one")
        hub.send("lead", "fixer", "two")
        hub.agents["fixer"].busy = True
        with pytest.raises(HubError, match="busy"):
            hub.rewind("lead", "fixer", "m1")
    finally:
        hub.agents["fixer"].busy = False
        hub.stop()


def test_checkpoints_survive_a_rename(short_dir):
    hub, _ = make_hub(short_dir, **LEAD_FIXER)
    try:
        hub.send("lead", "fixer", "one")
        hub.send("lead", "fixer", "two")
        hub.rename("renamed-run")
        assert [t["message_id"] for t in hub.context_turns("lead", "fixer")["turns"]] == ["m1", "m3"]
        assert hub.rewind("lead", "fixer", "m1")["turns_dropped"] == 1
    finally:
        hub.stop()


def test_context_over_the_socket(short_dir):
    hub, _ = make_hub(short_dir, **LEAD_FIXER)
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        hub.send("lead", "fixer", "one")
        hub.send("lead", "fixer", "two")
        listing = client.context("lead", "fixer")
        assert listing["ok"] and [t["message_id"] for t in listing["turns"]] == ["m1", "m3"]
        refused = client.context("fixer", "lead")
        assert not refused["ok"] and "isn't allowed" in refused["error"]
        done = client.context("lead", "fixer", action="rewind", after="m1", note="redo two")
        assert done["ok"] and done["turns_dropped"] == 1 and done["note_id"]
        assert not client.context("lead", "fixer", action="fork")["ok"]
        spawned = client.spawn("boss", "claude", manages=["fixer"])
        assert spawned["ok"] and hub.agents["boss"].spec.manages == {"fixer": {}}
    finally:
        server.stop()
        hub.stop()


# -------------------------------------------------------------- the pipeline

def test_claude_pipeline_fork_command():
    plain = ClaudePipeline(conversation_id="s1")._build_command()
    assert "--fork-session" not in plain and "--resume-session-at" not in plain
    fork = ClaudePipeline(conversation_id="s1", fork_session=True, fork_session_id="s2", resume_at="u9")
    cmd = fork._build_command()
    assert cmd[cmd.index("--resume") + 1] == "s1"
    assert cmd[cmd.index("--session-id") + 1] == "s2"
    assert cmd[cmd.index("--resume-session-at") + 1] == "u9"
    assert fork.conversation_id == "s2"  # known before the CLI reports it
