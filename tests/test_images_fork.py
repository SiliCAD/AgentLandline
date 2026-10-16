"""
Images attached to messages, and agents that start on a fork of an existing session
(`"session_id": ..., "fork": true` in a run config).
"""

import base64
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline import images
from agentlandline.agy_pipeline import AgyPipeline
from agentlandline.hub import AgentSpec, Hub, HubClient, HubError, HubServer, load_config

PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")


class Fake:
    def __init__(self, spec, env):
        self.spec, self.env = spec, env
        rewind = spec.rewind or {}
        self.conversation_id = rewind.get("session_id") or spec.session_id or f"session-{spec.name}"
        self.calls = []

    def add_event_listener(self, listener):
        pass

    def start(self, timeout=None):
        pass

    def close(self):
        pass

    def transcript_path(self):
        return None

    def send(self, prompt, timeout=None, **kwargs):
        self.calls.append((prompt, kwargs))
        return SimpleNamespace(response="seen", status="SUCCESS", error=None)


@pytest.fixture
def short_dir():
    path = tempfile.mkdtemp(prefix="ll-", dir="/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def png(tmp_path):
    path = tmp_path / "plot.png"
    path.write_bytes(PNG)
    return str(path)


def make_hub(home, **specs):
    made = {}

    def factory(spec, env):
        made[spec.name] = Fake(spec, env)
        return made[spec.name]

    hub = Hub([AgentSpec(name=n, **s) for n, s in specs.items()], run_id="img-run", home=home,
              socket_path=os.path.join(home, "hub.sock"), pipeline_factory=factory)
    hub.start()
    return hub, made


# ------------------------------------------------------------------ images

def test_image_helpers(png, tmp_path):
    assert images.check([png]) == [png]
    with pytest.raises(images.ImageError, match="not an image"):
        images.check([str(tmp_path / "notes.txt")])
    with pytest.raises(images.ImageError, match="no such file"):
        images.check([str(tmp_path / "missing.png")])
    stored = images.store(str(tmp_path / "run"), [png])
    assert stored[0].startswith(str(tmp_path / "run" / "images")) and stored[0].endswith("-plot.png")
    block = images.claude_blocks(stored)[0]
    assert block["source"]["media_type"] == "image/png" and base64.b64decode(block["source"]["data"]) == PNG
    assert "file viewing tool" in images.note(stored, inline=False)
    assert "shown with this message" in images.note(stored, inline=True)
    assert images.note([], inline=True) == ""


def test_images_reach_claude_inline_and_agy_by_path(short_dir, png):
    hub, made = make_hub(short_dir, coder={"backend": "claude"}, tester={"backend": "antigravity"})
    try:
        result = hub.send("user", "coder", "What does this plot show?", images=[png])
        record = result["message"]
        assert len(record["images"]) == 1 and os.path.dirname(record["images"][0]) == os.path.join(hub.run_dir, "images")
        prompt, kwargs = made["coder"].calls[-1]
        assert kwargs == {"images": record["images"]}
        assert record["images"][0] in prompt and "shown with this message" in prompt

        hub.send("user", "tester", "", images=[png])  # an image alone is a message too
        prompt, kwargs = made["tester"].calls[-1]
        assert kwargs == {} and "file viewing tool" in prompt
        assert hub.log.read_all()[-2]["images"]  # kept in the message log
    finally:
        hub.stop()


def test_bad_images_and_empty_messages_are_refused(short_dir, tmp_path):
    hub, _ = make_hub(short_dir, coder={})
    try:
        with pytest.raises(HubError, match="Can't attach image: .*no such file"):
            hub.send("user", "coder", "look", images=[str(tmp_path / "gone.png")])
        with pytest.raises(HubError, match="empty"):
            hub.send("user", "coder", "  ")
        assert hub.log.read_all() == []
    finally:
        hub.stop()


def test_images_over_the_socket_resolve_relative_paths(short_dir, png, monkeypatch):
    hub, made = make_hub(short_dir, coder={})
    server = HubServer(hub)
    server.start()
    try:
        monkeypatch.chdir(os.path.dirname(png))
        res = HubClient(hub.socket_path).send("user", "coder", "see", images=["plot.png"])
        assert res["ok"] and len(res["message"]["images"]) == 1
        assert made["coder"].calls[-1][1]["images"] == res["message"]["images"]
    finally:
        server.stop()
        hub.stop()


# -------------------------------------------------------------------- fork

def test_fork_needs_a_session(tmp_path):
    config = tmp_path / "team.json"
    config.write_text(json.dumps({"agents": {"coder": {"fork": True}}}))
    with pytest.raises(HubError, match="'fork' needs the 'session_id'"):
        load_config(str(config))
    config.write_text(json.dumps({"agents": {"coder": {"session_id": "s-base", "fork": True}}}))
    spec = load_config(str(config))["agents"][0]
    assert spec.fork and spec.session_id == "s-base"


def test_an_agent_can_start_on_a_fork_of_a_session(short_dir):
    hub, made = make_hub(short_dir, coder={"session_id": "s-base", "fork": True}, plain={"session_id": "s-other"})
    try:
        coder = made["coder"]
        assert coder.spec.rewind and coder.spec.session_id == "s-base"  # the pipeline forked s-base...
        assert "at" not in coder.spec.rewind                           # ...as a whole
        slot = hub.agents["coder"]
        assert slot.spec.session_id == coder.conversation_id != "s-base"
        assert (slot.spec.fork, slot.spec.forked_from, slot.spec.rewind) == (False, "s-base", None)
        assert made["plain"].spec.rewind is None  # resuming without fork continues the session itself

        saved = json.load(open(os.path.join(hub.run_dir, "run.json")))["agents"]
        assert saved["coder"]["session_id"] == slot.spec.session_id and saved["coder"]["forked_from"] == "s-base"
        assert "fork" not in saved["coder"] and "forked_from" not in saved["plain"]
        # Restarting from run.json continues the fork instead of forking again.
        again = load_config(os.path.join(hub.run_dir, "run.json"))["agents"][0]
        assert again.session_id == slot.spec.session_id and not again.fork
    finally:
        hub.stop()


def test_fork_over_the_socket(short_dir):
    hub, made = make_hub(short_dir, lead={})
    server = HubServer(hub)
    server.start()
    try:
        res = HubClient(hub.socket_path).spawn("coder", "claude", session_id="s-base", fork=True)
        assert res["ok"] and res["agent"]["session_id"] != "s-base"
        assert hub.agents["coder"].spec.forked_from == "s-base"
        refused = HubClient(hub.socket_path).spawn("tester", "claude", fork=True)
        assert not refused["ok"] and "needs the 'session_id'" in refused["error"]
    finally:
        server.stop()
        hub.stop()


def test_claude_fork_command():
    spec = AgentSpec("coder", session_id="s-base", rewind={"session_id": "s-new"})
    cmd = Hub._make_pipeline(spec, {})._build_command()
    assert cmd[cmd.index("--resume") + 1] == "s-base" and "--fork-session" in cmd
    assert cmd[cmd.index("--session-id") + 1] == "s-new" and "--resume-session-at" not in cmd


def test_agy_fork_copies_the_conversation(tmp_path):
    conversations = tmp_path / "conversations"
    conversations.mkdir()
    with sqlite3.connect(conversations / "base.db") as conn:
        conn.execute("CREATE TABLE trajectory_meta (trajectory_id text, cascade_id text)")
        conn.execute("INSERT INTO trajectory_meta VALUES ('t', 'base')")
    logs = tmp_path / "brain" / "base" / ".system_generated" / "logs"
    logs.mkdir(parents=True)
    (logs / "transcript.jsonl").write_text('{"step_index": 0}\n')
    with patch("os.path.expanduser", return_value=str(tmp_path)):
        spec = AgentSpec("tester", backend="antigravity", session_id="base", rewind={"session_id": "copy"})
        pipeline = Hub._make_pipeline(spec, {})
        with pytest.raises(FileExistsError):
            AgyPipeline.fork_conversation("base", "copy")
        with pytest.raises(FileNotFoundError):
            AgyPipeline.fork_conversation("nope", "copy2")
    assert pipeline.conversation_id == "copy"
    with sqlite3.connect(conversations / "copy.db") as conn:
        assert conn.execute("SELECT cascade_id FROM trajectory_meta").fetchone() == ("copy",)
    with sqlite3.connect(conversations / "base.db") as conn:
        assert conn.execute("SELECT cascade_id FROM trajectory_meta").fetchone() == ("base",)
    assert (tmp_path / "brain" / "copy" / ".system_generated" / "logs" / "transcript.jsonl").exists()


def test_agents_with_history_are_told_to_keep_using_it(short_dir):
    hub, made = make_hub(short_dir, fresh={}, forked={"session_id": "s-base", "fork": True}, resumed={"session_id": "s-old"})
    try:
        for name in ("fresh", "forked", "resumed"):
            hub.send("user", name, "hello")
        line = "This conversation continues earlier work (above)"
        assert line not in made["fresh"].calls[0][0]
        assert line in made["forked"].calls[0][0] and line in made["resumed"].calls[0][0]
    finally:
        hub.stop()
