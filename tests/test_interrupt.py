"""
Interrupting an agent's running turn: the pipelines' interrupt() (claude: a control
request on stdin; agy: SIGINT, then a fresh process on the same conversation) and the
hub's forced send, which puts the user's message first and interrupts the running turn.
No real `claude` or `agy` process is started.
"""

import json
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline.agy_pipeline import AgyPipeline
from agentlandline.claude_pipeline import ClaudePipeline
from agentlandline.hub import AgentSpec, Hub, HubClient, HubError, HubServer


class _Stdin:
    def __init__(self):
        self.lines = []

    def write(self, data):
        self.lines.extend(json.loads(line) for line in data.splitlines() if line.strip())

    def flush(self):
        pass

    def close(self):
        pass


class _Proc:
    def __init__(self):
        self.stdin = _Stdin()
        self.signals = []
        self.returncode = None

    def poll(self):
        return self.returncode

    def send_signal(self, sig):
        self.signals.append(sig)

    def terminate(self):
        self.returncode = -15

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        self.returncode = -9


def _wait(condition, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition not met in time"
        time.sleep(0.01)


def _run_send(pipeline, prompt):
    out = {}
    thread = threading.Thread(target=lambda: out.setdefault("result", pipeline.send(prompt, timeout=2)), daemon=True)
    thread.start()
    return thread, out


# --- ClaudePipeline ---------------------------------------------------------------

def test_claude_interrupt_sends_a_control_request_and_the_turn_ends_interrupted():
    p = ClaudePipeline(cwd="/tmp")
    p.proc, p._running = _Proc(), True
    assert p.interrupt() is False  # no turn running
    assert p.proc.stdin.lines == []

    thread, out = _run_send(p, "long job")
    _wait(lambda: len(p.proc.stdin.lines) == 1)
    assert p.interrupt() is True
    assert p.interrupt() is True  # asking again sends nothing more
    control = p.proc.stdin.lines[1:]
    assert len(control) == 1 and control[0]["type"] == "control_request"
    assert control[0]["request"] == {"subtype": "interrupt"} and control[0]["request_id"]
    # What the CLI answers with (checked against claude 2.1): a control_response, then an error result.
    p._handle_event({"type": "control_response", "response": {"subtype": "success"}}, threading.Event())
    p._handle_event({"type": "result", "subtype": "error_during_execution", "is_error": True}, threading.Event())
    thread.join(2)
    assert out["result"].status == "INTERRUPTED" and out["result"].error == "Interrupted."
    assert p.interrupt() is False and p._interrupt_requested is False


def test_claude_interrupt_before_the_prompt_is_written_lands_right_after_it():
    p = ClaudePipeline(cwd="/tmp")
    p.proc, p._running = _Proc(), True
    p._in_turn = True  # send() has begun (e.g. the CLI is restarting) but hasn't written the prompt
    assert p.interrupt() is True
    assert p.proc.stdin.lines == []
    p._prompt_written()
    assert [line["type"] for line in p.proc.stdin.lines] == ["control_request"]


# --- AgyPipeline ------------------------------------------------------------------

def test_agy_interrupt_signals_and_the_next_turn_resumes_in_a_new_process():
    p = AgyPipeline(cwd="/tmp", conversation_id="conv-1")
    first = _Proc()
    p.proc, p._running = first, True
    thread, out = _run_send(p, "long job")
    _wait(lambda: len(first.stdin.lines) == 1)
    assert p.interrupt() is True
    assert first.signals == [signal.SIGINT]
    # agy ends the turn with an "interrupted" result, then exits.
    p._handle_event({"event": "result", "result": {"status": "ERROR", "response": "", "error": "interrupted"}},
                    threading.Event())
    first.returncode = 130
    thread.join(2)
    assert out["result"].status == "INTERRUPTED"

    second = _Proc()

    def fake_start(timeout=30.0):
        assert p.resume_conversation_id == "conv-1"  # the same conversation, in a new process
        p.proc, p._running = second, True

    with patch.object(p, "start", fake_start):
        thread, out = _run_send(p, "next")
        _wait(lambda: len(second.stdin.lines) == 1)
        p._handle_event({"event": "result", "result": {"status": "SUCCESS", "response": "ok"}}, threading.Event())
        thread.join(2)
    assert out["result"].status == "SUCCESS" and out["result"].response == "ok"
    assert second.stdin.lines[0]["message"]["content"] == "next" and second.signals == []


# --- Hub: forced sends ---------------------------------------------------------------

class InterruptiblePipeline:
    """A claude-shaped fake: prompts containing 'long' run until interrupted."""

    def __init__(self, spec, env):
        self.spec = spec
        self.conversation_id = f"session-{spec.name}"
        self.prompts, self.listeners = [], []
        self.entered, self._interrupt = threading.Event(), threading.Event()
        self.in_turn = False

    def add_event_listener(self, listener):
        self.listeners.append(listener)

    def start(self, timeout=None):
        pass

    def close(self):
        self._interrupt.set()

    def transcript_path(self):
        return None

    def interrupt(self):
        if not self.in_turn:
            return False
        self._interrupt.set()
        return True

    def send(self, prompt, timeout=None):
        self.prompts.append(prompt)
        self.in_turn = True
        self.entered.set()
        try:
            if "long" in prompt and self._interrupt.wait(5):
                self._interrupt.clear()
                for listener in self.listeners:
                    listener({"type": "result", "subtype": "error_during_execution", "is_error": True})
                return SimpleNamespace(response="", status="INTERRUPTED", error="Interrupted.")
            return SimpleNamespace(response=f"{self.spec.name} got it", status="SUCCESS", error=None)
        finally:
            self.in_turn = False


@pytest.fixture
def hub():
    home = tempfile.mkdtemp(prefix="ll-", dir="/tmp")  # short path: macOS socket path limit
    pipelines = {}

    def factory(spec, env):
        pipelines[spec.name] = InterruptiblePipeline(spec, env)
        return pipelines[spec.name]

    instance = Hub([AgentSpec("a", backend="claude"), AgentSpec("b", backend="claude")], run_id="force", home=home,
                   socket_path=os.path.join(home, "hub.sock"), pipeline_factory=factory)
    instance.start()
    instance.pipelines = pipelines
    yield instance
    instance.stop()
    shutil.rmtree(home, ignore_errors=True)


def test_force_interrupts_the_running_turn_and_goes_ahead_of_waiting_messages(hub):
    events = []
    hub.subscribe(events.append)
    b = hub.pipelines["b"]
    hub.send("user", "b", "long job", wait=False)                 # m1: b works on it
    assert b.entered.wait(2)
    hub.send("a", "b", "something else", wait=False)              # m2: waits for b
    result = hub.send("user", "b", "use plan B", force=True)      # m3
    assert result["status"] == "REPLIED" and result["interrupted"] == "m1"
    assert result["reply"]["text"] == "b got it"
    _wait(lambda: len(b.prompts) == 3)
    assert "use plan B" in b.prompts[1] and "something else" in b.prompts[2]  # the forced one went first
    assert "interrupted your work on message m1 (from user)" in b.prompts[1]
    assert "interrupted" not in b.prompts[2]
    _wait(lambda: len(hub.log.read_all()) == 6)
    replies = {m["caused_by"]: m for m in hub.log.read_all() if m["from"] == "b"}
    assert replies["m1"]["text"] == "[interrupted by the user]" and replies["m1"]["to"] == "user"
    assert replies["m3"]["text"] == "b got it" and replies["m2"]["to"] == "a"
    assert any({"kind": "interrupted", "agent": "b", "message_id": "m1", "by": "user"}.items() <= e.items() for e in events)
    ends = [e for e in events if e["kind"] == "turn_end" and e["agent"] == "b"]
    assert ends[0].get("interrupted") is True and not any(e.get("interrupted") for e in ends[1:])


def test_force_on_an_idle_agent_just_delivers_first(hub):
    result = hub.send("user", "a", "hello", force=True)
    assert result["status"] == "REPLIED" and result["interrupted"] is None
    assert "interrupted" not in hub.pipelines["a"].prompts[0]


def test_only_the_user_can_force(hub):
    with pytest.raises(HubError, match="Only the user"):
        hub.send("a", "b", "stop that", force=True)


def test_force_over_the_socket(hub):
    server = HubServer(hub)
    server.start()
    try:
        client = HubClient(hub.socket_path)
        assert client.send("user", "b", "long job", wait=False)["ok"]
        assert hub.pipelines["b"].entered.wait(2)
        res = client.send("user", "b", "now this", wait=False, force=True)
        assert res["ok"] and res["status"] == "QUEUED" and res["interrupted"] == "m1"
        assert "force" not in json.dumps(client.send("user", "a", "plain", wait=False))
        refused = client.send("a", "b", "x", wait=False, force=True)
        assert not refused["ok"] and "Only the user" in refused["error"]
    finally:
        server.stop()
