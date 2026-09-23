"""
Unit tests for the multi-backend plumbing: pipeline_factory.py (backend name/alias
resolution), claude_pipeline.py (Claude Code's stream-json protocol), and AgentManager's
backend selection.

AgyPipeline (Antigravity) and ClaudePipeline (Claude Code) are independent, self-contained
classes with no shared base class - AgyPipeline is untouched from before Claude support was
added. These are pure offline tests: they feed synthetic stream-json events directly into
ClaudePipeline._handle_event() and drive AgentManager.initialize() with the underlying
pipeline.start() mocked out, so no `agy` or `claude` subprocess is ever spawned. (Live
subprocess integration lives in test_agy_pipeline.py / test_claude_pipeline.py.)
"""

import sys
import os
import json
import time
import threading
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import pytest

from claude_pipeline import ClaudePipeline, AgentTurnResult
from pipeline_factory import create_pipeline, normalize_backend
from agy_pipeline import AgyPipeline
from agent_manager import AgentManager


# ---------------------------------------------------------------------------
# Backend factory / alias resolution
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("alias,expected", [
    ("agy", "antigravity"),
    ("antigravity", "antigravity"),
    ("Antigravity-CLI", "antigravity"),
    ("claude", "claude"),
    ("claude-code", "claude"),
    ("CLAUDE-CLI", "claude"),
    (None, "antigravity"),  # default
])
def test_normalize_backend_aliases(alias, expected):
    assert normalize_backend(alias) == expected


def test_normalize_backend_rejects_unknown():
    with pytest.raises(ValueError):
        normalize_backend("gpt4")


def test_create_pipeline_dispatches_to_correct_class():
    assert isinstance(create_pipeline("agy", cwd="/tmp"), AgyPipeline)
    assert isinstance(create_pipeline("claude", cwd="/tmp"), ClaudePipeline)


def test_agy_pipeline_constructor_is_unaffected_by_claude_support():
    """Regression guard: AgyPipeline's original signature must stay exactly as it was."""
    p = AgyPipeline(cwd="/tmp", skip_permissions=True, model="m", effort="e", conversation_id="c")
    assert not hasattr(p, "fork_session")
    assert not hasattr(p, "permission_mode") or p.permission_mode is None
    with pytest.raises(TypeError):
        AgyPipeline(cwd="/tmp", fork_session=True)  # Claude-only kwarg, must not leak into AgyPipeline


def test_claude_command_includes_fork_session_when_resuming():
    p = ClaudePipeline(cwd="/tmp", conversation_id="parent-123", fork_session=True)
    cmd = p._build_command()
    assert cmd[0] == "claude"
    assert "--resume" in cmd and "parent-123" in cmd
    assert "--fork-session" in cmd


def test_claude_command_uses_permission_mode_when_not_skipping():
    p = ClaudePipeline(cwd="/tmp", skip_permissions=False, permission_mode="plan")
    cmd = p._build_command()
    assert "--dangerously-skip-permissions" not in cmd
    assert "--permission-mode" in cmd and "plan" in cmd


# ---------------------------------------------------------------------------
# ClaudePipeline.start() lifecycle (mocked subprocess.Popen, no real `claude`)
#
# Regression coverage for a real bug found via live testing: when resuming a
# session with --resume, Claude Code can stay silent on stdout (no system/init
# event) until the first turn is actually sent, unlike agy which announces
# init proactively. The original start() blocked forever waiting for that
# init event and raised TimeoutError, permanently deadlocking any --resume
# session that doesn't proactively announce itself - send_prompt() could
# never get called because initialize_agent() never returned successfully.
# ---------------------------------------------------------------------------

class _BlockingStdout:
    """Simulates a live process that never writes anything to stdout."""
    def readline(self):
        threading.Event().wait()  # blocks forever; thread is daemon, harmless in tests
        return ""


class _FakeStdin:
    def write(self, _data):
        pass

    def flush(self):
        pass

    def close(self):
        pass


class _FakeProc:
    def __init__(self, poll_result=None, stderr_text=""):
        self._poll_result = poll_result
        self.returncode = poll_result
        self.stdin = _FakeStdin()
        self.stdout = _BlockingStdout()
        self.stderr = _StderrStub(stderr_text)

    def poll(self):
        return self._poll_result

    def terminate(self):
        pass

    def wait(self, timeout=None):
        pass

    def kill(self):
        pass


class _StderrStub:
    def __init__(self, text):
        self._text = text

    def read(self):
        return self._text


def test_claude_start_does_not_raise_when_init_never_arrives_but_process_is_alive():
    """A live-but-quiet process (the --resume case) must not be treated as a startup failure."""
    fake_proc = _FakeProc(poll_result=None)  # still running
    p = ClaudePipeline(cwd="/tmp", conversation_id="some-session")
    with patch("claude_pipeline.subprocess.Popen", return_value=fake_proc):
        p.start(timeout=0.2)  # must return normally, not raise TimeoutError
    assert p._running is True
    assert p.proc is fake_proc


def test_claude_start_raises_with_stderr_when_process_dies_during_startup():
    """A process that actually exits during startup (bad args, auth failure, ...) must still raise."""
    fake_proc = _FakeProc(poll_result=1, stderr_text="Error: no conversation found with session ID: bogus")
    p = ClaudePipeline(cwd="/tmp", conversation_id="bogus")
    with patch("claude_pipeline.subprocess.Popen", return_value=fake_proc):
        with pytest.raises(RuntimeError, match="no conversation found"):
            p.start(timeout=0.2)


def test_claude_send_refreshes_conversation_id_after_fork_init_event():
    """
    Regression test for a bug found via live testing: forking (--resume <parent>
    --fork-session) makes Claude Code mint a brand new session id, but that id is
    only known once the *first turn's own* system/init event has been processed -
    which, per the start() fix above, can happen only after send() is already
    running, not during start(). send() used to snapshot conversation_id at the
    START of the turn (the stale parent id) instead of refreshing it once the
    turn (and its init event) completed, so callers got the parent id back
    instead of the real forked session id.
    """
    fake_proc = _FakeProc(poll_result=None)  # stays "alive" throughout
    p = ClaudePipeline(cwd="/tmp", conversation_id="parent-session-id", fork_session=True)
    p.proc = fake_proc
    p._running = True

    def simulate_reader_emitting_fork_init_then_result():
        time.sleep(0.05)  # let send() snapshot the stale conversation_id first
        ready = threading.Event()
        p._handle_event({
            "type": "system", "subtype": "init",
            "session_id": "forked-new-session-id", "tools": [], "permissionMode": "bypassPermissions"
        }, ready)
        p._handle_event({
            "type": "result", "subtype": "success", "result": "ok",
            "session_id": "forked-new-session-id", "is_error": False
        }, ready)

    reader = threading.Thread(target=simulate_reader_emitting_fork_init_then_result, daemon=True)
    reader.start()
    result = p.send("hello", timeout=2)
    reader.join(timeout=1)

    assert result.conversation_id == "forked-new-session-id", (
        f"expected the refreshed forked session id, got stale value {result.conversation_id!r}"
    )
    assert p.conversation_id == "forked-new-session-id"


# ---------------------------------------------------------------------------
# ClaudePipeline stream-json event parsing
# ---------------------------------------------------------------------------

def _start_turn(pipeline: ClaudePipeline):
    pipeline._current_tools_by_id = {}
    pipeline._current_turn_result = AgentTurnResult(
        conversation_id=pipeline.conversation_id or "", response="", status="UNKNOWN"
    )
    return pipeline._current_turn_result


def test_claude_init_event_sets_session_and_tools():
    p = ClaudePipeline(cwd="/tmp")
    ready = threading.Event()
    p._handle_event({
        "type": "system", "subtype": "init",
        "session_id": "sess-abc", "tools": ["Read", "Bash"],
        "permissionMode": "bypassPermissions"
    }, ready)

    assert p.conversation_id == "sess-abc"
    assert p.available_tools == ["Read", "Bash"]
    assert p.permission_mode == "bypassPermissions"
    assert ready.is_set()


def test_claude_tool_use_and_tool_result_roundtrip():
    p = ClaudePipeline(cwd="/tmp", conversation_id="sess-abc")
    ready = threading.Event()
    result = _start_turn(p)

    p._handle_event({"type": "assistant", "message": {"content": [
        {"type": "text", "text": "Checking..."},
        {"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {"command": "ls"}},
    ]}}, ready)

    assert len(result.tool_calls) == 1
    assert result.tool_calls[0].name == "Bash"
    assert result.tool_calls[0].state == "CALL_STARTED"

    p._handle_event({"type": "user", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "toolu_1", "content": "file1\nfile2", "is_error": False}
    ]}}, ready)

    assert result.tool_calls[0].state == "CALL_DONE"
    assert result.tool_calls[0].error is None


def test_claude_tool_result_error_is_recorded():
    p = ClaudePipeline(cwd="/tmp", conversation_id="sess-abc")
    ready = threading.Event()
    result = _start_turn(p)

    p._handle_event({"type": "assistant", "message": {"content": [
        {"type": "tool_use", "id": "toolu_9", "name": "Bash", "input": {"command": "rm -rf /"}},
    ]}}, ready)
    p._handle_event({"type": "user", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "toolu_9", "content": "permission denied", "is_error": True}
    ]}}, ready)

    assert result.tool_calls[0].state == "ERROR"
    assert result.tool_calls[0].error == "permission denied"


def test_claude_ask_user_question_is_captured_as_question():
    p = ClaudePipeline(cwd="/tmp", conversation_id="sess-abc")
    ready = threading.Event()
    result = _start_turn(p)

    p._handle_event({"type": "assistant", "message": {"content": [
        {"type": "tool_use", "id": "toolu_2", "name": "AskUserQuestion", "input": {"question": "Which env?"}},
    ]}}, ready)

    assert len(result.questions) == 1
    assert result.questions[0]["question"] == "Which env?"


def test_claude_result_event_finalizes_turn():
    p = ClaudePipeline(cwd="/tmp", conversation_id="sess-abc")
    ready = threading.Event()
    result = _start_turn(p)
    p._turn_done_event = threading.Event()

    p._handle_event({
        "type": "result", "subtype": "success", "result": "All done!",
        "session_id": "sess-abc", "is_error": False, "duration_ms": 1500,
        "usage": {"input_tokens": 10, "output_tokens": 20}, "total_cost_usd": 0.01
    }, ready)

    assert result.response == "All done!"
    assert result.status == "SUCCESS"
    assert result.duration_seconds == 1.5
    assert result.usage["total_cost_usd"] == 0.01
    assert p._turn_done_event.is_set()


def test_claude_result_event_error_status():
    p = ClaudePipeline(cwd="/tmp", conversation_id="sess-abc")
    ready = threading.Event()
    result = _start_turn(p)
    p._turn_done_event = threading.Event()

    p._handle_event({
        "type": "result", "subtype": "error_max_turns", "result": "Hit max turns",
        "session_id": "sess-abc", "is_error": True
    }, ready)

    assert result.status == "ERROR"
    assert result.error == "Hit max turns"


def test_project_dir_for_cwd_matches_claude_code_layout():
    # Ground truth observed directly under this machine's real ~/.claude/projects/.
    assert ClaudePipeline.project_dir_for_cwd("/Users/vs/function/AgentLandline") == "-Users-vs-function-AgentLandline"


# ---------------------------------------------------------------------------
# AgentManager backend selection (pipeline.start() mocked out, no subprocess)
# ---------------------------------------------------------------------------

def test_manager_initialize_defaults_to_antigravity():
    manager = AgentManager()
    with patch.object(AgyPipeline, "start", return_value=None):
        res = manager.initialize(conversation_id="conv-1", cwd="/tmp")
    assert res["status"] == "INITIALIZED"
    assert res["backend"] == "antigravity"
    assert isinstance(manager.pipeline, AgyPipeline)


def test_manager_initialize_selects_claude_backend():
    manager = AgentManager()
    with patch.object(ClaudePipeline, "start", return_value=None):
        res = manager.initialize(conversation_id="conv-1", cwd="/tmp", backend="claude-code", model="sonnet")
    assert res["status"] == "INITIALIZED"
    assert res["backend"] == "claude"
    assert isinstance(manager.pipeline, ClaudePipeline)
    assert manager.pipeline.model == "sonnet"


def test_manager_initialize_unknown_backend_returns_error():
    manager = AgentManager()
    res = manager.initialize(conversation_id="conv-1", cwd="/tmp", backend="gpt4")
    assert res["status"] == "ERROR"
    assert manager.pipeline is None


def test_manager_extract_last_command_claude_backend(tmp_path):
    manager = AgentManager()
    manager.backend = "claude"

    cwd = "/Users/vs/function/AgentLandline"
    project_dir = ClaudePipeline.project_dir_for_cwd(cwd)
    log_dir = os.path.join(str(tmp_path), "projects", project_dir)
    os.makedirs(log_dir, exist_ok=True)
    transcript_file = os.path.join(log_dir, "sess-xyz.jsonl")

    lines = [
        json.dumps({"type": "user", "message": {"role": "user", "content": "Add Claude support to the pipeline."}}),
        json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [
            {"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {"command": "ls"}}
        ]}}),
        json.dumps({"type": "user", "message": {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "toolu_1", "content": "file1"}
        ]}}),
        json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [
            {"type": "text", "text": "Done, Claude backend added."}
        ]}}),
    ]
    with open(transcript_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    with patch.dict(os.environ, {"CLAUDE_CONFIG_DIR": str(tmp_path)}):
        res = manager.extract_last_command(conversation_id="sess-xyz", cwd=cwd)

    assert "error" not in res, res
    assert res["last_user_prompt"] == "Add Claude support to the pipeline."
    assert res["last_agent_response"] == "Done, Claude backend added."


def test_manager_fork_true_with_claude_backend_sets_fork_session_flag():
    manager = AgentManager()
    captured = {}

    def fake_start(self, timeout=30.0):
        captured["fork_session"] = self.fork_session
        self.conversation_id = "forked-session-xyz"

    with patch.object(ClaudePipeline, "start", fake_start):
        res = manager.initialize(conversation_id="parent-session", cwd="/tmp", backend="claude", fork=True)

    assert captured["fork_session"] is True
    assert res["conversation_id"] == "forked-session-xyz"
