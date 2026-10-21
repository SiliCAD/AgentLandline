"""Offline Codex integration tests, including real subprocess pipes and cleanup."""

import json
import os
import subprocess
import sys
import threading
import time
from unittest.mock import patch

import pytest

from agentlandline.agent_manager import AgentManager
from agentlandline.codex_pipeline import CodexPipeline
from agentlandline.events import normalize
from agentlandline.hub import AgentSpec, Hub
from agentlandline.pipeline_factory import create_pipeline, normalize_backend


@pytest.fixture
def codex_cli(tmp_path, monkeypatch):
    executable = tmp_path / "codex"
    executable.write_text(f"#!{sys.executable}\n" + '''
import json, sys, time
prompt = sys.stdin.read()
if prompt == "crash":
    print("login required", file=sys.stderr)
    sys.exit(2)
if prompt == "noisy":
    for _ in range(3000):
        print("diagnostic " * 20, file=sys.stderr)
session = "copied-thread" if "fork" in sys.argv else "test-thread"
print(json.dumps({"type": "thread.started", "thread_id": session}), flush=True)
if prompt == "hang":
    time.sleep(30)
else:
    print("not json", flush=True)
    print("[]", flush=True)
    print(json.dumps({"type": "item.started", "item": {"id": "cmd", "type": "command_execution", "command": "pwd"}}), flush=True)
    print(json.dumps({"type": "item.completed", "item": {"id": "cmd", "type": "command_execution", "status": "completed", "exit_code": 0}}), flush=True)
    print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps({"prompt": prompt, "argv": sys.argv[1:]})}}), flush=True)
    if prompt == "failed":
        print(json.dumps({"type": "turn.failed", "error": {"message": "rate limit"}}), flush=True)
    elif prompt != "eof":
        print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 12, "output_tokens": 3}}), flush=True)
''')
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    return executable


@pytest.mark.parametrize("alias", ["codex", "CODEX-CLI", "openai"])
def test_registry(alias):
    assert normalize_backend(alias) == "codex"
    assert isinstance(create_pipeline(alias), CodexPipeline)
    assert AgentSpec.from_dict("coder", {"backend": alias}).backend == "codex"


def test_command_login_environment_and_mcp_config(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "do-not-inherit")
    monkeypatch.setenv("CODEX_API_KEY", "do-not-inherit")
    monkeypatch.setenv("CODEX_HOME", "/tmp/codex-login")
    pipeline = CodexPipeline(model="my-model", effort="high", extra_env={
        "LANDLINE_AGENT": "coder", "LANDLINE_SOCKET": "/tmp/team.sock", "LANDLINE_SPAWNS": "1"})
    env = pipeline._build_env()
    assert "OPENAI_API_KEY" not in env and "CODEX_API_KEY" not in env
    assert env["CODEX_HOME"] == "/tmp/codex-login"
    command = pipeline._build_command(["/tmp/a b.png"])
    assert command[:2] == ["codex", "exec"]
    assert command[-3:] == ["--image", "/tmp/a b.png", "-"]
    assert "--full-auto" not in command
    settings = [command[i + 1] for i, arg in enumerate(command[:-1]) if arg == "-c"]
    assert "mcp_servers.agent-landline.required=true" in settings
    assert 'mcp_servers.agent-landline.default_tools_approval_mode="approve"' in settings
    assert 'mcp_servers.agent-landline.enabled_tools=["message_agent", "list_agents", "start_agent", "stop_agent", "find_sessions"]' in settings
    assert 'mcp_servers.agent-landline.command=' + json.dumps(sys.executable) in settings
    assert any('"LANDLINE_SOCKET" = "/tmp/team.sock"' in value for value in settings)
    assert '--model' in command and "my-model" in command
    safe = CodexPipeline(skip_permissions=False)._build_command()
    assert 'sandbox_mode="read-only"' in safe and 'approval_policy="never"' in safe


def test_multiturn_resume_and_fork(codex_cli):
    pipeline = CodexPipeline()
    events = []
    pipeline.add_event_listener(events.append)
    try:
        pipeline.start()
        first = pipeline.send("hello\nworld", images=["/tmp/image.png"])
        assert first.status == "SUCCESS" and first.conversation_id == "test-thread"
        assert json.loads(first.response)["prompt"] == "hello\nworld"
        assert first.tool_calls[0].state == "CALL_DONE"
        assert first.usage == {"input_tokens": 12, "output_tokens": 3}
        second = pipeline.send("second")
        assert json.loads(second.response)["argv"][:3] == ["exec", "resume", "test-thread"]
        assert any(event["type"] == "thread.started" for event in events)
        assert pipeline.proc is None
    finally:
        pipeline.close()
    copy = CodexPipeline(conversation_id="parent", fork_session=True)
    try:
        result = copy.send("copy")
        assert json.loads(result.response)["argv"][:3] == ["exec", "fork", "parent"]
        assert result.conversation_id == "copied-thread" and not copy.fork_session
        assert json.loads(copy.send("next").response)["argv"][:3] == ["exec", "resume", "copied-thread"]
    finally:
        copy.close()


@pytest.mark.parametrize("prompt,expected", [("crash", "login required"), ("eof", "without completing")])
def test_errors_and_missing_terminal_event(codex_cli, prompt, expected):
    pipeline = CodexPipeline()
    try:
        result = pipeline.send(prompt)
        assert result.status == "ERROR" and expected in result.error
        assert pipeline.proc is None
        assert pipeline.send("recovered").status == "SUCCESS"
    finally:
        pipeline.close()


def test_timeout_kills_process_and_next_turn_resumes(codex_cli):
    pipeline = CodexPipeline()
    try:
        assert pipeline.send("hang", timeout=0.3).status == "TIMEOUT"
        assert pipeline.proc is None
        assert pipeline.send("next").status == "SUCCESS"
    finally:
        pipeline.close()


def test_stderr_drains_and_failure_is_emitted_once(codex_cli):
    pipeline = CodexPipeline()
    events = []
    pipeline.add_event_listener(events.append)
    try:
        assert pipeline.send("noisy", timeout=5).status == "SUCCESS"
        result = pipeline.send("failed")
        assert result.status == "ERROR" and result.error == "rate limit"
        assert sum(e.get("type") == "turn.failed" for e in events) == 1
    finally:
        pipeline.close()


def test_interrupt_latches_while_process_is_spawning(codex_cli):
    pipeline = CodexPipeline()
    spawning, release = threading.Event(), threading.Event()
    popen = subprocess.Popen
    results = []

    def slow_spawn(*args, **kwargs):
        spawning.set()
        assert release.wait(5)
        return popen(*args, **kwargs)

    thread = threading.Thread(target=lambda: results.append(pipeline.send("hang", timeout=None)))
    try:
        with patch("agentlandline.codex_pipeline.subprocess.Popen", side_effect=slow_spawn):
            thread.start()
            assert spawning.wait(5)
            assert pipeline.interrupt()
            release.set()
            thread.join(timeout=5)
            assert not thread.is_alive() and results[0].status == "INTERRUPTED"
        assert pipeline.proc is None
        assert pipeline.send("next").status == "SUCCESS"
    finally:
        release.set()
        pipeline.close()
        thread.join(timeout=5)


def test_interrupt_keeps_conversation(codex_cli):
    pipeline = CodexPipeline()
    results = []
    thread = threading.Thread(target=lambda: results.append(pipeline.send("hang", timeout=None)))
    try:
        thread.start()
        deadline = time.monotonic() + 5
        while not pipeline.conversation_id and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pipeline.conversation_id == "test-thread"
        proc = pipeline.proc
        assert pipeline.interrupt()
        thread.join(timeout=5)
        assert not thread.is_alive() and proc.poll() is not None
        assert results[0].status == "INTERRUPTED"
        assert not pipeline.interrupt()
        assert pipeline.send("next").status == "SUCCESS"
    finally:
        pipeline.close()
        thread.join(timeout=5)


def test_transcript_context_and_manager(tmp_path, monkeypatch, codex_cli):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    directory = tmp_path / "sessions" / "2026" / "10" / "21"
    directory.mkdir(parents=True)
    path = directory / "rollout-date-test-thread.jsonl"
    records = [
        {"type": "turn_context", "payload": {"model": "my-model"}},
        {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}},
        {"type": "response_item", "payload": {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hi"}]}},
        {"type": "event_msg", "payload": {"type": "token_count", "info": {
            "last_token_usage": {"input_tokens": 20, "output_tokens": 5}, "model_context_window": 1000}}},
    ]
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n")
    manager = AgentManager()
    try:
        assert manager.initialize("test-thread", backend="codex")["status"] == "INITIALIZED"
        assert isinstance(manager.pipeline, CodexPipeline)
        assert manager.status()["status"] == "RUNNING" and manager.status()["pid"] is None
        entries = manager.extract_last_command()
        assert entries["last_user_prompt"] == "hello" and entries["last_agent_response"] == "hi"
        assert entries["transcript_path"] == str(path)
        events = []
        manager.pipeline.add_event_listener(events.append)
        assert manager.send_prompt("next")["status"] == "SUCCESS"
        assert manager.status()["status"] == "RUNNING"
        context = next(e for e in events if e["type"] == "codex.context")
        normalized = normalize("codex", context)
        assert normalized[-1] == {"kind": "context", "prompt": 20, "output": 5, "context_windows": {"my-model": 1000}}
    finally:
        manager.exit()


def test_hub_codex_delivery_persistence_and_pending_fork(tmp_path, codex_cli):
    spec = AgentSpec.from_dict("coder", {"backend": "codex", "session_id": "parent", "fork": True})
    hub = Hub([spec], home=str(tmp_path), run_id="codex-test")
    events = []
    hub.subscribe(events.append)
    try:
        hub.start()
        saved = json.loads((tmp_path / "runs" / "codex-test" / "run.json").read_text())
        assert saved["agents"]["coder"]["fork"] and saved["agents"]["coder"]["session_id"] == "parent"
        reply = hub.send("user", "coder", "hello", timeout=5)
        assert reply["status"] == "REPLIED"
        saved = json.loads((tmp_path / "runs" / "codex-test" / "run.json").read_text())
        assert saved["agents"]["coder"]["session_id"] == "copied-thread"
        assert not saved["agents"]["coder"].get("fork")
        argv = json.loads(reply["reply"]["text"])["argv"]
        assert any('mcp_servers.agent-landline.env=' in arg for arg in argv)
        assert any(e["kind"] == "tool_start" for e in events)
    finally:
        hub.stop()


def test_normalize_codex_tools_reasoning_and_failure():
    def event(kind, item):
        return normalize("codex", {"type": kind, "item": item})
    assert event("item.completed", {"type": "reasoning", "text": "Checking"}) == [{"kind": "thinking", "text": "Checking"}]
    mcp = {"id": "t", "type": "mcp_tool_call", "tool": "message_agent", "arguments": {"to": "reviewer"}}
    assert event("item.started", mcp)[0]["input"] == {"to": "reviewer"}
    assert event("item.completed", {**mcp, "status": "failed", "error": {"message": "gone"}})[0]["ok"] is False
    assert event("item.completed", {**mcp, "status": "completed", "result": {"isError": True}})[0]["ok"] is False
    change = event("item.completed", {"id": "f", "type": "file_change", "changes": [{"path": "a.py"}]})
    assert [e["kind"] for e in change] == ["tool_start", "tool_end"]
    assert normalize("codex", {"type": "turn.failed"})[0]["ok"] is False
    assert normalize("codex", {"type": "turn.completed", "usage": {"input_tokens": 1000}})[0]["kind"] == "turn_end"


def test_missing_cli_is_actionable(monkeypatch):
    with patch("agentlandline.codex_pipeline.shutil.which", return_value=None):
        with pytest.raises(FileNotFoundError, match="codex login"):
            CodexPipeline().start()
