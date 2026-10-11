"""
Unit tests for agentlandline.events: raw stream-json events from each CLI map to
the shared normalized shape. Event shapes mirror real captures from claude 2.1.x
(`--verbose --include-partial-messages`) and agy 1.3.x.
"""

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline.events import normalize, agy_completed_step, agy_step_thinking, MAX_OUTPUT_CHARS


# --- claude -----------------------------------------------------------------

def test_claude_init_maps_to_session():
    assert normalize("claude", {"type": "system", "subtype": "init", "session_id": "s1", "tools": []}) == [
        {"kind": "session", "session_id": "s1"}
    ]


def test_claude_text_delta_from_partial_stream_event():
    raw = {"type": "stream_event", "event": {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Yes, 17"}}}
    assert normalize("claude", raw) == [{"kind": "text_delta", "text": "Yes, 17"}]


def test_claude_other_stream_events_are_ignored():
    raw = {"type": "stream_event", "event": {"type": "content_block_delta", "delta": {"type": "thinking_delta", "thinking": "", "estimated_tokens": 50}}}
    assert normalize("claude", raw) == []
    assert normalize("claude", {"type": "stream_event", "event": {"type": "message_stop"}}) == []


def test_claude_assistant_blocks_map_in_order():
    raw = {"type": "assistant", "message": {"content": [
        {"type": "thinking", "thinking": "", "signature": "x"},
        {"type": "text", "text": "Listing files."},
        {"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {"command": "ls"}},
    ]}}
    assert normalize("claude", raw) == [
        {"kind": "thinking", "text": ""},
        {"kind": "text", "text": "Listing files."},
        {"kind": "tool_start", "id": "toolu_1", "name": "Bash", "input": {"command": "ls"}},
    ]


def test_claude_tool_result_maps_to_tool_end():
    raw = {"type": "user", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "toolu_1", "content": "a.txt\nb.txt", "is_error": False},
    ]}}
    assert normalize("claude", raw) == [{"kind": "tool_end", "id": "toolu_1", "ok": True, "output": "a.txt\nb.txt"}]


def test_claude_tool_output_is_clipped():
    raw = {"type": "user", "message": {"content": [
        {"type": "tool_result", "tool_use_id": "t", "content": "x" * (MAX_OUTPUT_CHARS + 50), "is_error": True},
    ]}}
    [event] = normalize("claude", raw)
    assert event["ok"] is False
    assert event["output"].endswith("[50 more chars]")


def test_claude_result_maps_to_turn_end():
    raw = {"type": "result", "is_error": False, "duration_ms": 6596, "usage": {"output_tokens": 12}}
    assert normalize("claude", raw) == [{"kind": "turn_end", "ok": True, "duration_s": 6.596, "usage": {"output_tokens": 12}}]


# --- agy --------------------------------------------------------------------

def _step(**fields):
    return {"event": "step_update", "step_update": {"conversation_id": "c1", **fields}}


def test_agy_init_maps_to_session():
    assert normalize("antigravity", {"event": "init", "conversation_id": "c1", "init": {}}) == [
        {"kind": "session", "session_id": "c1"}
    ]


def test_agy_text_delta():
    raw = _step(step_index=3, state="ACTIVE", step_type="agent_response", text_delta="### Step")
    assert normalize("antigravity", raw) == [{"kind": "text_delta", "text": "### Step"}]


def test_agy_tool_lifecycle():
    start = _step(step_index=2, state="ACTIVE", step_type="tool", tool_name="run_command",
                  tool_info={"name": "run_command", "parameters": {"CommandLine": "ls"}})
    done = _step(step_index=2, state="DONE", step_type="tool", tool_name="run_command",
                 tool_info={"name": "run_command", "parameters": {"CommandLine": "ls"}, "output": "a.txt"})
    assert normalize("antigravity", start) == [{"kind": "tool_start", "id": "2", "name": "run_command", "input": {"CommandLine": "ls"}}]
    assert normalize("antigravity", done) == [{"kind": "tool_end", "id": "2", "ok": True, "output": "a.txt"}]


def test_agy_tool_error_reports_not_ok():
    raw = _step(step_index=4, state="DONE", step_type="tool", tool_info={"name": "x", "error": {"message": "boom"}})
    [event] = normalize("antigravity", raw)
    assert event["ok"] is False
    assert "boom" in event["output"]


def test_agy_result_maps_to_turn_end():
    raw = {"event": "result", "result": {"status": "ERROR", "duration_seconds": 1.5, "usage": {"total_tokens": 9}}}
    assert normalize("antigravity", raw) == [{"kind": "turn_end", "ok": False, "duration_s": 1.5, "usage": {"total_tokens": 9}}]


def test_agy_completed_step_detection():
    assert agy_completed_step(_step(step_index=1, state="DONE", step_type="agent_response")) == 1
    assert agy_completed_step(_step(step_index=1, state="ACTIVE", step_type="agent_response", text_delta="x")) is None
    assert agy_completed_step(_step(step_index=2, state="DONE", step_type="tool")) is None


def test_agy_step_thinking_reads_transcript(tmp_path):
    transcript = tmp_path / "transcript.jsonl"
    transcript.write_text("\n".join(json.dumps(e) for e in [
        {"step_index": 0, "type": "USER_INPUT", "content": "hi"},
        {"step_index": 1, "type": "PLANNER_RESPONSE", "thinking": "Need gm1 = 2*pi*GBW*Cc."},
        {"step_index": 3, "type": "PLANNER_RESPONSE", "content": "done"},
    ]) + "\n")
    assert agy_step_thinking(str(transcript), 1) == "Need gm1 = 2*pi*GBW*Cc."
    assert agy_step_thinking(str(transcript), 3) is None
    assert agy_step_thinking(None, 1) is None
    assert agy_step_thinking(str(tmp_path / "missing.jsonl"), 1) is None


def test_claude_init_reports_model():
    raw = {"type": "system", "subtype": "init", "session_id": "s1", "model": "claude-sonnet-5-5"}
    assert normalize("claude", raw) == [{"kind": "session", "session_id": "s1", "model": "claude-sonnet-5-5"}]
