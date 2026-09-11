"""
Test suite for AgyManager.
Verifies extract_last_command correctly parses long transcript files (>100 lines) without dropping user prompt.
"""

import sys
import os
import json
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agy_manager import AgyManager


def test_extract_last_command_long_turn(tmp_path=None):
    manager = AgyManager()

    # Create temporary transcript file
    if tmp_path is None:
        temp_dir = tempfile.mkdtemp()
    else:
        temp_dir = str(tmp_path)

    log_dir = os.path.join(temp_dir, "brain", "test-conv-123", ".system_generated", "logs")
    os.makedirs(log_dir, exist_ok=True)
    transcript_file = os.path.join(log_dir, "transcript.jsonl")

    lines = []
    # 1. User Input prompt line
    user_prompt_text = "Please analyze this large codebase and make refactorings."
    lines.append(json.dumps({
        "step_index": 1,
        "type": "USER_INPUT",
        "source": "USER_EXPLICIT",
        "content": user_prompt_text
    }))

    # 2. Add 200 intermediate tool call / reasoning steps
    for i in range(2, 202):
        lines.append(json.dumps({
            "step_index": i,
            "type": "TOOL_CALL",
            "name": f"tool_{i}",
            "content": f"Step output {i}"
        }))

    # 3. Agent response step line
    agent_response_text = "Refactoring completed successfully!"
    lines.append(json.dumps({
        "step_index": 202,
        "type": "PLANNER_RESPONSE",
        "content": agent_response_text
    }))

    with open(transcript_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    # Set environment variable to point to temp app data dir
    os.environ["ANTIGRAVITY_APP_DATA_DIR"] = temp_dir

    try:
        res = manager.extract_last_command(conversation_id="test-conv-123")

        assert "error" not in res, f"Unexpected error: {res.get('error')}"
        assert res.get("last_user_prompt") == user_prompt_text, (
            f"Expected '{user_prompt_text}', got '{res.get('last_user_prompt')}'"
        )
        assert res.get("last_agent_response") == agent_response_text, (
            f"Expected '{agent_response_text}', got '{res.get('last_agent_response')}'"
        )
        assert res.get("last_step_index") == 1
        print("test_extract_last_command_long_turn PASSED!")

    finally:
        os.environ.pop("ANTIGRAVITY_APP_DATA_DIR", None)


def test_fork_conversation():
    import sqlite3
    import shutil
    from unittest.mock import patch

    manager = AgyManager()
    temp_dir = tempfile.mkdtemp()
    conv_dir = os.path.join(temp_dir, "conversations")
    brain_dir = os.path.join(temp_dir, "brain", "parent-123", ".system_generated", "logs")
    os.makedirs(conv_dir, exist_ok=True)
    os.makedirs(brain_dir, exist_ok=True)

    # 1. Setup parent DB
    parent_db = os.path.join(conv_dir, "parent-123.db")
    with sqlite3.connect(parent_db) as conn:
        conn.execute("CREATE TABLE trajectory_meta (trajectory_id text, cascade_id text);")
        conn.execute("INSERT INTO trajectory_meta VALUES ('traj-1', 'parent-123');")
        conn.commit()

    # 2. Setup parent brain transcript
    parent_transcript = os.path.join(brain_dir, "transcript.jsonl")
    with open(parent_transcript, "w", encoding="utf-8") as f:
        f.write(json.dumps({"step_index": 1, "type": "USER_INPUT", "content": "Parent question"}) + "\n")

    with patch("os.path.expanduser", return_value=temp_dir):
        try:
            # Fork conversation
            fork_id = manager.fork_conversation(parent_id="parent-123", new_id="fork-test-999")
            assert fork_id == "fork-test-999", f"Expected fork-test-999, got {fork_id}"

            # Verify cloned DB
            fork_db = os.path.join(conv_dir, "fork-test-999.db")
            assert os.path.exists(fork_db), "Forked DB does not exist!"
            with sqlite3.connect(fork_db) as conn:
                row = conn.execute("SELECT cascade_id FROM trajectory_meta WHERE trajectory_id = 'traj-1';").fetchone()
                assert row and row[0] == "fork-test-999", f"Expected cascade_id 'fork-test-999', got {row}"

            # Verify cloned brain & transcript
            fork_transcript = os.path.join(temp_dir, "brain", "fork-test-999", ".system_generated", "logs", "transcript.jsonl")
            assert os.path.exists(fork_transcript), "Forked transcript does not exist!"
            with open(fork_transcript, "r", encoding="utf-8") as f:
                content = f.read()
                assert "Parent question" in content

            print("test_fork_conversation PASSED!")

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    test_extract_last_command_long_turn()
    test_fork_conversation()
