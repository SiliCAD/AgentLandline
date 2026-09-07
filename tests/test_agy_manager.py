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


if __name__ == "__main__":
    test_extract_last_command_long_turn()
