"""Unit tests for agentlandline.message_log."""

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from agentlandline.message_log import MessageLog, count_lines


def test_append_writes_one_json_line_per_message(tmp_path):
    log = MessageLog(str(tmp_path / "run"))
    log.append("user", "architect", "Design a 2-stage opamp")
    log.append("architect", "coder", "Size M1-M7", caused_by="m1")

    lines = (tmp_path / "run" / "messages.jsonl").read_text().splitlines()
    assert len(lines) == 2
    first, second = (json.loads(line) for line in lines)
    assert first["id"] == "m1" and first["from"] == "user" and first["to"] == "architect"
    assert first["caused_by"] is None
    assert second["id"] == "m2" and second["caused_by"] == "m1"
    assert second["ts"].endswith("Z")


def test_sender_transcript_is_linked_with_its_line_count(tmp_path):
    transcript = tmp_path / "session.jsonl"
    transcript.write_text("{}\n{}\n{}\n")
    log = MessageLog(str(tmp_path / "run"))
    record = log.append("coder", "tester", "Run PVT", from_session="s-1", from_transcript=str(transcript))
    assert record["from_session"] == "s-1"
    assert record["from_transcript"] == str(transcript)
    assert record["from_transcript_line"] == 3


def test_missing_transcript_gives_no_line_count(tmp_path):
    log = MessageLog(str(tmp_path / "run"))
    record = log.append("coder", "tester", "x", from_transcript=str(tmp_path / "nope.jsonl"))
    assert record["from_transcript_line"] is None


def test_reopening_a_run_continues_numbering(tmp_path):
    run = str(tmp_path / "run")
    MessageLog(run).append("user", "a", "one")
    log = MessageLog(run)
    assert log.append("a", "user", "two")["id"] == "m2"
    assert [r["text"] for r in log.read_all()] == ["one", "two"]


def test_count_lines():
    assert count_lines(None) is None
    assert count_lines("/definitely/not/here") is None
