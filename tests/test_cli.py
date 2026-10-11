"""Unit tests for the `landline` CLI: event printing and commands against a hub socket."""

import io
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
sys.path.insert(0, os.path.dirname(__file__))

from agentlandline.cli import EventPrinter, main
from agentlandline.hub import HubServer
from test_hub import make_hub, short_dir  # noqa: F401  (fixture)


def _printed(events):
    out = io.StringIO()
    printer = EventPrinter(stream=out, color=False)
    for e in events:
        printer.handle(e)
    return [line[9:] for line in out.getvalue().splitlines()]  # drop the clock


def test_streamed_text_is_buffered_until_the_next_event():
    lines = _printed([
        {"kind": "text_delta", "agent": "coder", "text": "Sizing "},
        {"kind": "text_delta", "agent": "coder", "text": "M1 now."},
        {"kind": "tool_start", "agent": "coder", "name": "run_command", "input": {"CommandLine": "ls"}},
        {"kind": "tool_end", "agent": "coder", "ok": False, "output": "boom"},
    ])
    assert lines == [
        "coder      Sizing M1 now.",
        'coder      ▸ run_command {"CommandLine": "ls"}',
        "coder        ✗ boom",
    ]


def test_complete_text_block_replaces_streamed_chunks():
    lines = _printed([
        {"kind": "text_delta", "agent": "a", "text": "Hel"},
        {"kind": "text", "agent": "a", "text": "Hello."},
    ])
    assert lines == ["a          Hello."]


def test_message_and_thinking_lines():
    lines = _printed([
        {"kind": "message", "id": "m2", "from": "architect", "to": "coder", "text": "Size   M1\n now"},
        {"kind": "thinking", "agent": "tester", "text": ""},
    ])
    assert lines[0] == "architect  ──▶ coder [m2] Size M1 now"
    assert "thinking not shown" in lines[1]


def test_send_ls_and_messages_commands(short_dir, capsys):  # noqa: F811
    hub, _ = make_hub(short_dir, {}, a={"role": "lead"}, b={})
    server = HubServer(hub)
    server.start()
    try:
        sock = ["--socket", hub.socket_path]
        assert main(sock + ["send", "a", "hello", "there"]) == 0
        assert capsys.readouterr().out.strip() == "a got it"

        assert main(sock + ["ls"]) == 0
        out = capsys.readouterr().out
        assert "run: test-run" in out and "a " in out and "lead" in out

        assert main(sock + ["messages"]) == 0
        assert len(capsys.readouterr().out.strip().splitlines()) == 2

        assert main(sock + ["send", "nobody", "x"]) == 1
        assert "Unknown agent" in capsys.readouterr().err
    finally:
        server.stop()
        hub.stop()


def test_commands_without_hub_fail_cleanly(short_dir, capsys):  # noqa: F811
    assert main(["--socket", os.path.join(short_dir, "none.sock"), "ls"]) == 1
    assert "landline up" in capsys.readouterr().err
