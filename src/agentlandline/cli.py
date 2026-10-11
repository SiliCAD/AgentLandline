"""
`landline`: start a multi-agent hub and talk to it from a terminal.

    landline up [run.json]         start a hub (with the agents in run.json) and print live activity
    landline send coder "..."      message an agent (as 'user') and print its reply
    landline spawn claude reviewer --role "..."   start another agent in the running hub
    landline ls                    list agents in the running hub
    landline tail                  follow live activity of the running hub
    landline messages              print the run's message log
"""

import argparse
import json
import signal
import sys
import threading
from datetime import datetime
from typing import Any, Dict, Optional

from .hub import Hub, HubServer, HubClient, HubError, load_config

PREVIEW_CHARS = 240
_COLORS = ["35", "36", "33", "32", "34", "31"]  # magenta, cyan, yellow, green, blue, red


class EventPrinter:
    """Turns hub events into one readable line each. Streamed text is buffered per agent."""

    def __init__(self, stream=sys.stdout, color: Optional[bool] = None):
        self.stream = stream
        self.color = stream.isatty() if color is None else color
        self._agent_colors: Dict[str, str] = {}
        self._pending_text: Dict[str, str] = {}

    def _paint(self, text: str, code: str) -> str:
        return f"\033[{code}m{text}\033[0m" if self.color else text

    def _name(self, agent: str) -> str:
        if agent == "user":
            return self._paint(f"{agent:<10}", "1")
        code = self._agent_colors.setdefault(agent, _COLORS[len(self._agent_colors) % len(_COLORS)])
        return self._paint(f"{agent:<10}", code)

    @staticmethod
    def _short(text: Any) -> str:
        text = text if isinstance(text, str) else json.dumps(text, default=str)
        text = " ".join(text.split())
        return text if len(text) <= PREVIEW_CHARS else text[:PREVIEW_CHARS] + "…"

    def _line(self, ts: str, agent: str, body: str):
        clock = ts[11:19] if ts and len(ts) >= 19 else datetime.now().strftime("%H:%M:%S")
        self.stream.write(f"{self._paint(clock, '2')} {self._name(agent)} {body}\n")
        self.stream.flush()

    def _flush_text(self, agent: str, ts: str):
        text = self._pending_text.pop(agent, "")
        if text.strip():
            self._line(ts, agent, self._short(text))

    def handle(self, event: Dict[str, Any]):
        kind, agent, ts = event.get("kind"), event.get("agent", ""), event.get("ts", "")

        if kind == "text_delta":
            self._pending_text[agent] = self._pending_text.get(agent, "") + event.get("text", "")
            return
        if kind == "text":
            self._pending_text.pop(agent, None)  # the complete block replaces the streamed chunks
            self._line(ts, agent, self._short(event.get("text", "")))
            return
        if agent:
            self._flush_text(agent, ts)

        if kind == "message":
            sender, to = event["from"], event["to"]
            self._flush_text(sender, ts)
            arrow = self._paint("──▶", self._agent_colors.get(sender, "1"))
            self._line(ts, sender, f"{arrow} {self._name(to).rstrip()} [{event['id']}] {self._short(event.get('text', ''))}")
        elif kind == "thinking":
            text = event.get("text") or "(thinking not shown by this CLI)"
            self._line(ts, agent, self._paint("💭 " + self._short(text), "2;3"))
        elif kind == "tool_start":
            self._line(ts, agent, f"▸ {event.get('name')} {self._short(event.get('input') or {})}")
        elif kind == "tool_end":
            mark = self._paint("✓", "32") if event.get("ok") else self._paint("✗", "31")
            self._line(ts, agent, f"  {mark} {self._short(event.get('output') or '')}")
        elif kind == "turn_start":
            self._line(ts, agent, self._paint(f"● working on {event.get('message_id')}", "2"))
        elif kind == "turn_end" and not event.get("ok"):
            self._line(ts, agent, self._paint("✗ turn ended with an error", "31"))


def _print_json(data: Any):
    print(json.dumps(data, indent=2, default=str))


def cmd_up(args) -> int:
    try:
        config = load_config(args.config) if args.config else {"run": None, "agents": []}
        hub = Hub(config["agents"], run_id=args.run or config["run"])
        server = HubServer(hub)
        server.start()
    except HubError as e:
        print(f"landline: {e}", file=sys.stderr)
        return 1

    printer = EventPrinter()
    hub.subscribe(printer.handle)
    if hub.agents:
        print(f"Starting {', '.join(hub.agents)}…", flush=True)
    try:
        hub.start()
    except Exception as e:
        print(f"landline: failed to start agents: {e}", file=sys.stderr)
        server.stop()
        return 1

    agents = ", ".join(hub.agents) or "no agents yet (add some with `landline spawn`)"
    print(f"Hub '{hub.run_id}' is up with {agents}.")
    print(f"  messages: {hub.log.path}")
    print(f"  socket:   {hub.socket_path}")
    print("  talk to it with `landline send <agent> <text>`; Ctrl-C to stop.\n", flush=True)

    # Handle SIGINT explicitly too: a hub launched in the background from a
    # non-interactive shell inherits SIGINT as ignored, so Ctrl-C/kill -INT
    # would otherwise never reach it.
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        while not stop.wait(0.5):
            pass
    finally:
        print("\nStopping agents…")
        server.stop()
        hub.stop()
    return 0


def cmd_send(args) -> int:
    res = HubClient(args.socket).send(args.sender, args.agent, " ".join(args.text), wait=not args.no_wait)
    if not res.get("ok"):
        print(f"landline: {res.get('error')}", file=sys.stderr)
        return 1
    if res.get("reply"):
        print(res["reply"]["text"])
    else:
        print(f"{res['status']} ({res['message']['id']})" + (f": {res['error']}" if res.get("error") else ""))
    return 0


def cmd_spawn(args) -> int:
    print(f"Starting {args.name}…", flush=True)
    res = HubClient(args.socket).spawn(args.name, args.backend, model=args.model, effort=args.effort,
                                       cwd=args.cwd, role=args.role, session_id=args.session)
    if not res.get("ok"):
        print(f"landline: {res.get('error')}", file=sys.stderr)
        return 1
    agent = res["agent"]
    print(f"{agent['name']} is up ({agent['backend']}" + (f", {agent['model']}" if agent.get("model") else "") + ").")
    return 0


def cmd_ls(args) -> int:
    res = HubClient(args.socket).list_agents()
    print(f"run: {res.get('run')}")
    for a in res.get("agents", []):
        state = "busy" if a["busy"] else "idle"
        queued = f", {a['queued']} queued" if a.get("queued") else ""
        role = f" — {a['role']}" if a.get("role") else ""
        print(f"  {a['name']:<12} {a['backend']:<12} {state}{queued}  session={a.get('session_id')}{role}")
    return 0


def cmd_tail(args) -> int:
    printer = EventPrinter()
    stream = HubClient(args.socket).subscribe()
    snapshot = next(stream)
    print(f"Following hub '{snapshot.get('run')}' ({', '.join(a['name'] for a in snapshot.get('agents', []))}). Ctrl-C to stop.")
    try:
        for event in stream:
            printer.handle(event)
    except KeyboardInterrupt:
        pass
    return 0


def cmd_messages(args) -> int:
    for record in HubClient(args.socket).messages().get("messages", []):
        print(json.dumps(record, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="landline", description="Run and talk to a multi-agent AgentLandline hub.")
    parser.add_argument("--socket", help="Hub socket path (default: $LANDLINE_SOCKET or ~/.agentlandline/hub.sock)")
    sub = parser.add_subparsers(dest="command", required=True)

    up = sub.add_parser("up", help="Start a hub (with the agents in a run config, if given) and print live activity")
    up.add_argument("config", nargs="?", help="Run config JSON: {\"run\": ..., \"agents\": {name: {backend, model, cwd, role, ...}}}. "
                                             "Without one the hub starts empty.")
    up.add_argument("--run", help="Run id (default: config 'run' or a timestamp)")
    up.set_defaults(func=cmd_up)

    send = sub.add_parser("send", help="Message an agent and print its reply")
    send.add_argument("agent")
    send.add_argument("text", nargs="+")
    send.add_argument("--from", dest="sender", default="user", help="Sender name (default: user)")
    send.add_argument("--no-wait", action="store_true", help="Return once queued instead of waiting for the reply")
    send.set_defaults(func=cmd_send)

    spawn = sub.add_parser("spawn", help="Start another agent in the running hub")
    spawn.add_argument("backend", help="claude or agy")
    spawn.add_argument("name")
    spawn.add_argument("--model")
    spawn.add_argument("--effort")
    spawn.add_argument("--cwd", help="Working folder (default: where the hub was started)")
    spawn.add_argument("--role", help="What this agent is for; the others are told")
    spawn.add_argument("--session", help="Resume this CLI session id instead of starting a new conversation")
    spawn.set_defaults(func=cmd_spawn)

    sub.add_parser("ls", help="List agents in the running hub").set_defaults(func=cmd_ls)
    sub.add_parser("tail", help="Follow live activity of the running hub").set_defaults(func=cmd_tail)
    sub.add_parser("messages", help="Print the run's message log").set_defaults(func=cmd_messages)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except HubError as e:
        print(f"landline: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
