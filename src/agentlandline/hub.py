"""
Hub: runs several named agent sessions side by side and routes messages between them.

- Each agent is an AgyPipeline or ClaudePipeline started from an AgentSpec, with
  an inbox. Messages are delivered one at a time; a message that arrives while
  the agent is mid-turn waits in the inbox until the turn ends.
- Every message (and every reply) is recorded in the run's MessageLog.
- Raw CLI events are normalized (agentlandline.events) and fanned out to
  subscribers together with message records, for live display.
- HubServer exposes the hub on a local Unix socket (one JSON request per
  connection). Agents reach it through the MCP server's hub-mode tools, which
  find the socket and their own name via the LANDLINE_* variables set here.

Run state lives in `<home>/runs/<run_id>/` (home: $LANDLINE_HOME or
~/.agentlandline): `messages.jsonl` plus `run.json` (agents and session ids).
"""

import json
import logging
import os
import queue
import re
import socket
import shutil
import socketserver
import threading
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional

from .agy_pipeline import AgyPipeline
from .claude_pipeline import ClaudePipeline
from .events import normalize, agy_completed_step, agy_step_thinking
from .message_log import MessageLog, count_lines
from .pipeline_factory import normalize_backend

logger = logging.getLogger("AgentLandlineHub")

USER = "user"
# Names that address something other than one agent: the user, and '@all' (every agent) in viewers.
RESERVED_NAMES = {USER, "all"}
AGENT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,31}$")
RUN_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def landline_home() -> str:
    return os.path.expanduser(os.environ.get("LANDLINE_HOME") or "~/.agentlandline")


def default_socket_path() -> str:
    return os.environ.get("LANDLINE_SOCKET") or os.path.join(landline_home(), "hub.sock")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class HubError(Exception):
    pass


@dataclass
class AgentSpec:
    name: str
    backend: str = "claude"
    model: Optional[str] = None
    effort: Optional[str] = None
    cwd: Optional[str] = None
    role: Optional[str] = None
    session_id: Optional[str] = None  # resume an existing CLI session

    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> "AgentSpec":
        if not AGENT_NAME.match(name or "") or name.lower() in RESERVED_NAMES:
            raise HubError(f"Invalid agent name '{name}': use letters, digits, - or _ (start with a letter), "
                           f"and not {' or '.join(repr(n) for n in sorted(RESERVED_NAMES))}.")
        known = {k: data[k] for k in ("backend", "model", "effort", "cwd", "role", "session_id") if data.get(k) is not None}
        spec = cls(name=name, **known)
        try:
            spec.backend = normalize_backend(spec.backend)
        except ValueError as e:
            raise HubError(str(e))
        if spec.cwd:
            spec.cwd = os.path.abspath(os.path.expanduser(spec.cwd))
        return spec


def load_config(path: str) -> Dict[str, Any]:
    """
    Reads a run config:
        {"run": "my-run",                            (optional)
         "agents": {"planner": {"backend": "claude", "model": "opus", "role": "..."},
                    "coder":   {"backend": "agy", "cwd": "~/projects/app"}}}
    Returns {"run": str|None, "agents": [AgentSpec, ...]}.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise HubError(f"Run config not found: {os.path.abspath(path)}")
    except ValueError as e:
        raise HubError(f"{path} is not valid JSON: {e}")
    if not isinstance(data, dict):
        raise HubError(f"{path}: expected a JSON object with an 'agents' key")
    agents = data.get("agents") or {}
    if not isinstance(agents, dict) or not agents:
        raise HubError(f"{path}: 'agents' must be a non-empty object of name -> settings")
    for name in agents:
        if name.lower() in RESERVED_NAMES:
            raise HubError(f"{path}: '{name}' is reserved and can't be an agent name")
    # A run's own run.json (written by the hub) is also a valid config: it lists
    # every agent, including ones spawned while it ran, with their session ids.
    return {"run": data.get("run"), "agents": [AgentSpec.from_dict(n, a or {}) for n, a in agents.items()]}


@dataclass
class _Delivery:
    record: Dict[str, Any]
    done: threading.Event = field(default_factory=threading.Event)
    reply: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class _AgentSlot:
    def __init__(self, spec: AgentSpec, pipeline):
        self.spec = spec
        self.pipeline = pipeline
        self.inbox: "queue.Queue[Optional[_Delivery]]" = queue.Queue()
        self.current_trigger: Optional[str] = None  # message id that started the running turn
        self.busy = False
        self.introduced = False
        self.stopping = False
        self.known_agents: set = set()  # other agents this agent has been told about
        self.reported_model: Optional[str] = None  # what the CLI says it is running
        # Session id from the CLI's init event. The pipelines notify listeners before
        # they store it themselves, so the hub keeps its own copy for run.json.
        self.reported_session: Optional[str] = None

    @property
    def session_id(self) -> Optional[str]:
        return self.pipeline.conversation_id or self.reported_session or self.spec.session_id
        self.thread: Optional[threading.Thread] = None


class Hub:
    def __init__(
        self,
        specs: List[AgentSpec],
        run_id: Optional[str] = None,
        home: Optional[str] = None,
        socket_path: Optional[str] = None,
        pipeline_factory: Optional[Callable[[AgentSpec, Dict[str, str]], Any]] = None,
    ):
        names = [s.name for s in specs]
        if len(set(names)) != len(names):
            raise HubError(f"Agent names must be unique: {names}")
        self.run_id = run_id or time.strftime("run-%Y%m%d-%H%M%S")
        self.home = home or landline_home()
        self.run_dir = os.path.join(self.home, "runs", self.run_id)
        self.socket_path = socket_path or default_socket_path()
        self.log = MessageLog(self.run_dir)
        self.started_at = _now()
        self._pipeline_factory = pipeline_factory or self._make_pipeline
        self._subscribers: List[Callable[[Dict[str, Any]], None]] = []
        self._lock = threading.Lock()
        self._waiting_on: Dict[str, str] = {}  # agent -> agent whose reply it is waiting for
        self._starting: set = set()  # names of agents being spawned right now
        self._run_file_lock = threading.Lock()  # run.json is rewritten from several threads
        # Agents stopped with stop_agent(): kept in run.json (marked stopped) so the
        # session can be resumed with them.
        self._stopped: Dict[str, _AgentSlot] = {}
        # Agents of an adopted run that aren't running (yet): kept in run.json as saved.
        self._dormant: Dict[str, Dict[str, Any]] = {}
        self.agents: Dict[str, _AgentSlot] = {}
        for spec in specs:
            self.agents[spec.name] = self._new_slot(spec)

    def _new_slot(self, spec: AgentSpec) -> _AgentSlot:
        env = {"LANDLINE_AGENT": spec.name, "LANDLINE_SOCKET": self.socket_path, "LANDLINE_RUN": self.run_id}
        return _AgentSlot(spec, self._pipeline_factory(spec, env))

    def _slots(self) -> List[_AgentSlot]:
        """Snapshot of the agents, safe to iterate while another thread spawns one."""
        with self._lock:
            return list(self.agents.values())

    # ------------------------------------------------------------------ setup

    @staticmethod
    def _make_pipeline(spec: AgentSpec, env: Dict[str, str]):
        if spec.backend == "claude":
            # Hub turns can legitimately run for a long time (an agent waiting on
            # another agent's reply is itself mid tool call), so lift Claude
            # Code's MCP tool-call timeout for message_agent.
            env = {**env, "MCP_TOOL_TIMEOUT": str(24 * 3600 * 1000)}
            return ClaudePipeline(cwd=spec.cwd, model=spec.model, effort=spec.effort,
                                  conversation_id=spec.session_id, extra_env=env, stream_partial=True)
        return AgyPipeline(cwd=spec.cwd, model=spec.model, effort=spec.effort,
                           conversation_id=spec.session_id, extra_env=env)

    # claude stays silent until its first prompt, so its start() always waits out
    # the timeout; a short one is enough to catch a process that dies on launch.
    START_TIMEOUTS = {"claude": 5.0, "antigravity": 60.0}

    def start(self):
        """Starts all agents in parallel; raises if any of them fails to start."""
        errors: Dict[str, str] = {}

        def _start(name: str, slot: _AgentSlot):
            try:
                self._start_pipeline(slot)
            except Exception as e:
                errors[name] = str(e)

        starters = [threading.Thread(target=_start, args=item) for item in self.agents.items()]
        for t in starters:
            t.start()
        for t in starters:
            t.join()
        if errors:
            self.stop()
            raise HubError("; ".join(f"{name}: {err}" for name, err in errors.items()))

        for slot in self._slots():
            self._start_worker(slot)
        self._write_run_file()
        logger.info(f"Hub {self.run_id} started with agents {list(self.agents)}")

    def _start_pipeline(self, slot: _AgentSlot):
        slot.pipeline.add_event_listener(lambda raw: self._on_raw_event(slot.spec.name, raw, slot))
        slot.pipeline.start(timeout=self.START_TIMEOUTS.get(slot.spec.backend, 60.0))

    def _start_worker(self, slot: _AgentSlot):
        slot.thread = threading.Thread(target=self._worker, args=(slot,), name=f"landline-{slot.spec.name}", daemon=True)
        slot.thread.start()

    def spawn(self, spec: AgentSpec) -> Dict[str, Any]:
        """
        Starts a new agent in the running hub. The other agents are told about
        it with their next message. Returns its list_agents() entry.
        """
        # Only the name is reserved under the lock, so several agents can start at once.
        with self._lock:
            if spec.name in self.agents or spec.name in self._starting:
                raise HubError(f"An agent named '{spec.name}' already exists.")
            self._starting.add(spec.name)
        try:
            slot = self._new_slot(spec)
            try:
                self._start_pipeline(slot)
            except Exception as e:
                try:
                    slot.pipeline.close()
                except Exception:
                    pass
                raise HubError(f"Couldn't start '{spec.name}': {e}")
            with self._lock:
                self.agents[spec.name] = slot
                self._stopped.pop(spec.name, None)
                self._dormant.pop(spec.name, None)
        finally:
            with self._lock:
                self._starting.discard(spec.name)
        self._start_worker(slot)
        self._write_run_file()
        info = self._agent_info(slot)
        self._emit({"kind": "agent_added", "agent": spec.name, "info": info})
        logger.info(f"Spawned agent {spec.name} ({spec.backend})")
        return info

    def stop_agent(self, name: str) -> Dict[str, Any]:
        """
        Stops one agent: its CLI process is closed, messages still queued for it
        fail with 'stopped', and it stays in run.json (marked stopped) so resuming
        the session can bring it back. Returns its last list_agents() entry.
        """
        with self._lock:
            slot = self.agents.pop(name, None)
            if slot is None:
                raise HubError(f"Unknown agent '{name}'. Agents: {list(self.agents)}")
            self._stopped[name] = slot
            for waiter, target in list(self._waiting_on.items()):
                if waiter == name:
                    self._waiting_on.pop(waiter, None)
        slot.stopping = True
        info = self._agent_info(slot)
        while True:
            try:
                delivery = slot.inbox.get_nowait()
            except queue.Empty:
                break
            if delivery is not None:
                delivery.error = f"Agent '{name}' was stopped."
                delivery.done.set()
        slot.inbox.put(None)
        try:
            slot.pipeline.close()
        except Exception as e:
            logger.warning(f"Error closing agent {name}: {e}")
        self._write_run_file()
        self._emit({"kind": "agent_removed", "agent": name})
        logger.info(f"Stopped agent {name}")
        return info

    def adopt(self, run_id: str) -> str:
        """
        Continues a saved run instead of the current one, which must not have any
        messages yet (e.g. a fresh hub before resuming a session into it). The
        hub's log and run.json move to that run, message numbering continues
        there, its agents stay listed in run.json until they're spawned again, and
        the empty run the hub started with is removed. Returns the run id.
        """
        if run_id == self.run_id:
            return run_id
        target = os.path.join(self.home, "runs", run_id)
        try:
            with open(os.path.join(target, "run.json"), "r", encoding="utf-8") as f:
                saved = json.load(f)
        except (OSError, ValueError):
            raise HubError(f"No saved session '{run_id}'.")
        if count_lines(self.log.path):
            raise HubError("This session already has messages, so it can't continue another one.")
        old_id, old_dir = self.run_id, self.run_dir
        with self.log._lock, self._run_file_lock:
            self.run_id, self.run_dir = run_id, target
            self.log = MessageLog(target)  # continues that run's numbering
            self.started_at = saved.get("started") or self.started_at
            with self._lock:
                self._dormant = {n: a for n, a in (saved.get("agents") or {}).items()
                                 if n not in self.agents and isinstance(a, dict)}
        if os.path.isdir(old_dir) and not count_lines(os.path.join(old_dir, MessageLog.FILENAME)):
            shutil.rmtree(old_dir, ignore_errors=True)
        self._write_run_file()
        self._emit({"kind": "run_switched", "run": run_id, "previous": old_id})
        logger.info(f"Hub continues run {run_id} (was {old_id})")
        return run_id

    def rename(self, new_run_id: str) -> str:
        """
        Renames the session: its folder under runs/ moves to the new name, which
        then identifies it everywhere (resume, listings). Returns the new id.
        """
        if not RUN_NAME.match(new_run_id or ""):
            raise HubError(f"Invalid session name '{new_run_id}': use letters, digits, '.', '-' or '_' (up to 64).")
        if new_run_id == self.run_id:
            return self.run_id
        new_dir = os.path.join(self.home, "runs", new_run_id)
        if os.path.exists(new_dir):
            raise HubError(f"A session named '{new_run_id}' already exists.")
        old = self.run_id
        # Hold the message log's and run.json's locks so nothing writes mid-move.
        with self.log._lock, self._run_file_lock:
            os.makedirs(os.path.dirname(new_dir), exist_ok=True)
            if os.path.exists(self.run_dir):
                os.rename(self.run_dir, new_dir)
            self.run_dir = new_dir
            self.log.run_dir = new_dir
            self.log.path = os.path.join(new_dir, MessageLog.FILENAME)
            self.run_id = new_run_id
        self._write_run_file()
        self._emit({"kind": "run_renamed", "run": new_run_id, "previous": old})
        logger.info(f"Renamed session {old} -> {new_run_id}")
        return new_run_id

    def stop(self):
        for slot in self._slots():
            slot.inbox.put(None)
            try:
                slot.pipeline.close()
            except Exception as e:
                logger.warning(f"Error closing agent {slot.spec.name}: {e}")
        self._write_run_file()

    # ----------------------------------------------------------- live events

    def subscribe(self, callback: Callable[[Dict[str, Any]], None]):
        with self._lock:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[Dict[str, Any]], None]):
        with self._lock:
            if callback in self._subscribers:
                self._subscribers.remove(callback)

    def _emit(self, event: Dict[str, Any]):
        event.setdefault("ts", _now())
        with self._lock:
            subscribers = list(self._subscribers)
        for callback in subscribers:
            try:
                callback(event)
            except Exception as e:
                logger.debug(f"Subscriber error: {e}")

    def _on_raw_event(self, name: str, raw: Dict[str, Any], slot: Optional[_AgentSlot] = None):
        slot = slot or self.agents[name]
        for event in normalize(slot.spec.backend, raw):
            if event["kind"] == "session":
                slot.reported_model = event.get("model") or slot.reported_model
                slot.reported_session = event.get("session_id") or slot.reported_session
                event = {**event, "model": self.model_in_use(slot)}
                self._write_run_file()
            self._emit({**event, "agent": name})
        if slot.spec.backend == "antigravity":
            step = agy_completed_step(raw)
            if step is not None:
                self._emit_agy_thinking(name, step, attempts=3)

    def _emit_agy_thinking(self, name: str, step: int, attempts: int):
        # The transcript line can land slightly after the stream event; retry briefly.
        text = agy_step_thinking(self.agents[name].pipeline.transcript_path(), step)
        if text:
            self._emit({"kind": "thinking", "text": text, "agent": name})
        elif attempts > 1:
            threading.Timer(0.5, self._emit_agy_thinking, args=(name, step, attempts - 1)).start()

    # -------------------------------------------------------------- messaging

    def send(self, sender: str, to: str, text: str, wait: bool = True, timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Sends `text` from `sender` (an agent name or 'user') to agent `to`.
        With wait=True, blocks until `to` has finished the turn this message
        starts and returns its reply; otherwise returns once it is queued.
        Sending to 'user' just records the message (there is no inbox to deliver to).
        """
        if sender != USER and sender not in self.agents:
            raise HubError(f"Unknown sender '{sender}'.")
        if to != USER and to not in self.agents:
            raise HubError(f"Unknown agent '{to}'. Agents: {list(self.agents)}")
        if to == sender:
            raise HubError("An agent can't message itself.")
        if not text or not text.strip():
            raise HubError("Message text is empty.")

        if wait and to != USER and sender != USER:
            self._check_wait_cycle(sender, to)

        record = self._record(sender, to, text)
        if to == USER:
            return {"status": "DELIVERED", "message": record}

        delivery = _Delivery(record)
        if wait and sender != USER:
            with self._lock:
                self._waiting_on[sender] = to
        target = self.agents[to]
        target.inbox.put(delivery)
        if target.stopping:  # stopped between the lookup and the put
            delivery.error = f"Agent '{to}' was stopped."
            delivery.done.set()
        if not wait:
            return {"status": "QUEUED", "message": record}

        try:
            finished = delivery.done.wait(timeout)
        finally:
            if sender != USER:
                with self._lock:
                    self._waiting_on.pop(sender, None)
        if not finished:
            return {"status": "TIMEOUT", "message": record, "error": f"No reply from '{to}' within {timeout}s; the message is still queued/in progress."}
        if delivery.error:
            return {"status": "ERROR", "message": record, "error": delivery.error}
        return {"status": "REPLIED", "message": record, "reply": delivery.reply}

    def _check_wait_cycle(self, sender: str, to: str):
        with self._lock:
            node, seen = to, set()
            while node in self._waiting_on and node not in seen:
                seen.add(node)
                node = self._waiting_on[node]
                if node == sender:
                    raise HubError(
                        f"'{to}' is already waiting on a reply from you ('{sender}'); waiting on it as well would "
                        f"deadlock. Put your answer in your reply instead, or send with wait=false."
                    )

    def _record(self, sender: str, to: str, text: str, caused_by: Optional[str] = None) -> Dict[str, Any]:
        session = transcript = None
        if sender != USER:
            slot = self.agents[sender]
            session = slot.pipeline.conversation_id
            transcript = slot.pipeline.transcript_path()
            if caused_by is None:
                caused_by = slot.current_trigger
        record = self.log.append(sender, to, text, caused_by=caused_by, from_session=session, from_transcript=transcript)
        self._emit({"kind": "message", **record})
        return record

    def _worker(self, slot: _AgentSlot):
        while True:
            delivery = slot.inbox.get()
            if delivery is None:
                return
            record = delivery.record
            slot.busy, slot.current_trigger = True, record["id"]
            self._emit({"kind": "turn_start", "agent": slot.spec.name, "message_id": record["id"]})
            try:
                result = slot.pipeline.send(self._frame(slot, record), timeout=None)
                if slot.stopping:
                    delivery.error = f"Agent '{slot.spec.name}' was stopped before replying."
                else:
                    reply_text = result.response or f"[{result.status}] {result.error or 'no response'}"
                    delivery.reply = self._record(slot.spec.name, record["from"], reply_text, caused_by=record["id"])
            except Exception as e:
                logger.error(f"Agent {slot.spec.name} failed handling {record['id']}: {e}")
                delivery.error = str(e)
            finally:
                slot.busy, slot.current_trigger = False, None
                self._emit({"kind": "idle", "agent": slot.spec.name})
                delivery.done.set()

    @staticmethod
    def _describe(slot: _AgentSlot) -> str:
        return f"- {slot.spec.name}" + (f": {slot.spec.role}" if slot.spec.role else "")

    def _frame(self, slot: _AgentSlot, record: Dict[str, Any]) -> str:
        header = f"[landline message {record['id']} from {record['from']}]\n"
        others_now = [s for s in self._slots() if s is not slot]
        if slot.introduced:
            joined = [s for s in others_now if s.spec.name not in slot.known_agents]
            left = sorted(slot.known_agents - {s.spec.name for s in others_now})
            slot.known_agents = {s.spec.name for s in others_now}
            note = ""
            if joined:
                note += "New agents have joined the session:\n" + "\n".join(self._describe(s) for s in joined) + "\n"
            if left:
                note += "Agents that have left the session: " + ", ".join(left) + "\n"
            return (note + "\n" if note else "") + header + record["text"]
        slot.introduced = True
        slot.known_agents = {s.spec.name for s in others_now}
        others = "\n".join(self._describe(s) for s in others_now)
        intro = (
            f"You are agent '{slot.spec.name}' in a multi-agent session."
            + (f" Your role: {slot.spec.role}" if slot.spec.role else "")
            + f"\nOther agents:\n{others}\n"
            "Messages to you arrive with a [landline message <id> from <sender>] header. Just do what the "
            "message asks and answer in your response: your final response is delivered back to the sender "
            "automatically, so don't use a tool to reply. To ask another agent something yourself, call the "
            "agent-landline `message_agent` tool with `to` and `text`; it returns their reply.\n\n"
        )
        return intro + header + record["text"]

    # ------------------------------------------------------------ inspection

    @staticmethod
    def model_in_use(slot: _AgentSlot) -> Optional[str]:
        """
        The model an agent actually runs: what its CLI reported (claude), else the
        configured one, else agy's default from its settings. None if unknown.
        """
        if slot.reported_model:
            return slot.reported_model
        if slot.spec.model:
            return slot.spec.model
        if slot.spec.backend == "antigravity":
            return AgyPipeline.default_model()
        return None

    def _agent_info(self, slot: _AgentSlot) -> Dict[str, Any]:
        return {
            "name": slot.spec.name,
            "backend": slot.spec.backend,
            "model": self.model_in_use(slot),
            "effort": slot.spec.effort,
            "role": slot.spec.role,
            "session_id": slot.session_id,
            "transcript": slot.pipeline.transcript_path(),
            "busy": slot.busy,
            "queued": slot.inbox.qsize(),
        }

    def list_agents(self) -> List[Dict[str, Any]]:
        return [self._agent_info(slot) for slot in self._slots()]

    def _run_file_agents(self):
        with self._lock:
            live = [(s.spec.name, s) for s in self.agents.values()]
            stopped = [(n, s) for n, s in self._stopped.items() if n not in self.agents]
        return live + stopped

    def _write_run_file(self):
        data = {
            "run": self.run_id,
            "started": self.started_at,
            "cwd": os.getcwd(),
            "socket": self.socket_path,
            "messages": self.log.path,
            "agents": {
                name: {**{k: v for k, v in asdict(slot.spec).items() if k != "name"},
                       # The folder the agent actually works in, even when the config left it
                       # to the hub's folder: resuming needs it (claude finds sessions by folder).
                       "cwd": slot.spec.cwd or getattr(slot.pipeline, "cwd", None),
                       "session_id": slot.session_id,
                       "model_in_use": self.model_in_use(slot),
                       **({"stopped": True} if slot.stopping else {}),
                       "transcript": slot.pipeline.transcript_path()}
                for name, slot in self._run_file_agents()
            },
        }
        with self._lock:
            dormant = {n: a for n, a in self._dormant.items() if n not in data["agents"]}
        data["agents"].update(dormant)
        os.makedirs(self.run_dir, exist_ok=True)
        tmp = os.path.join(self.run_dir, "run.json.tmp")
        with self._run_file_lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, os.path.join(self.run_dir, "run.json"))


# ---------------------------------------------------------------------------
# Local socket API: one JSON request line per connection, JSON response line(s).
#   {"op": "send", "from": "user", "to": "coder", "text": "...", "wait": true}
#   {"op": "list"}
#   {"op": "messages"}
#   {"op": "subscribe"}   -> streams events, one JSON line each, until disconnect
# ---------------------------------------------------------------------------

class HubServer:
    def __init__(self, hub: Hub, socket_path: Optional[str] = None):
        self.hub = hub
        self.socket_path = socket_path or hub.socket_path
        self._server: Optional[socketserver.ThreadingUnixStreamServer] = None

    def start(self):
        if os.path.exists(self.socket_path):
            if _socket_alive(self.socket_path):
                raise HubError(f"Another hub is already listening on {self.socket_path}.")
            os.unlink(self.socket_path)
        os.makedirs(os.path.dirname(self.socket_path), exist_ok=True)
        hub = self.hub

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                try:
                    request = json.loads(self.rfile.readline() or b"{}")
                except ValueError:
                    return self._reply({"ok": False, "error": "Request must be one JSON line."})
                op = request.get("op")
                try:
                    if op == "send":
                        result = hub.send(request.get("from") or USER, request.get("to") or "", request.get("text") or "",
                                          wait=request.get("wait", True), timeout=request.get("timeout"))
                        self._reply({"ok": True, **result})
                    elif op == "spawn":
                        fields = {k: request.get(k) for k in ("backend", "model", "effort", "cwd", "role", "session_id")}
                        info = hub.spawn(AgentSpec.from_dict(request.get("name") or "", fields))
                        self._reply({"ok": True, "agent": info})
                    elif op == "stop":
                        self._reply({"ok": True, "agent": hub.stop_agent(request.get("name") or "")})
                    elif op == "adopt":
                        self._reply({"ok": True, "run": hub.adopt(request.get("run") or "")})
                    elif op == "rename":
                        self._reply({"ok": True, "run": hub.rename(request.get("run") or "")})
                    elif op == "list":
                        self._reply({"ok": True, "run": hub.run_id, "agents": hub.list_agents()})
                    elif op == "messages":
                        self._reply({"ok": True, "run": hub.run_id, "messages": hub.log.read_all()})
                    elif op == "subscribe":
                        self._stream(hub)
                    else:
                        self._reply({"ok": False, "error": f"Unknown op '{op}'."})
                except HubError as e:
                    self._reply({"ok": False, "error": str(e)})

            def _reply(self, payload):
                try:
                    self.wfile.write((json.dumps(payload, default=str) + "\n").encode())
                except OSError:
                    pass

            def _stream(self, hub):
                events: "queue.Queue[Dict[str, Any]]" = queue.Queue()
                hub.subscribe(events.put)
                try:
                    self._reply({"ok": True, "run": hub.run_id, "agents": hub.list_agents()})
                    while True:
                        self.wfile.write((json.dumps(events.get(), default=str) + "\n").encode())
                        self.wfile.flush()
                except OSError:
                    pass
                finally:
                    hub.unsubscribe(events.put)

        self._server = socketserver.ThreadingUnixStreamServer(self.socket_path, Handler)
        self._server.daemon_threads = True
        threading.Thread(target=self._server.serve_forever, name="landline-hub-server", daemon=True).start()

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)


def _socket_alive(path: str) -> bool:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
            s.connect(path)
        return True
    except OSError:
        return False


class HubClient:
    def __init__(self, socket_path: Optional[str] = None):
        self.socket_path = socket_path or default_socket_path()

    def _connect(self) -> socket.socket:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            s.connect(self.socket_path)
        except OSError as e:
            s.close()
            raise HubError(f"No hub listening on {self.socket_path} ({e}). Start one with `landline up <config.json>`.")
        return s

    def request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._connect() as s:
            s.sendall((json.dumps(payload) + "\n").encode())
            with s.makefile("r", encoding="utf-8") as f:
                line = f.readline()
        if not line:
            raise HubError("Hub closed the connection without replying.")
        return json.loads(line)

    def send(self, sender: str, to: str, text: str, wait: bool = True, timeout: Optional[float] = None) -> Dict[str, Any]:
        return self.request({"op": "send", "from": sender, "to": to, "text": text, "wait": wait, "timeout": timeout})

    def list_agents(self) -> Dict[str, Any]:
        return self.request({"op": "list"})

    def stop_agent(self, name: str) -> Dict[str, Any]:
        return self.request({"op": "stop", "name": name})

    def rename(self, run_id: str) -> Dict[str, Any]:
        return self.request({"op": "rename", "run": run_id})

    def adopt(self, run_id: str) -> Dict[str, Any]:
        """Makes the hub continue a saved run (only while the current run has no messages)."""
        return self.request({"op": "adopt", "run": run_id})

    def spawn(self, name: str, backend: str, model: Optional[str] = None, effort: Optional[str] = None,
              cwd: Optional[str] = None, role: Optional[str] = None, session_id: Optional[str] = None) -> Dict[str, Any]:
        """Starts a new agent in the running hub (resuming `session_id` if given); returns once it is up or failed."""
        return self.request({"op": "spawn", "name": name, "backend": backend, "model": model,
                             "effort": effort, "cwd": cwd, "role": role, "session_id": session_id})

    def messages(self) -> Dict[str, Any]:
        return self.request({"op": "messages"})

    def subscribe(self) -> Iterator[Dict[str, Any]]:
        """Yields the hub snapshot first, then live events until the hub goes away."""
        with self._connect() as s:
            s.sendall(b'{"op": "subscribe"}\n')
            with s.makefile("r", encoding="utf-8") as f:
                for line in f:
                    yield json.loads(line)
