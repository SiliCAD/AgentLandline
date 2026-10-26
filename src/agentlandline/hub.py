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
Each folder gets its own hub socket by default (`<home>/hubs/<folder>-<hash>.sock`),
so hubs started in different folders are independent.
"""

import hashlib
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
import uuid
from dataclasses import dataclass, asdict, field, replace
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

from .agy_pipeline import AgyPipeline
from .claude_pipeline import ClaudePipeline
from .codex_pipeline import CodexPipeline
from .context_windows import ContextWindows
from .context import (Checkpoints, ContextError, REWIND_BACKENDS, WAKEUP_ALL, normalize_manages, normalize_spawns,
                      normalize_wakeup, rules_for)
from . import images as image_files
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


def default_socket_path(folder: Optional[str] = None) -> str:
    """
    $LANDLINE_SOCKET, or the socket for `folder` (default: the current one):
    <home>/hubs/<folder name>-<hash of its path>.sock. Hubs started in different
    folders are independent; in the same folder, clients find the one running there.
    """
    if os.environ.get("LANDLINE_SOCKET"):
        return os.environ["LANDLINE_SOCKET"]
    folder = os.path.realpath(folder or os.getcwd())
    name = re.sub(r"[^A-Za-z0-9._-]", "_", os.path.basename(folder) or "root")[:24]
    digest = hashlib.sha1(folder.encode("utf-8")).hexdigest()[:8]
    return os.path.join(landline_home(), "hubs", f"{name}-{digest}.sock")


def _duration(seconds: float) -> str:
    """'45s', '2m', '1h05m'."""
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m" + (f"{seconds % 60:02d}s" if seconds % 60 else "")
    return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"


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
    # With session_id: start on a copy of that session instead of continuing it, so the
    # agent begins with its context and the original stays as it is.
    fork: bool = False
    # The session this agent was forked from (set by the hub; kept in run.json).
    forked_from: Optional[str] = None
    # Agents whose context this agent may manage (agentlandline.context), in full form.
    manages: Optional[Dict[str, Dict[str, Any]]] = None
    # Whether (and how) this agent may start other agents (agentlandline.context.normalize_spawns).
    spawns: Optional[Dict[str, Any]] = None
    # The agent that started this one with start_agent (set by the hub; kept in run.json).
    spawned_by: Optional[str] = None
    # When the hub wakes this agent up with a message (agentlandline.context.normalize_wakeup).
    wakeup: Optional[List[Dict[str, Any]]] = None
    # Sessions this agent had before it was rewound, newest last.
    previous_sessions: List[str] = field(default_factory=list)
    # Set only while starting a rewound copy: {"at": <message uuid>, "session_id": <the fork's id>}.
    # Not saved in run.json.
    rewind: Optional[Dict[str, str]] = None
    # Its context window in tokens, for models the hub can't find one for (agentlandline.context_windows).
    context_window: Optional[int] = None
    # How full its context was when run.json was last written ({"used": tokens}), so a resumed
    # session shows it before the agent's next model call. Read from run.json's "context".
    last_context: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, name: str, data: Dict[str, Any]) -> "AgentSpec":
        if not AGENT_NAME.match(name or "") or name.lower() in RESERVED_NAMES:
            raise HubError(f"Invalid agent name '{name}': use letters, digits, - or _ (start with a letter), "
                           f"and not {' or '.join(repr(n) for n in sorted(RESERVED_NAMES))}.")
        known = {k: data[k] for k in ("backend", "model", "effort", "cwd", "role", "session_id") if data.get(k) is not None}
        spec = cls(name=name, **known)
        try:
            spec.manages = normalize_manages(data.get("manages"))
            spec.spawns = normalize_spawns(data.get("spawns"))
            spec.wakeup = normalize_wakeup(data.get("wakeup"), name)
        except ContextError as e:
            raise HubError(f"Agent '{name}': {e}")
        spec.spawned_by = data.get("spawned_by")
        spec.fork = bool(data.get("fork"))
        if spec.fork and not spec.session_id:
            raise HubError(f"Agent '{name}': 'fork' needs the 'session_id' of the session to fork.")
        spec.forked_from = data.get("forked_from")
        window = data.get("context_window")
        if window is not None:
            if isinstance(window, bool) or not isinstance(window, int) or window <= 0:
                raise HubError(f"Agent '{name}': 'context_window' must be a number of tokens.")
            spec.context_window = window
        saved = data.get("context")
        if isinstance(saved, dict) and isinstance(saved.get("used"), int):
            spec.last_context = {"used": saved["used"]}
        previous = data.get("previous_sessions") or []
        spec.previous_sessions = [str(s) for s in previous] if isinstance(previous, list) else []
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
        # The delivery being handled; set and cleared under `lock`, so a forced message
        # interrupts only the turn that was running when it arrived.
        self.current: Optional[_Delivery] = None
        self.lock = threading.Lock()
        self.interrupted: Optional[_Delivery] = None  # the turn the user interrupted
        self.busy = False
        self.introduced = False
        self.stopping = False
        self.known_agents: set = set()  # other agents this agent has been told about
        self.reported_model: Optional[str] = None  # what the CLI says it is running
        # How full its context window is: the tokens of its latest model call (prompt + output
        # so far), None until it makes one. `context_reported` is the window its CLI reported.
        saved = spec.last_context or {}
        self.context_prompt: Optional[int] = saved.get("used")
        self.context_output = 0
        self.context_reported: Optional[int] = None
        self.context_sent = 0.0  # when the last "context" event went out
        self.context_last: Optional[Dict[str, Any]] = None  # what it said
        # Session id from the CLI's init event. The pipelines notify listeners before
        # they store it themselves, so the hub keeps its own copy for run.json.
        self.reported_session: Optional[str] = None
        self.last_uuid: Optional[str] = None  # claude: the last assistant message of the running turn
        self.turn_tools = 0                   # tool calls in the running turn
        self.notice: Optional[str] = None     # put in front of the next message delivered to it
        self.restart_pending = False          # restart its CLI once the running turn ends (new tools)
        # The (manages, spawns, wakeup) its running CLI was started with, while a restart is pending:
        # what it goes back to if that restart fails.
        self.restart_from: Optional[Tuple[Any, Any, Any]] = None
        # When its last turn ended (time.monotonic(); None: no turn yet) and how: {message_id, from, outcome}.
        # The wake-up watcher reads them.
        self.idle_since: Optional[float] = None
        self.last_turn: Optional[Dict[str, Any]] = None
        # Started on an existing conversation (resumed or forked), so it already knows things.
        self.has_history = bool(spec.session_id or spec.forked_from)

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
        self.checkpoints = Checkpoints(lambda: self.run_dir)
        self.started_at = _now()
        self.context_windows = ContextWindows(self.home)
        self._pipeline_factory = pipeline_factory or self._make_pipeline
        self._subscribers: List[Callable[[Dict[str, Any]], None]] = []
        self._lock = threading.Lock()
        self._waiting_on: Dict[str, str] = {}  # agent -> agent whose reply it is waiting for
        # Senders that aren't agents, added by the user (add_sender): e.g. "eda-mcp" for a tool's events, so
        # its messages, and the agent's replies to them, aren't the user's.
        self.senders: Set[str] = set()
        self._starting: set = set()  # names of agents being spawned right now
        self._run_file_lock = threading.Lock()  # run.json is rewritten from several threads
        # Agents stopped with stop_agent(): kept in run.json (marked stopped) so the
        # session can be resumed with them.
        self._stopped: Dict[str, _AgentSlot] = {}
        # Agents of an adopted run that aren't running (yet): kept in run.json as saved.
        self._dormant: Dict[str, Dict[str, Any]] = {}
        # Wake-up rules: the idle stretch each agent's rules last fired for, {agent: {rule index: stretch}}.
        self._wakeup_fired: Dict[str, Dict[int, float]] = {}
        self._closing = threading.Event()
        self.agents: Dict[str, _AgentSlot] = {}
        for spec in specs:
            self.agents[spec.name] = self._new_slot(spec)

    def _new_slot(self, spec: AgentSpec) -> _AgentSlot:
        env = {"LANDLINE_AGENT": spec.name, "LANDLINE_SOCKET": self.socket_path, "LANDLINE_RUN": self.run_id}
        if spec.manages:
            env["LANDLINE_MANAGES"] = "1"  # the agent gets the manage_context tool
        if spec.spawns:
            env["LANDLINE_SPAWNS"] = "1"   # the agent gets start_agent, stop_agent and find_sessions
        if spec.fork or spec.rewind:
            spec = replace(spec, last_context=None)  # a copy: its size is known after its first call
        if spec.fork and spec.backend == "codex":
            # Codex assigns the copied thread's id on its first prompted turn. Keep
            # the source id and fork flag in run.json until that event arrives.
            spec = replace(spec, forked_from=spec.session_id)
        elif spec.fork:
            spec = replace(spec, rewind={"session_id": str(uuid.uuid4())}, forked_from=spec.session_id)
        if spec.rewind:
            # The pipeline starts on a copy of spec.session_id (cut at rewind["at"], if given); from
            # then on the agent is that copy, so run.json (and resuming the run) continues the copy.
            try:
                pipeline = self._pipeline_factory(spec, env)
            except (OSError, ValueError) as e:
                raise HubError(f"Couldn't copy session {spec.session_id} for '{spec.name}': {e}")
            spec = replace(spec, session_id=pipeline.conversation_id or spec.rewind["session_id"], fork=False, rewind=None)
            return _AgentSlot(spec, pipeline)
        return _AgentSlot(spec, self._pipeline_factory(spec, env))

    def _slots(self) -> List[_AgentSlot]:
        """Snapshot of the agents, safe to iterate while another thread spawns one."""
        with self._lock:
            return list(self.agents.values())

    # ------------------------------------------------------------------ setup

    @staticmethod
    def _make_pipeline(spec: AgentSpec, env: Dict[str, str]):
        if spec.backend == "codex":
            if spec.rewind:
                raise HubError("Codex conversations can't be cut at a message yet.")
            return CodexPipeline(cwd=spec.cwd, model=spec.model, effort=spec.effort,
                                 conversation_id=spec.session_id, fork_session=spec.fork, extra_env=env)
        if spec.backend == "claude":
            # Hub turns can legitimately run for a long time (an agent waiting on
            # another agent's reply is itself mid tool call), so lift Claude
            # Code's MCP tool-call timeout for message_agent.
            env = {**env, "MCP_TOOL_TIMEOUT": str(24 * 3600 * 1000)}
            rewind = spec.rewind or {}
            return ClaudePipeline(cwd=spec.cwd, model=spec.model, effort=spec.effort,
                                  conversation_id=spec.session_id, extra_env=env, stream_partial=True,
                                  fork_session=bool(rewind), fork_session_id=rewind.get("session_id"),
                                  resume_at=rewind.get("at"))
        session = spec.session_id
        if spec.rewind:
            if spec.rewind.get("at"):
                raise HubError("agy conversations can't be cut at a message yet.")
            session = AgyPipeline.fork_conversation(spec.session_id, spec.rewind["session_id"])
        return AgyPipeline(cwd=spec.cwd, model=spec.model, effort=spec.effort,
                           conversation_id=session, extra_env=env)

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
        threading.Thread(target=self._watch_idle, name="landline-wakeup", daemon=True).start()
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
            if spec.name in self.senders:
                raise HubError(f"'{spec.name}' is a sender in this session, not an agent.")
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
        self._closing.set()
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
        if slot.spec.backend == "claude" and raw.get("type") == "assistant" and raw.get("uuid"):
            slot.last_uuid = raw["uuid"]
        for event in normalize(slot.spec.backend, raw):
            if event["kind"] == "context":
                self._learn_window(slot, event.get("context_windows") or {})
                self._update_context(slot, event)
                continue
            if event["kind"] == "compacted":
                slot.context_prompt, slot.context_output = event.get("post_tokens"), 0
                self._emit_context(slot, now=True)
            if event["kind"] == "turn_end":
                self._learn_window(slot, event.get("context_windows") or {})
                if slot.context_prompt is not None:
                    self._emit_context(slot, now=True)  # the turn's last output count, and the window
            if event["kind"] == "tool_start":
                slot.turn_tools += 1
            if event["kind"] == "session":
                if slot.spec.backend == "codex" and raw.get("type") == "thread.started":
                    slot.spec = replace(slot.spec, session_id=event["session_id"], fork=False)
                slot.reported_model = event.get("model") or slot.reported_model
                slot.reported_session = event.get("session_id") or slot.reported_session
                event = {**event, "model": self.model_in_use(slot)}
                self._write_run_file()
            if event["kind"] == "turn_end" and slot.interrupted is not None:
                event = {**event, "interrupted": True}  # it can arrive before the "interrupted" event
            self._emit({**event, "agent": name})
        if slot.spec.backend == "antigravity":
            step = agy_completed_step(raw)
            if step is not None:
                self._emit_agy_thinking(name, step, attempts=3)

    CONTEXT_EVERY = 1.0  # seconds between "context" events while an agent writes

    def _update_context(self, slot: _AgentSlot, event: Dict[str, Any]):
        new_call = "prompt" in event
        if new_call:
            slot.context_prompt = event["prompt"]
        slot.context_output = event.get("output") or 0
        self._emit_context(slot, now=new_call)

    def _emit_context(self, slot: _AgentSlot, now: bool = False):
        if not now and time.monotonic() - slot.context_sent < self.CONTEXT_EVERY:
            return
        context = self.context_of(slot) or {"used": None, "window": None}
        if context == slot.context_last:  # e.g. agy's last step, then its turn's end
            return
        slot.context_sent, slot.context_last = time.monotonic(), context
        self._emit({"kind": "context", "agent": slot.spec.name, **context})

    def _learn_window(self, slot: _AgentSlot, windows: Dict[str, int]):
        """claude reported its model's window: remember it for this model (and the name it was asked for)."""
        model = self.model_in_use(slot)
        size = windows.get(model) if model in windows else (next(iter(windows.values())) if len(windows) == 1 else None)
        if not size:
            return
        slot.context_reported = size
        for name in {model, slot.spec.model} - {None}:
            self.context_windows.learn(name, size)

    def context_of(self, slot: _AgentSlot) -> Optional[Dict[str, Any]]:
        """How full the agent's context window is: {"used": tokens, "window": tokens or None}, or None if unknown."""
        if slot.context_prompt is None:
            return None
        window = slot.spec.context_window or slot.context_reported or self.context_windows.window(self.model_in_use(slot))
        return {"used": slot.context_prompt + slot.context_output, "window": window}

    def _emit_agy_thinking(self, name: str, step: int, attempts: int):
        # The transcript line can land slightly after the stream event; retry briefly.
        text = agy_step_thinking(self.agents[name].pipeline.transcript_path(), step)
        if text:
            self._emit({"kind": "thinking", "text": text, "agent": name})
        elif attempts > 1:
            threading.Timer(0.5, self._emit_agy_thinking, args=(name, step, attempts - 1)).start()

    # -------------------------------------------------------------- messaging

    def add_sender(self, name: str) -> List[str]:
        """
        Lets `name`, which isn't an agent, send messages: a tool or driver acting on its own account
        (e.g. "eda-mcp", sending a watcher agent the EDA calls it should look at). The agent's reply goes
        back to `name`, so it isn't a message to the user; the agent can still message the user itself.
        Returns the senders. Adding one twice is fine.
        """
        if not AGENT_NAME.match(name or "") or name.lower() in RESERVED_NAMES:
            raise HubError(f"Invalid sender name '{name}': use letters, digits, - or _ (start with a letter), "
                           f"and not {' or '.join(repr(n) for n in sorted(RESERVED_NAMES))}.")
        with self._lock:
            if name in self.agents or name in self._starting:
                raise HubError(f"'{name}' is an agent in this session.")
            self.senders.add(name)
            return sorted(self.senders)

    def send(self, sender: str, to: str, text: str, wait: bool = True, timeout: Optional[float] = None,
             images: Optional[List[str]] = None, force: bool = False) -> Dict[str, Any]:
        """
        Sends `text` from `sender` (an agent name, 'user', or a sender added with add_sender) to agent `to`.
        With wait=True, blocks until `to` has finished the turn this message
        starts and returns its reply; otherwise returns once it is queued.
        Sending to 'user' just records the message (there is no inbox to deliver to).
        `images` are image file paths; they're copied into the run and attached
        (agentlandline.images).
        With force=True (the user only), the message goes ahead of the ones waiting for
        `to`, and the turn `to` is running now is interrupted, so this one starts right away.
        The result then has `interrupted`: the id of the message whose turn was cut short.
        """
        if force and sender != USER:
            raise HubError("Only the user can interrupt an agent.")
        if sender != USER and sender not in self.agents and sender not in self.senders:
            raise HubError(f"Unknown sender '{sender}'.")
        if to != USER and to not in self.agents:
            raise HubError(f"Unknown agent '{to}'. Agents: {list(self.agents)}")
        if to == sender:
            raise HubError("An agent can't message itself.")
        try:
            attached = image_files.check(images or [])
        except image_files.ImageError as e:
            raise HubError(f"Can't attach image: {e}")
        if (not text or not text.strip()) and not attached:
            raise HubError("Message text is empty.")
        text = text or ""

        if wait and to != USER and sender in self.agents:
            self._check_wait_cycle(sender, to)

        record = self._record(sender, to, text, images=image_files.store(self.run_dir, attached) if attached else None)
        if to == USER:
            return {"status": "DELIVERED", "message": record}

        delivery = _Delivery(record)
        if wait and sender in self.agents:
            with self._lock:
                self._waiting_on[sender] = to
        target = self.agents[to]
        # A forced send's result says which turn it interrupted (None: the agent was idle, or finished first).
        extra = {"interrupted": self._force(target, delivery)} if force else {}
        if not force:
            target.inbox.put(delivery)
        if target.stopping:  # stopped between the lookup and the put
            delivery.error = f"Agent '{to}' was stopped."
            delivery.done.set()
        if not wait:
            return {"status": "QUEUED", "message": record, **extra}

        try:
            finished = delivery.done.wait(timeout)
        finally:
            if sender in self.agents:
                with self._lock:
                    self._waiting_on.pop(sender, None)
        if not finished:
            return {"status": "TIMEOUT", "message": record, "error": f"No reply from '{to}' within {timeout}s; the message is still queued/in progress.", **extra}
        if delivery.error:
            return {"status": "ERROR", "message": record, "error": delivery.error, **extra}
        return {"status": "REPLIED", "message": record, "reply": delivery.reply, **extra}

    FORCE_GRACE = 2.0  # how long a forced message waits for the running turn's CLI call to begin

    def _force(self, slot: _AgentSlot, delivery: _Delivery) -> Optional[str]:
        """Queues `delivery` first and interrupts the running turn. Returns the interrupted message's id."""
        with slot.lock:
            with slot.inbox.mutex:  # queue.Queue can't put at the front itself
                slot.inbox.queue.appendleft(delivery)
                slot.inbox.unfinished_tasks += 1
                slot.inbox.not_empty.notify()
            running = slot.current
        interrupt = getattr(slot.pipeline, "interrupt", None)
        if running is None or interrupt is None:
            return None
        deadline = time.monotonic() + self.FORCE_GRACE
        while True:
            with slot.lock:
                if slot.current is not running or running.done.is_set():
                    return None  # the turn ended on its own
                slot.interrupted = running  # before interrupt(): its reply may come back at once
                if interrupt():
                    slot.notice = (slot.notice or "") + (
                        f"[The user interrupted your work on message {running.record['id']} "
                        f"(from {running.record['from']}) to send you this.]\n")
                    break
                slot.interrupted = None
            if time.monotonic() > deadline:  # the worker took the message but its turn never started
                return None
            time.sleep(0.01)
        self._emit({"kind": "interrupted", "agent": slot.spec.name, "message_id": running.record["id"], "by": USER})
        logger.info(f"User interrupted {slot.spec.name}'s turn on {running.record['id']}")
        return running.record["id"]

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

    def _record(self, sender: str, to: str, text: str, caused_by: Optional[str] = None,
                images: Optional[List[str]] = None) -> Dict[str, Any]:
        session = transcript = None
        if sender in self.agents:  # not the user, nor a sender that isn't an agent
            slot = self.agents[sender]
            session = slot.pipeline.conversation_id
            transcript = slot.pipeline.transcript_path()
            if caused_by is None:
                caused_by = slot.current_trigger
        record = self.log.append(sender, to, text, caused_by=caused_by, from_session=session, from_transcript=transcript,
                                 images=images)
        self._emit({"kind": "message", **record})
        return record

    def _worker(self, slot: _AgentSlot):
        while True:
            delivery = slot.inbox.get()
            if delivery is None:
                return
            record = delivery.record
            with slot.lock:
                slot.current = delivery
            slot.busy, slot.current_trigger = True, record["id"]
            slot.last_uuid, slot.turn_tools = None, 0
            outcome = "finished"
            self._emit({"kind": "turn_start", "agent": slot.spec.name, "message_id": record["id"]})
            try:
                attached = record.get("images") or []
                if attached and slot.spec.backend in ("claude", "codex"):
                    result = slot.pipeline.send(self._frame(slot, record), timeout=None, images=attached)
                else:
                    result = slot.pipeline.send(self._frame(slot, record), timeout=None)
                if slot.stopping:
                    delivery.error = f"Agent '{slot.spec.name}' was stopped before replying."
                else:
                    reply_text = result.response or f"[{result.status}] {result.error or 'no response'}"
                    if slot.interrupted is delivery:
                        reply_text = "[interrupted by the user]" + (f"\n\n{result.response}" if result.response else "")
                        outcome = "interrupted"
                    elif result.status != "SUCCESS":
                        outcome = f"failed: {result.error or result.status}"
                    delivery.reply = self._record(slot.spec.name, record["from"], reply_text, caused_by=record["id"])
                    self._checkpoint(slot, record, delivery.reply, result)
            except Exception as e:
                logger.error(f"Agent {slot.spec.name} failed handling {record['id']}: {e}")
                delivery.error = str(e)
                outcome = f"failed: {e}"
            finally:
                with slot.lock:
                    slot.current = None
                    if slot.interrupted is delivery:
                        slot.interrupted = None
                slot.last_turn = {"message_id": record["id"], "from": record["from"], "outcome": outcome}
                slot.idle_since = time.monotonic()
                slot.busy, slot.current_trigger = False, None
                self._emit({"kind": "idle", "agent": slot.spec.name})
                delivery.done.set()
            if slot.restart_pending and not slot.stopping:
                try:  # its tools changed during the turn: restart now, on this worker's way out
                    self._restart(slot.spec.name)
                    self._write_run_file()
                    self._emit({"kind": "agent_updated", "agent": slot.spec.name,
                                "info": self._agent_info(self.agents[slot.spec.name]), "restart": "done"})
                except Exception as e:
                    logger.error(f"Couldn't restart {slot.spec.name} for its new permissions: {e}")
                    # Its old CLI keeps running with its old tools, so it keeps its old settings too.
                    if slot.restart_from is not None:
                        slot.spec.manages, slot.spec.spawns, slot.spec.wakeup = slot.restart_from
                        slot.restart_from = None
                    self._write_run_file()
                    self._emit({"kind": "agent_updated", "agent": slot.spec.name, "info": self._agent_info(slot),
                                "restart": "failed", "error": str(e)})

    @staticmethod
    def _describe(slot: _AgentSlot) -> str:
        return f"- {slot.spec.name}" + (f": {slot.spec.role}" if slot.spec.role else "")

    def _frame(self, slot: _AgentSlot, record: Dict[str, Any]) -> str:
        notice, slot.notice = slot.notice, None
        attached = record.get("images") or []
        inline = slot.spec.backend in ("claude", "codex") and all(
            os.path.getsize(p) <= image_files.MAX_INLINE_BYTES for p in attached if os.path.exists(p))
        return (notice or "") + self._frame_message(slot, record) + image_files.note(attached, inline)

    def _frame_message(self, slot: _AgentSlot, record: Dict[str, Any]) -> str:
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
        earlier = ("This conversation continues earlier work (above): keep using what you know from it.\n"
                   if slot.has_history else "")
        intro = (
            f"You are agent '{slot.spec.name}' in a multi-agent session."
            + (f" Your role: {slot.spec.role}" if slot.spec.role else "")
            + f"\nOther agents:\n{others}\n"
            "Messages to you arrive with a [landline message <id> from <sender>] header. Just do what the "
            "message asks and answer in your response: your final response is delivered back to the sender "
            "automatically, so don't use a tool to reply. To ask another agent something yourself, call the "
            "agent-landline `message_agent` tool with `to` and `text`; it returns their reply.\n"
            + earlier + "\n"
        )
        return intro + header + record["text"]

    # ---------------------------------------------------- context management

    def _checkpoint(self, slot: _AgentSlot, record: Dict[str, Any], reply: Dict[str, Any], result):
        """Records where the agent's conversation stood when it finished answering `record`."""
        if slot.spec.backend not in REWIND_BACKENDS or not slot.last_uuid or getattr(result, "status", None) != "SUCCESS":
            return
        self.checkpoints.append({
            "agent": slot.spec.name, "message_id": record["id"], "from": record["from"],
            "reply_id": reply["id"], "session_id": slot.session_id, "cut": slot.last_uuid,
            "tools": slot.turn_tools, "ts": _now(),
        })

    def _context_rules(self, caller: str, agent: str, action: str) -> Dict[str, Any]:
        """The settings for `caller` managing `agent`'s context; raises HubError if `action` isn't allowed."""
        target = self.agents.get(agent)
        if target is None:
            raise HubError(f"Unknown agent '{agent}'. Agents: {list(self.agents)}")
        if caller == agent:
            raise HubError("An agent can't manage its own context.")
        if caller == USER:  # the person running the session isn't limited by 'manages'
            rules = rules_for({agent: {"backends": list(REWIND_BACKENDS)}}, agent)
        else:
            slot = self.agents.get(caller)
            if slot is None:
                raise HubError(f"Unknown agent '{caller}'.")
            rules = rules_for(slot.spec.manages, agent)
            if rules is None:
                raise HubError(f"'{caller}' isn't allowed to manage '{agent}' (no 'manages' entry for it in run.json).")
        if action not in rules["actions"]:
            raise HubError(f"'{action}' isn't allowed for {agent} (actions: {', '.join(rules['actions']) or 'none'}).")
        if action == "rewind":
            if target.spec.backend not in rules["backends"]:
                raise HubError(f"Rewinding {target.spec.backend} agents isn't allowed for {agent} "
                               f"(backends: {', '.join(rules['backends'])}).")
            if target.spec.backend not in REWIND_BACKENDS:
                raise HubError(f"Rewinding {target.spec.backend} agents isn't supported yet (only {', '.join(REWIND_BACKENDS)}).")
        return rules

    def context_turns(self, caller: str, agent: str) -> Dict[str, Any]:
        """The turns `agent` can be rewound to, oldest first, with the start of each message and reply."""
        self._context_rules(caller, agent, "list")
        messages = {m["id"]: m for m in self.log.read_all()}

        def preview(message_id):
            text = (messages.get(message_id) or {}).get("text") or ""
            line = " ".join(text.split())
            return line if len(line) <= 160 else line[:160] + "…"

        turns = [{"message_id": t["message_id"], "from": t.get("from"), "message": preview(t["message_id"]),
                  "reply_id": t.get("reply_id"), "reply": preview(t.get("reply_id")), "tool_calls": t.get("tools", 0)}
                 for t in self.checkpoints.turns(agent)]
        slot = self.agents[agent]
        return {"agent": agent, "session_id": slot.session_id, "busy": slot.busy, "turns": turns}

    def rewind(self, caller: str, agent: str, after: str, note: Optional[str] = None) -> Dict[str, Any]:
        """
        Rewinds `agent` to just after it answered message `after`: its conversation is
        forked at that point and the agent restarts on the fork, under the same name.
        The old session is kept (previous_sessions). Files on disk are not changed.
        `note`, if given, is sent to the agent from `caller` afterwards.
        """
        rules = self._context_rules(caller, agent, "rewind")
        note = (note or "").strip() or None
        if rules["note"] == "required" and not note:
            raise HubError(f"Rewinding {agent} needs a note saying what it learned in the turns it will lose.")
        if rules["note"] == "off" and note:
            raise HubError(f"Notes aren't allowed when rewinding {agent}.")
        old = self.agents[agent]
        if old.busy or old.inbox.qsize():
            raise HubError(f"'{agent}' is busy; rewind it once it is idle.")
        turns = self.checkpoints.turns(agent)
        index = next((i for i, t in enumerate(turns) if t["message_id"] == after), None)
        if index is None:
            known = ", ".join(t["message_id"] for t in turns) or "none yet"
            raise HubError(f"'{agent}' has no turn that answered {after} in its current conversation (turns: {known}).")
        dropped = len(turns) - index - 1
        if dropped == 0:
            raise HubError(f"'{agent}' is already right after {after}; there is nothing to rewind.")
        cut, old_session = turns[index]["cut"], old.session_id
        new_session = str(uuid.uuid4())

        spec = replace(old.spec, session_id=old_session, rewind={"at": cut, "session_id": new_session},
                       previous_sessions=old.spec.previous_sessions + ([old_session] if old_session else []))
        notice = (f"[landline] Your conversation was rewound by {caller} to just after your reply to {after}: "
                  f"the {dropped} turn(s) after it are no longer in your memory. Files on disk were not changed, "
                  f"so they may still contain work from those turns.\n\n")
        slot = self._replace_slot(old, spec, "rewound", notice=notice)
        self.checkpoints.append({"agent": agent, "rewound_to": after, "by": caller, "session_id": new_session,
                                 "previous_session": old_session, "dropped": dropped, "ts": _now()})
        self._write_run_file()
        self._emit({"kind": "session", "agent": agent, "session_id": new_session, "model": self.model_in_use(slot)})
        self._emit({"kind": "agent_rewound", "agent": agent, "to": after, "by": caller, "dropped": dropped,
                    "session_id": new_session, "previous_session": old_session,
                    "transcript": slot.pipeline.transcript_path()})
        logger.info(f"{caller} rewound {agent} to after {after} ({dropped} turns dropped)")
        result = {"agent": agent, "rewound_to": after, "turns_dropped": dropped, "session_id": new_session,
                  "previous_session": old_session}
        if note:
            sent = self.send(caller, agent, note, wait=False)
            result["note_id"] = sent["message"]["id"]
        return result

    def _replace_slot(self, old: _AgentSlot, spec: AgentSpec, doing: str, notice: Optional[str] = None,
                      keep_queue: bool = False) -> _AgentSlot:
        """
        Starts a new CLI for agent `old` from `spec` and swaps it in under the same name: it takes
        over the old one's state (model, introductions); the old CLI is closed. The agent must be
        idle. With keep_queue, messages waiting for it move to the new one; otherwise a waiting
        message aborts the swap. `doing` names the action for errors ('rewound', 'restarted').
        """
        agent = old.spec.name
        slot = self._new_slot(spec)
        try:
            self._start_pipeline(slot)
        except Exception as e:
            try:
                slot.pipeline.close()
            except Exception:
                pass
            raise HubError(f"Couldn't start '{agent}' {doing}: {e}")
        slot.reported_model, slot.introduced = old.reported_model, old.introduced
        if doing == "restarted":  # same conversation, so the same context
            slot.context_prompt, slot.context_output = old.context_prompt, old.context_output
            slot.context_reported = old.context_reported
        slot.known_agents = set(old.known_agents)
        slot.notice = notice
        with self._lock:
            swapped = self.agents.get(agent) is old and not old.busy and (keep_queue or not old.inbox.qsize())
            if swapped:
                self.agents[agent] = slot
                old.stopping = True
        if not swapped:
            slot.pipeline.close()
            raise HubError(f"'{agent}' got a message while it was being {doing}; try again once it is idle.")
        while keep_queue:  # messages that were waiting for the old CLI
            try:
                delivery = old.inbox.get_nowait()
            except queue.Empty:
                break
            if delivery is not None:
                slot.inbox.put(delivery)
        old.inbox.put(None)
        try:
            old.pipeline.close()
        except Exception as e:
            logger.warning(f"Error closing {agent}'s old session: {e}")
        self._start_worker(slot)
        return slot

    # --------------------------------------------------------- permissions

    KEEP = object()  # set_permissions: leave this setting as it is

    def set_permissions(self, agent: str, manages: Any = None, spawns: Any = None, wakeup: Any = KEEP) -> Dict[str, Any]:
        """
        Replaces `agent`'s `manages` and `spawns` settings (and its `wakeup` rules, when given) while
        it runs; they take effect at once and are saved in run.json. Its CLI only learns about added
        or removed tools when it starts, so if the set of tools changes, the agent is restarted on its
        own conversation: now if it is idle, else right after its current turn. Returns its list entry
        and when it restarts.
        """
        slot = self.agents.get(agent)
        if slot is None:
            raise HubError(f"Unknown agent '{agent}'. Agents: {list(self.agents)}")
        try:
            new_manages, new_spawns = normalize_manages(manages), normalize_spawns(spawns)
            new_wakeup = slot.spec.wakeup if wakeup is self.KEEP else normalize_wakeup(wakeup, agent)
        except ContextError as e:
            raise HubError(f"Agent '{agent}': {e}")
        if new_manages and agent in new_manages:
            raise HubError("An agent can't manage its own context.")
        tools_change = (bool(slot.spec.manages), bool(slot.spec.spawns)) != (bool(new_manages), bool(new_spawns))
        old = slot.spec.manages, slot.spec.spawns, slot.spec.wakeup
        slot.spec.manages, slot.spec.spawns, slot.spec.wakeup = new_manages, new_spawns, new_wakeup
        restart = None
        if tools_change:
            restart = "after its turn" if slot.busy else "now"
            if slot.busy:
                if not slot.restart_pending:
                    slot.restart_from = old
                slot.restart_pending = True
            else:
                try:
                    slot = self._restart(agent)
                except HubError:
                    # Its old CLI keeps running with its old tools, so it keeps its old settings too.
                    slot.spec.manages, slot.spec.spawns, slot.spec.wakeup = old
                    raise
        if new_wakeup != old[2]:  # changed rules start over: they fire on the next idle stretch
            self._wakeup_fired.pop(agent, None)
        self._write_run_file()
        info = self._agent_info(slot)
        self._emit({"kind": "agent_updated", "agent": agent, "info": info, "restart": restart})
        logger.info(f"Permissions of {agent} set (manages={new_manages}, spawns={new_spawns}, wakeup={new_wakeup}, "
                    f"restart={restart})")
        return {"agent": info, "restart": restart}

    def _restart(self, agent: str) -> _AgentSlot:
        """Restarts `agent`'s CLI on its own conversation (so it gets the tools its settings give it)."""
        old = self.agents[agent]
        old.restart_pending = False
        spec = replace(old.spec, session_id=old.session_id or old.spec.session_id)
        slot = self._replace_slot(old, spec, "restarted", keep_queue=True)
        if slot.session_id:
            self._emit({"kind": "session", "agent": agent, "session_id": slot.session_id, "model": self.model_in_use(slot)})
        return slot

    # ------------------------------------------------------------- waking up

    WAKEUP_TICK = 1.0  # seconds between checks of the wake-up rules

    def _watch_idle(self):
        while not self._closing.wait(self.WAKEUP_TICK):
            try:
                self.check_wakeups()
            except Exception as e:
                logger.error(f"Wake-up check failed: {e}")

    @staticmethod
    def _working(slot: _AgentSlot) -> bool:
        return slot.busy or slot.inbox.qsize() > 0

    def check_wakeups(self, now: Optional[float] = None) -> List[str]:
        """
        Sends each agent's wake-up message whose rule is met: every agent it watches is idle (nothing
        running or queued), the last of them went idle at least `idle_for` seconds ago, and at least one
        of them worked since the rule last fired. So a rule fires once per idle stretch, and never for a
        team that hasn't done anything yet. Returns the agents woken up.
        """
        now = time.monotonic() if now is None else now
        slots = {s.spec.name: s for s in self._slots()}
        woken = []
        for name, slot in slots.items():
            for index, rule in enumerate(slot.spec.wakeup or []):
                names = [n for n in slots if n != name] if rule["when"] == WAKEUP_ALL else rule["when"]
                watched = [slots[n] for n in names if n in slots and n != name]
                if not watched or any(self._working(s) for s in watched):
                    continue
                ended = [s.idle_since for s in watched if s.idle_since is not None]
                # An idle stretch is known by when its last agent went idle; a rule fires once per stretch.
                stretch = max(ended) if ended else None
                if stretch is None or now - stretch < rule["idle_for"] \
                        or stretch == self._wakeup_fired.get(name, {}).get(index):
                    continue
                self._wakeup_fired.setdefault(name, {})[index] = stretch
                try:
                    self.send(USER, name, self._wakeup_text(rule, watched, now), wait=False)
                except HubError as e:  # e.g. it is being stopped
                    logger.warning(f"Couldn't wake up {name}: {e}")
                    continue
                logger.info(f"Woke up {name}: {', '.join(s.spec.name for s in watched)} idle")
                woken.append(name)
        return woken

    @staticmethod
    def _wakeup_text(rule: Dict[str, Any], watched: List[_AgentSlot], now: float) -> str:
        if rule["when"] == WAKEUP_ALL:
            who = "All the other agents have"
        else:
            who = ", ".join(s.spec.name for s in watched) + (" have" if len(watched) > 1 else " has")
        lines = [f"[wake-up: {who} been idle for {_duration(rule['idle_for'])} or more]", rule["message"].strip(), "",
                 "Where each one stopped:"]
        for s in watched:
            turn = s.last_turn
            if turn is None:
                lines.append(f"- {s.spec.name}: hasn't handled a message yet")
            else:
                idle = f"idle {_duration(now - s.idle_since)}" if s.idle_since is not None else "idle"
                lines.append(f"- {s.spec.name}: {idle}; its last turn, on {turn['message_id']} from {turn['from']}, "
                             f"{turn['outcome']}")
        return "\n".join(lines)

    # ------------------------------------------------- agents starting agents

    def _spawn_rules(self, caller: str) -> Dict[str, Any]:
        slot = self.agents.get(caller)
        if slot is None:
            raise HubError(f"Unknown agent '{caller}'.")
        if not slot.spec.spawns:
            raise HubError(f"'{caller}' isn't allowed to start agents (no 'spawns' setting in run.json).")
        return slot.spec.spawns

    def start_agent(self, caller: str, name: str, backend: Optional[str] = None, model: Optional[str] = None,
                    effort: Optional[str] = None, role: Optional[str] = None, cwd: Optional[str] = None,
                    session_id: Optional[str] = None, fork: bool = False, from_agent: Optional[str] = None,
                    after: Optional[str] = None) -> Dict[str, Any]:
        """
        `caller` starts a new agent in this session, if its `spawns` rules allow it:
        - a new conversation (no session_id, no from_agent);
        - an existing session by id: continued (session_id), or copied (fork=True);
        - a copy of agent `from_agent`'s conversation: as it is now, or cut just after its
          reply to message `after` (claude agents), like a rewind that keeps the original.
        The new agent records who started it (spawned_by). Returns its list_agents() entry.
        """
        rules = self._spawn_rules(caller)
        mine = [s for s in self._slots() if s.spec.spawned_by == caller]
        if len(mine) >= rules["max"]:
            raise HubError(f"'{caller}' already runs {len(mine)} agent(s) it started ({', '.join(s.spec.name for s in mine)}); "
                           f"its limit is {rules['max']}. Stop one first.")
        source = None
        if from_agent:
            if session_id:
                raise HubError("Give either from_agent or session_id, not both.")
            source = self.agents.get(from_agent)
            if source is None:
                raise HubError(f"Unknown agent '{from_agent}'. Agents: {list(self.agents)}")
            if "*" not in rules["fork_from"] and from_agent not in rules["fork_from"]:
                raise HubError(f"'{caller}' isn't allowed to copy {from_agent} (fork_from: {', '.join(rules['fork_from']) or 'none'}).")
            if not source.session_id:
                raise HubError(f"'{from_agent}' has no conversation to copy yet.")
            session_id, fork = source.session_id, True
            backend = backend or source.spec.backend
            if normalize_backend(backend) != source.spec.backend:
                raise HubError(f"A copy of {from_agent} runs on its backend ({source.spec.backend}).")
            model = model or source.spec.model
            effort = effort or source.spec.effort
            role = role or source.spec.role
            cwd = cwd or source.spec.cwd or getattr(source.pipeline, "cwd", None)
        elif session_id:
            if not rules["sessions"]:
                raise HubError(f"'{caller}' isn't allowed to open existing sessions (spawns.sessions is false).")
            user = next((s.spec.name for s in self._slots() if s.session_id == session_id), None)
            if user and not fork:
                raise HubError(f"Session {session_id} is in use by '{user}'. Use fork=true to start on a copy of it.")
        if after and not from_agent:
            raise HubError("'after' needs from_agent: it names a message that agent answered.")
        try:
            backend = normalize_backend(backend or "claude")
        except ValueError as e:
            raise HubError(str(e))
        if backend not in rules["backends"]:
            raise HubError(f"'{caller}' may start {', '.join(rules['backends'])} agents, not {backend}.")
        caller_slot = self.agents[caller]
        spec = AgentSpec.from_dict(name, {"backend": backend, "model": model, "effort": effort, "role": role,
                                          "cwd": cwd or caller_slot.spec.cwd or getattr(caller_slot.pipeline, "cwd", None),
                                          "session_id": session_id})
        spec.spawned_by = caller
        if after:
            if source.spec.backend not in REWIND_BACKENDS:
                raise HubError(f"Copying {source.spec.backend} agents at a message isn't supported yet; "
                               f"leave out 'after' to copy {from_agent} as it is now.")
            turns = self.checkpoints.turns(from_agent)
            turn = next((t for t in turns if t["message_id"] == after), None)
            if turn is None:
                known = ", ".join(t["message_id"] for t in turns) or "none yet"
                raise HubError(f"'{from_agent}' has no turn that answered {after} in its current conversation (turns: {known}).")
            spec.rewind = {"at": turn["cut"], "session_id": str(uuid.uuid4())}
            spec.forked_from = session_id
        else:
            spec.fork = bool(fork and session_id)
        info = self.spawn(spec)
        logger.info(f"{caller} started {name}" + (f" from {from_agent}" + (f" after {after}" if after else "") if from_agent else ""))
        return info

    def stop_started(self, caller: str, name: str) -> Dict[str, Any]:
        """`caller` stops an agent it started (if its spawns rules allow stopping)."""
        rules = self._spawn_rules(caller)
        target = self.agents.get(name)
        if target is None:
            raise HubError(f"Unknown agent '{name}'. Agents: {list(self.agents)}")
        if target.spec.spawned_by != caller:
            raise HubError(f"'{caller}' can only stop agents it started; {name} wasn't.")
        if not rules["stop"]:
            raise HubError(f"'{caller}' isn't allowed to stop agents (spawns.stop is false).")
        return self.stop_agent(name)

    def find_sessions(self, caller: str, query: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Saved sessions on this machine (this hub's home), newest first, one entry per agent:
        its run, name, backend, model, role, folder and session id. `query` keeps the ones whose
        run, agent name, role or folder contain it (case-insensitive).
        """
        if caller != USER and not self._spawn_rules(caller)["sessions"]:
            raise HubError(f"'{caller}' isn't allowed to open existing sessions (spawns.sessions is false).")
        running = {s.session_id: s.spec.name for s in self._slots() if s.session_id}
        found = []
        runs_dir = os.path.join(self.home, "runs")
        for run in os.listdir(runs_dir) if os.path.isdir(runs_dir) else []:
            try:
                with open(os.path.join(runs_dir, run, "run.json"), "r", encoding="utf-8") as f:
                    saved = json.load(f)
            except (OSError, ValueError):
                continue
            log = os.path.join(runs_dir, run, MessageLog.FILENAME)
            last = os.path.getmtime(log) if os.path.exists(log) else os.path.getmtime(os.path.join(runs_dir, run, "run.json"))
            for agent, a in (saved.get("agents") or {}).items():
                if not isinstance(a, dict) or not a.get("session_id"):
                    continue
                entry = {"run": run, "agent": agent, "backend": a.get("backend"),
                         "model": a.get("model_in_use") or a.get("model"), "role": a.get("role"),
                         "cwd": a.get("cwd") or saved.get("cwd"), "session_id": a["session_id"],
                         "last_active": datetime.fromtimestamp(last, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "messages": count_lines(log) or 0,
                         "in_use_by": running.get(a["session_id"])}
                text = " ".join(str(entry[k] or "") for k in ("run", "agent", "role", "cwd")).lower()
                if query and query.lower() not in text:
                    continue
                found.append((last, entry))
        found.sort(key=lambda pair: pair[0], reverse=True)
        return [entry for _, entry in found[:max(1, min(limit, 100))]]

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
            "spawned_by": slot.spec.spawned_by,
            "forked_from": slot.spec.forked_from,
            "manages": slot.spec.manages,
            "spawns": slot.spec.spawns,
            "wakeup": slot.spec.wakeup,
            "busy": slot.busy,
            "queued": slot.inbox.qsize(),
            "context": self.context_of(slot),
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
                name: {**{k: v for k, v in asdict(slot.spec).items()
                          if k not in ("name", "rewind", "last_context")
                          and not (k in ("manages", "spawns", "wakeup", "spawned_by", "previous_sessions", "fork",
                                         "forked_from", "context_window")
                                   and not v)},
                       # The folder the agent actually works in, even when the config left it
                       # to the hub's folder: resuming needs it (claude finds sessions by folder).
                       "cwd": slot.spec.cwd or getattr(slot.pipeline, "cwd", None),
                       "session_id": slot.session_id,
                       "model_in_use": self.model_in_use(slot),
                       **({"stopped": True} if slot.stopping else {}),
                       **({"context": {"used": slot.context_prompt + slot.context_output}}
                          if slot.context_prompt is not None else {}),
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
#   {"op": "send", "from": "user", "to": "coder", "text": "...", "wait": true, "images": ["/path/plot.png"]}
#   {"op": "list"}
#   {"op": "sender", "name": "eda-mcp"}   (user only) -> {"senders": [...]}: lets a non-agent send
#   {"op": "context", "from": "lead", "agent": "fixer", "action": "list" | "rewind", "after": "m7", "note": "..."}
#   {"op": "team", "from": "lead", "action": "start" | "stop" | "sessions", "name": "fixer-2", ...}
#   {"op": "permissions", "agent": "lead", "manages": {...} | null, "spawns": {...} | true | null,
#    "wakeup": [{"when": "all", "idle_for": 60, "message": "..."}] | null}   (user only; no "wakeup": unchanged)
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
                                          wait=request.get("wait", True), timeout=request.get("timeout"),
                                          images=request.get("images"), force=bool(request.get("force")))
                        self._reply({"ok": True, **result})
                    elif op == "spawn":
                        fields = {k: request.get(k) for k in ("backend", "model", "effort", "cwd", "role", "session_id",
                                                              "manages", "fork", "spawns", "wakeup")}
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
                    elif op == "sender":
                        if (request.get("from") or USER) != USER:
                            raise HubError("Only the user can add a sender.")
                        self._reply({"ok": True, "senders": hub.add_sender(request.get("name") or "")})
                    elif op == "permissions":
                        if (request.get("from") or USER) != USER:
                            self._reply({"ok": False, "error": "Only the user can change permissions."})
                        else:
                            # Without a "wakeup" key, the agent's wake-up rules stay as they are.
                            wakeup = request["wakeup"] if "wakeup" in request else Hub.KEEP
                            self._reply({"ok": True, **hub.set_permissions(request.get("agent") or "", request.get("manages"),
                                                                            request.get("spawns"), wakeup)})
                    elif op == "team":
                        caller, action = request.get("from") or USER, request.get("action")
                        if action == "start":
                            fields = {k: request.get(k) for k in ("backend", "model", "effort", "role", "cwd", "session_id",
                                                                  "from_agent", "after")}
                            self._reply({"ok": True, "agent": hub.start_agent(caller, request.get("name") or "",
                                                                              fork=bool(request.get("fork")), **fields)})
                        elif action == "stop":
                            self._reply({"ok": True, "agent": hub.stop_started(caller, request.get("name") or "")})
                        elif action == "sessions":
                            self._reply({"ok": True, "sessions": hub.find_sessions(caller, request.get("query"),
                                                                                    int(request.get("limit") or 20))})
                        else:
                            self._reply({"ok": False, "error": f"Unknown action '{action}': use start, stop or sessions."})
                    elif op == "context":
                        caller, agent = request.get("from") or USER, request.get("agent") or ""
                        if request.get("action", "list") == "list":
                            self._reply({"ok": True, **hub.context_turns(caller, agent)})
                        elif request.get("action") == "rewind":
                            self._reply({"ok": True, **hub.rewind(caller, agent, request.get("after") or "", request.get("note"))})
                        else:
                            self._reply({"ok": False, "error": f"Unknown action '{request.get('action')}': use list or rewind."})
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
            raise HubError(f"No hub listening on {self.socket_path} ({e}). Each folder has its own hub: run this "
                           f"in the folder where the hub was started, pass --socket, or start one with `landline up <config.json>`.")
        return s

    def request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._connect() as s:
            s.sendall((json.dumps(payload) + "\n").encode())
            with s.makefile("r", encoding="utf-8") as f:
                line = f.readline()
        if not line:
            raise HubError("Hub closed the connection without replying.")
        return json.loads(line)

    def send(self, sender: str, to: str, text: str, wait: bool = True, timeout: Optional[float] = None,
             images: Optional[List[str]] = None, force: bool = False) -> Dict[str, Any]:
        """
        `images`: image file paths to attach (absolute, or relative to this process's folder).
        `force` (the user only): interrupt `to`'s running turn and handle this message first.
        """
        payload = {"op": "send", "from": sender, "to": to, "text": text, "wait": wait, "timeout": timeout}
        if force:
            payload["force"] = True
        if images:
            payload["images"] = [os.path.abspath(os.path.expanduser(p)) for p in images]
        return self.request(payload)

    def list_agents(self) -> Dict[str, Any]:
        return self.request({"op": "list"})

    def add_sender(self, name: str) -> Dict[str, Any]:
        """Lets `name`, which isn't an agent, send messages (Hub.add_sender)."""
        return self.request({"op": "sender", "name": name})

    def stop_agent(self, name: str) -> Dict[str, Any]:
        return self.request({"op": "stop", "name": name})

    def rename(self, run_id: str) -> Dict[str, Any]:
        return self.request({"op": "rename", "run": run_id})

    def adopt(self, run_id: str) -> Dict[str, Any]:
        """Makes the hub continue a saved run (only while the current run has no messages)."""
        return self.request({"op": "adopt", "run": run_id})

    def spawn(self, name: str, backend: str, model: Optional[str] = None, effort: Optional[str] = None,
              cwd: Optional[str] = None, role: Optional[str] = None, session_id: Optional[str] = None,
              manages: Any = None, fork: bool = False, spawns: Any = None, wakeup: Any = None) -> Dict[str, Any]:
        """
        Starts a new agent in the running hub, resuming `session_id` if given (or, with
        fork=True, starting on a copy of it); returns once it is up or failed.
        """
        return self.request({"op": "spawn", "name": name, "backend": backend, "model": model, "effort": effort,
                             "cwd": cwd, "role": role, "session_id": session_id, "manages": manages, "fork": fork,
                             "spawns": spawns, "wakeup": wakeup})

    def set_permissions(self, agent: str, manages: Any = None, spawns: Any = None, wakeup: Any = Hub.KEEP) -> Dict[str, Any]:
        """Replaces an agent's `manages` and `spawns` settings, and its `wakeup` rules if given (see Hub.set_permissions)."""
        payload = {"op": "permissions", "from": USER, "agent": agent, "manages": manages, "spawns": spawns}
        if wakeup is not Hub.KEEP:
            payload["wakeup"] = wakeup
        return self.request(payload)

    def team(self, sender: str, action: str, **fields) -> Dict[str, Any]:
        """Agents starting agents: action 'start' (name, backend, ..., from_agent, after), 'stop' (name) or 'sessions' (query)."""
        return self.request({"op": "team", "from": sender, "action": action, **fields})

    def context(self, sender: str, agent: str, action: str = "list", after: Optional[str] = None,
                note: Optional[str] = None) -> Dict[str, Any]:
        """Lists the turns `agent` can be rewound to, or rewinds it to just after message `after`."""
        return self.request({"op": "context", "from": sender, "agent": agent, "action": action, "after": after, "note": note})

    def messages(self) -> Dict[str, Any]:
        return self.request({"op": "messages"})

    def subscribe(self) -> Iterator[Dict[str, Any]]:
        """Yields the hub snapshot first, then live events until the hub goes away."""
        with self._connect() as s:
            s.sendall(b'{"op": "subscribe"}\n')
            with s.makefile("r", encoding="utf-8") as f:
                for line in f:
                    yield json.loads(line)
