"""
Context management: which agents may rewind which, and the per-turn checkpoints
a rewind cuts at.

An agent's `manages` setting (in its run config / run.json) lists the agents
whose context it may manage, either as names:

    "manages": ["fixer"]

or per agent, with settings ("*" covers every agent not listed by name):

    "manages": {"fixer": {"actions": ["list", "rewind"], "note": "required"},
                "*":     {"actions": ["list"]}}

Checkpoints are kept in the run folder (`checkpoints.jsonl`), one line per
finished turn plus one per rewind, so they survive resumes and renames. A rewind
drops the agent's later checkpoints: they belong to the conversation it left.
"""

import json
import os
import threading
from typing import Any, Dict, List, Optional

from .pipeline_factory import normalize_backend

ACTIONS = ("list", "rewind")
NOTE_MODES = ("optional", "required", "off")
DEFAULT_RULES: Dict[str, Any] = {
    "actions": ["list", "rewind"],
    "cut": "turn",
    "files": "keep",
    "note": "optional",
    "backends": ["claude"],
}
# Settings that are accepted in configs but not built yet: a config asking for
# them fails at load time instead of silently doing something else.
NOT_SUPPORTED = {
    ("cut", "step"): "cutting inside a turn (cut: step) isn't supported yet; use cut: turn",
    ("files", "snapshot"): "restoring files on a rewind (files: snapshot) isn't supported yet; use files: keep",
}
# Backends a rewind can actually run on today.
REWIND_BACKENDS = ("claude",)


class ContextError(ValueError):
    pass


def normalize_manages(value: Any) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Checks a `manages` setting and returns it in full form ({agent: settings}),
    or None when the agent manages no one. Raises ContextError with a readable
    message for anything malformed.
    """
    if value in (None, [], {}):
        return None
    if isinstance(value, str):
        value = [value]
    if isinstance(value, list):
        if not all(isinstance(n, str) and n for n in value):
            raise ContextError("'manages' must list agent names, e.g. [\"fixer\"]")
        value = {n: {} for n in value}
    if not isinstance(value, dict):
        raise ContextError("'manages' must be a list of agent names or an object of agent -> settings")
    out: Dict[str, Dict[str, Any]] = {}
    for target, settings in value.items():
        if settings is None:
            settings = {}
        if not isinstance(settings, dict):
            raise ContextError(f"manages.{target}: settings must be an object")
        unknown = set(settings) - set(DEFAULT_RULES)
        if unknown:
            raise ContextError(f"manages.{target}: unknown setting(s) {', '.join(sorted(unknown))}; "
                               f"known: {', '.join(DEFAULT_RULES)}")
        rules = {k: settings[k] for k in settings}
        if "actions" in rules:
            actions = rules["actions"]
            if not isinstance(actions, list) or any(a not in ACTIONS for a in actions):
                raise ContextError(f"manages.{target}.actions: use any of {', '.join(ACTIONS)}")
        if "note" in rules and rules["note"] not in NOTE_MODES:
            raise ContextError(f"manages.{target}.note: use one of {', '.join(NOTE_MODES)}")
        for key in ("cut", "files"):
            if key in rules:
                if (key, rules[key]) in NOT_SUPPORTED:
                    raise ContextError(f"manages.{target}: {NOT_SUPPORTED[(key, rules[key])]}")
                if rules[key] != DEFAULT_RULES[key]:
                    raise ContextError(f"manages.{target}.{key}: unknown value {rules[key]!r}")
        if "backends" in rules:
            backends = rules["backends"]
            if not isinstance(backends, list):
                raise ContextError(f"manages.{target}.backends: must be a list, e.g. [\"claude\"]")
            try:
                rules["backends"] = [normalize_backend(b) for b in backends]
            except ValueError as e:
                raise ContextError(f"manages.{target}.backends: {e}")
        out[target] = rules
    return out


def rules_for(manages: Optional[Dict[str, Dict[str, Any]]], target: str) -> Optional[Dict[str, Any]]:
    """The settings that apply to managing `target`, defaults filled in; None if not allowed at all."""
    if not manages:
        return None
    settings = manages.get(target, manages.get("*"))
    if settings is None:
        return None
    return {**DEFAULT_RULES, **settings}


class Checkpoints:
    """The run's `checkpoints.jsonl`: one line per finished turn, one per rewind."""

    FILENAME = "checkpoints.jsonl"

    def __init__(self, run_dir_getter):
        self._run_dir = run_dir_getter  # the run folder can move (rename, adopt)
        self._lock = threading.Lock()

    @property
    def path(self) -> str:
        return os.path.join(self._run_dir(), self.FILENAME)

    def append(self, entry: Dict[str, Any]):
        with self._lock:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def turns(self, agent: str) -> List[Dict[str, Any]]:
        """The agent's turns in its current conversation, oldest first."""
        out: List[Dict[str, Any]] = []
        try:
            with self._lock, open(self.path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except FileNotFoundError:
            return out
        for line in lines:
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            if entry.get("agent") != agent:
                continue
            if "rewound_to" in entry:
                keep = next((i for i, t in enumerate(out) if t["message_id"] == entry["rewound_to"]), None)
                if keep is not None:
                    del out[keep + 1:]
            elif entry.get("message_id"):
                out.append(entry)
        return out


# --------------------------------------------------------------------------
# Starting agents: an agent's `spawns` setting lets it start other agents in
# the session (start_agent), stop the ones it started, and look through saved
# sessions (find_sessions):
#
#     "spawns": true
#     "spawns": {"max": 2, "backends": ["claude"], "sessions": false, "fork_from": ["fixer"], "stop": true}

DEFAULT_SPAWNS: Dict[str, Any] = {
    "max": 3,                              # agents it started that may run at once
    "backends": ["claude", "antigravity"],
    "sessions": True,                      # may open existing sessions (by id) and find_sessions
    "fork_from": ["*"],                    # agents of this session it may copy ("*": any)
    "stop": True,                          # may stop the agents it started
}


def normalize_spawns(value: Any) -> Optional[Dict[str, Any]]:
    """Checks a `spawns` setting; returns its rules with defaults filled in, or None if it may not start agents."""
    if value is None or value is False:
        return None
    if value is True:
        return dict(DEFAULT_SPAWNS)
    if not isinstance(value, dict):
        raise ContextError("'spawns' must be true or an object of settings")
    unknown = set(value) - set(DEFAULT_SPAWNS)
    if unknown:
        raise ContextError(f"spawns: unknown setting(s) {', '.join(sorted(unknown))}; known: {', '.join(DEFAULT_SPAWNS)}")
    rules = {**DEFAULT_SPAWNS, **value}
    if not isinstance(rules["max"], int) or isinstance(rules["max"], bool) or rules["max"] < 1:
        raise ContextError("spawns.max: a number of agents, at least 1")
    if not isinstance(rules["backends"], list) or not rules["backends"]:
        raise ContextError("spawns.backends: a list, e.g. [\"claude\"]")
    try:
        rules["backends"] = [normalize_backend(b) for b in rules["backends"]]
    except ValueError as e:
        raise ContextError(f"spawns.backends: {e}")
    if not isinstance(rules["fork_from"], list) or not all(isinstance(n, str) for n in rules["fork_from"]):
        raise ContextError("spawns.fork_from: a list of agent names, or [\"*\"]")
    for key in ("sessions", "stop"):
        if not isinstance(rules[key], bool):
            raise ContextError(f"spawns.{key}: true or false")
    return rules
