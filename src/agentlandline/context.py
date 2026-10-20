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
