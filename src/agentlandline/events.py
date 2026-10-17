"""
Normalized live events.

AgyPipeline and ClaudePipeline hand every raw stream-json line to their event
listeners, but the two CLIs speak different protocols. `normalize()` maps one raw
event from either backend to zero or more small dicts with a shared shape, so a
consumer can render any agent's activity without knowing which CLI it runs:

    {"kind": "session",    "session_id": str, "model": str}   # model: claude only
    {"kind": "text_delta", "text": str}                    # streamed text chunk
    {"kind": "text",       "text": str}                    # complete text block (claude)
    {"kind": "thinking",   "text": str}                    # may be "" when the CLI redacts it
    {"kind": "tool_start", "id": str, "name": str, "input": dict}
    {"kind": "tool_end",   "id": str, "ok": bool, "output": str}
    {"kind": "turn_end",   "ok": bool, "duration_s": float, "usage": dict,
                           "context_windows": {model: int}}         # claude only, when it reports them
    {"kind": "context",    "prompt": int, "output": int}           # tokens of the model call in progress
    {"kind": "context",    "output": int}                          # claude: its output so far
    {"kind": "compacted",  "trigger": str, "pre_tokens": int, "post_tokens": int}   # claude

`context` events say how full the agent's context window is, live: claude's at the start
of each model call (`message_start`) and as it writes (`message_delta`), agy's as each of
its steps ends. The tokens in the window are prompt + output. Calls made by claude's own
subagents (Task tool) are left out: they have their own context.

These events are for live display only and are not persisted.
"""

import json
from typing import Any, Dict, List, Optional

# Live events are for display; long tool outputs are clipped.
MAX_OUTPUT_CHARS = 2000


def _clip(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, default=str)
    if len(text) > MAX_OUTPUT_CHARS:
        return text[:MAX_OUTPUT_CHARS] + f"... [{len(text) - MAX_OUTPUT_CHARS} more chars]"
    return text


def normalize(backend: str, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Maps one raw stream-json event from `backend` ('claude' or 'antigravity') to normalized events."""
    if backend == "claude":
        return _normalize_claude(raw)
    return _normalize_agy(raw)


def _normalize_claude(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    kind = raw.get("type")

    if kind == "system" and raw.get("subtype") == "init":
        event = {"kind": "session", "session_id": raw.get("session_id")}
        if raw.get("model"):
            event["model"] = raw["model"]
        return [event]

    if kind == "system" and raw.get("subtype") == "compact_boundary":
        meta = raw.get("compact_metadata") or {}
        return [{"kind": "compacted", "trigger": meta.get("trigger"), "pre_tokens": meta.get("pre_tokens"),
                 "post_tokens": meta.get("post_tokens")}]

    if kind == "stream_event":
        event = raw.get("event") or {}
        delta = event.get("delta") or {}
        if event.get("type") == "content_block_delta" and delta.get("type") == "text_delta" and delta.get("text"):
            return [{"kind": "text_delta", "text": delta["text"]}]
        if raw.get("parent_tool_use_id"):  # a subagent's call: not this agent's context
            return []
        if event.get("type") == "message_start":
            usage = (event.get("message") or {}).get("usage") or {}
            return [{"kind": "context", "prompt": _prompt_tokens(usage), "output": usage.get("output_tokens") or 0}]
        if event.get("type") == "message_delta" and (event.get("usage") or {}).get("output_tokens") is not None:
            return [{"kind": "context", "output": event["usage"]["output_tokens"]}]
        return []

    if kind == "assistant":
        out = []
        for block in (raw.get("message") or {}).get("content") or []:
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype == "thinking":
                out.append({"kind": "thinking", "text": block.get("thinking") or ""})
            elif btype == "text" and block.get("text"):
                out.append({"kind": "text", "text": block["text"]})
            elif btype == "tool_use":
                out.append({"kind": "tool_start", "id": block.get("id"), "name": block.get("name", "unknown"), "input": block.get("input") or {}})
        return out

    if kind == "user":
        content = (raw.get("message") or {}).get("content")
        out = []
        for block in content if isinstance(content, list) else []:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                out.append({"kind": "tool_end", "id": block.get("tool_use_id"), "ok": not block.get("is_error"), "output": _clip(block.get("content"))})
        return out

    if kind == "result":
        event = {
            "kind": "turn_end",
            "ok": not raw.get("is_error"),
            "duration_s": (raw.get("duration_ms") or 0) / 1000.0,
            "usage": raw.get("usage") or {},
        }
        windows = {model: info["contextWindow"] for model, info in (raw.get("modelUsage") or {}).items()
                   if isinstance(info, dict) and isinstance(info.get("contextWindow"), int)}
        if windows:
            event["context_windows"] = windows
        return [event]

    return []


def _prompt_tokens(usage: Dict[str, Any]) -> int:
    """claude: what a model call was sent, cached or not. That is what fills the context window."""
    return sum(usage.get(k) or 0 for k in ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"))


def _normalize_agy(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    kind = raw.get("event")

    if kind == "init":
        return [{"kind": "session", "session_id": raw.get("conversation_id")}]

    if kind == "step_update":
        step = raw.get("step_update") or {}
        step_type, state = step.get("step_type"), step.get("state")
        step_id = str(step.get("step_index"))

        if step_type == "agent_response":
            out = [{"kind": "text_delta", "text": step["text_delta"]}] if step.get("text_delta") else []
            if state == "DONE" and isinstance(step.get("usage"), dict):  # the last step can carry text too
                usage = step["usage"]  # this step's model call (the turn's result only has the sum)
                out.append({"kind": "context", "prompt": usage.get("input_tokens") or 0,
                            "output": usage.get("output_tokens") or 0})
            if out:
                return out

        if step_type == "tool":
            info = step.get("tool_info") or {}
            if state == "ACTIVE":
                return [{"kind": "tool_start", "id": step_id, "name": step.get("tool_name") or info.get("name", "unknown"), "input": info.get("parameters") or {}}]
            if state in ("DONE", "ERROR"):
                err = info.get("error")
                ok = state == "DONE" and not err
                return [{"kind": "tool_end", "id": step_id, "ok": ok, "output": _clip(info.get("output") if ok else (err or info.get("output") or ""))}]
        return []

    if kind == "result":
        result = raw.get("result") or {}
        return [{
            "kind": "turn_end",
            "ok": result.get("status", "SUCCESS") == "SUCCESS",
            "duration_s": result.get("duration_seconds") or 0.0,
            "usage": result.get("usage") or {},
        }]

    return []


def agy_completed_step(raw: Dict[str, Any]) -> Optional[int]:
    """Step index of an agy `agent_response` step that just finished, else None."""
    if raw.get("event") != "step_update":
        return None
    step = raw.get("step_update") or {}
    if step.get("step_type") == "agent_response" and step.get("state") == "DONE":
        try:
            return int(step.get("step_index"))
        except (TypeError, ValueError):
            return None
    return None


def agy_step_thinking(transcript_path: Optional[str], step_index: int) -> Optional[str]:
    """
    agy streams only a thinking token count, but writes each model step's thinking
    text to its on-disk transcript. Returns that text for `step_index`, if present.
    """
    if not transcript_path:
        return None
    try:
        with open(transcript_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return None
    for line in reversed(lines):
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        try:
            if int(entry.get("step_index")) == step_index:
                return entry.get("thinking") or None
        except (TypeError, ValueError):
            continue
    return None
