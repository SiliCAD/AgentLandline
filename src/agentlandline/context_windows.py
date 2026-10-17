"""
How big an agent's context window is, for showing how full it is.

claude reports its model's window (`modelUsage.<model>.contextWindow`) at the end of each
turn; the hub remembers those per model in `<landline home>/context_windows.json`, so later
sessions know them from the start. agy never reports one, so its models come from the table
below. An agent's `context_window` setting (run config) overrides both.
"""

import json
import os
import re
import threading
from typing import Dict, Optional

# Lowercased model name (id or display name) pattern -> window in tokens. First match wins.
# claude models run by Claude Code aren't listed: Claude Code reports their window itself.
KNOWN_WINDOWS = [
    (r"gemini", 1_048_576),               # agy: Gemini 3.x Flash
    (r"claude.*(sonnet|opus).*4[-. ]6", 200_000),  # agy: Claude Sonnet 4.6, Opus 4.6
    (r"gpt-oss", 131_072),                # agy: GPT-OSS 120B
]

FILE_NAME = "context_windows.json"


def _key(model: str) -> str:
    return model.strip().lower()


class ContextWindows:
    """The windows learned from claude, saved in `home`, plus the table above."""

    def __init__(self, home: str):
        self.path = os.path.join(home, FILE_NAME)
        self._lock = threading.Lock()
        self._learned: Optional[Dict[str, int]] = None

    def _load(self) -> Dict[str, int]:
        if self._learned is None:
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._learned = {k: v for k, v in data.items() if isinstance(v, int) and v > 0}
            except (OSError, ValueError, AttributeError):
                self._learned = {}
        return self._learned

    def window(self, model: Optional[str]) -> Optional[int]:
        if not model:
            return None
        with self._lock:
            learned = self._load().get(_key(model))
        if learned:
            return learned
        name = _key(model)
        return next((size for pattern, size in KNOWN_WINDOWS if re.search(pattern, name)), None)

    def learn(self, model: str, size: int):
        """Remembers a window a CLI reported; saved for later sessions."""
        if not model or not isinstance(size, int) or size <= 0:
            return
        with self._lock:
            learned = self._load()
            if learned.get(_key(model)) == size:
                return
            learned[_key(model)] = size
            try:
                os.makedirs(os.path.dirname(self.path), exist_ok=True)
                tmp = self.path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(learned, f, indent=2, sort_keys=True)
                os.replace(tmp, self.path)
            except OSError:
                pass  # still known for this session


def short_tokens(n: int) -> str:
    """12345 -> '12.3k', 1048576 -> '1.05M'."""
    if n >= 999_500:  # would round to 1000k
        return f"{n / 1_000_000:.3g}M"
    if n >= 1_000:
        return f"{n / 1_000:.3g}k"
    return str(n)


def describe(context: Optional[Dict[str, Optional[int]]]) -> Optional[str]:
    """'410k / 1M (41%)' for {"used": ..., "window": ...}; '14k' when the window is unknown; None if nothing is known."""
    if not context or context.get("used") is None:
        return None
    used, window = context["used"], context.get("window")
    if not window:
        return short_tokens(used)
    return f"{short_tokens(used)} / {short_tokens(window)} ({round(100 * used / window)}%)"
