"""
MessageLog: append-only record of which agent sent which message to whom.

One JSON object per line in `<run_dir>/messages.jsonl`:

    {"id": "m3", "ts": "...Z", "from": "tester", "to": "coder", "text": "...",
     "caused_by": "m2",
     "from_session": "<sender's CLI session id>",
     "from_transcript": "<path to the sender's CLI transcript>",
     "from_transcript_line": 412}

Conversation history is not copied. Each record links to the sender's own CLI
transcript, and `from_transcript_line` is that transcript's line count when the
message was sent, i.e. where to start reading for the reasoning behind it.
`caused_by` is the message that started the sender's current turn, so records
chain back to the original request.
"""

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def count_lines(path: Optional[str]) -> Optional[int]:
    """Number of lines in a file, or None if there is no file."""
    if not path:
        return None
    try:
        with open(path, "rb") as f:
            return sum(1 for _ in f)
    except OSError:
        return None


class MessageLog:
    FILENAME = "messages.jsonl"

    def __init__(self, run_dir: str):
        self.run_dir = run_dir
        self.path = os.path.join(run_dir, self.FILENAME)
        os.makedirs(run_dir, exist_ok=True)
        self._lock = threading.Lock()
        # Continue numbering when reopening an existing run.
        self._next = (count_lines(self.path) or 0) + 1

    def append(
        self,
        sender: str,
        recipient: str,
        text: str,
        caused_by: Optional[str] = None,
        from_session: Optional[str] = None,
        from_transcript: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Writes one message record and returns it."""
        with self._lock:
            record = {
                "id": f"m{self._next}",
                "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "from": sender,
                "to": recipient,
                "text": text,
                "caused_by": caused_by,
                "from_session": from_session,
                "from_transcript": from_transcript,
                "from_transcript_line": count_lines(from_transcript),
            }
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            self._next += 1
            return record

    def read_all(self) -> List[Dict[str, Any]]:
        """All records in the order they were written."""
        if not os.path.exists(self.path):
            return []
        with open(self.path, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
