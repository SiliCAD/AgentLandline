"""
Images attached to messages.

The hub copies each attached image into the run folder (`runs/<run>/images/`), so
the message log keeps working links, and records their paths on the message.
How an agent sees them depends on its CLI:

- claude accepts images in its stream-json input, so they're sent with the
  message itself (up to MAX_INLINE_BYTES each), and their paths are listed too;
- agy's stream-json input is text only, but its view_file tool reads images,
  so the message lists the paths and asks the agent to open them.
"""

import base64
import os
import shutil
import uuid
from typing import Any, Dict, List

MEDIA_TYPES = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
               ".gif": "image/gif", ".webp": "image/webp"}
MAX_BYTES = 20 * 1024 * 1024        # per attached image
MAX_INLINE_BYTES = 5 * 1024 * 1024  # larger ones reach claude by path only (the API's limit)


class ImageError(ValueError):
    pass


def media_type(path: str) -> str:
    return MEDIA_TYPES.get(os.path.splitext(path)[1].lower(), "")


def check(paths: List[str]) -> List[str]:
    """Absolute paths of the given images; raises ImageError naming the first unusable one."""
    out = []
    for path in paths or []:
        full = os.path.abspath(os.path.expanduser(str(path)))
        if not media_type(full):
            raise ImageError(f"{path}: not an image ({', '.join(sorted(MEDIA_TYPES))})")
        if not os.path.isfile(full):
            raise ImageError(f"{path}: no such file")
        if os.path.getsize(full) > MAX_BYTES:
            raise ImageError(f"{path}: larger than {MAX_BYTES // (1024 * 1024)} MB")
        out.append(full)
    return out


def store(run_dir: str, paths: List[str]) -> List[str]:
    """Copies the (checked) images into the run folder; returns the copies' paths."""
    folder = os.path.join(run_dir, "images")
    os.makedirs(folder, exist_ok=True)
    out = []
    for path in paths:
        target = os.path.join(folder, f"{uuid.uuid4().hex[:8]}-{os.path.basename(path)}")
        shutil.copyfile(path, target)
        out.append(target)
    return out


def note(paths: List[str], inline: bool) -> str:
    """The line added to a message that has images."""
    if not paths:
        return ""
    listed = "\n".join(f"- {p}" for p in paths)
    if inline:
        return f"\n\n[Attached image{'s' * (len(paths) > 1)}, shown with this message:\n{listed}]"
    return (f"\n\n[Attached image{'s' * (len(paths) > 1)}: open "
            f"{'them' if len(paths) > 1 else 'it'} with your file viewing tool to see "
            f"{'them' if len(paths) > 1 else 'it'}:\n{listed}]")


def claude_blocks(paths: List[str]) -> List[Dict[str, Any]]:
    """claude stream-json image blocks for the images small enough to send inline."""
    blocks = []
    for path in paths:
        try:
            if os.path.getsize(path) > MAX_INLINE_BYTES:
                continue
            with open(path, "rb") as f:
                data = base64.b64encode(f.read()).decode("ascii")
        except OSError:
            continue
        blocks.append({"type": "image", "source": {"type": "base64", "media_type": media_type(path), "data": data}})
    return blocks
