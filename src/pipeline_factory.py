"""
Backend registry for AgentLandline agent pipelines.
Resolves a human-friendly backend name/alias (e.g. 'claude', 'agy') to a
pipeline class so AgentManager can construct the right one. AgyPipeline and
ClaudePipeline are independent, self-contained modules (no shared base
class) with slightly different constructor kwargs, so this module only
does name resolution; AgentManager builds each backend's kwargs itself.
"""

from typing import Dict, Type, Optional, Union

from agy_pipeline import AgyPipeline
from claude_pipeline import ClaudePipeline

DEFAULT_BACKEND = "antigravity"

# Accepted spellings -> canonical backend key.
BACKEND_ALIASES: Dict[str, str] = {
    "antigravity": "antigravity",
    "antigravity-cli": "antigravity",
    "agy": "antigravity",
    "gemini": "antigravity",
    "claude": "claude",
    "claude-code": "claude",
    "claude-cli": "claude",
    "anthropic": "claude",
}

BACKEND_REGISTRY: Dict[str, Type] = {
    "antigravity": AgyPipeline,
    "claude": ClaudePipeline,
}


def normalize_backend(backend: Optional[str]) -> str:
    """Resolves a raw backend string to its canonical registry key, raising on unknown input."""
    key = (backend or DEFAULT_BACKEND).strip().lower()
    resolved = BACKEND_ALIASES.get(key)
    if not resolved:
        supported = sorted(set(BACKEND_ALIASES.values()))
        raise ValueError(
            f"Unknown agent backend '{backend}'. Supported backends: {supported} "
            f"(aliases: {sorted(BACKEND_ALIASES)})."
        )
    return resolved


def pipeline_class_for(backend: Optional[str] = DEFAULT_BACKEND) -> Type:
    """Returns the pipeline class for the given backend name/alias."""
    return BACKEND_REGISTRY[normalize_backend(backend)]


def create_pipeline(backend: Optional[str] = DEFAULT_BACKEND, **kwargs) -> Union[AgyPipeline, ClaudePipeline]:
    """Constructs the pipeline for the given backend name/alias with the given constructor kwargs."""
    cls = pipeline_class_for(backend)
    return cls(**kwargs)
