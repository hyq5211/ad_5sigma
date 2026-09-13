"""Access to the public task definitions shipped with the SDK."""

from __future__ import annotations

from importlib.resources import files
import json
from typing import Any


def load_public_config(name: str) -> dict[str, Any]:
    if name not in {"fault_taxonomy", "network_elements"}:
        raise ValueError(f"unknown public config: {name}")
    resource = files(__package__).joinpath(f"{name}.json")
    return json.loads(resource.read_text(encoding="utf-8"))
