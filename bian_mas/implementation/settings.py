"""Load public challenge configuration for both execution modes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from aiops_challenge_2026.config import load_public_config


HERE = Path(__file__).resolve().parent


def load_config() -> dict[str, Any]:
    config = json.loads((HERE / "config" / "topology.json").read_text(encoding="utf-8"))
    network_elements = load_public_config("network_elements")
    taxonomy = load_public_config("fault_taxonomy")
    config.update(taxonomy)
    config["cities"] = network_elements["cities"]
    config["candidate_roles"] = network_elements["device_roles"]
    config["region_aliases"] = {city: city for city in network_elements["cities"]}
    return config
