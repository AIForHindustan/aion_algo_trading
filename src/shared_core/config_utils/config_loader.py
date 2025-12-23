from pathlib import Path
from typing import Any, Dict
import yaml


def load_shared_config(config_name: str) -> Dict[str, Any]:
    """Load config from shared_core that's safe for all services."""
    config_path = Path(__file__).resolve().parents[1] / "config" / config_name
    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
