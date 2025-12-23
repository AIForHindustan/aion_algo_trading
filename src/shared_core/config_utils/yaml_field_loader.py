"""
Shared YAML Field Loader
========================

Exposes helper functions for working with the optimized_field_mapping.yaml file.
This module centralizes the logic that previously lived under intraday_trading/utils
so all services (scanner, alert filters, dashboard, etc.) can import the same
helpers via ``shared_core.config_utils``. Always import from this module (e.g.
``from shared_core.config_utils.yaml_field_loader import …``); the legacy
``utils.yaml_field_loader`` shim only exists for backward compatibility and will
be removed once all call sites migrate.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

logger = logging.getLogger(__name__)

_yaml_config_cache: Optional[Dict[str, Any]] = None
_yaml_config_path: Optional[str] = None


def _default_config_candidates() -> List[Path]:
    """
    Return candidate paths (in priority order) for optimized_field_mapping.yaml.
    """
    repo_root = Path(__file__).resolve().parents[2]
    return [
        # Allow overriding via env for custom deployments
        Path(os.environ["FIELD_MAPPING_YAML_PATH"]).expanduser()
        if os.environ.get("FIELD_MAPPING_YAML_PATH")
        else None,
        repo_root / "shared_core" / "config" / "optimized_field_mapping.yaml",
        repo_root / "intraday_trading" / "config" / "optimized_field_mapping.yaml",
        repo_root / "config" / "optimized_field_mapping.yaml",
    ]


def _resolve_config_path(explicit_path: Optional[Union[str, Path]] = None) -> Path:
    """
    Resolve the YAML path by checking explicit env/input overrides before falling
    back to the known repo locations.
    """
    if explicit_path:
        candidate = Path(explicit_path).expanduser()
        if candidate.exists():
            return candidate
        logger.warning("Field mapping override %s does not exist", candidate)

    for candidate in _default_config_candidates():
        if candidate and candidate.exists():
            return candidate

    # Final fallback – return the last candidate even if it does not exist so the
    # caller can log a helpful error.
    fallback = Path(
        os.environ.get("FIELD_MAPPING_YAML_PATH", "")
    ) or Path("optimized_field_mapping.yaml")
    return fallback


@lru_cache(maxsize=1)
def _load_yaml_config(config_path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """Load YAML configuration with caching."""
    global _yaml_config_cache, _yaml_config_path

    target_path = _resolve_config_path(config_path)

    if _yaml_config_cache is not None and _yaml_config_path == str(target_path):
        return _yaml_config_cache

    if not target_path.exists():
        logger.error("YAML config file not found: %s", target_path)
        _yaml_config_cache = {}
        _yaml_config_path = str(target_path)
        return _yaml_config_cache

    try:
        with target_path.open("r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}
            _yaml_config_cache = config
            _yaml_config_path = str(target_path)
            logger.info("✅ Loaded YAML config: %s", target_path)
            return _yaml_config_cache
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.error("❌ Error loading YAML config %s: %s", target_path, exc, exc_info=True)
        _yaml_config_cache = {}
        _yaml_config_path = str(target_path)
        return _yaml_config_cache


def get_yaml_config() -> Dict[str, Any]:
    """Return the cached YAML configuration."""
    return _load_yaml_config()


def _direct_lookup(mapping: Dict[str, str], field_name: str) -> str:
    if not mapping:
        return field_name
    if field_name in mapping:
        return mapping[field_name]
    for alias, canonical in mapping.items():
        if canonical == field_name:
            return canonical
    return field_name


def resolve_session_field(field_name: str) -> str:
    """Resolve Redis session field name from YAML config."""
    if not field_name:
        return field_name
    return _direct_lookup(get_yaml_config().get("redis_session_fields", {}), field_name)


def resolve_calculated_field(field_name: str) -> str:
    """Resolve calculated indicator field name from YAML config."""
    if not field_name:
        return field_name

    mapping = get_yaml_config().get("calculated_indicator_fields", {})
    if not mapping:
        return field_name

    if field_name in mapping:
        canonical = mapping[field_name]
        if canonical == "price_change_pct" and field_name in {"net_change", "change"}:
            return "price_change_pct"
        return canonical

    for alias, canonical in mapping.items():
        if canonical == field_name:
            return canonical
    return field_name


def get_redis_session_fields() -> Dict[str, str]:
    return get_yaml_config().get("redis_session_fields", {})


def get_calculated_indicator_fields() -> Dict[str, str]:
    return get_yaml_config().get("calculated_indicator_fields", {})


def resolve_volume_profile_field(field_name: str) -> str:
    if not field_name:
        return field_name
    return _direct_lookup(get_yaml_config().get("volume_profile_fields", {}), field_name)


def get_volume_profile_fields() -> Dict[str, str]:
    return get_yaml_config().get("volume_profile_fields", {})


def get_volume_profile_patterns() -> Dict[str, str]:
    return get_yaml_config().get("volume_profile_patterns", {})


def get_websocket_core_fields() -> Dict[str, str]:
    return get_yaml_config().get("websocket_core_fields", {})


def get_required_fields(asset_type: str) -> List[str]:
    config = get_yaml_config().get("field_validation", {})
    universal = config.get("universal_required", [])
    asset_required = config.get(f"{asset_type}_required", [])
    return universal + asset_required


def get_asset_specific_fields(asset_type: str) -> List[str]:
    config = get_yaml_config().get("asset_websocket_fields", {}).get(asset_type, {})
    return config.get("additional_fields", [])


def get_preferred_timestamp(data: Dict[str, Any]) -> Optional[Union[str, float, int]]:
    config = get_yaml_config()
    priority = config.get("timestamp_priority", {})
    primary = priority.get("primary", "exchange_timestamp")
    fallback = priority.get("fallback", "timestamp_ns")

    if primary in data and data[primary]:
        return data[primary]
    if fallback in data and data[fallback]:
        return data[fallback]
    return None


def normalize_indicator_record(
    record: Dict[str, Any],
    include_calculated_fields: bool = True,
    asset_type: str = "equity",
) -> Dict[str, Any]:
    """
    Normalize indicator dictionaries in-place according to YAML mappings.
    """
    if not record:
        return record

    normalized = {}
    session_fields = get_redis_session_fields()
    indicator_fields = get_calculated_indicator_fields() if include_calculated_fields else {}

    for key, value in record.items():
        canonical = _direct_lookup(session_fields, key)
        canonical = _direct_lookup(indicator_fields, canonical)
        normalized[canonical] = value

    # Attach preferred timestamp if possible
    preferred_ts = get_preferred_timestamp(normalized)
    if preferred_ts and "preferred_timestamp" not in normalized:
        normalized["preferred_timestamp"] = preferred_ts

    return normalized


def normalize_session_record(
    record: Dict[str, Any],
    include_aliases: bool = False,
) -> Dict[str, Any]:
    """
    Normalize Redis session payloads using the YAML alias map.
    """
    if not isinstance(record, dict):
        return record

    session_fields = get_redis_session_fields()
    normalized: Dict[str, Any] = dict(record) if include_aliases else {}

    for alias, canonical in session_fields.items():
        if alias in record:
            normalized[canonical] = record[alias]
            if include_aliases and canonical != alias:
                normalized[alias] = record[alias]

    return normalized or dict(record)


def validate_data(data: Dict[str, Any], asset_type: str = "equity") -> Dict[str, Any]:
    """
    Validate data payloads using the YAML validation rules.
    """
    validation_rules = get_yaml_config().get("field_validation", {})
    required_fields = validation_rules.get("universal_required", []) + validation_rules.get(
        f"{asset_type}_required", []
    )

    errors = [field for field in required_fields if field not in data or data[field] in (None, "")]
    warnings: List[str] = []

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
    }


def get_session_key_pattern(symbol: str, date: Optional[str] = None) -> str:
    """
    Build the canonical Redis session key from YAML-configured templates.
    """
    config = get_yaml_config()
    pattern = config.get("redis_session_key_pattern", "session:{symbol}:{date}")
    if not date:
        date = datetime.utcnow().strftime("%Y-%m-%d")
    return pattern.format(symbol=symbol, date=date)


def get_volume_profile_key(symbol: str, date: Optional[str] = None) -> str:
    config = get_yaml_config()
    pattern = config.get("volume_profile_key_pattern", "volume_profile:{symbol}:{date}")
    if not date:
        date = datetime.utcnow().strftime("%Y-%m-%d")
    return pattern.format(symbol=symbol, date=date)


def resolve_redis_session_field(field_name: str) -> str:
    """
    Alias maintained for backwards compatibility with legacy helpers.
    """
    return resolve_session_field(field_name)


class FieldMappingManager:
    """
    Lightweight helper that exposes frequently used mapping utilities.
    """

    def __init__(self):
        self.mapping_config = get_yaml_config()
        self._session_fields = get_redis_session_fields()
        self._indicator_fields = get_calculated_indicator_fields()

    def apply_field_mapping(self, record: Dict[str, Any], asset_type: str = "equity") -> Dict[str, Any]:
        if not isinstance(record, dict):
            return record

        normalized = {}
        combined_mapping = {}
        combined_mapping.update(self.mapping_config.get("websocket_core_fields", {}))
        combined_mapping.update(self._session_fields)
        combined_mapping.update(self._indicator_fields)

        for key, value in record.items():
            canonical = combined_mapping.get(key, key)
            normalized[canonical] = value

        # Ensure asset-specific fields are carried forward
        asset_fields = (
            self.mapping_config.get("asset_websocket_fields", {})
            .get(asset_type, {})
            .get("additional_fields", [])
        )
        for field in asset_fields:
            if field in record:
                normalized[field] = record[field]

        return normalized

    def normalize_session_record(self, record: Dict[str, Any], include_aliases: bool = False) -> Dict[str, Any]:
        return normalize_session_record(record, include_aliases)

    def normalize_indicator_record(self, record: Dict[str, Any], include_calculated_fields: bool = True) -> Dict[str, Any]:
        return normalize_indicator_record(record, include_calculated_fields)

    def resolve_session_field(self, field_name: str) -> str:
        return resolve_session_field(field_name)

    def resolve_redis_session_field(self, field_name: str) -> str:
        return resolve_redis_session_field(field_name)

    def resolve_calculated_field(self, field_name: str) -> str:
        return resolve_calculated_field(field_name)

    def get_session_key_pattern(self, symbol: str, date: Optional[str] = None) -> str:
        return get_session_key_pattern(symbol, date)

    def validate_data(self, data: Dict[str, Any], asset_type: str = "equity") -> Dict[str, Any]:
        return validate_data(data, asset_type)

    def get_redis_session_fields(self) -> Dict[str, str]:
        return dict(self._session_fields)

    def get_calculated_indicator_fields(self) -> Dict[str, str]:
        return dict(self._indicator_fields)


_FIELD_MAPPING_MANAGER: Optional[FieldMappingManager] = None


def get_field_mapping_manager() -> FieldMappingManager:
    global _FIELD_MAPPING_MANAGER
    if _FIELD_MAPPING_MANAGER is None:
        _FIELD_MAPPING_MANAGER = FieldMappingManager()
    return _FIELD_MAPPING_MANAGER


def apply_field_mapping(record: Dict[str, Any], asset_type: str = "equity") -> Dict[str, Any]:
    return get_field_mapping_manager().apply_field_mapping(record, asset_type)


__all__ = [
    "get_yaml_config",
    "resolve_session_field",
    "resolve_calculated_field",
    "get_redis_session_fields",
    "get_calculated_indicator_fields",
    "resolve_volume_profile_field",
    "get_volume_profile_fields",
    "get_volume_profile_patterns",
    "get_websocket_core_fields",
    "get_required_fields",
    "get_asset_specific_fields",
    "get_preferred_timestamp",
    "normalize_indicator_record",
    "normalize_session_record",
    "validate_data",
    "get_session_key_pattern",
    "get_volume_profile_key",
    "resolve_redis_session_field",
    "FieldMappingManager",
    "get_field_mapping_manager",
    "apply_field_mapping",
]
