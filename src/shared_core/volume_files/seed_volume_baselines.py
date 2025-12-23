#!/opt/homebrew/bin/python3.13
"""
Seed Volume Baselines From 20-Day Averages
=========================================

Bootstrap Redis `volume:baseline:*` keys using the canonical
`config/volume_averages_20d.json` file.  This is useful when DB0/DB5
have been flushed and the trading system needs baseline data before
real-time statistics can rebuild themselves.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for dynamic_thresholds
project_root = Path(__file__).resolve().parents[2]
parent_root = project_root.parent  # Go up to aion_algo_trading level
if str(parent_root) not in sys.path:
    sys.path.insert(0, str(parent_root))

from shared_core.redis_clients.redis_key_standards import RedisKeyStandards
from utils.redis_client import get_redis_client

# Bucket multipliers for legacy compatibility
BUCKET_MULTIPLIERS = {
    "1min": 1.0, "2min": 2.0, "5min": 5.0, "10min": 10.0,
    "15min": 15.0, "30min": 30.0, "60min": 60.0,
}


TRADING_MINUTES_PER_DAY = 6 * 60 + 15  # 9:15 – 15:30 IST -> 375 minutes


def _resolve_base_symbol(symbol: str) -> str:
    """Collapse instrument identifiers down to their base trading symbol."""
    upper = symbol.upper()
    import re

    expiry_match = re.search(r"\d{2}[A-Z]{3}", upper)
    if expiry_match:
        upper = upper.replace(expiry_match.group(), "")

    if upper.endswith("FUT"):
        upper = upper[:-3]

    if re.search(r"\d+(CE|PE)$", upper):
        upper = re.sub(r"\d+(CE|PE)$", "", upper)

    return upper


def seed_baselines(volume_file: Path) -> None:
    if not volume_file.exists():
        raise FileNotFoundError(f"Volume averages file not found: {volume_file}")

    with volume_file.open("r", encoding="utf-8") as handle:
        volume_data = json.load(handle)

    from shared_core.redis_clients.unified_data_storage import get_unified_storage
    storage = get_unified_storage()
    baseline_client = getattr(storage, "redis", None)
    if baseline_client is None:
        baseline_client = storage._get_client() if hasattr(storage, "_get_client") else None
    if baseline_client is None:
        raise RuntimeError("Unable to acquire Redis baseline client (DB 1 - unified)")

    seeded = 0
    skipped = 0

    for raw_symbol, payload in volume_data.items():
        try:
            avg_volume = float(payload.get("avg_volume_20d") or 0.0)
            if not math.isfinite(avg_volume) or avg_volume <= 0:
                skipped += 1
                continue

            canonical_symbol = RedisKeyStandards.canonical_symbol(raw_symbol)
            if not canonical_symbol:
                skipped += 1
                continue

            baseline_1min = avg_volume / TRADING_MINUTES_PER_DAY
            # ✅ UPDATED: Use canonical symbol (includes exchange / expiry metadata)
            # ✅ FIXED: Use DatabaseAwareKeyBuilder directly
            baseline_data = {
                'avg_volume_20d': str(avg_volume),
                'baseline_1min': str(baseline_1min),
                'source': 'seed_script',
                'last_updated': str(datetime.now().isoformat())
            }
            storage.store_volume_baseline(canonical_symbol, baseline_data, ttl=86400)

            # Pre-compute other bucket resolutions for faster cold starts (legacy format for compatibility)
            for bucket, multiplier in BUCKET_MULTIPLIERS.items():
                normalized = RedisKeyStandards.canonical_symbol(canonical_symbol)
                bucket_key = f"volume:baseline:{normalized}:{bucket}"
                bucket_value = baseline_1min * multiplier
                baseline_client.set(bucket_key, bucket_value)
                baseline_client.expire(bucket_key, 86400)  # 24 hours TTL

            seeded += 1
        except Exception as exc:  # pragma: no cover - defensive logging only
            print(f"⚠️  Failed to seed baseline for {raw_symbol}: {exc}", file=sys.stderr)
            skipped += 1

    print(f"✅ Seeded baselines for {seeded} symbols (skipped {skipped})")


if __name__ == "__main__":
    volume_file = Path(__file__).resolve().parents[2] / "config" / "volume_averages_20d.json"
    seed_baselines(volume_file)
