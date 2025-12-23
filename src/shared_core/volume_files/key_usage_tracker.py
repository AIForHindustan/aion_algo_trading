# shared_core/volume_files/key_usage_tracker.py
"""
Key Usage Tracker - Monitors Redis key access patterns during migration.
Helps identify which consumers are using legacy vs new keys.
"""

import logging
import threading
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class KeyUsageTracker:
    """
    Monitors which Redis keys are being accessed.
    Helps identify migration progress and which consumers use which keys.
    
    Usage:
        tracker = KeyUsageTracker.get_instance()
        tracker.track_write('ind:NFONIFTY27JAN60000CE', is_legacy=True)
        stats = tracker.get_stats()
    """
    _instance = None
    _lock = threading.Lock()
    
    LEGACY_KEY_PATTERNS = [
        'ind:',
        'vol:baseline:',
        'vol:session:',
        'volume_profile:',
        'bucket_incremental_volume:',
    ]
    
    NEW_KEY_PATTERNS = [
        'volume:unified:',
    ]
    
    @classmethod
    def get_instance(cls) -> 'KeyUsageTracker':
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        self.key_write_counts: Dict[str, int] = defaultdict(int)
        self.key_read_counts: Dict[str, int] = defaultdict(int)
        self.first_access: Dict[str, str] = {}
        self._enabled = True
    
    def enable(self):
        """Enable tracking."""
        self._enabled = True
        logger.info("📊 Key usage tracking ENABLED")
    
    def disable(self):
        """Disable tracking (for performance in production)."""
        self._enabled = False
        logger.info("📊 Key usage tracking DISABLED")
    
    def track_write(self, key: str, is_legacy: bool = False):
        """Track a write to a key."""
        if not self._enabled:
            return
        
        self.key_write_counts[key] += 1
        
        if key not in self.first_access:
            self.first_access[key] = datetime.now().isoformat()
        
        # Log periodically (every 1000 writes to same key)
        if self.key_write_counts[key] % 1000 == 0:
            key_type = "LEGACY" if is_legacy else "UNIFIED"
            logger.info(f"📝 [{key_type}] {key}: {self.key_write_counts[key]} writes")
    
    def track_read(self, key: str):
        """Track a read from a key."""
        if not self._enabled:
            return
        
        self.key_read_counts[key] += 1
    
    def _is_legacy_key(self, key: str) -> bool:
        """Check if key matches legacy patterns."""
        return any(key.startswith(p) for p in self.LEGACY_KEY_PATTERNS)
    
    def _is_new_key(self, key: str) -> bool:
        """Check if key matches new unified patterns."""
        return any(key.startswith(p) for p in self.NEW_KEY_PATTERNS)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current key usage statistics."""
        legacy_writes = sum(
            c for k, c in self.key_write_counts.items()
            if self._is_legacy_key(k)
        )
        new_writes = sum(
            c for k, c in self.key_write_counts.items()
            if self._is_new_key(k)
        )
        
        total_writes = legacy_writes + new_writes
        migration_pct = (new_writes / max(1, total_writes)) * 100
        
        return {
            'legacy_writes': legacy_writes,
            'new_writes': new_writes,
            'total_writes': total_writes,
            'migration_progress': f"{migration_pct:.1f}%",
            'unique_legacy_keys': sum(1 for k in self.key_write_counts if self._is_legacy_key(k)),
            'unique_new_keys': sum(1 for k in self.key_write_counts if self._is_new_key(k)),
            'tracking_enabled': self._enabled
        }
    
    def get_top_keys(self, n: int = 10, key_type: str = 'all') -> list:
        """Get top N most accessed keys."""
        if key_type == 'legacy':
            items = [(k, c) for k, c in self.key_write_counts.items() if self._is_legacy_key(k)]
        elif key_type == 'new':
            items = [(k, c) for k, c in self.key_write_counts.items() if self._is_new_key(k)]
        else:
            items = list(self.key_write_counts.items())
        
        return sorted(items, key=lambda x: x[1], reverse=True)[:n]
    
    def reset(self):
        """Reset all counters."""
        self.key_write_counts.clear()
        self.key_read_counts.clear()
        self.first_access.clear()
        logger.info("📊 Key usage counters RESET")
    
    def print_summary(self):
        """Print a summary of key usage."""
        stats = self.get_stats()
        print("\n" + "=" * 60)
        print("📊 KEY USAGE SUMMARY")
        print("=" * 60)
        print(f"Legacy writes:     {stats['legacy_writes']:,}")
        print(f"Unified writes:    {stats['new_writes']:,}")
        print(f"Migration progress: {stats['migration_progress']}")
        print(f"Unique legacy keys: {stats['unique_legacy_keys']}")
        print(f"Unique new keys:    {stats['unique_new_keys']}")
        print()
        
        print("Top 5 Legacy Keys:")
        for key, count in self.get_top_keys(5, 'legacy'):
            print(f"  {key}: {count:,}")
        
        print("\nTop 5 Unified Keys:")
        for key, count in self.get_top_keys(5, 'new'):
            print(f"  {key}: {count:,}")
        print("=" * 60 + "\n")


# Convenience function
def get_key_tracker() -> KeyUsageTracker:
    """Get the global key usage tracker instance."""
    return KeyUsageTracker.get_instance()
