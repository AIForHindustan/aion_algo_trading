#!/usr/bin/env python3
"""
Registry Sync Monitoring

Monitors synchronization between COMPREHENSIVE and INTRADAY_FOCUS registries.
Tracks new instruments, sync status, and alerts on issues.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path
import json

from .instrument_registry import RegistryType, RegistrySynchronizer
from .registry_factory import get_comprehensive_registry, get_intraday_focus_registry

logger = logging.getLogger(__name__)


class RegistrySyncMonitor:
    """Monitor registry synchronization status and health."""
    
    def __init__(self):
        self.synchronizer = RegistrySynchronizer()
        self.last_sync_time: Optional[float] = None
        self.sync_history: List[Dict[str, Any]] = []
        self.max_history = 100  # Keep last 100 sync records
    
    async def check_sync_status(self) -> Dict[str, Any]:
        """
        Check current sync status between registries.
        
        Returns:
            Dict with sync status information
        """
        comprehensive_registry = get_comprehensive_registry()
        focus_registry = get_intraday_focus_registry()
        
        comprehensive_count = comprehensive_registry.stats.get('total_unified_instruments', 0)
        focus_count = focus_registry.stats.get('total_unified_instruments', 0)
        
        # Get focus symbols
        focus_symbols = focus_registry.config.focus_symbols
        
        # Check for missing instruments
        comprehensive_instruments = comprehensive_registry._unified_instruments
        focus_instruments = focus_registry._unified_instruments
        
        missing_in_focus = []
        for unified_id, instrument in comprehensive_instruments.items():
            # Check if instrument should be in focus set
            if unified_id not in focus_instruments:
                # Check if it matches focus criteria
                if self._is_in_focus_set(instrument, focus_symbols):
                    missing_in_focus.append({
                        "unified_id": unified_id,
                        "canonical_symbol": instrument.canonical_symbol,
                        "brokers": list(instrument.broker_instruments.keys()),
                    })
        
        status = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "comprehensive_count": comprehensive_count,
            "focus_count": focus_count,
            "focus_symbols_count": len(focus_symbols),
            "missing_in_focus_count": len(missing_in_focus),
            "missing_in_focus": missing_in_focus[:10],  # First 10 for logging
            "last_sync_time": self.last_sync_time,
            "sync_needed": len(missing_in_focus) > 0,
        }
        
        return status
    
    def _is_in_focus_set(self, instrument, focus_symbols: set) -> bool:
        """Check if instrument is in focus set."""
        # Check canonical symbol
        if instrument.canonical_symbol in focus_symbols:
            return True
        
        # Check all broker instruments
        for broker_insts in instrument.broker_instruments.values():
            for broker_inst in broker_insts:
                # Extract base symbol from tradingsymbol
                clean_symbol = self._clean_symbol(broker_inst.tradingsymbol)
                if clean_symbol in focus_symbols:
                    return True
        
        return False
    
    def _clean_symbol(self, symbol: str) -> str:
        """Clean symbol to extract base name."""
        symbol_upper = symbol.upper()
        for index in ['NIFTY', 'BANKNIFTY', 'SENSEX']:
            if symbol_upper.startswith(index):
                return index
        return symbol_upper
    
    async def sync_and_monitor(self) -> Dict[str, Any]:
        """
        Perform sync and monitor the results.
        
        Returns:
            Dict with sync results and metrics
        """
        start_time = time.time()
        
        try:
            # Perform sync
            sync_results = await self.synchronizer.sync_new_instruments()
            
            sync_time = time.time() - start_time
            self.last_sync_time = time.time()
            
            # Record sync history
            sync_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sync_time_seconds": sync_time,
                "synced_count": sync_results.get("synced_count", 0),
                "total_focus_instruments": sync_results.get("total_focus_instruments", 0),
                "success": True,
            }
            
            self.sync_history.append(sync_record)
            
            # Keep only last N records
            if len(self.sync_history) > self.max_history:
                self.sync_history = self.sync_history[-self.max_history:]
            
            logger.info(
                f"✅ Registry sync completed: {sync_results.get('synced_count', 0)} instruments "
                f"synced in {sync_time:.2f}s"
            )
            
            return {
                "success": True,
                "sync_results": sync_results,
                "sync_time_seconds": sync_time,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
        except Exception as e:
            sync_time = time.time() - start_time
            
            # Record failed sync
            sync_record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sync_time_seconds": sync_time,
                "error": str(e),
                "success": False,
            }
            
            self.sync_history.append(sync_record)
            
            logger.error(f"❌ Registry sync failed: {e}", exc_info=True)
            
            return {
                "success": False,
                "error": str(e),
                "sync_time_seconds": sync_time,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    
    def get_sync_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent sync history."""
        return self.sync_history[-limit:]
    
    def get_sync_statistics(self) -> Dict[str, Any]:
        """Get sync statistics."""
        if not self.sync_history:
            return {
                "total_syncs": 0,
                "successful_syncs": 0,
                "failed_syncs": 0,
                "avg_sync_time": 0,
                "last_sync": None,
            }
        
        successful = [s for s in self.sync_history if s.get("success", False)]
        failed = [s for s in self.sync_history if not s.get("success", False)]
        
        avg_sync_time = (
            sum(s.get("sync_time_seconds", 0) for s in successful) / len(successful)
            if successful else 0
        )
        
        return {
            "total_syncs": len(self.sync_history),
            "successful_syncs": len(successful),
            "failed_syncs": len(failed),
            "avg_sync_time_seconds": avg_sync_time,
            "last_sync": self.sync_history[-1] if self.sync_history else None,
        }
    
    def export_monitoring_report(self, output_path: Optional[Path] = None) -> Dict[str, Any]:
        """Export monitoring report to JSON."""
        if output_path is None:
            output_path = Path(__file__).parent / "registry_sync_monitoring_report.json"
        
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sync_statistics": self.get_sync_statistics(),
            "recent_sync_history": self.get_sync_history(limit=50),
        }
        
        try:
            with open(output_path, "w") as f:
                json.dump(report, f, indent=2)
            logger.info(f"✅ Monitoring report exported to {output_path}")
        except Exception as e:
            logger.error(f"❌ Failed to export monitoring report: {e}")
        
        return report


# Singleton instance
_monitor_instance: Optional[RegistrySyncMonitor] = None


def get_registry_sync_monitor() -> RegistrySyncMonitor:
    """Get singleton registry sync monitor instance."""
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = RegistrySyncMonitor()
    return _monitor_instance


async def main():
    """CLI entry point for monitoring."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor registry synchronization")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check sync status"
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Perform sync and monitor"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show sync statistics"
    )
    parser.add_argument(
        "--history",
        type=int,
        default=10,
        help="Show sync history (default: 10)"
    )
    parser.add_argument(
        "--export",
        type=str,
        help="Export monitoring report to JSON file"
    )
    
    args = parser.parse_args()
    
    monitor = get_registry_sync_monitor()
    
    if args.check:
        status = await monitor.check_sync_status()
        print("\n📊 Sync Status:")
        print(f"  Comprehensive: {status['comprehensive_count']} instruments")
        print(f"  Focus: {status['focus_count']} instruments")
        print(f"  Focus Symbols: {status['focus_symbols_count']}")
        print(f"  Missing in Focus: {status['missing_in_focus_count']}")
        print(f"  Sync Needed: {status['sync_needed']}")
        
        if status['missing_in_focus']:
            print("\n  Missing Instruments (first 10):")
            for inst in status['missing_in_focus']:
                print(f"    - {inst['canonical_symbol']} ({inst['unified_id']})")
    
    if args.sync:
        result = await monitor.sync_and_monitor()
        if result['success']:
            print(f"\n✅ Sync completed: {result['sync_results']['synced_count']} instruments")
        else:
            print(f"\n❌ Sync failed: {result['error']}")
    
    if args.stats:
        stats = monitor.get_sync_statistics()
        print("\n📈 Sync Statistics:")
        print(f"  Total Syncs: {stats['total_syncs']}")
        print(f"  Successful: {stats['successful_syncs']}")
        print(f"  Failed: {stats['failed_syncs']}")
        print(f"  Avg Sync Time: {stats['avg_sync_time_seconds']:.2f}s")
    
    if args.history:
        history = monitor.get_sync_history(limit=args.history)
        print(f"\n📋 Recent Sync History (last {len(history)}):")
        for record in history:
            status = "✅" if record.get('success') else "❌"
            print(f"  {status} {record['timestamp']}: {record.get('synced_count', 0)} instruments")
    
    if args.export:
        report = monitor.export_monitoring_report(Path(args.export))
        print(f"\n✅ Report exported to {args.export}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

