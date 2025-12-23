# shared_core/redis_clients/symbol_guard.py
"""
Symbol Guard - Prevents symbol corruption throughout the system.

Symbols can get corrupted when:
- bytes are converted using str(b'symbol') instead of b'symbol'.decode()
- This creates patterns like "NSEB'NIFTY" = "NSE" + "b'" (bytes marker)

This guard catches and fixes such corruptions at all entry points.
"""

import re
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class SymbolGuard:
    """Prevents symbol corruption throughout the system"""
    
    # Valid symbol pattern: exchange prefix + alphanumeric
    VALID_PATTERN = re.compile(r'^(NFO|BFO|NSE|BSE|MCX|CDS)[A-Z0-9]+$')
    
    # Corruption patterns to fix
    CORRUPTION_PATTERNS = [
        ('NSEB', 'NFO'),   # NSE + b' (bytes marker) → NFO for options
        ('BFOB', 'BFO'),   # BFO + b' → BFO
        ('MCXB', 'MCX'),   # MCX + b' → MCX
        ('CDSB', 'CDS'),   # CDS + b' → CDS
    ]
    
    def __init__(self, max_log_entries: int = 1000):
        self.corruption_log: List[Dict[str, Any]] = []
        self.max_log_entries = max_log_entries
        self._fix_count = 0
    
    def guard_symbol(self, symbol: Any, context: str = "unknown") -> Optional[str]:
        """
        Validate and fix symbol at any entry point.
        
        Args:
            symbol: Raw symbol (could be bytes, str, or None)
            context: Where the symbol came from (for logging)
            
        Returns:
            Clean, validated symbol string
        """
        if symbol is None:
            return None
        
        original = symbol
        
        # Fix if bytes - PROPER decode, not str()
        if isinstance(symbol, bytes):
            try:
                symbol = symbol.decode('utf-8', errors='ignore')
            except Exception:
                # Last resort: remove bytes markers manually
                symbol = str(symbol)[2:-1]  # Remove b'...'
        
        # Convert to string
        symbol = str(symbol)
        
        # Remove quotes and apostrophes (from bytes artifact)
        symbol = symbol.replace("'", "").replace('"', '')
        
        # Fix known corruption patterns
        for corrupted, correct in self.CORRUPTION_PATTERNS:
            if corrupted in symbol:
                symbol = symbol.replace(corrupted, correct)
        
        # Uppercase and strip
        symbol = symbol.upper().strip()
        
        # Log if changed
        if str(original) != symbol:
            self._fix_count += 1
            
            # Keep log bounded
            if len(self.corruption_log) < self.max_log_entries:
                self.corruption_log.append({
                    'timestamp': datetime.now().isoformat(),
                    'context': context,
                    'original': str(original)[:100],
                    'fixed': symbol,
                    'type': 'bytes_to_string' if isinstance(original, bytes) else 'quote_removal'
                })
            
            # Log warning periodically (not too often)
            if self._fix_count % 100 == 0:
                logger.warning(f"⚠️ [SYMBOL_GUARD] Fixed {self._fix_count} corruptions so far")
        
        # Validate final result
        if symbol and not self.VALID_PATTERN.match(symbol):
            # Try to infer exchange prefix
            if 'NIFTY' in symbol or 'BANKNIFTY' in symbol:
                if not symbol.startswith(('NFO', 'NSE')):
                    symbol = 'NFO' + symbol if 'NIFTY' in symbol else symbol
            elif 'SENSEX' in symbol:
                if not symbol.startswith('BFO'):
                    symbol = 'BFO' + symbol
        
        return symbol
    
    def get_report(self) -> Dict[str, Any]:
        """Get corruption report"""
        by_context: Dict[str, int] = {}
        by_type: Dict[str, int] = {}
        
        for entry in self.corruption_log:
            ctx = entry.get('context', 'unknown')
            typ = entry.get('type', 'unknown')
            by_context[ctx] = by_context.get(ctx, 0) + 1
            by_type[typ] = by_type.get(typ, 0) + 1
        
        return {
            'total_fixes': self._fix_count,
            'logged_fixes': len(self.corruption_log),
            'by_context': by_context,
            'by_type': by_type,
            'recent': self.corruption_log[-10:] if self.corruption_log else []
        }
    
    def reset(self):
        """Reset counters and log"""
        self.corruption_log.clear()
        self._fix_count = 0


# Global singleton instance
_symbol_guard: Optional[SymbolGuard] = None


def get_symbol_guard() -> SymbolGuard:
    """Get or create the global SymbolGuard instance"""
    global _symbol_guard
    if _symbol_guard is None:
        _symbol_guard = SymbolGuard()
    return _symbol_guard


def guard_symbol(symbol: Any, context: str = "unknown") -> Optional[str]:
    """Convenience function to guard a symbol using the global instance"""
    return get_symbol_guard().guard_symbol(symbol, context)
