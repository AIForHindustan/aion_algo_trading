#!/opt/homebrew/bin/python3.13
"""
Debug Script: 7-Pattern Live Detection Investigator (Stream Based)
==================================================================

This script traces the detection of the 7 core patterns using LIVE Redis data
from the 'ticks:intraday:processed' stream. This represents the EXACT data
flow that the scanner and strategies are consuming.

Target Patterns:
1. order_flow_breakout
2. gamma_exposure_reversal
3. microstructure_divergence
4. cross_asset_arbitrage
5. vix_momentum
6. ict_iron_condor
7. game_theory_signal

Methodology:
- Reads last N entries from the live stream.
- Reconstructs the exact payload.
- Feeds it into PatternDetector.
- Reports detections and misses (with reasons).
"""

import sys
import os
import logging
import json
import time
import argparse
from typing import Dict, Any, List, Optional
from collections import defaultdict

from shared_core.redis_clients.redis_client import get_redis_client
# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
intraday_root = script_dir  # intraday_trading/
project_root = os.path.dirname(intraday_root)  # aion_algo_trading/

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
if str(intraday_root) not in sys.path:
    sys.path.insert(0, str(intraday_root))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("PatternDebug")

# Import Core Components
try:
    from shared_core.redis_clients import UnifiedRedisManager
    from shared_core.redis_clients.redis_key_standards import DatabaseAwareKeyBuilder
    from patterns.singleton_factory import PatternDetectorSingletonFactory
except ImportError as e:
    logger.error(f"Failed to import core components: {e}")
    sys.exit(1)


class LivePatternTracer:
    def __init__(self, mode: str = "live", specific_symbol: Optional[str] = None):
        self.mode = mode
        self.target_symbol = specific_symbol
        self.stream_key = DatabaseAwareKeyBuilder.live_processed_stream() # e.g. ticks:intraday:processed
        
        # Initialize Redis
        try:
            # We need DB1 for ticks
            self.redis_client = get_redis_client(process_name="pattern_debug", db=1)
            logger.info("✅ Connected to Redis (DB 1)")
            logger.info(f"✅ Monitoring Stream: {self.stream_key}")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            sys.exit(1)
            
        # Initialize Detector
        try:
            self.detector = PatternDetectorSingletonFactory.get_instance(caller="pattern_debug.py")
            logger.info("✅ PatternDetector initialized")
        except Exception as e:
            logger.error(f"❌ PatternDetector initialization failed: {e}")
            sys.exit(1)
            
    def fetch_live_data(self, symbol: str) -> Dict[str, Any]:
        """
        Reconstruct the full data payload for a symbol from Redis.
        This mirrors scanner_main.py's data gathering.
        """
        data = {"symbol": symbol}
        
        # 1. Fetch Tick Data
        tick_key = DatabaseAwareKeyBuilder.live_ticks_latest(symbol)
        
        # Attempt to fetch as Hash first (standard)
        try:
            tick_raw = self.redis_client.hgetall(tick_key)
            if tick_raw:
                tick_data = {}
                for k, v in tick_raw.items():
                    k_str = k.decode('utf-8') if isinstance(k, bytes) else k
                    v_str = v.decode('utf-8') if isinstance(v, bytes) else v
                    # Try converting to number
                    try:
                        if '.' in v_str:
                             tick_data[k_str] = float(v_str)
                        else:
                             tick_data[k_str] = int(v_str)
                    except:
                        tick_data[k_str] = v_str
                data.update(tick_data)
        except Exception as e:
            logger.warning(f"   ⚠️  Error fetching tick data for {symbol}: {e}")
            return None

        if not data.get('last_price'):
            # logger.warning(f"   ⚠️  No live tick data for {symbol}")
            return None
            
        return data

    def fetch_stream_data(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch latest entries from the stream"""
        try:
            # XREVRANGE key + - COUNT limit
            entries = self.redis_client.xrevrange(self.stream_key, count=limit)
            if not entries:
                return []
            
            parsed_data = []
            for msg_id, payload in entries:
                if b'data' in payload:
                    try:
                        json_str = payload[b'data'].decode('utf-8')
                        data = json.loads(json_str)
                        data['_stream_id'] = msg_id.decode('utf-8')
                        parsed_data.append(data)
                    except Exception as e:
                        continue
            return parsed_data
        except Exception as e:
            logger.error(f"❌ Error fetching stream data: {e}")
            return []

    def run_trace(self):
        """Execute the trace"""
        logger.info(f"🚀 Starting Trace in {self.mode.upper()} mode")
        
        # Fetch latest data
        logger.info("🔍 Fetching latest 100 entries from stream...")
        stream_data = self.fetch_stream_data(limit=100)
        
        if not stream_data:
            logger.warning("⚠️  Stream is empty! No data flowing.")
            return

        # Deduplicate by symbol (taking latest)
        latest_by_symbol = {}
        for d in stream_data:
            sym = d.get('symbol')
            if sym and sym not in latest_by_symbol:
                latest_by_symbol[sym] = d
        
        # Filter if specific symbol requested
        if self.target_symbol:
            # 1. Try finding in stream first
            if self.target_symbol in latest_by_symbol:
                targets = [latest_by_symbol[self.target_symbol]]
            else:
                logger.warning(f"⚠️  Symbol {self.target_symbol} not found in recent stream data. Trying manual fetch...")
                # Manual fetch construction
                manual_payload = self.fetch_live_data(self.target_symbol)
                if manual_payload:
                     # Add dummy stream fields
                     manual_payload['_stream_id'] = 'manual'
                     targets = [manual_payload]
                else:
                     targets = []
        else:
            targets = list(latest_by_symbol.values())
            
        logger.info(f"📋 Targets: {len(targets)} active symbols found in stream")
        
        for i, payload in enumerate(targets):
            symbol = payload.get('symbol')
            logger.info(f"\n[{i+1}/{len(targets)}] Analysis: {symbol}")
            
            # Enrich from ind:{symbol} if critical fields are zero
            vol_ratio = float(payload.get('volume_ratio') or 0.0)
            
            if vol_ratio <= 0.0 and symbol:
                 # Try fetching from indicator key
                 # Try multiple variations: "ind:SYMBOL", "ind:NFOSYMBOL", "ind:NSESYMBOL"
                 candidate_keys = [f"ind:{symbol}", f"ind:NFO{symbol}", f"ind:NSE{symbol}"]
                 
                 for ind_key in candidate_keys:
                     try:
                         # Fetch volume_ratio and potentially IV
                         enriched_vals = self.redis_client.hmget(ind_key, ['volume_ratio', 'iv', 'atr', 'rsi', 'adx'])
                         
                         if enriched_vals[0]:
                             payload['volume_ratio'] = float(enriched_vals[0])
                             logger.info(f"   ✨ Enriched volume_ratio from {ind_key}: {enriched_vals[0].decode('utf-8')}")
                         
                         if enriched_vals[1] and not payload.get('iv'):
                             payload['iv'] = float(enriched_vals[1])
                         
                         # Map others if needed by pattern logic
                         if enriched_vals[2]: payload['atr'] = float(enriched_vals[2])
                         if enriched_vals[3]: payload['rsi'] = float(enriched_vals[3])
                         if enriched_vals[4]: payload['adx'] = float(enriched_vals[4])
                         
                         # If we found volume, we can stop looking
                         if payload.get('volume_ratio', 0) > 0:
                             break
                             
                     except Exception as e:
                         # logger.warning(f"   ⚠️ Failed to enrich from {ind_key}: {e}")
                         pass

            # Re-read after enrichment
            vol_ratio = payload.get('volume_ratio')
            iv = payload.get('iv') or payload.get('greeks', {}).get('iv') or payload.get('implied_volatility')
            price = payload.get('last_price')
            
            logger.info(f"   Price: {price} | VolRatio: {vol_ratio} | IV: {iv}")
            
            # Run Detection
            start_t = time.time()
            try:
                patterns = self.detector.detect_patterns(payload, use_pipelining=False)
                duration = (time.time() - start_t) * 1000
                
                logger.info(f"   ⏱️  Detection took: {duration:.2f}ms")
                
                if patterns:
                    logger.info(f"   ✅ PATTERNS DETECTED: {len(patterns)}")
                    for p in patterns:
                        name = p.get('pattern', p.get('pattern_type', 'UNKNOWN'))
                        conf = p.get('confidence', 0)
                        logger.info(f"      🎯 {name} (Conf: {conf})")
                else:
                    logger.info("   ⚪ No patterns detected")
                    self._diagnose_miss(symbol, payload)
                    
            except Exception as e:
                logger.error(f"   ❌ Detection Error: {e}")
                # import traceback
                # traceback.print_exc()

    def _diagnose_miss(self, symbol: str, data: Dict[str, Any]):
        """Analyze why a pattern might have been missed"""
        # 1. Check Volume Gate
        vol_ratio = float(data.get('volume_ratio', 0) or 0)
        if vol_ratio <= 0:
            logger.info("      ❓ Miss Reason: Zero/Negative Volume Ratio")
            return
            
        # 2. Check Option Gates
        if "CE" in symbol or "PE" in symbol:
            delta = data.get('delta') or data.get('greeks', {}).get('delta')
            if delta and abs(float(delta)) < 0.05:
                logger.info(f"      ❓ Miss Reason: Low Delta ({delta})")
                
            iv = data.get('iv') or data.get('greeks', {}).get('iv') or data.get('implied_volatility')
            if iv and float(iv) <= 0.01:
                logger.info(f"      ❓ Miss Reason: Low IV ({iv})")

    def run_synthetic_test(self):
        """Run verification with hardcoded synthetic data guaranteed to trigger patterns"""
        logger.info("\n🧪 RUNNING SYNTHETIC VERIFICATION")
        
        # 1. Breakout Case
        breakout_data = {
            "symbol": "NSE:TEST-BREAKOUT",
            "last_price": 1005.0, # Deviating from microprice
            "volume_ratio": 50.0,
            "price_change_pct": 2.5,
            "ema_5": 980,
            "ema_20": 950,
            "rsi": 65,
            "timestamp_ms": int(time.time() * 1000),
            
            # Critical Order Flow Fields
            "order_flow_imbalance": 2000,   # > 1000
            "cumulative_volume_delta": 6000, # > 5000
            "microprice": 1000.0            # gives 0.005 deviation (> 0.0015)
        }
        
        logger.info("\n   🧪 Test Case 1: Order Flow Breakout")
        patterns = self.detector.detect_patterns(breakout_data, use_pipelining=False)
        self._print_results(patterns)
        
    def _print_results(self, patterns):
        if patterns:
            for p in patterns:
                logger.info(f"      ✅ {p.get('pattern')} detected")
        else:
             logger.info("      ❌ Failed to detect expected pattern")

def main():
    parser = argparse.ArgumentParser(description="Pattern Debugger")
    parser.add_argument("--mode", choices=["live", "synthetic"], default="live")
    parser.add_argument("--symbol", help="Specific filter", default=None)
    
    args = parser.parse_args()
    
    tracer = LivePatternTracer(mode=args.mode, specific_symbol=args.symbol)
    
    if args.mode == "live":
        tracer.run_trace()
    else:
        tracer.run_synthetic_test()

if __name__ == "__main__":
    main()
