#!/usr/bin/env python3
import time
import sys
import logging
from shared_core.redis_clients.redis_client import get_redis_client

def monitor_pipeline():
    """Real-time pipeline monitoring"""
    # ✅ Standardized: Use get_redis_client for M4 pooling
    r = get_redis_client(process_name="pipeline_monitor", db=1)
    
    print("\033[1;36m📊 REAL-TIME PIPELINE MONITOR\033[0m")
    print("=" * 60)
    
    try:
        last_raw = r.xlen('ticks:intraday:processed')
        last_enriched = r.xlen('ticks:intraday:enriched')
    except Exception as e:
        print(f"❌ Error connecting to Redis: {e}")
        return
    
    try:
        while True:
            time.sleep(5)
            
            try:
                current_raw = r.xlen('ticks:intraday:processed')
                current_enriched = r.xlen('ticks:intraday:enriched')
                
                raw_delta = current_raw - last_raw
                enriched_delta = current_enriched - last_enriched
                
                # Check pending messages
                try:
                    pending = r.xpending('ticks:intraday:processed', 'data_pipeline_group')
                    pending_count = pending['pending'] if pending else 0
                except:
                    pending_count = 0
                
                timestamp = time.strftime('%H:%M:%S')
                print(f"\n⏰ {timestamp}")
                
                # Colors for visibility
                raw_color = "\033[32m" if raw_delta > 0 else ""
                enriched_color = "\033[34m" if enriched_delta > 0 else ""
                reset = "\033[0m"
                
                print(f"   Raw:      {current_raw:<8} ({raw_color}Δ: {raw_delta:>+4}{reset})")
                print(f"   Enriched: {current_enriched:<8} ({enriched_color}Δ: {enriched_delta:>+4}{reset})")
                
                # Alerts
                if pending_count > 10:
                    print(f"   \033[1;33m⚠️  WARNING: {pending_count} pending messages!\033[0m")
                else:
                    print(f"   Pending:  {pending_count}")
                    
                if raw_delta > 0 and enriched_delta == 0:
                    print(f"   \033[1;31m🔴 CRITICAL: Pipeline blocked!\033[0m")
                    print(f"      Raw growing by {raw_delta} but enriched stuck")
                
                last_raw = current_raw
                last_enriched = current_enriched
                
            except Exception as loop_err:
                print(f"⚠️ Error in monitor loop: {loop_err}")
                time.sleep(2)
                
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")

if __name__ == "__main__":
    monitor_pipeline()
