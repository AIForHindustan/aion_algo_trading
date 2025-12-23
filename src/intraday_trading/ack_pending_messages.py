#!/opt/homebrew/bin/python3.13
"""
Acknowledge pending messages to unblock consumers
"""
import sys

from shared_core.redis_clients.redis_client import get_redis_client
def acknowledge_pending_messages():
    """Acknowledge pending messages for a specific consumer"""
    try:
        # ✅ FIXED: Use UnifiedRedisManager for centralized connection management
        from shared_core.redis_clients import UnifiedRedisManager, get_key_builder
        
        # Connect to Redis DB 1 (realtime)
        r = get_redis_client(process_name="ack_pending_messages", db=1, decode_responses=True)
        
        # ✅ CENTRALIZED: Use key builder for stream key
        builder = get_key_builder()
        stream_key = builder.live_processed_stream()
        group_name = 'scanner_group'
        
        # Auto-detect current active scanner (find consumer with lowest idle time)
        consumers = r.xinfo_consumers(stream_key, group_name)
        if not consumers:
            print(f"❌ No consumers found in {group_name}")
            return False
        
        # Find most recently active consumer
        active_consumer = None
        min_idle = float('inf')
        for cons in consumers:
            cons_name = cons.get('name', b'unknown')
            if isinstance(cons_name, bytes):
                cons_name = cons_name.decode('utf-8')
            idle = cons.get('idle', 0) / 1000
            if idle < min_idle:
                min_idle = idle
                active_consumer = cons_name
        
        if not active_consumer:
            print(f"❌ Could not find active consumer")
            return False
        
        consumer_name = active_consumer
        print(f"🎯 Auto-detected active consumer: {consumer_name} (idle {min_idle:.1f}s)")
        
        print(f"🔧 Acknowledging pending messages for {consumer_name}...")
        
        # Get pending messages for this consumer
        # XPENDING returns: [message_id, consumer, idle_time, delivery_count]
        pending_info = r.xpending_range(
            stream_key,
            group_name,
            min='-',
            max='+',
            count=100,
            consumername=consumer_name
        )
        
        if not pending_info:
            print(f"✅ No pending messages for {consumer_name}")
            return True
        
        print(f"📊 Found {len(pending_info)} pending messages")
        
        # Acknowledge all pending messages
        message_ids = [info['message_id'] for info in pending_info]
        if message_ids:
            ack_count = r.xack(stream_key, group_name, *message_ids)
            print(f"✅ Acknowledged {ack_count} messages")
        
        # Verify cleanup
        remaining = r.xpending_range(
            stream_key,
            group_name,
            min='-',
            max='+',
            count=10,
            consumername=consumer_name
        )
        
        if remaining:
            print(f"⚠️  {len(remaining)} messages still pending")
        else:
            print(f"✅ All pending messages cleared!")
        
        return True
        
    except Exception as e:
        if "Connection" in str(type(e).__name__) or "Cannot connect" in str(e):
            print("❌ Cannot connect to Redis on localhost:6379")
        else:
            raise
        return False
    except Exception as e:
        print(f"❌ Error during acknowledgment: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if '--confirm' in sys.argv:
        acknowledge_pending_messages()
    else:
        print("⚠️  This script will acknowledge pending Redis stream messages.")
        print("    This will mark them as processed (skip them).")
        print("    Run with --confirm to proceed: python ack_pending_messages.py --confirm")
        sys.exit(1)
