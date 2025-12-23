# ARCHITECTURE: Clear Separation of Responsibilities

## RAW DATA FLOW
intraday_crawler.py → ticks:intraday:processed (Stream) → data_pipeline.py (12 workers)

## PROCESSED DATA FLOW
data_pipeline.py → ind:{symbol} (Hash) + scanner:processed (Stream) → scanner_main.py

## PATTERN DETECTION
scanner_main.py → patterns:global (Stream) → alert_manager.py → RedisNotifier

## KEY RULES:
1. Only data_pipeline.py reads from ticks:intraday:processed
2. Scanner reads from scanner:processed (enriched data)
3. Each has own consumer group
4. No direct Redis() calls - use get_redis_client()
