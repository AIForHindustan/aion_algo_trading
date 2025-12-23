# News Integration Guide

## Overview

Your alert system now has complete news integration! This guide shows how to use the news enrichment features.

## Files Created

- `news_flat_ingestor.py` - Core news ingestion and enrichment
- `alerts/news_enrichment_integration.py` - Integration utilities
- `alerts/alert_news_hook.py` - Drop-in hooks for existing system
- `alerts/news_integration_example.py` - Complete integration example

## Quick Start

### 1. Basic News Enrichment

```python
from alerts.alert_news_hook import enrich_alert_if_needed

# In your alert processing pipeline
enriched_alert = enrich_alert_if_needed(alert_data, redis_client)
```

### 2. News-Driven Alerts

```python
from alerts.alert_news_hook import create_news_driven_alert

# Create alert from news
news_alert = create_news_driven_alert(
    symbol="CANBK",
    news_items=news_items,
    redis_client=redis_client
)
```

### 3. Complete Integration

```python
from alerts.news_integration_example import NewsIntegratedAlertSystem

# Initialize integrated system
system = NewsIntegratedAlertSystem()

# Process alerts with news
enriched_alert = system.process_alert_with_news(alert_data)

# Send enriched notifications
system.send_enriched_alert(alert_data)
```

## Features

### ✅ News Ingestion
- Reads from your existing Redis news keys (`news:item:*`, `news:latest:*`)
- Maps fields to standard schema
- Creates bounded streams (`news:{symbol}`, `news:global`)
- Deduplication to prevent spam

### ✅ Alert Enrichment
- Automatic news context attachment
- Sentiment analysis
- Impact assessment
- Human-readable formatting

### ✅ Notification Templates
- Enhanced Telegram messages with news
- Sentiment indicators (🟢 Positive, 🔴 Negative, 🟡 Neutral)
- News headlines and sources
- Related news count

### ✅ Integration Hooks
- Drop-in functions for existing code
- Automatic enrichment decisions
- Multiple enrichment strategies (real-time, analysis, etc.)

## Configuration

### Field Mapping
The system maps your news fields to standard schema:

```python
field_map = {
    "source": "news_source",
    "title": "headline", 
    "link": "url",
    "date": "published_time",
    "publisher": "source_name",
    "collected_at": "ingestion_timestamp",
    "sentiment": "sentiment_score",
}
```

### Enrichment Settings
- **Real-time alerts**: 30 minutes lookback, 2 news items
- **Analysis**: 120 minutes lookback, 5 news items
- **Default**: 60 minutes lookback, 3 news items

## Usage Examples

### 1. Enrich Existing Alerts

```python
# Before sending notification
enriched_alert = enrich_alert_before_notification(alert_data, redis_client)

# Format and send
message = HumanReadableAlertTemplates.format_telegram_alert(enriched_alert)
```

### 2. Create News Alerts

```python
# Get recent news for symbol
news_items = news_enricher.fetch_recent_news("CANBK", since_ms, limit=5)

# Create news-driven alert
news_alert = create_news_driven_alert("CANBK", news_items, redis_client)
```

### 3. Batch Processing

```python
# Process multiple alerts
for alert in alerts:
    enriched = enrich_alert_if_needed(alert, redis_client)
    send_notification(enriched)
```

## Integration Points

### 1. Alert Manager
Add news enrichment before sending notifications:

```python
# In your alert manager
from alerts.alert_news_hook import enrich_alert_before_notification

def send_alert(self, alert_data):
    enriched = enrich_alert_before_notification(alert_data, self.redis_client)
    # Send enriched alert...
```

### 2. Filters
News context is automatically detected and used in filtering:

```python
# News context is available in alert_data
if alert_data.get('news') or alert_data.get('news_context'):
    # Handle news-driven alerts
```

### 3. Notifiers
Enhanced templates automatically include news:

```python
# News is automatically formatted in messages
message = HumanReadableAlertTemplates.format_telegram_alert(alert_data)
```

## Monitoring

### News Ingestion Stats
```python
# Check ingestion status
ingestor = NewsIngestorFlat(redis_client)
total_ingested = ingestor.scan_and_ingest(["news:item:*", "news:latest:*"])
```

### News Summary
```python
# Get news summary for symbol
enricher = NewsEnricher(redis_client)
summary = enricher.fetch_recent_news("CANBK", since_ms, limit=10)
```

## Troubleshooting

### No News Found
- Check if news keys exist in Redis
- Verify symbol mapping in `resolve_symbols` function
- Check time range (news might be older than lookback period)

### Enrichment Errors
- Ensure Redis client is properly configured
- Check news stream keys (`news:{symbol}`, `news:global`)
- Verify field mapping matches your news data

### Performance
- News enrichment adds ~10-50ms per alert
- Use appropriate lookback periods
- Consider caching for high-frequency alerts

## Next Steps

1. **Customize Symbol Resolution**: Update `resolve_symbols` function in `news_flat_ingestor.py`
2. **Adjust Enrichment Logic**: Modify `should_enrich_with_news` in `alert_news_hook.py`
3. **Add News Sources**: Extend field mapping for additional news sources
4. **Sentiment Analysis**: Enhance sentiment scoring algorithms
5. **News Filtering**: Add news relevance filtering based on keywords

## Support

The news integration is designed to be:
- **Non-intrusive**: Works with existing alert system
- **Configurable**: Easy to customize for your needs
- **Performant**: Minimal impact on alert processing
- **Extensible**: Easy to add new features

For questions or issues, check the logs and ensure Redis connectivity.
