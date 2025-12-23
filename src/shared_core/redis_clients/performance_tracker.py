import time
import asyncio
from prometheus_client import Counter, Histogram, start_http_server


# Metrics
alert_latency = Histogram('alert_latency_seconds', 'Alert processing latency')
alerts_processed = Counter('alerts_processed_total', 'Total alerts processed')


class PerformanceTracker:
    def __init__(self):
        self.alert_timestamps = {}

    def track_alert(self, alert_id: str):
        self.alert_timestamps[alert_id] = time.time()

    def record_latency(self, alert_id: str):
        if alert_id in self.alert_timestamps:
            latency = time.time() - self.alert_timestamps[alert_id]
            alert_latency.observe(latency)
            alerts_processed.inc()

            # Remove tracked alert
            del self.alert_timestamps[alert_id]
