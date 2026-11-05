"""Bounded JSONL latency logs; no credentials, request bodies, or balances."""
from collections import defaultdict
from datetime import datetime, timezone
import json
import logging
from logging.handlers import RotatingFileHandler
import math
from pathlib import Path
import threading
import time
import uuid


def distribution(values):
    values = sorted(values)
    if not values:
        return {'count': 0}
    def percentile(p):
        return round(values[max(0, math.ceil(len(values) * p) - 1)], 3)
    return {'count': len(values), 'min': round(values[0], 3),
            'mean': round(sum(values) / len(values), 3), 'p50': percentile(.5),
            'p95': percentile(.95), 'p99': percentile(.99), 'max': round(values[-1], 3)}


def metric_values(record):
    # Initial/recovery snapshots are not measurements of live change detection.
    if record.get('baseline') or record.get('event') == 'monitor_latency_summary':
        return
    group = record['event']
    if record.get('lane'):
        group += '.' + record['lane']
    if record.get('event') == 'monitor_order_delivery':
        group += '.' + record['status']
    for key, value in record.items():
        if key.endswith('_ms') and type(value) in (int, float) and math.isfinite(value) and value >= 0:
            yield group + '.' + key, value


class LatencyLog:
    def __init__(self, path, summary_seconds=60):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.handler = RotatingFileHandler(path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8')
        path.chmod(0o600)
        self.run_id = str(uuid.uuid4())
        self.lock = threading.RLock()
        self.metrics = defaultdict(list)
        self.counts = defaultdict(int)
        self.summary_seconds = summary_seconds
        self.since = time.monotonic()

    def _write(self, record):
        message = json.dumps(record, sort_keys=True, allow_nan=False)
        self.handler.handle(logging.LogRecord('bitget.monitor', logging.INFO, '', 0, message, (), None))

    def emit(self, event, **fields):
        with self.lock:
            record = {'event': event, 'ts': datetime.now(timezone.utc).isoformat(),
                      'run_id': self.run_id, **fields}
            self._write(record)
            self.counts[event] += 1
            for key, value in metric_values(record):
                self.metrics[key].append(value)
            if time.monotonic() - self.since >= self.summary_seconds:
                self.summary()

    def summary(self):
        with self.lock:
            record = {'event': 'monitor_latency_summary', 'ts': datetime.now(timezone.utc).isoformat(),
                      'run_id': self.run_id, 'window_seconds': round(time.monotonic() - self.since, 3),
                      'counts': dict(self.counts),
                      'metrics_ms': {key: distribution(values) for key, values in self.metrics.items()}}
            self._write(record)
            print(json.dumps(record, sort_keys=True), flush=True)
            self.metrics.clear()
            self.counts.clear()
            self.since = time.monotonic()

    def close(self):
        self.summary()
        self.handler.close()
