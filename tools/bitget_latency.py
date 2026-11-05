#!/usr/bin/env python3
"""Summarize raw Stock+ monitor JSONL logs, including rotated files."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime
import glob
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bitget.telemetry import distribution, metric_values


def summarize(paths, since=None):
    metrics = defaultdict(list)
    events = Counter()
    missing = Counter()
    malformed = 0
    for path in paths:
        with open(path) as stream:
            for line in stream:
                try:
                    row = json.loads(line)
                    stamp = datetime.fromisoformat(row['ts'].replace('Z', '+00:00'))
                    if since is not None and stamp < since:
                        continue
                    if row.get('event') == 'monitor_latency_summary':
                        continue  # Do not count the same raw samples twice.
                    events[row['event']] += 1
                    for key, value in metric_values(row):
                        metrics[key].append(value)
                    if row['event'] == 'monitor_order_delivery' and not row.get('baseline'):
                        if row.get('broker_to_kafka_ack_ms') is None:
                            missing[row['status']] += 1
                except (ValueError, KeyError, TypeError):
                    malformed += 1
    result = {}
    for key, values in sorted(metrics.items()):
        result[key] = distribution(values)
        if key.endswith('broker_to_kafka_ack_ms'):
            result[key]['under_1000ms_pct'] = round(100 * sum(v < 1000 for v in values) / len(values), 2)
    return {'files': list(map(str, paths)), 'events': dict(events), 'malformed_lines': malformed,
            'live_deliveries_without_broker_latency': dict(missing), 'metrics_ms': result,
            'notes': ['Initial/recovery snapshots are excluded from latency distributions.',
                      'Broker latency requires updatedAt and depends on clock alignment and timestamp precision.',
                      'Kafka acknowledgement latency ends at the broker acknowledgement, not downstream consumption.',
                      'Polling observes cumulative state; intermediate fills between polls may be coalesced.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--log', default=str(Path(__file__).resolve().parents[1] / '.state/bitget.monitor.jsonl'))
    parser.add_argument('--since', help='ISO timestamp including timezone')
    args = parser.parse_args()
    since = datetime.fromisoformat(args.since.replace('Z', '+00:00')) if args.since else None
    if since is not None and since.utcoffset() is None:
        parser.error('--since must include a timezone')
    paths = sorted(p for p in glob.glob(args.log + '*') if p == args.log or p.removeprefix(args.log + '.').isdigit())
    if not paths:
        parser.error('No monitor logs found; the monitor must be started first')
    print(json.dumps(summarize(paths, since), indent=2))


if __name__ == '__main__':
    main()
