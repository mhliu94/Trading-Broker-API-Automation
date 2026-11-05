"""Independent, read-only Stock+ order and account monitoring.

Stock+ has no documented order WebSocket. The REST fallback uses a shared quota,
short post-action bursts, persistent connections, and a durable Kafka outbox.
It cannot promise sustained sub-second detection under the published quota.
"""
from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import queue
import sqlite3
import threading
import time

from common.errors import AuthenticationError, BrokerError, ConfigurationError
from common.publication import validate_order_snapshot
from common.utils import parse_time, today_window
from .rate_limit import MonitorStopped
from .telemetry import LatencyLog


CLOSED = {'FILLED', 'CANCELED', 'REJECTED', 'EXPIRED'}


def fingerprint(snapshot):
    state = {k: v for k, v in snapshot.items() if k not in {'updated_at', 'sequence'}}
    return hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()[:24]


class StockMonitor:
    def __init__(self, runner, publisher):
        self.runner, self.adapter, self.publisher = runner, runner.adapter, publisher
        self.settings = runner.config.get('monitor', {})
        self.poll_seconds = self._seconds('poll_seconds', 1.2, .02, 30)
        self.burst_seconds = self._seconds('burst_seconds', 2, 0, 5)
        self.burst_poll_seconds = self._seconds('burst_poll_seconds', .25, .02, 30)
        self.account_seconds = self._seconds('account_seconds', 5, 1, 300)
        self.reconcile_seconds = self._seconds('reconcile_seconds', 60, 30, 3600)
        self.discovery_seconds = self._seconds('discovery_seconds', 300, 60, 86400)
        summary_seconds = self._seconds('summary_seconds', 60, 10, 3600)
        self.routing = {k: runner.config['routing'][k] for k in ('account_id', 'account_num_id')
                        if k in runner.config['routing']}
        self.log_path = Path(self.settings.get('log_path', runner.journal_path.with_suffix('.monitor.jsonl')))
        self.log = LatencyLog(self.log_path, summary_seconds)
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.account_wake = threading.Event()
        self.lock = threading.Lock()
        self.inbox = queue.Queue(maxsize=4)
        self.priority = deque(maxlen=256)
        self.threads = []
        self.failure = None
        self.status = {}
        self.started_at = datetime.now(timezone.utc)
        self.burst_until = 0.0
        self.account_until = 0.0
        self.account_trigger = None
        self.known = {}
        self.broker_times = {}
        self.last_seen = {}
        self.observations = {}
        self.pending = {}
        self.last_live_seen = set()
        self.detail_turn = False
        self.detail_cursor = 0
        self.closed = False

    def _seconds(self, key, default, low, high):
        value = self.settings.get(key, default)
        if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
            raise ConfigurationError(f'monitor.{key} must be a finite number in {low}..{high}')
        return float(value)

    def start(self):
        # Restore active orders even when they were created before midnight.
        self.known = {s['order_id']: s for s in self.publisher.known_orders()}
        self.publisher.delivery_observer = self._delivered
        self.adapter.request_observer = self.log.emit
        self.log.emit('monitor_started', transport='rest', poll_seconds=self.poll_seconds,
                      burst_poll_seconds=self.burst_poll_seconds, burst_seconds=self.burst_seconds,
                      command_reserve=self.adapter._limiter.reserve,
                      sustained_subsecond_guaranteed=False, restored_orders=len(self.known))
        for name, target in (('orders', self._orders), ('account', self._account), ('reconcile', self._reconcile)):
            thread = threading.Thread(target=self._worker, args=(name, target), name='bitget-' + name)
            thread.start()
            self.threads.append(thread)

    def _worker(self, name, target):
        try:
            with self.adapter.monitoring_requests(name, self.stop_event):
                target()
        except MonitorStopped:
            pass
        except Exception as exc:
            self._error(name, exc, fatal=True)

    def _error(self, lane, exc, fatal=False):
        # Error bodies may contain credentials or broker/account data.
        self.log.emit('monitor_error', lane=lane, error_type=type(exc).__name__, fatal=fatal)
        with self.lock:
            self.status[lane + '_error'] = type(exc).__name__
            if fatal or isinstance(exc, AuthenticationError):
                self.failure = exc
                self.stop_event.set()
                self.wake.set()
                self.account_wake.set()

    def _success(self, lane):
        with self.lock:
            self.status[lane + '_last_success'] = datetime.now(timezone.utc).isoformat()
            self.status.pop(lane + '_error', None)

    def snapshot(self):
        with self.lock:
            return {'transport': 'rest', 'status': 'failed' if self.failure else 'running',
                    'log_path': str(self.log_path), **self.status}

    def notify(self, result):
        """Wake on completed commands; acknowledgements are never treated as fills."""
        if result.get('action') not in {'place_order', 'cancel_order', 'cancel_open_orders',
                                        'set_trading_enabled', 'positions', 'orders_today'}:
            return
        identities = set()
        def scan(value):
            if isinstance(value, dict):
                for key, child in value.items():
                    if key in {'orderId', 'order_id', 'broker_order_id'} and isinstance(child, str) and child:
                        identities.add(child)
                    elif isinstance(child, (dict, list)):
                        scan(child)
            elif isinstance(value, list):
                for child in value:
                    scan(child)
        scan(result.get('data'))
        with self.lock:
            for identity in sorted(identities):
                if identity not in self.priority:
                    self.priority.append(identity)
            self.burst_until = time.monotonic() + self.burst_seconds
        self.log.emit('monitor_command_wakeup', action=result['action'], command_id=result.get('id'),
                      order_count=len(identities))
        self.wake.set()
        self.account_wake.set()

    def _observe(self, rows, source, observed_at, observed_mono):
        for raw in rows:
            try:
                if not self.adapter._is_stock(raw):
                    continue
                snapshot = validate_order_snapshot({**self.adapter.snapshot_order(raw, observed_at),
                                                     **self.routing, 'schema_version': 1})
                identity = snapshot['order_id']
                previous = self.known.get(identity)
                broker_time = parse_time(raw['updatedAt']) if raw.get('updatedAt') else None
                if observed_mono < self.last_seen.get(identity, 0):
                    continue
                if source in {'history', 'discovery'} and previous is not None and broker_time is None:
                    # An undated history response cannot overwrite a live poll.
                    if previous['status'] not in CLOSED:
                        with self.lock:
                            if identity not in self.priority:
                                self.priority.append(identity)
                    continue
                older = broker_time is not None and identity in self.broker_times and broker_time < self.broker_times[identity]
                regression = previous is not None and (
                    snapshot['filled_quantity'] < previous['filled_quantity'] or
                    previous['status'] in CLOSED and snapshot['status'] not in CLOSED)
                if older or regression:
                    self.log.emit('monitor_stale_observation', order_id=identity, source=source)
                    continue
                gap = (observed_mono - self.last_seen[identity]) * 1000 if identity in self.last_seen else None
                self.last_seen[identity] = observed_mono
                first_observation = identity not in self.last_live_seen
                self.last_live_seen.add(identity)
                if broker_time is not None:
                    self.broker_times[identity] = broker_time
                if previous is not None and fingerprint(previous) == fingerprint(snapshot):
                    continue
                # First observation after restart is recovery, even if durable
                # state differs. It must not contaminate live latency percentiles.
                baseline = first_observation and parse_time(snapshot['created_at']) < self.started_at
                delta = (observed_at - broker_time).total_seconds() * 1000 if broker_time else None
                metadata = {'order_id': identity, 'status': snapshot['status'], 'state_id': fingerprint(snapshot),
                            'source': source, 'baseline': baseline, 'observed_at': observed_at.isoformat(),
                            'broker_updated_at': broker_time.isoformat() if broker_time else None,
                            'broker_timestamp_source': 'updatedAt' if broker_time else 'missing',
                            'broker_time_resolution': ('fractional_seconds' if '.' in str(raw.get('updatedAt')) else 'seconds')
                            if broker_time else None,
                            'clock_skew_detected': delta is not None and delta < 0,
                            'broker_to_observed_ms': round(delta, 3) if delta is not None and delta >= 0 else None,
                            'observation_gap_ms': round(gap, 3) if gap is not None else None}
                self.observations[identity] = (metadata, observed_mono)
                self.known[identity] = snapshot
                self.pending[identity] = snapshot
                self.log.emit('monitor_order_observed', **metadata)
                with self.lock:
                    self.account_until = time.monotonic() + 3
                    self.account_trigger = observed_mono
                self.account_wake.set()
                result = self.publisher.publish_orders([snapshot])
                if result['failed']:
                    self.log.emit('monitor_publication_failure', failed=result['failed'])
            except MonitorStopped:
                raise
            except Exception as exc:
                self._error('order_mapping', exc)

    def _delivered(self, snapshot, acknowledgement):
        pending = self.pending.get(snapshot['order_id'])
        if pending is not None and fingerprint(pending) == fingerprint(snapshot):
            self.pending.pop(snapshot['order_id'], None)
        metadata, observed_mono = self.observations.get(snapshot['order_id'], ({}, None))
        if metadata.get('state_id') != fingerprint(snapshot):
            self.log.emit('monitor_order_delivery', order_id=snapshot['order_id'], status=snapshot['status'],
                          baseline=True, source='outbox_recovery', sequence=snapshot['sequence'], **acknowledgement)
            return
        broker_time = metadata.get('broker_updated_at')
        broker_ms = (datetime.now(timezone.utc) - parse_time(broker_time)).total_seconds() * 1000 if broker_time else None
        self.log.emit('monitor_order_delivery', **metadata, sequence=snapshot['sequence'],
                      observed_to_kafka_ack_ms=round((time.monotonic() - observed_mono) * 1000, 3),
                      broker_to_kafka_ack_ms=round(broker_ms, 3) if broker_ms is not None and broker_ms >= 0 else None,
                      **acknowledgement)

    def _orders(self):
        next_poll = 0.0
        backoff = 0.0
        retry_at = 0.0
        previous_poll = None
        today_ids = set()
        while not self.stop_event.is_set():
            if self.wake.wait(max(0, next_poll - time.monotonic())):
                self.wake.clear()
            if self.stop_event.is_set():
                break
            if self.stop_event.wait(max(0, retry_at - time.monotonic())):
                break
            while True:
                try:
                    rows, source, observed_at, observed_mono = self.inbox.get_nowait()
                except queue.Empty:
                    break
                self._observe(rows, source, observed_at, observed_mono)
            started = time.monotonic()
            try:
                with self.lock:
                    identity = self.priority.popleft() if self.priority else None
                missing = sorted(k for k, v in self.known.items() if v['status'] not in CLOSED and k not in today_ids)
                if identity is None and missing and self.detail_turn:
                    identity = missing[self.detail_cursor % len(missing)]
                    self.detail_cursor += 1
                self.detail_turn = not self.detail_turn
                if identity:
                    rows = [self.adapter.stock_order_detail(identity)]
                    source = 'detail'
                else:
                    rows = self.adapter._today()
                    today_ids = {r.get('orderId') for r in rows}
                    source = 'today'
                observed_at, observed_mono = datetime.now(timezone.utc), time.monotonic()
                self._observe(rows, source, observed_at, observed_mono)
                # Also retry a latest observation whose first durable write failed.
                result = self.publisher.publish_orders(list(self.pending.values()))
                self.log.emit('monitor_poll', source=source, rows=len(rows),
                              poll_gap_ms=round((started - previous_poll) * 1000, 3) if previous_poll is not None else None,
                              cycle_ms=round((time.monotonic() - started) * 1000, 3), retry_failures=result['failed'])
                previous_poll = started
                self._success('orders')
                backoff = 0.0
                retry_at = 0.0
            except MonitorStopped:
                break
            except Exception as exc:
                self._error('orders', exc)
                backoff = min(30, max(1, backoff * 2))
                retry_at = time.monotonic() + backoff
            with self.lock:
                interval = self.burst_poll_seconds if time.monotonic() < self.burst_until else self.poll_seconds
            next_poll = max(started + interval, time.monotonic() + backoff)

    def _account_enabled(self, db):
        if self.runner.config.get('trading_enabled') is not True:
            return False
        scope = self.runner.scope
        enabled = db.execute('SELECT enabled FROM settings WHERE scope=?', (scope,)).fetchone()
        unresolved = db.execute("SELECT 1 FROM commands WHERE scope=? AND state IN ('pending','unknown') LIMIT 1", (scope,)).fetchone()
        timed = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='timed_cancellations'").fetchone()
        if timed:
            timed = db.execute("SELECT 1 FROM timed_cancellations WHERE scope=? AND state NOT IN ('complete','authentication_rejected') LIMIT 1", (scope,)).fetchone()
        return not (enabled and not enabled[0] or unresolved or timed)

    def _account(self):
        # Never share the command journal's SQLite connection across threads.
        db = sqlite3.connect(self.runner.journal_path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
        previous = None
        next_refresh = 0.0
        backoff = 0.0
        retry_at = 0.0
        try:
            while not self.stop_event.is_set():
                self.account_wake.wait(max(0, next_refresh - time.monotonic()))
                self.account_wake.clear()
                if self.stop_event.is_set():
                    break
                if self.stop_event.wait(max(0, retry_at - time.monotonic())):
                    break
                started = time.monotonic()
                try:
                    account = {**self.adapter.snapshot_account(), **self.routing,
                               'trading_enabled': self._account_enabled(db)}
                    state = json.dumps(account, sort_keys=True)
                    changed = state != previous
                    account['ts'] = datetime.now(timezone.utc).isoformat()
                    observed = time.monotonic()
                    self.publisher.publish_account(account)
                    with self.lock:
                        trigger = self.account_trigger
                    self.log.emit('monitor_account', changed=changed, baseline=previous is None,
                                  refresh_ms=round((observed - started) * 1000, 3),
                                  observed_to_kafka_ack_ms=round((time.monotonic() - observed) * 1000, 3),
                                  since_order_observed_ms=round((time.monotonic() - trigger) * 1000, 3)
                                  if trigger is not None and started >= trigger else None)
                    previous = state
                    self._success('account')
                    backoff = 0.0
                    retry_at = 0.0
                except MonitorStopped:
                    break
                except Exception as exc:
                    self._error('account', exc)
                    backoff = min(30, max(1, backoff * 2))
                    retry_at = time.monotonic() + backoff
                with self.lock:
                    interval = 1 if time.monotonic() < self.account_until else self.account_seconds
                    if interval != 1:
                        self.account_trigger = None
                next_refresh = max(started + interval, time.monotonic() + backoff)
                # Coalesce changes during this fetch; bound asset reads to 1 Hz.
                if self.stop_event.wait(max(0, started + 1 - time.monotonic())):
                    break
        finally:
            db.close()

    def _reconcile(self):
        next_discovery = 0.0
        while not self.stop_event.is_set():
            try:
                if time.monotonic() >= next_discovery:
                    rows = self.adapter.open_stock_orders()
                    source = 'discovery'
                    next_discovery = time.monotonic() + self.discovery_seconds
                else:
                    start, end = today_window(self.runner.config.get('timezone', 'America/New_York'))
                    rows = self.adapter._history(math.floor(start.timestamp()), math.ceil(end.timestamp()) - 1)
                    source = 'history'
                batch = (rows, source, datetime.now(timezone.utc), time.monotonic())
                while not self.stop_event.is_set():
                    try:
                        self.inbox.put(batch, timeout=.25)
                        break
                    except queue.Full:
                        continue
                self._success('reconcile')
            except MonitorStopped:
                break
            except Exception as exc:
                self._error('reconcile', exc)
            self.stop_event.wait(self.reconcile_seconds)

    def close(self):
        if self.closed:
            return
        self.stop_event.set()
        self.wake.set()
        self.account_wake.set()
        # Requests have finite timeouts; quota/backoff waits are interruptible.
        # Join before the consumer closes the publisher/producer they use.
        for thread in self.threads:
            thread.join()
        self.publisher.delivery_observer = None
        self.adapter.request_observer = None
        self.log.close()
        self.closed = True
