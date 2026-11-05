from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from bitget.adapter import Adapter, PREFIX
from bitget.monitor import StockMonitor
from bitget.rate_limit import MonitorStopped, TradeRateLimiter
from common.errors import AuthenticationError, ConfigurationError, RequestRejected
from common.publication import SnapshotPublisher
from common.runner import Runner
from tools.bitget_latency import summarize


def raw(**changes):
    now = datetime.now(timezone.utc)
    return {'orderId': '001', 'symbol': 'AAPL.US', 'side': 'Buy', 'orderType': 'LO',
            'status': 'New', 'submittedAt': (now - timedelta(minutes=5)).isoformat(),
            'updatedAt': (now - timedelta(milliseconds=150)).isoformat(), 'quantity': '3',
            'executedQuantity': '0', 'executedPrice': None, 'price': '190.5',
            'currency': 'USD', **changes}


class Producer:
    def __init__(self):
        self.sent = []
        self.fail = False

    def send(self, topic, *, key, value):
        def get(timeout):
            if self.fail:
                raise TimeoutError()
            self.sent.append((topic, json.loads(value)))
            return SimpleNamespace(partition=0, offset=len(self.sent))
        return SimpleNamespace(get=get)


@pytest.fixture
def monitor(tmp_path):
    config = {'journal_account': 'test-stock-monitor', 'trading_enabled': False,
              'api_key': 'secret-key', 'api_secret': 'secret-value', 'passphrase': 'secret-pass',
              'routing': {'account_id': 'TEST'}, 'timezone': 'America/New_York',
              'monitor': {'enabled': True, 'rate_limit_path': str(tmp_path / 'quota.json')}}
    runner = Runner('bitget', config, execute=True, journal_path=tmp_path / 'bitget.sqlite3')
    pub = SnapshotPublisher(tmp_path / 'pub.sqlite3', config['routing'], Producer(), allow_prior_orders=True)
    instance = StockMonitor(runner, pub)
    pub.delivery_observer = instance._delivered
    yield instance
    instance.close()
    pub.close()
    runner.close()


def observe(monitor, rows, source='today'):
    monitor._observe(rows, source, datetime.now(timezone.utc), time.monotonic())


def logs(monitor):
    return [json.loads(line) for line in monitor.log_path.read_text().splitlines()]


def test_fill_and_cancel_publish_immediately_without_waiting_for_account(monitor):
    initial = raw()
    observe(monitor, [initial])
    monitor.account_wake.clear()
    partial = {**initial, 'status': 'PartialFilled', 'executedQuantity': '1', 'executedPrice': '190'}
    observe(monitor, [partial])
    observe(monitor, [{**partial, 'status': 'Canceled'}])
    assert [r['status'] for _, r in monitor.publisher.producer.sent] == ['NEW', 'PARTIALLY_FILLED', 'CANCELED']
    assert monitor.account_wake.is_set()
    deliveries = [r for r in logs(monitor) if r['event'] == 'monitor_order_delivery']
    assert deliveries[0]['baseline']
    assert not deliveries[1]['baseline'] and not deliveries[2]['baseline']
    assert deliveries[2]['broker_to_kafka_ack_ms'] >= 150
    assert deliveries[2]['observed_to_kafka_ack_ms'] >= 0


def test_missing_broker_timestamp_is_not_fabricated_latency(monitor):
    initial = raw(updatedAt=None)
    observe(monitor, [initial])
    observe(monitor, [{**initial, 'status': 'Filled', 'executedQuantity': '3', 'executedPrice': '190'}])
    delivery = [r for r in logs(monitor) if r['event'] == 'monitor_order_delivery'][-1]
    assert delivery['broker_to_observed_ms'] is None
    assert delivery['broker_to_kafka_ack_ms'] is None
    assert delivery['broker_timestamp_source'] == 'missing'
    summary = summarize([monitor.log_path])
    assert summary['live_deliveries_without_broker_latency'] == {'FILLED': 1}
    assert 'monitor_order_delivery.FILLED.broker_to_kafka_ack_ms' not in summary['metrics_ms']


def test_regressing_or_old_snapshots_do_not_undo_fill(monitor):
    initial = raw()
    observe(monitor, [initial])
    filled = {**initial, 'status': 'Filled', 'executedQuantity': '3', 'executedPrice': '190'}
    observe(monitor, [filled])
    observe(monitor, [initial])
    observe(monitor, [{**initial, 'updatedAt': None}], source='history')
    assert len(monitor.publisher.producer.sent) == 2
    assert monitor.known['001']['status'] == 'FILLED'


def test_unchanged_restored_order_establishes_live_baseline(monitor):
    initial = raw()
    observe(monitor, [initial])
    monitor.last_live_seen.clear()
    monitor.observations.clear()
    observe(monitor, [initial])
    observe(monitor, [{**initial, 'status': 'Canceled'}])
    delivery = [r for r in logs(monitor) if r['event'] == 'monitor_order_delivery'][-1]
    assert delivery['baseline'] is False


def test_prior_day_gtc_cancellation_is_published(monitor):
    initial = raw(submittedAt=(datetime.now(timezone.utc) - timedelta(days=3)).isoformat())
    observe(monitor, [initial])
    observe(monitor, [{**initial, 'status': 'Canceled'}])
    assert [r['status'] for _, r in monitor.publisher.producer.sent] == ['NEW', 'CANCELED']


def test_kafka_outage_retains_fill_and_retries_without_any_trade(monitor):
    initial = raw()
    observe(monitor, [initial])
    monitor.publisher.producer.fail = True
    filled = {**initial, 'status': 'Filled', 'executedQuantity': '3', 'executedPrice': '190'}
    observe(monitor, [filled])
    assert '001' in monitor.pending
    assert monitor.publisher.known_orders()[0]['status'] == 'FILLED'
    monitor.publisher.producer.fail = False
    monitor.publisher.publish_orders(list(monitor.pending.values()))
    assert not monitor.pending
    assert [r['status'] for _, r in monitor.publisher.producer.sent] == ['NEW', 'FILLED']
    assert monitor.runner.journal.unresolved(monitor.runner.scope) == []


def test_invalid_row_does_not_hide_valid_sibling_or_infer_cancellation(monitor):
    observe(monitor, [raw(status='unknown'), raw(orderId='002')])
    observe(monitor, [])
    assert [r['order_id'] for _, r in monitor.publisher.producer.sent] == ['002']


def test_quota_is_shared_and_background_leaves_command_reserve(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])
    monkeypatch.setattr('time.sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))
    first = TradeRateLimiter(tmp_path / 'quota.json')
    second = TradeRateLimiter(tmp_path / 'quota.json')
    for _ in range(25):
        first.acquire(background=True)
        now[0] += .021
    stop = threading.Event()
    stop.wait = lambda seconds: stop.set()
    with pytest.raises(MonitorStopped):
        second.acquire(background=True, stop=stop)
    before = now[0]
    second.acquire()
    assert now[0] - before < .1
    state = json.loads((tmp_path / 'quota.json').read_text())
    assert len(state['requests']) == 26


def test_background_burst_never_exceeds_sliding_budget(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])
    monkeypatch.setattr('time.sleep', lambda seconds: now.__setitem__(0, now[0] + seconds + 1e-8))
    limiter = TradeRateLimiter(tmp_path / 'quota.json')
    times = []
    for _ in range(55):
        limiter.acquire(background=True)
        times.append(now[0])
    assert all(sum(0 <= t - earlier < 30 for earlier in times) <= 25 for t in times)


def test_1200ms_polling_fits_background_budget_with_five_command_slots(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])
    monkeypatch.setattr('time.sleep', lambda seconds: now.__setitem__(0, now[0] + seconds + 1e-8))
    limiter = TradeRateLimiter(tmp_path / 'quota.json')
    for poll in range(100):
        now[0] = 100 + poll * 1.2
        assert limiter.acquire(background=True) < .001
    assert limiter.reserve == 5


def test_reconciliation_priority_deferral_is_bounded(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])
    monkeypatch.setattr('time.sleep', lambda seconds: now.__setitem__(0, now[0] + seconds + 1e-8))
    limiter = TradeRateLimiter(tmp_path / 'quota.json')
    for _ in range(20):
        limiter.acquire(background=True)
        now[0] += .021
    before = now[0]
    limiter.acquire(background=True, low_priority=True)
    assert now[0] - before < 2


def test_429_cooldown_also_covers_asset_queries_without_spending_trade_slots(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr('time.monotonic', lambda: now[0])
    monkeypatch.setattr('time.sleep', lambda seconds: now.__setitem__(0, now[0] + seconds + 1e-8))
    limiter = TradeRateLimiter(tmp_path / 'quota.json')
    limiter.throttle('7')
    assert limiter.wait_cooldown() >= 7000
    assert json.loads((tmp_path / 'quota.json').read_text())['requests'] == []


def test_quota_reservations_are_atomic_across_local_instances(tmp_path):
    path = tmp_path / 'quota.json'
    errors = []
    def acquire():
        try:
            limiter = TradeRateLimiter(path)
            for _ in range(6):
                limiter.acquire()
        except Exception as exc:
            errors.append(exc)
    threads = [threading.Thread(target=acquire) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)
    assert not errors and not any(t.is_alive() for t in threads)
    requests = json.loads(path.read_text())['requests']
    assert len(requests) == 18
    assert all(b - a >= .0199 for a, b in zip(requests, requests[1:]))


def test_request_pooling_metrics_and_telemetry_failure_cannot_change_ack(monitor):
    response = Mock(status_code=200, headers={})
    response.json.return_value = {'code': '00000', 'data': {'orderId': '001'}}
    session = Mock()
    session.request.return_value = response
    monitor.adapter.request_observer = monitor.log.emit
    with patch('requests.Session', return_value=session):
        with monitor.adapter.monitoring_requests('orders', monitor.stop_event):
            monitor.adapter._request('GET', PREFIX + '/trade/order-detail', {'orderId': '001'})
            monitor.adapter._request('GET', PREFIX + '/trade/order-detail', {'orderId': '001'})
        assert session.request.call_count == 2
        session.close.assert_called_once()
        monitor.adapter.request_observer = Mock(side_effect=RuntimeError('logging failed'))
        assert monitor.adapter._request('POST', PREFIX + '/trade/place-order', payload={}) == {'orderId': '001'}
    records = logs(monitor)
    assert len([r for r in records if r['event'] == 'monitor_request']) == 2
    text = monitor.log_path.read_text()
    assert all(secret not in text for secret in ('secret-key', 'secret-value', 'secret-pass', 'ACCESS-SIGN'))


def test_429_sets_shared_cooldown_and_does_not_retry_write(monitor):
    response = Mock(status_code=429, headers={'Retry-After': '7'})
    response.json.return_value = {'code': '429', 'msg': 'Too many requests', 'data': None}
    with patch('requests.Session') as session:
        session.return_value.request.return_value = response
        with pytest.raises(RequestRejected):
            monitor.adapter._request('POST', PREFIX + '/trade/place-order', payload={})
        assert session.return_value.request.call_count == 1
    state = json.loads(Path(monitor.settings['rate_limit_path']).read_text())
    assert state['cooldown'] > time.monotonic() + 6


def test_command_ack_only_wakes_monitor_never_invents_a_fill(monitor):
    monitor.notify({'action': 'place_order', 'status': 'submitted', 'id': 'command-1', 'data': {'orderId': '001'}})
    assert monitor.wake.is_set() and list(monitor.priority) == ['001']
    assert monitor.burst_until > time.monotonic()
    assert not monitor.publisher.producer.sent


def test_slow_account_read_does_not_block_order_worker_and_shutdown_joins(monitor):
    entered = threading.Event()
    release = threading.Event()
    delivered = threading.Event()
    initial = raw(submittedAt=datetime.now(timezone.utc).isoformat(), status='Filled',
                  executedQuantity='3', executedPrice='190')
    initial['updatedAt'] = initial['submittedAt']
    def account():
        entered.set()
        release.wait(3)
        return {'cash': 1, 'positions': []}
    monitor.adapter.snapshot_account = account
    monitor.adapter._today = Mock(return_value=[initial])
    monitor.adapter.open_stock_orders = Mock(return_value=[])
    actual_send = monitor.publisher.producer.send
    def send(topic, **kwargs):
        if topic == 'order-status':
            delivered.set()
        return actual_send(topic, **kwargs)
    monitor.publisher.producer.send = send
    try:
        monitor.start()
        assert entered.wait(2)
        assert delivered.wait(2)
    finally:
        release.set()
        monitor.close()
    assert not any(t.is_alive() for t in monitor.threads)


def test_authentication_failure_is_visible_to_consumer(monitor):
    monitor._error('orders', AuthenticationError('denied'))
    assert monitor.stop_event.is_set()
    assert isinstance(monitor.failure, AuthenticationError)


def test_summary_excludes_recovery_and_reports_percentiles(monitor):
    monitor.log.emit('monitor_order_delivery', status='FILLED', baseline=True, broker_to_kafka_ack_ms=999999)
    for value in (100, 200, 300, 1200):
        monitor.log.emit('monitor_order_delivery', status='FILLED', baseline=False, broker_to_kafka_ack_ms=value)
    monitor.log.summary()
    result = summarize([monitor.log_path])
    stats = result['metrics_ms']['monitor_order_delivery.FILLED.broker_to_kafka_ack_ms']
    assert stats == {'count': 4, 'min': 100, 'mean': 450, 'p50': 200, 'p95': 1200, 'p99': 1200,
                     'max': 1200, 'under_1000ms_pct': 75.0}


def test_consumer_starts_monitor_not_blocking_snapshots_and_commits_before_wakeup(monkeypatch, tmp_path):
    from test_consumer_runtime import setup
    from common.consumer import consume
    runner, config, consumer, producer, publisher, calls = setup(monkeypatch, tmp_path)
    runner.broker = 'bitget'
    runner.config['monitor'] = {'enabled': True}
    worker = Mock(failure=None)
    worker.snapshot.return_value = {'status': 'running'}
    worker.start.side_effect = lambda: calls.append('monitor_start')
    worker.notify.side_effect = lambda result: calls.append('monitor_wakeup')
    worker.close.side_effect = lambda: calls.append('monitor_close')
    publisher.close.side_effect = lambda: calls.append('publisher_close')
    monkeypatch.setattr('bitget.monitor.StockMonitor', Mock(return_value=worker))
    consume(runner, config, max_messages=1)
    assert calls == ['monitor_start', 'poll', 'poll', 'execute', 'commit', 'monitor_wakeup',
                     'monitor_close', 'publisher_close']
    assert publisher.allow_prior_orders is True


def test_monitor_auth_failure_stops_consumer_before_next_command(monkeypatch, tmp_path):
    from test_consumer_runtime import setup
    from common.consumer import consume
    runner, config, consumer, _, _, _ = setup(monkeypatch, tmp_path)
    runner.broker = 'bitget'
    runner.config['monitor'] = {'enabled': True}
    worker = Mock(failure=AuthenticationError('denied'))
    monkeypatch.setattr('bitget.monitor.StockMonitor', Mock(return_value=worker))
    consume(runner, config, max_messages=1)
    consumer.poll.assert_not_called()
    runner.process.assert_not_called()
    worker.close.assert_called_once()


def test_future_broker_timestamp_is_flagged_not_negative_latency(monitor):
    observe(monitor, [raw(updatedAt=(datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat())])
    delivery = [r for r in logs(monitor) if r['event'] == 'monitor_order_delivery'][-1]
    assert delivery['clock_skew_detected'] is True
    assert delivery['broker_to_observed_ms'] is None and delivery['broker_to_kafka_ack_ms'] is None
